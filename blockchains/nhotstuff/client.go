package nhotstuff

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"diablo-benchmark/core"

	"diablo-benchmark/blockchains/nhotstuff/clientpb"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

type qspec struct {
	faulty int
}

func (q *qspec) ExecCommandQF(_ *clientpb.Command, signatures map[uint32]*emptypb.Empty) (*emptypb.Empty, bool) {
	if len(signatures) < q.faulty+1 {
		return nil, false
	}
	return &emptypb.Empty{}, true
}

func numFaulty(n int) int {
	return (n - 1) / 3
}

type BlockchainClient struct {
	logger       core.Logger
	mgr          *clientpb.Manager
	gorumsConfig *clientpb.Configuration
	rebuildCfg   func(map[string]uint32, int) (*clientpb.Configuration, error)
	cfgVersion   uint64
	activeNodes  int
	seq          uint64
	clientID     uint32
	// semaphore limits the number of concurrently in-flight commands to prevent
	// cmdCache overload on servers during fault recovery.  When the channel is
	// full, TriggerInteraction blocks until an existing command completes.
	// The cap is sized to roughly 1 second of nominal load so that the queue
	// drains in much less than one viewDuration even during fault recovery.
	inflight chan struct{}

	mu       sync.RWMutex
	allNodes map[string]uint32
	dead     map[uint32]struct{}
}

var (
	unavailableEndpointRE = regexp.MustCompile(`dial tcp\s+([^:\s]+:\d+):\s+connect:\s+connection refused`)
	timeoutKindRE         = regexp.MustCompile(`context deadline exceeded \(errors: (\d+), replies: (\d+)\)`)
)

const perCmdTimeout = 10 * time.Second

func newClient(logger core.Logger, view []string) (*BlockchainClient, error) {
	logger.Tracef("new hotstuff client")

	creds := insecure.NewCredentials()
	mgrOpts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(
			grpc.WithTransportCredentials(creds),
			// Keepalive lets gRPC detect dead nodes (SIGKILL'd replicas) within a few
			// seconds rather than waiting for the TCP timeout or the 30s context deadline.
			// Without keepalive, gorums waits for all N nodes to respond; with 2 dead
			// nodes, the quorum call blocks until the context deadline (30s), holding
			// semaphore slots and preventing new transactions from being submitted.
			// With these settings, dead nodes are detected in ~5s (2s idle + 3s timeout),
			// gorums' cancelPendingMsgs fires, the Incomplete error is returned, and
			// semaphore slots are released promptly so the protocol recovers normally.
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				// Send a ping after this duration of inactivity on the connection.
				// 2s ensures we detect dead nodes well within one view duration (2s).
				Time: 2 * time.Second,
				// Wait this long for a ping ACK before declaring the connection dead.
				Timeout: 3 * time.Second,
				// Send pings even when there are no active RPC calls (required to
				// detect dead replicas between command submissions).
				PermitWithoutStream: true,
			}),
		),
	}

	mgr := clientpb.NewManager(mgrOpts...)

	nodes := make(map[string]uint32, len(view))
	for i, addr := range view {
		// IDs start from 1, matching typical replica IDs
		// 1338 is the statically assigned client port for HotStuff
		nodes[addr+":1338"] = uint32(i + 1)
	}

	faulty := numFaulty(len(view))
	gorumsConfig, err := mgr.NewConfiguration(&qspec{faulty: faulty}, gorums.WithNodeMap(nodes))
	if err != nil {
		mgr.Close()
		return nil, fmt.Errorf("failed to create gorums configuration: %w", err)
	}

	// Generate a random clientID to ensure uniqueness across separate
	// diablo secondary processes (each process has its own address space,
	// so atomic counters would collide).
	var cidBytes [4]byte
	if _, err := rand.Read(cidBytes[:]); err != nil {
		mgr.Close()
		return nil, fmt.Errorf("failed to generate random client ID: %w", err)
	}
	cid := binary.BigEndian.Uint32(cidBytes[:])
	if cid == 0 {
		cid = 1 // avoid zero clientID
	}

	// inflight semaphore cap: 1 second of nominal load (200 TPS → 200 slots per client).
	// Under normal operation inflight stays near 0 (round-trip ≈ 20 ms).
	// Under fault it bounds the cmdCache backlog so the protocol can drain the
	// queue in roughly inflight/throughput < 1 s after recovery.
	const inflightCap = 1

	return &BlockchainClient{
		logger:       logger,
		mgr:          mgr,
		gorumsConfig: gorumsConfig,
		rebuildCfg: func(nodeMap map[string]uint32, faulty int) (*clientpb.Configuration, error) {
			return mgr.NewConfiguration(&qspec{faulty: faulty}, gorums.WithNodeMap(nodeMap))
		},
		seq:         0,
		clientID:    cid,
		inflight:    make(chan struct{}, inflightCap),
		allNodes:    nodes,
		dead:        make(map[uint32]struct{}),
		activeNodes: len(nodes),
	}, nil
}

func (c *BlockchainClient) DecodePayload(bytes []byte) (interface{}, error) {
	return bytes, nil
}

func (c *BlockchainClient) TriggerInteraction(iact core.Interaction) error {
	payload, ok := iact.Payload().([]byte)
	if !ok {
		return fmt.Errorf("invalid payload type")
	}

	// Acquire a slot in the inflight semaphore.  This bounds the number of
	// concurrent pending gRPC calls so that the server's cmdCache never
	// accumulates more commands than the protocol can drain before the client
	// timeout expires.  We block here until a slot is available; Diablo already
	// launched this call in its own goroutine (nsecondary.go:154), so blocking
	// is safe and provides natural back-pressure.
	c.inflight <- struct{}{}
	defer func() { <-c.inflight }()
	iact.ReportSubmit()

	// Keep timeout short so stuck quorum calls release inflight slots quickly.

	seq := atomic.AddUint64(&c.seq, 1)
	cmd := &clientpb.Command{
		ClientID:       c.clientID,
		SequenceNumber: seq,
		Data:           payload,
	}

	cfg, cfgVersion, activeNodes, deadNodes := c.currentConfigSnapshot()
	cfgSize := cfg.Size()
	start := time.Now()
	c.logger.Debugf("exec start clientID=%d seq=%d cfgVersion=%d active=%d cfgSize=%d dead=%d inflight=%d", c.clientID, seq, cfgVersion, activeNodes, cfgSize, deadNodes, len(c.inflight))
	if cfgSize != activeNodes {
		c.logger.Warnf("config size mismatch seq=%d cfgVersion=%d active=%d cfgSize=%d", seq, cfgVersion, activeNodes, cfgSize)
	}
	ctx, cancel := context.WithTimeout(context.Background(), perCmdTimeout)
	promise := cfg.ExecCommand(ctx, cmd)
	c.logger.Debugf("exec promise-created clientID=%d seq=%d cfgVersion=%d", c.clientID, seq, cfgVersion)
	_, err := promise.Get()
	cancel()
	c.logger.Debugf("exec get-return clientID=%d seq=%d cfgVersion=%d err=%v elapsed=%s", c.clientID, seq, cfgVersion, err, time.Since(start))

	if err == nil {
		iact.ReportCommit()
		return nil
	}
	c.pruneUnavailableNodes(err)
	errKind := classifyExecError(err)
	if strings.Contains(err.Error(), "context deadline exceeded") {
		c.logger.Warnf("exec timeout clientID=%d seq=%d cfgVersion=%d active=%d dead=%d inflight=%d kind=%s", c.clientID, seq, cfgVersion, activeNodes, deadNodes, len(c.inflight), errKind)
		if errKind == "timeout_errors_0_replies_0" {
			activeEndpoints, deadEndpoints := c.nodeStateSnapshot()
			c.logger.Warnf("exec timeout with no node errors clientID=%d seq=%d cfgVersion=%d activeEndpoints=%v deadEndpoints=%v", c.clientID, seq, cfgVersion, activeEndpoints, deadEndpoints)
		}
	}

	// Any error (timeout, context cancelled, blockchain forked, etc.) is a
	// clean abort.  We do NOT retry: a retry would consume another semaphore
	// slot and re-queue a command that the protocol has already decided to drop
	// (forked) or that arrived after the timeout window.
	iact.ReportAbort()
	return err
}

func (c *BlockchainClient) currentConfig() *clientpb.Configuration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gorumsConfig
}

func (c *BlockchainClient) currentConfigSnapshot() (*clientpb.Configuration, uint64, int, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gorumsConfig, c.cfgVersion, c.activeNodes, len(c.dead)
}

func (c *BlockchainClient) nodeStateSnapshot() (active []string, dead []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	active = make([]string, 0, len(c.allNodes))
	dead = make([]string, 0, len(c.dead))
	for endpoint, id := range c.allNodes {
		if _, isDead := c.dead[id]; isDead {
			dead = append(dead, endpoint)
			continue
		}
		active = append(active, endpoint)
	}
	sort.Strings(active)
	sort.Strings(dead)
	return active, dead
}

func extractUnavailableEndpoints(errString string) []string {
	matches := unavailableEndpointRE.FindAllStringSubmatch(errString, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	endpoints := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) != 2 {
			continue
		}
		ep := m[1]
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints)
	return endpoints
}

func classifyExecError(err error) string {
	errStr := err.Error()
	m := timeoutKindRE.FindStringSubmatch(errStr)
	if len(m) == 3 {
		return fmt.Sprintf("timeout_errors_%s_replies_%s", m[1], m[2])
	}
	return "other"
}

func (c *BlockchainClient) pruneUnavailableNodes(err error) {
	endpoints := extractUnavailableEndpoints(err.Error())
	if len(endpoints) == 0 {
		if classifyExecError(err) == "timeout_errors_0_replies_0" {
			activeEndpoints, deadEndpoints := c.nodeStateSnapshot()
			c.logger.Warnf("timeout without unavailable endpoints; configuration unchanged activeEndpoints=%v deadEndpoints=%v", activeEndpoints, deadEndpoints)
		}
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	for _, endpoint := range endpoints {
		id, exists := c.allNodes[endpoint]
		if !exists {
			continue
		}
		if _, exists := c.dead[id]; exists {
			continue
		}
		c.dead[id] = struct{}{}
		changed = true
	}
	if !changed {
		return
	}

	active := make(map[string]uint32, len(c.allNodes))
	for addr, id := range c.allNodes {
		if _, dead := c.dead[id]; dead {
			continue
		}
		active[addr] = id
	}
	if len(active) == 0 {
		return
	}

	cfg, buildErr := c.rebuildCfg(active, numFaulty(len(active)))
	if buildErr != nil {
		c.logger.Warnf("failed to rebuild gorums configuration with active nodes: %v", buildErr)
		return
	}
	c.gorumsConfig = cfg
	c.cfgVersion++
	c.activeNodes = len(active)
	c.logger.Infof("updated gorums configuration: version=%d active=%d dead=%d", c.cfgVersion, c.activeNodes, len(c.dead))
	if strings.Contains(err.Error(), "context deadline exceeded") {
		c.logger.Warnf("pruned unavailable endpoints after timeout: %v", endpoints)
	}
}
