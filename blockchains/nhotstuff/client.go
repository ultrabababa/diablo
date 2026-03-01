package nhotstuff

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"diablo-benchmark/core"

	"diablo-benchmark/blockchains/nhotstuff/clientpb"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	seq          uint64
	clientID     uint32
}

func newClient(logger core.Logger, view []string) (*BlockchainClient, error) {
	logger.Tracef("new hotstuff client")

	creds := insecure.NewCredentials()
	mgrOpts := []gorums.ManagerOption{
		gorums.WithGrpcDialOptions(grpc.WithTransportCredentials(creds)),
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

	return &BlockchainClient{
		logger:       logger,
		mgr:          mgr,
		gorumsConfig: gorumsConfig,
		seq:          0,
		clientID:     cid,
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

	seq := atomic.AddUint64(&c.seq, 1)

	cmd := &clientpb.Command{
		ClientID:       c.clientID,
		SequenceNumber: seq,
		Data:           payload,
	}

	iact.ReportSubmit()

	// HotStuff client typically uses some timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	promise := c.gorumsConfig.ExecCommand(ctx, cmd)
	_, err := promise.Get()
	if err != nil {
		iact.ReportAbort()
		return err
	}

	iact.ReportCommit()
	return nil
}
