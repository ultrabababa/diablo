package nasonninohotstuff

import (
	"diablo-benchmark/core"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultInflightCap = 4096
const mempoolSendAttempts = 5

type clientConfig struct {
	inflightCap int
	mempoolMode string
}

func parseClientConfig(params map[string]string) (clientConfig, error) {
	cfg := clientConfig{inflightCap: defaultInflightCap, mempoolMode: "round_robin"}
	if params == nil {
		return cfg, nil
	}

	if v, ok := params["client_inflight"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return cfg, fmt.Errorf("invalid client_inflight parameter: %q", v)
		}
		cfg.inflightCap = n
	}

	if v, ok := params["client_mempool_mode"]; ok {
		switch v {
		case "round_robin", "single":
			cfg.mempoolMode = v
		default:
			return cfg, fmt.Errorf("invalid client_mempool_mode parameter: %q", v)
		}
	}

	return cfg, nil
}

type pooledConn struct {
	mu   sync.Mutex
	conn net.Conn
}

type BlockchainClient struct {
	logger        core.Logger
	mempoolAddrs  []string
	bridgeBaseURL []string
	conns         []*pooledConn
	inflight      chan struct{}
	rr            uint64
	httpClient    *http.Client
	mempoolMode   string
}

func newClient(logger core.Logger, view []string, cfg clientConfig) (*BlockchainClient, error) {
	if len(view) == 0 {
		return nil, fmt.Errorf("empty view for asonnino-hotstuff client")
	}

	mempoolAddrs := make([]string, 0, len(view))
	bridgeBaseURL := make([]string, 0, len(view))
	for _, host := range view {
		octets := strings.Split(host, ".")
		if len(octets) != 4 {
			return nil, fmt.Errorf("invalid host in view: %s", host)
		}
		last, err := strconv.Atoi(octets[3])
		if err != nil || last < 1 {
			return nil, fmt.Errorf("invalid host in view: %s", host)
		}
		mempoolPort := 25000 + (last - 1)
		bridgePort := 26000 + (last - 1)
		mempoolAddrs = append(mempoolAddrs, fmt.Sprintf("%s:%d", host, mempoolPort))
		bridgeBaseURL = append(bridgeBaseURL, fmt.Sprintf("http://%s:%d/status/", host, bridgePort))
	}

	return &BlockchainClient{
		logger:        logger,
		mempoolAddrs:  mempoolAddrs,
		bridgeBaseURL: bridgeBaseURL,
		conns: func() []*pooledConn {
			out := make([]*pooledConn, len(mempoolAddrs))
			for i := range out {
				out[i] = &pooledConn{}
			}
			return out
		}(),
		inflight: make(chan struct{}, cfg.inflightCap),
		httpClient: &http.Client{
			Timeout: 1200 * time.Millisecond,
		},
		mempoolMode: cfg.mempoolMode,
	}, nil
}

func (c *BlockchainClient) DecodePayload(bytes []byte) (interface{}, error) {
	return bytes, nil
}

func (c *BlockchainClient) TriggerInteraction(iact core.Interaction) error {
	payload, ok := iact.Payload().([]byte)
	if !ok {
		iact.ReportAbort()
		return fmt.Errorf("invalid payload type for asonnino-hotstuff")
	}

	if len(payload) == 0 {
		iact.ReportAbort()
		return fmt.Errorf("empty payload for asonnino-hotstuff")
	}

	idx := c.selectEndpoint()
	endpoint := c.mempoolAddrs[idx]
	txID := parseTxID(payload)

	c.inflight <- struct{}{}
	defer func() { <-c.inflight }()

	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	if err := c.sendFrame(idx, endpoint, frame); err != nil {
		iact.ReportAbort()
		return err
	}

	iact.ReportSubmit()
	return c.confirm(iact, idx, txID)
}

func (c *BlockchainClient) selectEndpoint() int {
	if c.mempoolMode == "single" {
		return 0
	}
	return int(atomic.AddUint64(&c.rr, 1)-1) % len(c.mempoolAddrs)
}

func (c *BlockchainClient) sendFrame(idx int, endpoint string, frame []byte) error {
	backoffs := []time.Duration{
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
	}
	var lastErr error

	for attempt := 0; attempt < mempoolSendAttempts; attempt++ {
		if err := c.sendFrameOnce(idx, endpoint, frame); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt < len(backoffs) {
			time.Sleep(backoffs[attempt])
		}
	}

	return lastErr
}

func (c *BlockchainClient) sendFrameOnce(idx int, endpoint string, frame []byte) error {
	p := c.conns[idx]
	p.mu.Lock()
	defer p.mu.Unlock()

	write := func(conn net.Conn) error {
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		_, err := conn.Write(frame)
		return err
	}

	if p.conn == nil {
		conn, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
		if err != nil {
			return fmt.Errorf("dial %s failed: %w", endpoint, err)
		}
		p.conn = conn
	}

	if err := write(p.conn); err == nil {
		return nil
	}

	_ = p.conn.Close()
	p.conn = nil

	conn, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s failed: %w", endpoint, err)
	}
	p.conn = conn
	if err := write(p.conn); err != nil {
		_ = p.conn.Close()
		p.conn = nil
		return fmt.Errorf("write to %s failed: %w", endpoint, err)
	}
	return nil
}

func parseTxID(payload []byte) uint64 {
	if len(payload) >= 9 && payload[0] == 0 {
		return binary.BigEndian.Uint64(payload[1:9])
	}
	if len(payload) >= 8 {
		return binary.BigEndian.Uint64(payload[:8])
	}
	var b [8]byte
	copy(b[:], payload)
	return binary.BigEndian.Uint64(b[:])
}

type txStatus struct {
	State string `json:"state"`
}

func (c *BlockchainClient) confirm(iact core.Interaction, idx int, txID uint64) error {
	const maxWait = 120 * time.Second
	const pollInterval = 500 * time.Millisecond

	deadline := time.Now().Add(maxWait)
	url := c.bridgeBaseURL[idx] + strconv.FormatUint(txID, 10)

	for time.Now().Before(deadline) {
		state, err := c.fetchState(url)
		if err == nil {
			switch state {
			case "committed":
				iact.ReportCommit()
				return nil
			case "aborted":
				iact.ReportAbort()
				return fmt.Errorf("transaction %d aborted by bridge", txID)
			}
		}
		time.Sleep(pollInterval)
	}

	iact.ReportAbort()
	return fmt.Errorf("transaction %d not confirmed within %s", txID, maxWait)
}

func (c *BlockchainClient) fetchState(url string) (string, error) {
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var parsed txStatus
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}

	return parsed.State, nil
}
