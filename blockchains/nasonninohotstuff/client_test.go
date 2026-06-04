package nasonninohotstuff

import (
	"diablo-benchmark/core"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testLogger struct{}

func (testLogger) Fatalf(string, ...interface{}) {}
func (testLogger) Errorf(string, ...interface{}) {}
func (testLogger) Warnf(string, ...interface{})  {}
func (testLogger) Infof(string, ...interface{})  {}
func (testLogger) Debugf(string, ...interface{}) {}
func (testLogger) Tracef(string, ...interface{}) {}
func (testLogger) Extend(string) core.Logger {
	return testLogger{}
}

func TestParseClientConfigDefaultsInflightCap(t *testing.T) {
	cfg, err := parseClientConfig(nil)
	if err != nil {
		t.Fatalf("parseClientConfig(nil) error = %v", err)
	}

	if got, want := cfg.inflightCap, defaultInflightCap; got != want {
		t.Fatalf("inflightCap = %d, want %d", got, want)
	}
	if got, want := cfg.mempoolMode, "round_robin"; got != want {
		t.Fatalf("mempoolMode = %q, want %q", got, want)
	}
}

func TestParseClientConfigOverridesInflightCap(t *testing.T) {
	cfg, err := parseClientConfig(map[string]string{"client_inflight": "8192"})
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}

	if got, want := cfg.inflightCap, 8192; got != want {
		t.Fatalf("inflightCap = %d, want %d", got, want)
	}
}

func TestParseClientConfigRejectsInvalidInflightCap(t *testing.T) {
	for _, value := range []string{"0", "-1", "abc"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseClientConfig(map[string]string{"client_inflight": value})
			if err == nil {
				t.Fatalf("parseClientConfig() succeeded for client_inflight=%q", value)
			}
		})
	}
}

func TestParseClientConfigSupportsSingleMempoolMode(t *testing.T) {
	cfg, err := parseClientConfig(map[string]string{"client_mempool_mode": "single"})
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}

	if got, want := cfg.mempoolMode, "single"; got != want {
		t.Fatalf("mempoolMode = %q, want %q", got, want)
	}
}

func TestParseClientConfigRejectsInvalidMempoolMode(t *testing.T) {
	_, err := parseClientConfig(map[string]string{"client_mempool_mode": "random"})
	if err == nil {
		t.Fatal("parseClientConfig() succeeded for invalid client_mempool_mode")
	}
}

func TestNewClientUsesConfiguredInflightCap(t *testing.T) {
	client, err := newClient(testLogger{}, []string{"127.0.0.1"}, clientConfig{inflightCap: 8192, mempoolMode: "round_robin"})
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}

	if got, want := cap(client.inflight), 8192; got != want {
		t.Fatalf("inflight cap = %d, want %d", got, want)
	}
}

func TestSelectEndpointUsesRoundRobinByDefault(t *testing.T) {
	client, err := newClient(testLogger{}, []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}, clientConfig{inflightCap: 1, mempoolMode: "round_robin"})
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}

	for i, want := range []int{0, 1, 2, 0} {
		if got := client.selectEndpoint(); got != want {
			t.Fatalf("selectEndpoint call %d = %d, want %d", i, got, want)
		}
	}
}

func TestSelectEndpointSingleModeAlwaysUsesFirstEndpoint(t *testing.T) {
	client, err := newClient(testLogger{}, []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}, clientConfig{inflightCap: 1, mempoolMode: "single"})
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}

	for i := 0; i < 4; i++ {
		if got, want := client.selectEndpoint(), 0; got != want {
			t.Fatalf("selectEndpoint call %d = %d, want %d", i, got, want)
		}
	}
}

func TestBuilderEncodesMinimalNumberPayload(t *testing.T) {
	builder := newBuilder(testLogger{})

	payload := builder.encodePayload()

	if got, want := len(payload), 9; got != want {
		t.Fatalf("payload length = %d, want %d", got, want)
	}
	if payload[0] != 0 {
		t.Fatalf("payload marker = %d, want 0", payload[0])
	}
	if got, want := binary.BigEndian.Uint64(payload[1:9]), uint64(1); got != want {
		t.Fatalf("payload sequence = %d, want %d", got, want)
	}
}

type stubInteraction struct {
	payload   []byte
	submitted bool
	committed bool
	aborted   bool
}

func (s *stubInteraction) Payload() interface{} { return s.payload }
func (s *stubInteraction) ReportSubmit()        { s.submitted = true }
func (s *stubInteraction) ReportCommit()        { s.committed = true }
func (s *stubInteraction) ReportAbort()         { s.aborted = true }

func TestTriggerInteractionDoesNotReportSubmitWhenSendFails(t *testing.T) {
	client := &BlockchainClient{
		logger:        testLogger{},
		mempoolAddrs:  []string{"127.0.0.1:1"},
		bridgeBaseURL: []string{"http://127.0.0.1:1/status/"},
		conns:         []*pooledConn{{}},
		inflight:      make(chan struct{}, 1),
		httpClient:    &http.Client{Timeout: 10 * time.Millisecond},
	}
	iact := &stubInteraction{payload: []byte{0, 0, 0, 0, 0, 0, 0, 0, 1}}

	err := client.TriggerInteraction(iact)

	if err == nil {
		t.Fatal("TriggerInteraction succeeded, want send failure")
	}
	if iact.submitted {
		t.Fatal("ReportSubmit called even though sendFrame failed")
	}
	if !iact.aborted {
		t.Fatal("ReportAbort was not called")
	}
}

func TestTriggerInteractionReportsSubmitAfterSendSucceeds(t *testing.T) {
	mempool := newFrameRecorder(t)
	defer mempool.Close()

	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(txStatus{State: "committed"})
	}))
	defer bridge.Close()

	client := &BlockchainClient{
		logger:        testLogger{},
		mempoolAddrs:  []string{mempool.Addr().String()},
		bridgeBaseURL: []string{bridge.URL + "/status/"},
		conns:         []*pooledConn{{}},
		inflight:      make(chan struct{}, 1),
		httpClient:    bridge.Client(),
	}
	iact := &stubInteraction{payload: []byte{0, 0, 0, 0, 0, 0, 0, 0, 7}}

	if err := client.TriggerInteraction(iact); err != nil {
		t.Fatalf("TriggerInteraction() error = %v", err)
	}
	if !iact.submitted {
		t.Fatal("ReportSubmit was not called")
	}
	if !iact.committed {
		t.Fatal("ReportCommit was not called")
	}
	if iact.aborted {
		t.Fatal("ReportAbort was called")
	}

	frame := mempool.Frame()
	if got, want := len(frame), 13; got != want {
		t.Fatalf("frame length = %d, want %d", got, want)
	}
	if got, want := binary.BigEndian.Uint32(frame[:4]), uint32(9); got != want {
		t.Fatalf("frame payload length = %d, want %d", got, want)
	}
}

type frameRecorder struct {
	t        *testing.T
	listener net.Listener
	frameCh  chan []byte
}

func newFrameRecorder(t *testing.T) *frameRecorder {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	rec := &frameRecorder{
		t:        t,
		listener: listener,
		frameCh:  make(chan []byte, 1),
	}
	go rec.accept()
	return rec
}

func (r *frameRecorder) Addr() net.Addr {
	return r.listener.Addr()
}

func (r *frameRecorder) Close() {
	_ = r.listener.Close()
}

func (r *frameRecorder) Frame() []byte {
	r.t.Helper()

	select {
	case frame := <-r.frameCh:
		return frame
	case <-time.After(time.Second):
		r.t.Fatal("timed out waiting for mempool frame")
		return nil
	}
}

func (r *frameRecorder) accept() {
	conn, err := r.listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	header := make([]byte, 4)
	if _, err := conn.Read(header); err != nil {
		return
	}
	size := binary.BigEndian.Uint32(header)
	payload := make([]byte, size)
	if _, err := conn.Read(payload); err != nil {
		return
	}

	frame := append(header, payload...)
	r.frameCh <- frame
}
