package nhotstuff

import (
	"errors"
	"testing"
	"time"

	"diablo-benchmark/blockchains/nhotstuff/clientpb"
	"diablo-benchmark/core"
	"google.golang.org/protobuf/types/known/emptypb"
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

func TestNewClientLimitsInflightCommands(t *testing.T) {
	client, err := newClient(testLogger{}, []string{"127.0.0.1"})
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	defer client.mgr.Close()

	if got, want := cap(client.inflight), 1; got != want {
		t.Fatalf("inflight cap = %d, want %d", got, want)
	}
}

func TestPerCommandTimeoutIsShort(t *testing.T) {
	if got, want := perCmdTimeout, 10*time.Second; got != want {
		t.Fatalf("perCmdTimeout = %v, want %v", got, want)
	}
}

type stubInteraction struct {
	payload   []byte
	submitCh  chan struct{}
	submitted bool
	committed bool
	aborted   bool
}

func (s *stubInteraction) Payload() interface{} { return s.payload }
func (s *stubInteraction) ReportSubmit() {
	s.submitted = true
	if s.submitCh != nil {
		close(s.submitCh)
		s.submitCh = nil
	}
}
func (s *stubInteraction) ReportCommit() { s.committed = true }
func (s *stubInteraction) ReportAbort()  { s.aborted = true }

func TestTriggerInteractionReportsSubmitAfterInflightAcquisition(t *testing.T) {
	client := &BlockchainClient{
		logger:   testLogger{},
		inflight: make(chan struct{}, 1),
	}
	client.inflight <- struct{}{}
	second := &stubInteraction{payload: []byte("two"), submitCh: make(chan struct{})}

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- errors.New("panic after submit")
			}
		}()
		done <- client.TriggerInteraction(second)
	}()

	select {
	case <-second.submitCh:
		t.Fatal("ReportSubmit happened before inflight slot was available")
	case <-time.After(50 * time.Millisecond):
	}

	<-client.inflight

	select {
	case <-second.submitCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ReportSubmit did not happen after inflight slot became available")
	}

	if err := <-done; err == nil {
		t.Fatal("expected TriggerInteraction to fail after submit")
	}
}

func TestExtractUnavailableEndpoints(t *testing.T) {
	errText := `quorum call error: context deadline exceeded (errors: 2, replies: 0)
node errors:
	node 9: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.30.10.10:1338: connect: connection refused"
	node 5: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.30.10.9:1338: connect: connection refused"
	node 9: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.30.10.10:1338: connect: connection refused"`

	endpoints := extractUnavailableEndpoints(errText)
	if got, want := len(endpoints), 2; got != want {
		t.Fatalf("len(endpoints) = %d, want %d (endpoints=%v)", got, want, endpoints)
	}
	if endpoints[0] != "10.30.10.10:1338" || endpoints[1] != "10.30.10.9:1338" {
		t.Fatalf("endpoints = %v, want [10.30.10.10:1338 10.30.10.9:1338]", endpoints)
	}
}

func TestPruneUnavailableNodesRebuildsConfiguration(t *testing.T) {
	client := &BlockchainClient{
		logger: testLogger{},
		allNodes: map[string]uint32{
			"n1:1338": 1,
			"n2:1338": 2,
			"n3:1338": 3,
			"n4:1338": 4,
		},
		dead: make(map[uint32]struct{}),
	}

	var rebuilt map[string]uint32
	client.rebuildCfg = func(nodeMap map[string]uint32, _ int) (*clientpb.Configuration, error) {
		rebuilt = nodeMap
		return &clientpb.Configuration{}, nil
	}

	err := errors.New("quorum call error: context deadline exceeded (errors: 2, replies: 0)\nnode errors:\n\tnode 4: rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing: dial tcp n4:1338: connect: connection refused\"\n\tnode 2: rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing: dial tcp n2:1338: connect: connection refused\"")
	client.pruneUnavailableNodes(err)

	if _, ok := client.dead[2]; !ok {
		t.Fatalf("expected node 2 in dead set")
	}
	if _, ok := client.dead[4]; !ok {
		t.Fatalf("expected node 4 in dead set")
	}
	if len(rebuilt) != 2 {
		t.Fatalf("rebuilt active node map size = %d, want 2", len(rebuilt))
	}
	if rebuilt["n1:1338"] != 1 || rebuilt["n3:1338"] != 3 {
		t.Fatalf("rebuilt active node map = %v, want n1,n3 only", rebuilt)
	}
}

func TestPruneUnavailableNodesIgnoresNonUnavailableError(t *testing.T) {
	client := &BlockchainClient{
		logger: testLogger{},
		allNodes: map[string]uint32{
			"n1:1338": 1,
			"n2:1338": 2,
		},
		dead: make(map[uint32]struct{}),
	}
	called := false
	client.rebuildCfg = func(nodeMap map[string]uint32, _ int) (*clientpb.Configuration, error) {
		called = true
		return &clientpb.Configuration{}, nil
	}

	client.pruneUnavailableNodes(errors.New("quorum call error: context deadline exceeded (errors: 0, replies: 0)"))

	if called {
		t.Fatal("rebuildCfg should not be called without unavailable node entries")
	}
	if len(client.dead) != 0 {
		t.Fatalf("dead set should remain empty, got %v", client.dead)
	}
}

func TestNodeStateSnapshotSeparatesActiveAndDead(t *testing.T) {
	client := &BlockchainClient{
		allNodes: map[string]uint32{
			"n1:1338": 1,
			"n2:1338": 2,
			"n3:1338": 3,
		},
		dead: map[uint32]struct{}{
			2: {},
		},
	}

	active, dead := client.nodeStateSnapshot()

	if got, want := len(active), 2; got != want {
		t.Fatalf("len(active) = %d, want %d (active=%v)", got, want, active)
	}
	if got, want := len(dead), 1; got != want {
		t.Fatalf("len(dead) = %d, want %d (dead=%v)", got, want, dead)
	}
	if active[0] != "n1:1338" || active[1] != "n3:1338" {
		t.Fatalf("active = %v, want [n1:1338 n3:1338]", active)
	}
	if dead[0] != "n2:1338" {
		t.Fatalf("dead = %v, want [n2:1338]", dead)
	}
}

func TestExecCommandQFRequiresFaultyPlusOneReplies(t *testing.T) {
	q := &qspec{faulty: 2}
	tooFew := map[uint32]*emptypb.Empty{1: {}, 2: {}}
	_, ok := q.ExecCommandQF(&clientpb.Command{}, tooFew)
	if ok {
		t.Fatal("ExecCommandQF should not succeed with only 2 replies when f=2")
	}

	enough := map[uint32]*emptypb.Empty{1: {}, 2: {}, 3: {}}
	_, ok = q.ExecCommandQF(&clientpb.Command{}, enough)
	if !ok {
		t.Fatal("ExecCommandQF should succeed with f+1 replies")
	}
}

func TestClassifyExecError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "timeout with node errors",
			err:  errors.New("quorum call error: context deadline exceeded (errors: 2, replies: 0)"),
			want: "timeout_errors_2_replies_0",
		},
		{
			name: "timeout with no replies",
			err:  errors.New("quorum call error: context deadline exceeded (errors: 0, replies: 0)"),
			want: "timeout_errors_0_replies_0",
		},
		{
			name: "other",
			err:  errors.New("rpc error: code = Aborted desc = blockchain was forked"),
			want: "other",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyExecError(tc.err)
			if got != tc.want {
				t.Fatalf("classifyExecError() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPerCommandTimeoutBackToTenSeconds(t *testing.T) {
	if got, want := perCmdTimeout, 10*time.Second; got != want {
		t.Fatalf("perCmdTimeout = %v, want %v", got, want)
	}
}

func TestCurrentConfigSnapshotReportsSizeConsistently(t *testing.T) {
	client, err := newClient(testLogger{}, []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"})
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	defer client.mgr.Close()

	cfg, _, active, _ := client.currentConfigSnapshot()
	if got, want := cfg.Size(), active; got != want {
		t.Fatalf("cfg.Size() = %d, want activeNodes %d", got, want)
	}
}
