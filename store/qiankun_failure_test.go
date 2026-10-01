package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func Test_QiankunCommitBeforeApplyLeaderLoss(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("open follower 1: %v", err)
	}
	defer s1.Close(true)
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("join follower 1: %v", err)
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("wait for follower 1 leader: %v", err)
	}

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("open follower 2: %v", err)
	}
	defer s2.Close(true)
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), true)); err != nil {
		t.Fatalf("join follower 2: %v", err)
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("wait for follower 2 leader: %v", err)
	}

	if _, _, err := s0.Execute(context.Background(), executeRequestFromString(
		`CREATE TABLE operation_log (request_id TEXT PRIMARY KEY, value TEXT NOT NULL)`, false, false)); err != nil {
		t.Fatalf("create operation log: %v", err)
	}

	gate := newBlockingFSMApplyGate()
	s0.testApplyGate = gate
	gate.Arm()
	defer gate.Release()
	requestDone := make(chan error, 1)
	go func() {
		_, _, err := s0.Execute(context.Background(), &proto.ExecuteRequest{Request: &proto.Request{
			Transaction: true,
			Statements: []*proto.Statement{{
				Sql: `INSERT INTO operation_log(request_id, value) VALUES('req-1', 'committed')`,
			}},
		}})
		requestDone <- err
	}()

	blockedIndex := gate.WaitUntilBlocked(t, 10*time.Second)
	commitIndex, err := s0.LeaderCommitIndex()
	if err != nil {
		t.Fatalf("leader commit index: %v", err)
	}
	if commitIndex < blockedIndex {
		t.Fatalf("log %d reached FSM before commit index %d", blockedIndex, commitIndex)
	}
	if applied := s0.DBAppliedIndex(); applied >= blockedIndex {
		t.Fatalf("database applied index %d reached blocked log %d", applied, blockedIndex)
	}

	if err := s0.Stepdown(false, ""); err != nil {
		t.Fatalf("step down blocked leader: %v", err)
	}
	newLeader := waitForLeaderStore(t, 10*time.Second, s1, s2)
	gate.Release()
	var requestErr error
	select {
	case requestErr = <-requestDone:
	case <-time.After(10 * time.Second):
		t.Fatal("request did not return after releasing apply gate")
	}
	t.Logf("old leader direct Store call returned after transfer: %v", requestErr)

	if got := queryOperationCount(t, newLeader, "req-1"); got != 1 {
		t.Fatalf("reconciled request count = %d, want 1", got)
	}
}

type blockingFSMApplyGate struct {
	armed       atomic.Bool
	once        sync.Once
	releaseOnce sync.Once
	blocked     chan uint64
	release     chan struct{}
}

func newBlockingFSMApplyGate() *blockingFSMApplyGate {
	return &blockingFSMApplyGate{blocked: make(chan uint64, 1), release: make(chan struct{})}
}

func (g *blockingFSMApplyGate) Arm() {
	g.armed.Store(true)
}

func (g *blockingFSMApplyGate) BeforeApply(index, _ uint64) {
	if !g.armed.Load() {
		return
	}
	g.once.Do(func() {
		g.blocked <- index
		<-g.release
	})
}

func (g *blockingFSMApplyGate) AfterApply(_, _ uint64) {}

func (g *blockingFSMApplyGate) WaitUntilBlocked(t *testing.T, timeout time.Duration) uint64 {
	t.Helper()
	select {
	case index := <-g.blocked:
		return index
	case <-time.After(timeout):
		t.Fatal("timed out waiting for FSM apply gate")
		return 0
	}
}

func (g *blockingFSMApplyGate) Release() {
	g.releaseOnce.Do(func() {
		close(g.release)
	})
}

func waitForLeaderStore(t *testing.T, timeout time.Duration, stores ...*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, store := range stores {
			if store.IsLeader() {
				return store
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for new leader")
	return nil
}

func queryOperationCount(t *testing.T, store *Store, requestID string) int64 {
	t.Helper()
	rows, _, _, err := store.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{Statements: []*proto.Statement{{
			Sql:        `SELECT COUNT(*) FROM operation_log WHERE request_id = ?`,
			Parameters: []*proto.Parameter{{Value: &proto.Parameter_S{S: requestID}}},
		}}},
		Level: proto.ConsistencyLevel_STRONG,
	})
	if err != nil {
		t.Fatalf("query reconciled operation: %v", err)
	}
	return rows[0].Values[0].Parameters[0].GetI()
}
