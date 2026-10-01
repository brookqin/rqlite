package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// A node-local capacity failure must end the process, then replay the committed
// transaction after the capacity constraint is removed. No host disk is filled.
func Test_QiankunFatalWriteProcessRecovery(t *testing.T) {
	if mode := os.Getenv("RQLITE_QK_FATAL_MODE"); mode != "" {
		qiankunFatalWriteChild(t, mode)
		return
	}
	for _, method := range []string{"execute", "request"} {
		t.Run(method, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			run := func(mode string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^Test_QiankunFatalWriteProcessRecovery$", "-test.timeout=40s")
				cmd.Env = append(os.Environ(), "RQLITE_QK_FATAL_MODE="+mode,
					"RQLITE_QK_FATAL_METHOD="+method, "RQLITE_QK_FATAL_DIR="+dir,
					"RQLITE_QK_FATAL_ADDRESS="+address)
				return cmd.CombinedOutput()
			}
			output, err := run("fail")
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 ||
				!bytes.Contains(output, []byte("aborting to prevent divergence between nodes")) {
				t.Fatalf("expected fatal process exit, got %v:\n%s", err, output)
			}
			if bytes.Contains(output, []byte("qk-private-bound-value")) {
				t.Fatal("bound parameter leaked in fatal process output")
			}
			if output, err := run("recover"); err != nil {
				t.Fatalf("committed transaction did not recover: %v:\n%s", err, output)
			}
		})
	}
}

func qiankunFatalWriteChild(t *testing.T, mode string) {
	t.Helper()
	layer := mustMockLayer(os.Getenv("RQLITE_QK_FATAL_ADDRESS"))
	defer layer.Close()
	s := New(&Config{DBConf: NewDBConfig(), Dir: os.Getenv("RQLITE_QK_FATAL_DIR"), ID: "fatal-node"}, layer)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)
	if mode == "fail" {
		if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	if mode == "recover" {
		rows, _, _, err := s.Query(context.Background(), &proto.QueryRequest{
			Request: &proto.Request{Statements: []*proto.Statement{{Sql: "SELECT value, (SELECT length(payload) FROM data), (SELECT name FROM data) FROM marker"}}},
			Level:   proto.ConsistencyLevel_STRONG,
		})
		if err != nil || len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 {
			t.Fatalf("recovery query failed: %v, %v", rows, err)
		}
		values := rows[0].Values[0].Parameters
		if len(values) != 3 || values[0].GetI() != 2 || values[1].GetI() != 1048576 || values[2].GetS() != "qk-private-bound-value" {
			t.Fatal("replayed transaction did not restore all expected values")
		}
		return
	}
	for _, statement := range []string{
		"CREATE TABLE data (name TEXT, payload BLOB)",
		"CREATE TABLE marker (value INTEGER)",
		"INSERT INTO marker VALUES (0)",
	} {
		results, _, err := s.Execute(context.Background(), executeRequestFromString(statement, false, false))
		if err != nil || len(results) != 1 || results[0].GetError() != "" {
			t.Fatalf("setup failed: %v, %v", results, err)
		}
	}
	rows, err := s.db.QueryStringStmt("PRAGMA page_count")
	if err != nil || len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 {
		t.Fatalf("page count failed: %v, %v", rows, err)
	}
	// This connection-local limit affects only the failing node and is absent
	// when its process reopens the database. It is not a replicated PRAGMA.
	results, err := s.db.Execute(&proto.Request{Statements: []*proto.Statement{{
		Sql: fmt.Sprintf("PRAGMA max_page_count=%d", rows[0].Values[0].Parameters[0].GetI()),
	}}}, false)
	if err != nil || len(results) != 1 || results[0].GetError() != "" {
		t.Fatalf("page limit failed: %v, %v", results, err)
	}
	request := &proto.Request{Transaction: true, Statements: []*proto.Statement{
		{Sql: "UPDATE marker SET value=1"},
		{Sql: "INSERT INTO data VALUES (?, zeroblob(1048576))", Parameters: []*proto.Parameter{{Value: &proto.Parameter_S{S: "qk-private-bound-value"}}}},
		{Sql: "UPDATE marker SET value=2"},
	}}
	if os.Getenv("RQLITE_QK_FATAL_METHOD") == "execute" {
		_, _, _ = s.Execute(context.Background(), &proto.ExecuteRequest{Request: request})
	} else {
		_, _, _, _ = s.Request(context.Background(), &proto.ExecuteQueryRequest{Request: request, Level: proto.ConsistencyLevel_STRONG})
	}
	t.Fatal("fatal SQLite write returned without ending the process")
}
