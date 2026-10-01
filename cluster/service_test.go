package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	rqlitetcp "github.com/rqlite/rqlite/v10/tcp"
	"github.com/rqlite/rqlite/v10/testdata/x509"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if ml.Addr().String() != s.Addr() {
		t.Fatalf("service returned incorrect address")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetAPIAddr(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	if exp, got := "foo", s.GetAPIAddr(); exp != got {
		t.Fatalf("got incorrect API address, exp %s, got %s", exp, got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMeta(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	s.SetVersion("1.0.0")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	c.SetLocalVersion("2.0.0")
	if v := c.GetLocalVersion(); v != "2.0.0" {
		t.Fatalf("failed to get correct local version, exp %s, got %s", "2.0.0", v)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "http://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "2.0.0", meta.Version)
	}

	s.EnableHTTPS(true)

	// Test fetch via network.
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", meta.Version)
	}

	// Test fetch via local call.
	addr := s.GetNodeAPIURL()
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}
	ver := s.GetVersion()
	if ver != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", ver)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMetaLocal(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	defer s.Close()
	// Global counters survive repeated test runs in the same process.
	localRequests := stats.Get(numGetNodeAPIRequestLocal).(*expvar.Int).Value()

	// Test by enabling local answering
	c := NewClient(ml, 30*time.Second)
	if err := c.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address locally: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address locally, exp %s, got %s", "http://foo", meta.Url)
	}

	// Check stats to confirm local response.
	if stats.Get(numGetNodeAPIRequestLocal).(*expvar.Int).Value() != localRequests+1 {
		t.Fatalf("failed to confirm request served locally")
	}
}

func Test_NewServiceNodeMetaProviderRemoteAndLocal(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	mgr.commitIndex = 42
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	s.SetAPIAddr("api.example:8200")
	s.SetVersion("10.2.7-qiankun")
	provider := &staticNodeMetaProvider{meta: &proto.NodeMeta{
		Url: "spoofed", CommitIndex: 999, Version: "spoofed",
		NodeId: "qk-a", RuntimeMode: "standby", Sealed: false, Ready: true,
		AppliedIndex: 41, SchemaChecksum: "schema-v1",
	}}
	if err := s.SetNodeMetaProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	client := NewClient(ml, time.Second)
	assertNodeMeta := func(t *testing.T, meta *proto.NodeMeta) {
		t.Helper()
		if meta.Url != "http://api.example:8200" || meta.CommitIndex != 42 || meta.Version != "10.2.7-qiankun" {
			t.Fatalf("service-owned metadata not authoritative: %+v", meta)
		}
		if meta.NodeId != "qk-a" || meta.RuntimeMode != "standby" || meta.Sealed || !meta.Ready || meta.AppliedIndex != 41 || meta.SchemaChecksum != "schema-v1" {
			t.Fatalf("provider metadata not projected: %+v", meta)
		}
	}
	remote, err := client.GetNodeMeta(context.Background(), s.Addr(), noRetries, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertNodeMeta(t, remote)
	if err := client.SetLocal(s.Addr(), s); err != nil {
		t.Fatal(err)
	}
	local, err := client.GetNodeMeta(context.Background(), s.Addr(), noRetries, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertNodeMeta(t, local)

	local.NodeId = "mutated"
	again, err := s.GetNodeMeta()
	if err != nil {
		t.Fatal(err)
	}
	if again.NodeId != "qk-a" {
		t.Fatalf("returned metadata aliases provider state: %+v", again)
	}
}

func Test_NewServiceNodeMetaProviderLifecycle(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if err := s.SetNodeMetaProvider(&staticNodeMetaProvider{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetNodeMetaProvider(nil); !errors.Is(err, ErrServiceOpen) {
		t.Fatalf("expected ErrServiceOpen, got %v", err)
	}
}

func Test_NewServiceSetGetNodeMetaTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	exp := "http://foo"
	if meta.Url != exp {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", exp, meta.Url)
	}

	s.EnableHTTPS(true)
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceGetCommitIndex(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	s.EnableHTTPS(true)

	// Test fetch via network.
	mgr.commitIndex = 1234
	c := NewClient(ml, 30*time.Second)
	idx, err := c.GetCommitIndex(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if idx != 1234 {
		t.Fatalf("failed to get correct node commit index, exp %d, got %d", 1234, idx)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceTestExecuteQueryAuthNoCredentials(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	// Test that for a cluster with no credential store configured
	// all users are authed for both operations
	var c CredentialStore = nil
	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), nil, 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), nil, 5*time.Second, noRetries)
	if err != nil {
		t.Fatal(err)
	}
}

// Test_NewServiceTestExecuteQueryAuth tests that for a cluster with a credential
// store, configured users with execute permissions can execute and users with
// query permissions can query, and can't if they don't have those permissions.
func Test_NewServiceTestExecuteQueryAuth(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	f := func(username string, password string, perm string) bool {
		if username == "alice" && password == "secret1" && perm == "execute" {
			return true
		} else if username == "bob" && password == "secret1" && perm == "query" {
			return true
		}
		return false
	}
	c := &mockCredentialStore{aaFunc: f}

	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("alice improperly unauthorized to execute")
	}
	_, _, err = cl.Execute(context.Background(), er, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, defaultMaxRetries)
	if err == nil {
		t.Fatal("bob improperly authorized to execute")
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		fmt.Println(err)
		t.Fatal("bob improperly unauthorized to query")
	}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		t.Fatal("alice improperly authorized to query")
	}
}

func Test_NewServiceNotify(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.notifyFn = func(n *command.NotifyRequest) error {
		defer wg.Done()
		if n.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", n.Id)
		}
		if n.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", n.Address)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a notify request.
	nr := &command.NotifyRequest{
		Id:      "foo",
		Address: "localhost",
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to notify node: %s", err)
	}

	// Ensure that the notify function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to notify node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceJoin(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.joinFn = func(j *command.JoinRequest) error {
		defer wg.Done()
		if j.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", j.Id)
		}
		if j.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", j.Address)
		}
		if !j.Voter {
			t.Fatalf("failed to get correct voter setting, exp %t, got %t", true, j.Voter)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a Join request.
	jr := &command.JoinRequest{
		Id:      "foo",
		Address: "localhost",
		Voter:   true,
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to join node: %s", err)
	}

	// Ensure the join function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to join node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func TestServiceNodeRequestAuthorizer(t *testing.T) {
	for _, operation := range []string{"notify", "join"} {
		for _, rejected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rejected=%t", operation, rejected), func(t *testing.T) {
				ml := mustNewMockTransport()
				mm := mustNewMockManager()
				var managerCalls atomic.Int32
				mm.notifyFn = func(*command.NotifyRequest) error {
					managerCalls.Add(1)
					return nil
				}
				mm.joinFn = func(*command.JoinRequest) error {
					managerCalls.Add(1)
					return nil
				}
				authorizer := &recordingNodeRequestAuthorizer{nodeID: "node-2", address: "node-2:8300"}
				if rejected {
					authorizer.err = errors.New("peer identity mismatch")
				}
				s := New(ml, mustNewMockDatabase(), mm, mustNewMockCredentialStore())
				if err := s.SetNodeRequestAuthorizer(authorizer); err != nil {
					t.Fatal(err)
				}
				if err := s.Open(); err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				if err := s.SetNodeRequestAuthorizer(authorizer); !errors.Is(err, ErrServiceOpen) {
					t.Fatalf("setter after open error = %v", err)
				}
				client := NewClient(ml, 30*time.Second)
				var err error
				if operation == "notify" {
					err = client.Notify(context.Background(), &command.NotifyRequest{Id: "node-2", Address: "node-2:8300"}, s.Addr(), nil, 5*time.Second)
				} else {
					err = client.Join(context.Background(), &command.JoinRequest{Id: "node-2", Address: "node-2:8300", Voter: true}, s.Addr(), nil, 5*time.Second)
				}
				if rejected {
					if err == nil || err.Error() != "unauthorized" {
						t.Fatalf("rejected request error = %v", err)
					}
					if managerCalls.Load() != 0 {
						t.Fatal("manager called after node authorization rejection")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if managerCalls.Load() != 1 {
						t.Fatalf("manager calls = %d", managerCalls.Load())
					}
				}
				if authorizer.calls.Load() != 1 || !authorizer.sawConnection.Load() {
					t.Fatalf("authorizer calls=%d saw_connection=%t", authorizer.calls.Load(), authorizer.sawConnection.Load())
				}
			})
		}
	}
}

func TestServiceNodeRequestAuthorizerReceivesMuxConnection(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	mux, err := rqlitetcp.NewMux(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	go mux.Serve()
	authorizer := &recordingNodeRequestAuthorizer{
		nodeID: "node-2", address: "node-2:8300", requireUnwrap: true,
	}
	service := New(mux.Listen(MuxClusterHeader), mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if err := service.SetNodeRequestAuthorizer(authorizer); err != nil {
		t.Fatal(err)
	}
	if err := service.Open(); err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	client := NewClient(rqlitetcp.NewDialer(MuxClusterHeader, nil), 5*time.Second)
	if err := client.Notify(context.Background(), &command.NotifyRequest{Id: "node-2", Address: "node-2:8300"}, raw.Addr().String(), nil, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if authorizer.calls.Load() != 1 || !authorizer.sawUnwrappedConnection.Load() {
		t.Fatalf("authorizer calls=%d saw_unwrapped=%t", authorizer.calls.Load(), authorizer.sawUnwrappedConnection.Load())
	}
}

func Test_ServiceHandleHighwaterMarkUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	responses, err := c.BroadcastHWM(context.Background(), 987654, nil, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}
}

func Test_ServiceRegisterHWMUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a channel to receive highwater mark updates
	hwmCh := make(chan uint64, 1)
	s.RegisterHWMUpdate(hwmCh)

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	testHWM := uint64(123456)
	responses, err := c.BroadcastHWM(context.Background(), testHWM, nil, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}

	// Check that we received the update on the channel
	select {
	case hwmUpdate := <-hwmCh:
		if hwmUpdate != testHWM {
			t.Fatalf("expected highwater_mark to be %d, got: %d", testHWM, hwmUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for highwater mark update on channel")
	}
}

// Test_ServiceRegisterHWMUpdate_WithAuth checks that the CDC service checks credentials.
func Test_ServiceRegisterHWMUpdate_WithAuth(t *testing.T) {
	creds := &proto.Credentials{Username: "bob", Password: "passwd"}
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	credStr := mustNewMockCredentialStore()
	credStr.aaFunc = func(username, password, perm string) bool {
		return username == creds.Username && password == creds.Password
	}
	s := New(ml, mustNewMockDatabase(), mgr, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	testHWM := uint64(123456)
	responses, err := c.BroadcastHWM(context.Background(), testHWM, creds, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}

	// Change required creds and ensure auth kicks in to deny.
	credStr.aaFunc = func(username, password, perm string) bool {
		return username == creds.Username && password == "foo"
	}
	responses, err = c.BroadcastHWM(context.Background(), testHWM, creds, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}
	resp, ok = responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}
	if resp.Error != "unauthorized" {
		t.Fatal("expected an error")
	}
}

func Test_ServiceClosesIdleConnection(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}
	s.connTimeout = 500 * time.Millisecond

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Connect but send nothing. The service should close the connection
	// after ConnReadTimeout.
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("failed to connect to service: %s", err)
	}
	defer conn.Close()

	// The server should close the connection after the read timeout.
	one := make([]byte, 1)
	_, err = conn.Read(one)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got: %v", err)
	}
}

func Test_ServiceSetConnectionLimitOpen(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	if err := s.SetConnectionLimit(1); !errors.Is(err, ErrServiceOpen) {
		t.Fatalf("expected ErrServiceOpen, got: %v", err)
	}
}

func Test_ServiceConnLimiterRejectAndRelease(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if err := s.SetConnectionLimit(1); err != nil {
		t.Fatalf("failed to set connection limit: %s", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	md := &mockDialer{Dialer: ml}
	t.Cleanup(md.CloseAll)

	c1 := NewClient(md, 30*time.Second)
	_, err := c1.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node metadata on first connection: %s", err)
	}

	// The first client's pooled connection remains open and holds the limiter slot.
	c2 := NewClient(md, 30*time.Second)
	_, err = c2.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err == nil {
		t.Fatalf("expected request on rejected connection to fail")
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		t.Fatalf("expected connection to be rejected without timing out, got: %v", err)
	}

	// Releasing the idle connection should free its limiter slot.
	if err := md.CloseConn(0); err != nil {
		t.Fatalf("failed to close first connection: %s", err)
	}

	// A new client request should be accepted once capacity is available again.
	c3 := NewClient(md, 30*time.Second)
	_, err = c3.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to get node metadata after release: %s", err)
	}
}

type mockDialer struct {
	Dialer
	mu    sync.Mutex
	conns []net.Conn
}

func (d *mockDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := d.Dialer.Dial(address, timeout)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()

	return conn, nil
}

func (d *mockDialer) CloseConn(index int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if index < 0 || index >= len(d.conns) {
		return fmt.Errorf("connection %d not tracked", index)
	}
	return d.conns[index].Close()
}

func (d *mockDialer) CloseAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
}

type mockTransport struct {
	tn              net.Listener
	remoteEncrypted bool
}

func (ml *mockTransport) Accept() (c net.Conn, err error) {
	return ml.tn.Accept()
}

func (ml *mockTransport) Addr() net.Addr {
	return ml.tn.Addr()
}

func (ml *mockTransport) Close() (err error) {
	return ml.tn.Close()
}

func (ml *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if ml.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
}

func mustNewMockTransport() *mockTransport {
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockTransport{
		tn: tn,
	}
}

func mustNewMockTLSTransport() *mockTransport {
	tn := mustNewMockTransport()
	return &mockTransport{
		tn:              tls.NewListener(tn, mustCreateTLSConfig()),
		remoteEncrypted: true,
	}
}

type mockDatabase struct {
	executeFn func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error)
	queryFn   func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error)
	requestFn func(rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error)
	backupFn  func(br *command.BackupRequest, dst io.Writer) (int, error)
	loadFn    func(lr *command.LoadRequest) error
}

func (m *mockDatabase) Execute(ctx context.Context, er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
	return m.executeFn(er)
}

func (m *mockDatabase) Query(ctx context.Context, qr *command.QueryRequest) ([]*command.QueryRows, command.ConsistencyLevel, uint64, error) {
	rows, idx, err := m.queryFn(qr)
	return rows, command.ConsistencyLevel_NONE, idx, err
}

func (m *mockDatabase) Request(ctx context.Context, rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
	if m.requestFn == nil {
		return []*command.ExecuteQueryResponse{}, 0, 0, nil
	}
	return m.requestFn(rr)
}

func (m *mockDatabase) Backup(ctx context.Context, br *command.BackupRequest, dst io.Writer) (int, error) {
	if m.backupFn == nil {
		return 0, nil
	}
	return m.backupFn(br, dst)
}

func (m *mockDatabase) Load(ctx context.Context, lr *command.LoadRequest) error {
	if m.loadFn == nil {
		return nil
	}
	return m.loadFn(lr)
}

func mustNewMockDatabase() *mockDatabase {
	e := func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return []*command.ExecuteQueryResponse{}, 0, nil
	}
	q := func(er *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return []*command.QueryRows{}, 0, nil
	}
	return &mockDatabase{executeFn: e, queryFn: q}
}

type MockManager struct {
	removeNodeFn func(rn *command.RemoveNodeRequest) error
	notifyFn     func(n *command.NotifyRequest) error
	joinFn       func(j *command.JoinRequest) error
	leaderAddrFn func() (string, error)
	stepdownFn   func(wait bool, id string) error
	commitIndex  uint64
}

type staticNodeMetaProvider struct {
	meta *proto.NodeMeta
}

func (p *staticNodeMetaProvider) NodeMeta() *proto.NodeMeta {
	return p.meta
}

type recordingNodeRequestAuthorizer struct {
	nodeID                 string
	address                string
	err                    error
	requireUnwrap          bool
	calls                  atomic.Int32
	sawConnection          atomic.Bool
	sawUnwrappedConnection atomic.Bool
}

func (a *recordingNodeRequestAuthorizer) AuthorizeNodeRequest(conn net.Conn, nodeID, address string) error {
	a.calls.Add(1)
	a.sawConnection.Store(conn != nil)
	if unwrapper, ok := conn.(interface{ Unwrap() net.Conn }); ok && unwrapper.Unwrap() != nil {
		a.sawUnwrappedConnection.Store(true)
	} else if a.requireUnwrap {
		return errors.New("connection is not unwrappable")
	}
	if nodeID != a.nodeID || address != a.address {
		return errors.New("node request projection mismatch")
	}
	return a.err
}

func (m *MockManager) Remove(ctx context.Context, rn *command.RemoveNodeRequest) error {
	if m.removeNodeFn == nil {
		return nil
	}
	return m.removeNodeFn(rn)
}

func (m *MockManager) Notify(n *command.NotifyRequest) error {
	if m.notifyFn == nil {
		return nil
	}
	return m.notifyFn(n)
}

func (m *MockManager) Join(j *command.JoinRequest) error {
	if m.joinFn == nil {
		return nil
	}
	return m.joinFn(j)
}

func (m *MockManager) Stepdown(wait bool, id string) error {
	if m.stepdownFn == nil {
		return nil
	}
	return m.stepdownFn(wait, id)
}

func (m *MockManager) LeaderAddr() (string, error) {
	if m.leaderAddrFn == nil {
		return "", nil
	}
	return m.leaderAddrFn()
}

func (m *MockManager) CommitIndex() (uint64, error) {
	return m.commitIndex, nil
}

func mustNewMockManager() *MockManager {
	return &MockManager{}
}

func mustCreateTLSConfig() *tls.Config {
	var err error

	certFile := x509.CertExampleDotComFile("")
	defer fsutil.Remove(certFile)
	keyFile := x509.KeyExampleDotComFile("")
	defer fsutil.Remove(keyFile)

	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic("failed to create TLS config")
	}

	return config
}

type mockCredentialStore struct {
	HasPermOK bool
	aaFunc    func(username, password, perm string) bool
}

func (m *mockCredentialStore) AA(username, password, perm string) bool {
	if m == nil {
		return true
	}

	if m.aaFunc != nil {
		return m.aaFunc(username, password, perm)
	}
	return m.HasPermOK
}

func mustNewMockCredentialStore() *mockCredentialStore {
	return &mockCredentialStore{HasPermOK: true}
}

func makeCredentials(username, password string) *proto.Credentials {
	return &proto.Credentials{
		Username: username,
		Password: password,
	}
}
