package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/proto-tap/sql-tap/broker"
	"github.com/proto-tap/sql-tap/ci"
	"github.com/proto-tap/sql-tap/proxy"
	"github.com/proto-tap/sql-tap/server"
)

func startTapServer(t *testing.T) (string, *broker.Broker) {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	b := broker.New(64)
	srv := server.New(b, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String(), b
}

// publishOnSubscribe publishes events once run has subscribed, i.e. while the command is running.
func publishOnSubscribe(t *testing.T, b *broker.Broker, events ...proxy.Event) {
	t.Helper()
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for b.SubscriberCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		for _, ev := range events {
			b.Publish(ev)
		}
	}()
}

func query(id string, nplus1 bool) proxy.Event {
	return proxy.Event{
		ID: id, Op: proxy.OpQuery, Query: "SELECT id FROM orders",
		NormalizedQuery: "SELECT id FROM orders", NPlus1: nplus1,
	}
}

// sleepCmd keeps the command running long enough for publishOnSubscribe to deliver events.
var sleepCmd = []string{"--", "sh", "-c", "sleep 0.3"}

func TestRunRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		events     []proxy.Event
		cmd        []string
		wantCode   int
		wantStderr string
	}{
		{name: "clean", events: []proxy.Event{query("1", false)}, cmd: sleepCmd, wantCode: 0,
			wantStderr: "Command:  sh -c sleep 0.3"},
		{name: "problems", events: []proxy.Event{query("1", true)}, cmd: sleepCmd, wantCode: 1, wantStderr: "[N+1]"},
		{name: "no statements", cmd: sleepCmd, wantCode: 2, wantStderr: "proxy port"},
		{name: "command fails", events: []proxy.Event{query("1", false)},
			cmd: []string{"--", "sh", "-c", "sleep 0.3; exit 3"}, wantCode: 3, wantStderr: "Captured:"},
		{name: "command not found", cmd: []string{"--", "sql-tap-no-such-command"}, wantCode: 2, wantStderr: "start"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			addr, b := startTapServer(t)
			publishOnSubscribe(t, b, tt.events...)

			var stdout, stderr bytes.Buffer
			args := append([]string{"-drain=50ms", addr}, tt.cmd...)
			code := runRun(args, strings.NewReader(""), &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}

func TestRunRun_Out(t *testing.T) {
	t.Parallel()

	addr, b := startTapServer(t)
	publishOnSubscribe(t, b, query("1", false), query("2", false))

	path := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	args := append([]string{"-drain=50ms", "-out=" + path, addr}, sleepCmd...)
	if code := runRun(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}

	f, err := os.Open(path) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	r, err := ci.DecodeReport(f)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Command, " "); got != "sh -c sleep 0.3" {
		t.Errorf("Command = %q, want %q", got, "sh -c sleep 0.3")
	}
	if got := r.Statements(); got != 2 {
		t.Errorf("Statements() = %d, want 2", got)
	}
}

func TestRunRun_CommandOutputGoesToStdout(t *testing.T) {
	t.Parallel()

	addr, b := startTapServer(t)
	publishOnSubscribe(t, b, query("1", false))

	var stdout, stderr bytes.Buffer
	args := []string{"-drain=50ms", addr, "--", "sh", "-c", "echo hello; sleep 0.3"}
	if code := runRun(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if stdout.String() != "hello\n" {
		t.Errorf("stdout = %q, want only the command output", stdout.String())
	}
	if !strings.Contains(stderr.String(), "sql-tap CI Report") {
		t.Errorf("stderr = %q, want the report", stderr.String())
	}
}

func TestRunRun_UsageErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing separator", args: []string{"localhost:9091", "go", "test"}, wantStderr: "Usage:"},
		{name: "missing command", args: []string{"localhost:9091", "--"}, wantStderr: "Usage:"},
		{name: "unknown format", args: []string{"-format=xml", "localhost:9091", "--", "true"},
			wantStderr: "unknown -format"},
		{name: "unreachable sql-tapd", args: []string{"-subscribe-timeout=200ms", "localhost:1", "--", "true"},
			wantStderr: "localhost:1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if code := runRun(tt.args, strings.NewReader(""), &stdout, &stderr); code != 2 {
				t.Errorf("exit code = %d, want 2", code)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}
