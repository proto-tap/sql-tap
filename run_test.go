package main

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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

func TestFinishRun_IncompleteMeasurement(t *testing.T) {
	t.Parallel()

	r := ci.Result{Templates: []ci.TemplateStat{{Query: "SELECT id FROM orders", Count: 1}}}
	recvErr := errors.New("recv: rpc error: code = Unavailable")
	for _, collectErr := range []error{ci.ErrStreamEnded, ci.ErrDrainLimit, recvErr} {
		var stderr bytes.Buffer
		out := filepath.Join(t.TempDir(), "report.json")
		code := finishRun(&stderr, []string{"true"}, nil, r, collectErr, "text", out)
		if code != 2 {
			t.Errorf("%v: exit code = %d, want 2", collectErr, code)
		}
		if !strings.Contains(stderr.String(), "may be incomplete") || !strings.Contains(stderr.String(), "Captured:") {
			t.Errorf("%v: stderr = %q, want the report and an incompleteness error", collectErr, stderr.String())
		}
		if got := readTestReport(t, out).Incomplete; got != collectErr.Error() {
			t.Errorf("%v: Incomplete = %q, want %q", collectErr, got, collectErr.Error())
		}
	}
}

func readTestReport(t *testing.T, path string) ci.Result {
	t.Helper()
	r, err := readReport(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func commandError(t *testing.T, script string) error {
	t.Helper()
	err := exec.CommandContext(t.Context(), "sh", "-c", script).Run() //nolint:gosec // fixed test scripts
	if err == nil {
		t.Fatalf("sh -c %q succeeded, want a failure", script)
	}
	return err //nolint:wrapcheck // finishRun inspects the *exec.ExitError as returned by Wait
}

func TestFinishRun_CommandFailureWins(t *testing.T) {
	t.Parallel()

	clean := ci.Result{Templates: []ci.TemplateStat{{Query: "SELECT id FROM orders", Count: 1}}}
	unwritable := filepath.Join(t.TempDir(), "missing", "report.json")

	tests := []struct {
		name       string
		script     string
		collectErr error
		out        string
		want       int
	}{
		{name: "exit code", script: "exit 3", want: 3},
		{name: "signal", script: "kill -TERM $$", want: 128 + int(syscall.SIGTERM)},
		{name: "collect error", script: "exit 3", collectErr: errors.New("recv: boom"), want: 3},
		{name: "unwritable out", script: "exit 3", out: unwritable, want: 3},
		{name: "incomplete", script: "exit 3", collectErr: ci.ErrStreamEnded, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			waitErr := commandError(t, tt.script)
			var stderr bytes.Buffer
			if got := finishRun(&stderr, []string{"sh"}, waitErr, clean, tt.collectErr, "text", tt.out); got != tt.want {
				t.Errorf("exit code = %d, want %d (stderr: %s)", got, tt.want, stderr.String())
			}
		})
	}
}

func TestSignalGroup_ReachesGrandchildren(t *testing.T) {
	t.Parallel()

	// sh prints the pid of a background sleep (a grandchild of sql-tap) and waits for it.
	cmd := newCommand(t.Context(), []string{"sh", "-c", "sleep 30 & echo $!; wait"}, nil, nil, nil)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })

	if err := signalGroup(cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(grandchild, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("grandchild %d is still running after SIGTERM to the group", grandchild)
}

func TestFinishRun_OverwritesStaleOutOnCollectError(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "report.json")
	stale := ci.Result{Command: []string{"stale"}, Templates: []ci.TemplateStat{{Query: "SELECT 1", Count: 9}}}
	if err := writeReportFile(out, stale); err != nil {
		t.Fatal(err)
	}

	partial := ci.Result{Templates: []ci.TemplateStat{{Query: "SELECT id FROM orders", Count: 1}}}
	var stderr bytes.Buffer
	if code := finishRun(&stderr, []string{"true"}, nil, partial, errors.New("recv: boom"), "text", out); code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	got := readTestReport(t, out)
	if got.Incomplete != "recv: boom" || got.Statements() != 1 {
		t.Errorf("report = %+v, want the partial result marked incomplete", got)
	}
}

func TestRunRun_EmptiesStaleOutOnEarlyFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args func(out string) []string
	}{
		{name: "unreachable sql-tapd", args: func(out string) []string {
			return []string{"-subscribe-timeout=200ms", "-out=" + out, "localhost:1", "--", "true"}
		}},
		{name: "usage error", args: func(out string) []string {
			return []string{"-out=" + out, "localhost:9091", "go", "test"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := filepath.Join(t.TempDir(), "report.json")
			if err := writeReportFile(out, ci.Result{Templates: []ci.TemplateStat{{Query: "SELECT 1", Count: 9}}}); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(out, 0o600); err != nil {
				t.Fatal(err)
			}

			var stdout, stderr bytes.Buffer
			if code := runRun(tt.args(out), strings.NewReader(""), &stdout, &stderr); code != 2 {
				t.Errorf("exit code = %d, want 2", code)
			}
			info, err := os.Stat(out)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 0 {
				t.Errorf("stale report still has %d bytes, want it emptied", info.Size())
			}
			if perm := info.Mode().Perm(); perm != 0o600 {
				t.Errorf("permissions = %o, want 600 kept", perm)
			}
			if _, err := readReport(out); err == nil {
				t.Error("an emptied report must not decode, so that diff rejects it")
			}
		})
	}
}

func TestRunRun_KeepsOutPermissions(t *testing.T) {
	t.Parallel()

	addr, b := startTapServer(t)
	publishOnSubscribe(t, b, query("1", false))

	out := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(out, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	args := append([]string{"-drain=50ms", "-out=" + out, addr}, sleepCmd...)
	if code := runRun(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permissions = %o, want 600 kept for a report that may contain SQL", perm)
	}
}
