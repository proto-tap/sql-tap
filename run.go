package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/proto-tap/sql-tap/ci"
)

const (
	runExitOK       = 0
	runExitProblems = 1
	runExitError    = 2
)

func runRun(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sql-tap run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "sql-tap run — Measure SQL traffic while a command runs\n\n"+
			"Usage:\n  sql-tap run [flags] <addr> -- <command> [args...]\n\n"+
			"The report is written to stderr; the command keeps stdin/stdout/stderr.\n"+
			"Exit codes: the command's exit code if it fails; otherwise 0 clean, 1 problems found,\n"+
			"2 error (no statements captured, subscription not confirmed, incomplete measurement)\n\nFlags:\n")
		fs.PrintDefaults()
	}
	format := fs.String("format", "text", "report format on stderr: text or json")
	out := fs.String("out", "", "also write the report as JSON to this file")
	subscribeTimeout := fs.Duration("subscribe-timeout", 5*time.Second,
		"how long to wait for sql-tapd to confirm the subscription")
	drainIdle := fs.Duration("drain", 200*time.Millisecond,
		"after the command exits, stop once no event has arrived for this long")
	drainMax := fs.Duration("drain-max", 5*time.Second, "upper bound on draining after the command exits")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return runExitOK
		}
		return runExitError
	}
	rest := fs.Args()
	if len(rest) < 3 || rest[1] != "--" {
		fs.Usage()
		return runExitError
	}
	if *format != "text" && *format != "json" {
		_, _ = fmt.Fprintf(stderr, "Error: unknown -format %q (want text or json)\n", *format)
		return runExitError
	}
	addr, command := rest[0], rest[2:]

	ctx := context.Background()
	w, err := ci.Subscribe(ctx, addr, *subscribeTimeout)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return runExitError
	}

	cmd := newCommand(ctx, command, stdin, stdout, stderr)
	if err := cmd.Start(); err != nil {
		w.Close()
		_, _ = fmt.Fprintf(stderr, "Error: start %s: %v\n", command[0], err)
		return runExitError
	}
	stopForwarding := forwardSignals(cmd.Process.Pid)

	stop := make(chan struct{})
	type collected struct {
		result ci.Result
		err    error
	}
	done := make(chan collected, 1)
	go func() {
		r, err := w.Collect(stop, ci.Drain{Idle: *drainIdle, Max: *drainMax})
		done <- collected{result: r, err: err}
	}()

	waitErr := cmd.Wait()
	stopForwarding()
	close(stop)
	c := <-done

	return finishRun(stderr, command, waitErr, c.result, c.err, *format, *out)
}

func finishRun(
	stderr io.Writer, command []string, waitErr error, result ci.Result, collectErr error, format, out string,
) int {
	// A failed command wins over every other outcome, so test failures are never hidden.
	failure := commandFailure(waitErr)
	if waitErr != nil && failure == runExitError {
		_, _ = fmt.Fprintf(stderr, "Error: %s: %v\n", command[0], waitErr)
	}
	exit := func(code int) int {
		if failure != 0 {
			return failure
		}
		return code
	}

	incomplete := errors.Is(collectErr, ci.ErrDrainLimit) || errors.Is(collectErr, ci.ErrStreamEnded)
	if collectErr != nil && !incomplete {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", collectErr)
		return exit(runExitError)
	}
	result.Command = command
	if incomplete {
		result.Incomplete = collectErr.Error()
	}

	if err := writeReport(stderr, result, format); err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return exit(runExitError)
	}
	if out != "" {
		if err := writeReportFile(out, result); err != nil {
			_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
			return exit(runExitError)
		}
	}

	if failure != 0 {
		return failure
	}
	if result.Statements() == 0 {
		_, _ = fmt.Fprintln(stderr, "Error: no statements captured. "+proxyHint)
		return runExitError
	}
	if incomplete {
		_, _ = fmt.Fprintf(stderr, "Error: %v; the report may be incomplete.\n", collectErr)
		if errors.Is(collectErr, ci.ErrDrainLimit) {
			_, _ = fmt.Fprintln(stderr, "Check whether another application is using the same sql-tapd.")
		}
		return runExitError
	}
	if result.HasProblems() {
		return runExitProblems
	}
	return runExitOK
}

// commandFailure returns the exit status to propagate for a failed command, or 0 if it succeeded.
// A command killed by a signal yields 128 + the signal number, as shells do.
func commandFailure(waitErr error) int {
	if waitErr == nil {
		return 0
	}
	exitErr, ok := errors.AsType[*exec.ExitError](waitErr)
	if !ok {
		return runExitError
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	if code := exitErr.ExitCode(); code > 0 {
		return code
	}
	return runExitError
}

// newCommand runs the command in its own process group, so that signals can reach every process it
// spawns (e.g. test binaries under `go test`). The group is not the terminal's foreground group,
// so a command reading from a terminal stdin is stopped by SIGTTIN.
func newCommand(ctx context.Context, command []string, stdin io.Reader, stdout, stderr io.Writer) *exec.Cmd {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...) //nolint:gosec // running the user's command is the purpose
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// signalGroup sends sig to every process in the process group led by pgid.
func signalGroup(pgid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pgid, sig); err != nil {
		return fmt.Errorf("signal process group %d: %w", pgid, err)
	}
	return nil
}

// forwardSignals relays SIGINT and SIGTERM to the process group pgid until the returned function is called.
func forwardSignals(pgid int) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case sig := <-ch:
				if s, ok := sig.(syscall.Signal); ok {
					_ = signalGroup(pgid, s)
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}
