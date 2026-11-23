package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/proto-tap/sql-tap/ci"
)

func writeTestReport(t *testing.T, name string, templates ...ci.TemplateStat) string {
	t.Helper()
	return writeTestReportWithCommand(t, name, nil, templates...)
}

func writeTestReportWithCommand(t *testing.T, name string, command []string, templates ...ci.TemplateStat) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := writeReportFile(path, ci.Result{Command: command, Templates: templates}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunDiff(t *testing.T) {
	t.Parallel()

	const q = "SELECT id FROM orders"
	base := writeTestReport(t, "base.json", ci.TemplateStat{Query: q, Count: 10})
	same := writeTestReport(t, "same.json", ci.TemplateStat{Query: q, Count: 10})
	more := writeTestReport(t, "more.json", ci.TemplateStat{Query: q, Count: 11})
	empty := writeTestReport(t, "empty.json")
	partial := filepath.Join(t.TempDir(), "partial.json")
	if err := writeReportFile(partial, ci.Result{
		Incomplete: "events kept arriving until the drain limit",
		Templates:  []ci.TemplateStat{{Query: q, Count: 2}},
	}); err != nil {
		t.Fatal(err)
	}
	slow := writeTestReport(t, "slow.json", ci.TemplateStat{Query: q, Count: 10, SlowCount: 1})
	cmdAll := writeTestReportWithCommand(t, "all.json", []string{"go", "test", "./..."},
		ci.TemplateStat{Query: q, Count: 10})
	cmdOne := writeTestReportWithCommand(t, "one.json", []string{"go", "test", "./order/..."},
		ci.TemplateStat{Query: q, Count: 10})

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "no regressions", args: []string{base, same}, wantCode: 0, wantStdout: "No regressions."},
		{name: "regression", args: []string{base, more}, wantCode: 1, wantStdout: "[STATEMENTS] 10 -> 11"},
		{name: "allowed increase", args: []string{"-allow-increase=10", base, more}, wantCode: 0},
		{name: "markdown", args: []string{"-format=markdown", base, same}, wantCode: 0, wantStdout: "### sql-tap diff"},
		{name: "json", args: []string{"-format=json", base, more}, wantCode: 1, wantStdout: `"kind": "STATEMENTS"`},
		{name: "json keeps arrows", args: []string{"-format=json", base, slow}, wantCode: 0,
			wantStdout: `"[SLOW] 0 -> 1"`},
		{name: "no statements", args: []string{base, empty}, wantCode: 2, wantStderr: "proxy port"},
		{name: "missing file", args: []string{base, "nonexistent.json"}, wantCode: 2, wantStderr: "open nonexistent.json"},
		{name: "incomplete report", args: []string{base, partial}, wantCode: 2, wantStderr: "measure again"},
		{name: "unknown format", args: []string{"-format=html", base, same}, wantCode: 2, wantStderr: "unknown -format"},
		{name: "wrong arg count", args: []string{base}, wantCode: 2, wantStderr: "Usage:"},
		{name: "negative allowance", args: []string{"-allow-increase=-5", base, same}, wantCode: 2,
			wantStderr: "non-negative"},
		{name: "NaN allowance", args: []string{"-allow-increase=NaN", base, same}, wantCode: 2,
			wantStderr: "non-negative"},
		{name: "help", args: []string{"-h"}, wantCode: 0, wantStderr: "Exit codes:"},
		{name: "slow warns by default", args: []string{base, slow}, wantCode: 0, wantStdout: "Warnings:"},
		{name: "fail on slow", args: []string{"-fail-on-slow", base, slow}, wantCode: 1, wantStdout: "[SLOW] 0 -> 1"},
		{name: "command mismatch", args: []string{cmdAll, cmdOne}, wantCode: 2, wantStderr: "-allow-command-mismatch"},
		{name: "command mismatch allowed", args: []string{"-allow-command-mismatch", cmdAll, cmdOne}, wantCode: 0,
			wantStdout: "commands differ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := runDiff(tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			if !strings.Contains(stdout.String(), tt.wantStdout) {
				t.Errorf("stdout = %q, want to contain %q", stdout.String(), tt.wantStdout)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}
