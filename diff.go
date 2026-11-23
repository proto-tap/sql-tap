package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/proto-tap/sql-tap/ci"
)

const (
	diffExitOK         = 0
	diffExitRegression = 1
	diffExitError      = 2
)

func runDiff(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sql-tap diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "sql-tap diff — Compare two CI reports written by -out\n\n"+
			"Usage:\n  sql-tap diff [flags] <before.json> <after.json>\n\n"+
			"Exit codes: 0 no regressions, 1 regressions found,\n"+
			"2 error (no statements captured, incomplete report, reports measured with different commands)\n\nFlags:\n")
		fs.PrintDefaults()
	}
	format := fs.String("format", "text", "output format: text, markdown or json")
	allowIncrease := fs.Float64("allow-increase", 0, "tolerated statement count increase in percent")
	failOnSlow := fs.Bool("fail-on-slow", false,
		"treat slow query increases as regressions (by default they only warn, since timing varies between runs)")
	allowCommandMismatch := fs.Bool("allow-command-mismatch", false,
		"compare reports measured with different commands, with a warning")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return diffExitOK
		}
		return diffExitError
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return diffExitError
	}
	if *format != "text" && *format != "markdown" && *format != "json" {
		_, _ = fmt.Fprintf(stderr, "Error: unknown -format %q (want text, markdown or json)\n", *format)
		return diffExitError
	}

	before, err := readReport(fs.Arg(0))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return diffExitError
	}
	after, err := readReport(fs.Arg(1))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return diffExitError
	}

	c, err := ci.Diff(before, after, ci.DiffOptions{
		AllowIncreasePercent: *allowIncrease,
		FailOnSlow:           *failOnSlow,
		AllowCommandMismatch: *allowCommandMismatch,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		switch {
		case errors.Is(err, ci.ErrNoStatements):
			_, _ = fmt.Fprintln(stderr, proxyHint)
		case errors.Is(err, ci.ErrCommandMismatch):
			_, _ = fmt.Fprintln(stderr, "Measure both reports with the same command, or pass -allow-command-mismatch.")
		case errors.Is(err, ci.ErrIncompleteReport):
			_, _ = fmt.Fprintln(stderr, "The measurement may have missed events; measure again.")
		}
		return diffExitError
	}

	if err := writeComparison(stdout, c, *format); err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return diffExitError
	}
	if c.HasRegressions() {
		return diffExitRegression
	}
	return diffExitOK
}

func readReport(path string) (ci.Result, error) {
	f, err := os.Open(path) //nolint:gosec // path is given by the user as an argument
	if err != nil {
		return ci.Result{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	r, err := ci.DecodeReport(f)
	if err != nil {
		return ci.Result{}, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func writeComparison(w io.Writer, c ci.Comparison, format string) error {
	var err error
	switch format {
	case "markdown":
		_, err = io.WriteString(w, c.Markdown())
	case "json":
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		err = enc.Encode(c)
	default:
		_, err = io.WriteString(w, c.Report())
	}
	if err != nil {
		return fmt.Errorf("write comparison: %w", err)
	}
	return nil
}
