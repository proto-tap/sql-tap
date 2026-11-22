package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/proto-tap/sql-tap/ci"
	"github.com/proto-tap/sql-tap/tui"
)

var version = "dev"

func main() {
	fs := flag.NewFlagSet("sql-tap", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "sql-tap — Watch SQL traffic in real-time\n\nUsage:\n  sql-tap [flags] <addr>\n\nFlags:\n")
		fs.PrintDefaults()
	}

	showVersion := fs.Bool("version", false, "show version and exit")
	ciMode := fs.Bool("ci", false,
		"run in CI mode: collect events until SIGTERM/SIGINT or stream ends, then report and exit")
	format := fs.String("format", "text", "CI report format on stdout: text or json")
	out := fs.String("out", "", "also write the CI report as JSON to this file")

	_ = fs.Parse(os.Args[1:])

	if *showVersion {
		fmt.Printf("sql-tap %s\n", version)
		return
	}

	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(1)
	}

	addr := fs.Arg(0)
	if *ciMode {
		runCI(addr, *format, *out)
	} else {
		monitor(addr)
	}
}

func monitor(addr string) {
	m := tui.New(addr)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runCI(addr, format, out string) {
	os.Exit(runCIExitCode(addr, format, out))
}

func runCIExitCode(addr, format, out string) int {
	if format != "text" && format != "json" {
		fmt.Fprintf(os.Stderr, "Error: unknown -format %q (want text or json)\n", format)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	result, err := ci.Run(ctx, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	if result.Statements() == 0 {
		fmt.Fprintln(os.Stderr, noStatementsWarning)
	}

	if err := writeReport(os.Stdout, result, format); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if out != "" {
		if err := writeReportFile(out, result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
	}

	if result.HasProblems() {
		return 1
	}
	return 0
}

const noStatementsWarning = "Warning: no statements captured. " +
	"Check that your application connects through the sql-tapd proxy port, not the database port."

func writeReport(w io.Writer, r ci.Result, format string) error {
	if format == "text" {
		if _, err := io.WriteString(w, r.Report()); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		return nil
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return nil
}

func writeReportFile(path string, r ci.Result) error {
	f, err := os.Create(path) //nolint:gosec // path is given by the user via -out
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if err := writeReport(f, r, "json"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}
