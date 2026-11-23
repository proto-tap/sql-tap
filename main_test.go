package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/proto-tap/sql-tap/ci"
)

func TestWriteReport(t *testing.T) {
	t.Parallel()

	r := ci.Result{TotalQueries: 2}

	tests := []struct {
		format string
		want   string
	}{
		{format: "text", want: "Captured: 2 queries"},
		{format: "json", want: `"total_queries": 2`},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := writeReport(&buf, r, tt.format); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(buf.String(), tt.want) {
				t.Errorf("output = %q, want to contain %q", buf.String(), tt.want)
			}
		})
	}
}

func TestWriteReport_JSONKeepsSQLOperators(t *testing.T) {
	t.Parallel()

	const q = "SELECT id FROM orders WHERE total > ? AND note <> '&'"
	var buf bytes.Buffer
	r := ci.Result{Templates: []ci.TemplateStat{{Query: q, Count: 1}}}
	if err := writeReport(&buf, r, "json"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), q) {
		t.Errorf("output = %s, want the query unescaped", buf.String())
	}
}

func TestWriteReportFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeReportFile(path, ci.Result{TotalQueries: 3}); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(path) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Version      int `json:"version"`
		TotalQueries int `json:"total_queries"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	if got.Version != ci.SchemaVersion || got.TotalQueries != 3 {
		t.Errorf("got %+v, want version %d and total_queries 3", got, ci.SchemaVersion)
	}
}

func TestPrintCIWarnings(t *testing.T) {
	t.Parallel()

	stmt := []ci.TemplateStat{{Query: "SELECT id FROM orders", Count: 1}}
	tests := []struct {
		name string
		r    ci.Result
		want []string
	}{
		{name: "clean", r: ci.Result{Templates: stmt}},
		{name: "no statements", r: ci.Result{}, want: []string{"no statements captured"}},
		{name: "dropped", r: ci.Result{Templates: stmt, DroppedEvents: 3}, want: []string{"dropped 3 events"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			printCIWarnings(&buf, tt.r)
			if len(tt.want) == 0 && buf.Len() != 0 {
				t.Errorf("output = %q, want none", buf.String())
			}
			for _, w := range tt.want {
				if !strings.Contains(buf.String(), w) {
					t.Errorf("output = %q, want to contain %q", buf.String(), w)
				}
			}
		})
	}
}
