package ci_test

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/proto-tap/sql-tap/ci"
)

const (
	qOrders = "SELECT id FROM orders"
	qItems  = "SELECT id FROM items WHERE order_id = ?"
	qBatch  = "SELECT id FROM items WHERE order_id IN (?)"
)

func result(templates ...ci.TemplateStat) ci.Result {
	var r ci.Result
	for _, t := range templates {
		r.TotalDuration += t.TotalDuration
	}
	r.Templates = templates
	return r
}

func TestDiff_FixedNPlus1(t *testing.T) {
	t.Parallel()

	before := result(
		ci.TemplateStat{Query: qOrders, Count: 1, TotalDuration: time.Millisecond},
		ci.TemplateStat{Query: qItems, Count: 40, TotalDuration: 40 * time.Millisecond, NPlus1Count: 36},
	)
	after := result(
		ci.TemplateStat{Query: qOrders, Count: 1, TotalDuration: time.Millisecond},
		ci.TemplateStat{Query: qBatch, Count: 1, TotalDuration: 2 * time.Millisecond},
	)

	c, err := ci.Diff(before, after, ci.DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if c.HasRegressions() {
		t.Errorf("Regressions = %+v, want none", c.Regressions)
	}
	if c.Statements != (ci.Delta{Before: 41, After: 2}) {
		t.Errorf("Statements = %+v, want 41 -> 2", c.Statements)
	}
	if c.NPlus1 != (ci.Delta{Before: 36, After: 0}) {
		t.Errorf("NPlus1 = %+v, want 36 -> 0", c.NPlus1)
	}
	if c.DurationBefore != 41*time.Millisecond || c.DurationAfter != 3*time.Millisecond {
		t.Errorf("Duration = %s -> %s, want 41ms -> 3ms", c.DurationBefore, c.DurationAfter)
	}

	gotQueries := make([]string, 0, len(c.Templates))
	for _, d := range c.Templates {
		gotQueries = append(gotQueries, d.Query)
	}
	// qOrders is unchanged and omitted; qItems changed by 40, qBatch by 1.
	if want := []string{qItems, qBatch}; !slices.Equal(gotQueries, want) {
		t.Errorf("Templates = %v, want %v", gotQueries, want)
	}
}

func TestDiff_Regressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		before ci.Result
		after  ci.Result
		opts   ci.DiffOptions
		want   []ci.Regression
	}{
		{
			name:   "unchanged",
			before: result(ci.TemplateStat{Query: qOrders, Count: 10}),
			after:  result(ci.TemplateStat{Query: qOrders, Count: 10}),
		},
		{
			name:   "statements increased",
			before: result(ci.TemplateStat{Query: qOrders, Count: 10}),
			after:  result(ci.TemplateStat{Query: qOrders, Count: 11}),
			want:   []ci.Regression{{Kind: ci.RegressionStatements, Before: 10, After: 11}},
		},
		{
			name:   "statements increase within allowance",
			before: result(ci.TemplateStat{Query: qOrders, Count: 10}),
			after:  result(ci.TemplateStat{Query: qOrders, Count: 11}),
			opts:   ci.DiffOptions{AllowIncreasePercent: 10},
		},
		{
			name:   "statements increase exactly at allowance",
			before: result(ci.TemplateStat{Query: qOrders, Count: 100}),
			after:  result(ci.TemplateStat{Query: qOrders, Count: 113}),
			opts:   ci.DiffOptions{AllowIncreasePercent: 13},
		},
		{
			name:   "statements increase beyond allowance",
			before: result(ci.TemplateStat{Query: qOrders, Count: 10}),
			after:  result(ci.TemplateStat{Query: qOrders, Count: 12}),
			opts:   ci.DiffOptions{AllowIncreasePercent: 10},
			want:   []ci.Regression{{Kind: ci.RegressionStatements, Before: 10, After: 12}},
		},
		{
			name:   "new N+1 template",
			before: result(ci.TemplateStat{Query: qItems, Count: 10}),
			after:  result(ci.TemplateStat{Query: qItems, Count: 10, NPlus1Count: 6}),
			want: []ci.Regression{
				{Kind: ci.RegressionNPlus1, Before: 0, After: 6},
				{Kind: ci.RegressionNPlus1, Query: qItems, After: 6},
			},
		},
		{
			name: "slow moves to another template without total increase, with FailOnSlow",
			before: result(
				ci.TemplateStat{Query: qOrders, Count: 1, SlowCount: 1},
				ci.TemplateStat{Query: qBatch, Count: 1},
			),
			after: result(
				ci.TemplateStat{Query: qOrders, Count: 1},
				ci.TemplateStat{Query: qBatch, Count: 1, SlowCount: 1},
			),
			opts: ci.DiffOptions{FailOnSlow: true},
			want: []ci.Regression{{Kind: ci.RegressionSlow, Query: qBatch, After: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := ci.Diff(tt.before, tt.after, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(c.Regressions, tt.want) {
				t.Errorf("Regressions = %+v, want %+v", c.Regressions, tt.want)
			}
		})
	}
}

func TestDiff_NoStatements(t *testing.T) {
	t.Parallel()

	some := result(ci.TemplateStat{Query: qOrders, Count: 1})
	tests := []struct {
		name          string
		before, after ci.Result
	}{
		{name: "before empty", before: ci.Result{TotalQueries: 3}, after: some},
		{name: "after empty", before: some, after: ci.Result{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ci.Diff(tt.before, tt.after, ci.DiffOptions{})
			if !errors.Is(err, ci.ErrNoStatements) {
				t.Errorf("err = %v, want ErrNoStatements", err)
			}
		})
	}
}

func TestDiff_SlowWarnsByDefault(t *testing.T) {
	t.Parallel()

	before := result(ci.TemplateStat{Query: qOrders, Count: 1})
	after := result(ci.TemplateStat{Query: qOrders, Count: 1, SlowCount: 1})

	c, err := ci.Diff(before, after, ci.DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c.HasRegressions() {
		t.Errorf("Regressions = %+v, want none", c.Regressions)
	}
	if !slices.Contains(c.Warnings, "[SLOW] 0 -> 1") {
		t.Errorf("Warnings = %q, want to contain %q", c.Warnings, "[SLOW] 0 -> 1")
	}
}

func TestDiff_Commands(t *testing.T) {
	t.Parallel()

	withCommand := func(cmd ...string) ci.Result {
		r := result(ci.TemplateStat{Query: qOrders, Count: 1})
		r.Command = cmd
		return r
	}
	all := withCommand("go", "test", "./...")
	order := withCommand("go", "test", "./order/...")
	none := withCommand()

	tests := []struct {
		name          string
		before, after ci.Result
		opts          ci.DiffOptions
		wantErr       error
		wantWarning   string
	}{
		{name: "same", before: all, after: all},
		{name: "different", before: all, after: order, wantErr: ci.ErrCommandMismatch},
		{name: "different but allowed", before: all, after: order, opts: ci.DiffOptions{AllowCommandMismatch: true},
			wantWarning: `commands differ: before "go test ./...", after "go test ./order/..."`},
		{name: "before missing", before: none, after: all, wantWarning: "before report does not record its command"},
		{name: "after missing", before: all, after: none, wantWarning: "after report does not record its command"},
		{name: "both missing", before: none, after: none, wantWarning: "neither report records its command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := ci.Diff(tt.before, tt.after, tt.opts)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if tt.wantWarning == "" {
				if len(c.Warnings) != 0 {
					t.Errorf("Warnings = %q, want none", c.Warnings)
				}
				return
			}
			if !slices.ContainsFunc(c.Warnings, func(w string) bool { return strings.HasPrefix(w, tt.wantWarning) }) {
				t.Errorf("Warnings = %q, want one starting with %q", c.Warnings, tt.wantWarning)
			}
		})
	}
}

func TestDiff_InvalidAllowance(t *testing.T) {
	t.Parallel()

	r := result(ci.TemplateStat{Query: qOrders, Count: 10})
	for _, p := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := ci.Diff(r, r, ci.DiffOptions{AllowIncreasePercent: p}); !errors.Is(err, ci.ErrInvalidAllowance) {
			t.Errorf("AllowIncreasePercent=%v: err = %v, want ErrInvalidAllowance", p, err)
		}
	}
}

func TestDiff_IncompleteReport(t *testing.T) {
	t.Parallel()

	complete := result(ci.TemplateStat{Query: qOrders, Count: 10})
	incomplete := result(ci.TemplateStat{Query: qOrders, Count: 2})
	incomplete.Incomplete = "sql-tapd ended the stream before measurement finished"

	dropped := result(ci.TemplateStat{Query: qOrders, Count: 10})
	dropped.DroppedEvents = 1 // e.g. a -ci report, which is not marked Incomplete

	for _, tt := range []struct{ before, after ci.Result }{
		{before: incomplete, after: complete},
		{before: complete, after: incomplete},
		{before: dropped, after: complete},
		{before: complete, after: dropped},
	} {
		if _, err := ci.Diff(tt.before, tt.after, ci.DiffOptions{}); !errors.Is(err, ci.ErrIncompleteReport) {
			t.Errorf("err = %v, want ErrIncompleteReport", err)
		}
	}
}
