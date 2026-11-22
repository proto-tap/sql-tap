package ci_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/proto-tap/sql-tap/ci"
)

func fixedComparison() ci.Comparison {
	return ci.Comparison{
		Statements:     ci.Delta{Before: 41, After: 2},
		NPlus1:         ci.Delta{Before: 36, After: 0},
		DurationBefore: 41 * time.Millisecond,
		DurationAfter:  3 * time.Millisecond,
		Templates: []ci.TemplateDiff{
			{
				Query:  qItems,
				Before: ci.TemplateStat{Query: qItems, Count: 40, NPlus1Count: 36},
			},
			{
				Query: qBatch,
				After: ci.TemplateStat{Query: qBatch, Count: 1},
			},
		},
	}
}

func regressedComparison() ci.Comparison {
	return ci.Comparison{
		Warnings:   []string{"[SLOW] 0 -> 1"},
		Statements: ci.Delta{Before: 10, After: 11},
		NPlus1:     ci.Delta{Before: 0, After: 6},
		Regressions: []ci.Regression{
			{Kind: ci.RegressionStatements, Before: 10, After: 11},
			{Kind: ci.RegressionNPlus1, Query: "SELECT a | b FROM t", After: 6},
		},
	}
}

func TestComparison_Report(t *testing.T) {
	t.Parallel()

	report := fixedComparison().Report()
	assertContains(t, report, "Statements: 41 -> 2 (-39)")
	assertContains(t, report, "N+1:        36 -> 0 (-36)")
	assertContains(t, report, "Total time: 41.0ms -> 3.0ms")
	assertContains(t, report, "40 -> 0  "+qItems)
	assertContains(t, report, "No regressions.")

	report = regressedComparison().Report()
	assertContains(t, report, "[STATEMENTS] 10 -> 11")
	assertContains(t, report, "[N+1] SELECT a | b FROM t  (new, 6)")
	assertContains(t, report, "Warnings:\n  [SLOW] 0 -> 1")
	assertContains(t, report, "Exit: 1 (2 regressions)")
}

func TestComparison_Markdown(t *testing.T) {
	t.Parallel()

	want := "### sql-tap diff\n\n" +
		"| Metric | Before | After | Change |\n" +
		"| --- | ---: | ---: | ---: |\n" +
		"| Statements | 41 | 2 | -39 |\n" +
		"| N+1 | 36 | 0 | -36 |\n" +
		"| Slow | 0 | 0 | +0 |\n" +
		"| Total time (ms) | 41.0 | 3.0 | -38.0 |\n" +
		"\nNo regressions.\n" +
		"\n<details><summary>Changed templates (2)</summary>\n\n" +
		"| Query | Before | After |\n" +
		"| --- | ---: | ---: |\n" +
		"| `" + qItems + "` | 40 | 0 |\n" +
		"| `" + qBatch + "` | 0 | 1 |\n" +
		"\n</details>\n"
	if got := fixedComparison().Markdown(); got != want {
		t.Errorf("Markdown =\n%s\nwant\n%s", got, want)
	}

	md := regressedComparison().Markdown()
	assertContains(t, md, "- STATEMENTS: 10 → 11")
	assertContains(t, md, "- N+1 (new, 6): `SELECT a \\| b FROM t`")
	assertContains(t, md, "**Warnings:**\n\n- [SLOW] 0 -> 1")
}

func TestComparison_Markdown_BacktickQuery(t *testing.T) {
	t.Parallel()

	c := ci.Comparison{
		Statements: ci.Delta{Before: 1, After: 1},
		Templates:  []ci.TemplateDiff{{Query: "SELECT `id` FROM t", After: ci.TemplateStat{Count: 1}}},
	}
	assertContains(t, c.Markdown(), "| `` SELECT `id` FROM t `` |")
}

func TestComparison_MarshalJSON(t *testing.T) {
	t.Parallel()

	b, err := json.Marshal(regressedComparison())
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,` +
		`"statements":{"before":10,"after":11},"n_plus_1":{"before":0,"after":6},"slow":{"before":0,"after":0},` +
		`"total_ms":{"before":0,"after":0},"templates":[],` +
		`"regressions":[{"kind":"STATEMENTS","before":10,"after":11},` +
		`{"kind":"N+1","query":"SELECT a | b FROM t","before":0,"after":6}],` +
		`"warnings":["[SLOW] 0 -\u003e 1"]}`
	if string(b) != want {
		t.Errorf("MarshalJSON =\n%s\nwant\n%s", b, want)
	}
}
