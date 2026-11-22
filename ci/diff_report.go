package ci

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Report formats the comparison as human-readable text.
func (c Comparison) Report() string {
	var b strings.Builder
	b.WriteString("sql-tap diff\n")
	b.WriteString("============\n")
	fmt.Fprintf(&b, "Statements: %s\n", c.Statements)
	fmt.Fprintf(&b, "N+1:        %s\n", c.NPlus1)
	fmt.Fprintf(&b, "Slow:       %s\n", c.Slow)
	fmt.Fprintf(&b, "Total time: %.1fms -> %.1fms\n", millis(c.DurationBefore), millis(c.DurationAfter))

	if len(c.Templates) > 0 {
		b.WriteString("\nChanged templates:\n")
		for _, t := range c.Templates {
			fmt.Fprintf(&b, "  %d -> %d  %s\n", t.Before.Count, t.After.Count, t.Query)
		}
	}

	if !c.HasRegressions() {
		b.WriteString("\nNo regressions.\n")
		return b.String()
	}
	b.WriteString("\nRegressions:\n")
	for _, r := range c.Regressions {
		fmt.Fprintf(&b, "  %s\n", r)
	}
	fmt.Fprintf(&b, "\nExit: 1 (%d regressions)\n", len(c.Regressions))
	return b.String()
}

// Markdown formats the comparison for pull request descriptions.
func (c Comparison) Markdown() string {
	var b strings.Builder
	b.WriteString("### sql-tap diff\n\n")
	b.WriteString("| Metric | Before | After | Change |\n")
	b.WriteString("| --- | ---: | ---: | ---: |\n")
	fmt.Fprintf(&b, "| Statements | %d | %d | %+d |\n", c.Statements.Before, c.Statements.After, c.Statements.change())
	fmt.Fprintf(&b, "| N+1 | %d | %d | %+d |\n", c.NPlus1.Before, c.NPlus1.After, c.NPlus1.change())
	fmt.Fprintf(&b, "| Slow | %d | %d | %+d |\n", c.Slow.Before, c.Slow.After, c.Slow.change())
	before, after := millis(c.DurationBefore), millis(c.DurationAfter)
	fmt.Fprintf(&b, "| Total time (ms) | %.1f | %.1f | %+.1f |\n", before, after, after-before)

	if c.HasRegressions() {
		b.WriteString("\n**Regressions:**\n\n")
		for _, r := range c.Regressions {
			fmt.Fprintf(&b, "- %s\n", r.markdown())
		}
	} else {
		b.WriteString("\nNo regressions.\n")
	}

	if len(c.Templates) > 0 {
		fmt.Fprintf(&b, "\n<details><summary>Changed templates (%d)</summary>\n\n", len(c.Templates))
		b.WriteString("| Query | Before | After |\n")
		b.WriteString("| --- | ---: | ---: |\n")
		for _, t := range c.Templates {
			fmt.Fprintf(&b, "| %s | %d | %d |\n", mdCode(t.Query), t.Before.Count, t.After.Count)
		}
		b.WriteString("\n</details>\n")
	}
	return b.String()
}

func (d Delta) change() int {
	return d.After - d.Before
}

func (d Delta) String() string {
	return fmt.Sprintf("%d -> %d (%+d)", d.Before, d.After, d.change())
}

func (r Regression) String() string {
	if r.Query == "" {
		return fmt.Sprintf("[%s] %d -> %d", r.Kind, r.Before, r.After)
	}
	return fmt.Sprintf("[%s] %s  (new, %d)", r.Kind, r.Query, r.After)
}

func (r Regression) markdown() string {
	if r.Query == "" {
		return fmt.Sprintf("%s: %d → %d", r.Kind, r.Before, r.After)
	}
	return fmt.Sprintf("%s (new, %d): %s", r.Kind, r.After, mdCode(r.Query))
}

// mdCode wraps s in an inline code span that is safe inside a table cell.
func mdCode(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	if strings.Contains(s, "`") {
		return "`` " + s + " ``"
	}
	return "`" + s + "`"
}

type jsonComparison struct {
	Version     int                `json:"version"`
	Statements  jsonDelta          `json:"statements"`
	NPlus1      jsonDelta          `json:"n_plus_1"`
	Slow        jsonDelta          `json:"slow"`
	TotalMs     jsonDurationDelta  `json:"total_ms"`
	Templates   []jsonTemplateDiff `json:"templates"`
	Regressions []jsonRegression   `json:"regressions"`
}

type jsonDelta struct {
	Before int `json:"before"`
	After  int `json:"after"`
}

type jsonDurationDelta struct {
	Before float64 `json:"before"`
	After  float64 `json:"after"`
}

type jsonTemplateDiff struct {
	Query  string    `json:"query"`
	Count  jsonDelta `json:"count"`
	NPlus1 jsonDelta `json:"n_plus_1"`
	Slow   jsonDelta `json:"slow"`
}

type jsonRegression struct {
	Kind   RegressionKind `json:"kind"`
	Query  string         `json:"query,omitempty"`
	Before int            `json:"before"`
	After  int            `json:"after"`
}

// MarshalJSON encodes the comparison for machine consumers.
func (c Comparison) MarshalJSON() ([]byte, error) {
	out := jsonComparison{
		Version:     SchemaVersion,
		Statements:  jsonDelta(c.Statements),
		NPlus1:      jsonDelta(c.NPlus1),
		Slow:        jsonDelta(c.Slow),
		TotalMs:     jsonDurationDelta{Before: millis(c.DurationBefore), After: millis(c.DurationAfter)},
		Templates:   make([]jsonTemplateDiff, 0, len(c.Templates)),
		Regressions: make([]jsonRegression, 0, len(c.Regressions)),
	}
	for _, t := range c.Templates {
		out.Templates = append(out.Templates, jsonTemplateDiff{
			Query:  t.Query,
			Count:  jsonDelta{Before: t.Before.Count, After: t.After.Count},
			NPlus1: jsonDelta{Before: t.Before.NPlus1Count, After: t.After.NPlus1Count},
			Slow:   jsonDelta{Before: t.Before.SlowCount, After: t.After.SlowCount},
		})
	}
	for _, r := range c.Regressions {
		out.Regressions = append(out.Regressions, jsonRegression(r))
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal comparison: %w", err)
	}
	return b, nil
}
