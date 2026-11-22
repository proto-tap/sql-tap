package ci

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrNoStatements is returned by Diff when either report captured no statements,
// which usually means the application did not connect through the proxy.
var ErrNoStatements = errors.New("no statements captured")

// DiffOptions controls what Diff treats as a regression.
type DiffOptions struct {
	// AllowIncreasePercent is the statement count increase tolerated before it counts as a regression.
	AllowIncreasePercent float64
}

// RegressionKind categorizes a regression found by Diff.
type RegressionKind string

const (
	RegressionStatements RegressionKind = "STATEMENTS"
	RegressionNPlus1     RegressionKind = "N+1"
	RegressionSlow       RegressionKind = "SLOW"
)

// Regression describes a metric that got worse. Query is empty for totals.
type Regression struct {
	Kind   RegressionKind
	Query  string
	Before int
	After  int
}

// Delta holds a count before and after a change.
type Delta struct {
	Before int
	After  int
}

// TemplateDiff pairs the stats of one template in both reports.
// A side where the template did not appear is the zero value.
type TemplateDiff struct {
	Query  string
	Before TemplateStat
	After  TemplateStat
}

// Comparison is the outcome of Diff.
type Comparison struct {
	Statements     Delta
	NPlus1         Delta
	Slow           Delta
	DurationBefore time.Duration
	DurationAfter  time.Duration
	// Templates lists only templates whose counts changed,
	// sorted by the absolute change in Count descending.
	Templates   []TemplateDiff
	Regressions []Regression
}

// HasRegressions reports whether any metric got worse.
func (c Comparison) HasRegressions() bool {
	return len(c.Regressions) > 0
}

// Diff compares two results and reports regressions from before to after.
func Diff(before, after Result, opts DiffOptions) (Comparison, error) {
	if before.Statements() == 0 {
		return Comparison{}, fmt.Errorf("before: %w", ErrNoStatements)
	}
	if after.Statements() == 0 {
		return Comparison{}, fmt.Errorf("after: %w", ErrNoStatements)
	}

	c := Comparison{
		Statements:     Delta{Before: before.Statements(), After: after.Statements()},
		DurationBefore: before.TotalDuration,
		DurationAfter:  after.TotalDuration,
		Templates:      diffTemplates(before.Templates, after.Templates),
	}
	for _, t := range before.Templates {
		c.NPlus1.Before += t.NPlus1Count
		c.Slow.Before += t.SlowCount
	}
	for _, t := range after.Templates {
		c.NPlus1.After += t.NPlus1Count
		c.Slow.After += t.SlowCount
	}

	limit := float64(c.Statements.Before) * (1 + opts.AllowIncreasePercent/100)
	if float64(c.Statements.After) > limit {
		c.Regressions = append(c.Regressions, Regression{
			Kind: RegressionStatements, Before: c.Statements.Before, After: c.Statements.After,
		})
	}
	c.Regressions = append(c.Regressions, flagRegressions(RegressionNPlus1, c.NPlus1, c.Templates,
		func(t TemplateStat) int { return t.NPlus1Count })...)
	c.Regressions = append(c.Regressions, flagRegressions(RegressionSlow, c.Slow, c.Templates,
		func(t TemplateStat) int { return t.SlowCount })...)

	return c, nil
}

// flagRegressions reports an increased total, plus every template flagged for the first time.
func flagRegressions(
	kind RegressionKind, total Delta, templates []TemplateDiff, count func(TemplateStat) int,
) []Regression {
	var rs []Regression
	if total.After > total.Before {
		rs = append(rs, Regression{Kind: kind, Before: total.Before, After: total.After})
	}
	for _, t := range templates {
		if count(t.Before) == 0 && count(t.After) > 0 {
			rs = append(rs, Regression{Kind: kind, Query: t.Query, After: count(t.After)})
		}
	}
	return rs
}

func diffTemplates(before, after []TemplateStat) []TemplateDiff {
	byQuery := make(map[string]TemplateDiff)
	for _, t := range before {
		byQuery[t.Query] = TemplateDiff{Query: t.Query, Before: t}
	}
	for _, t := range after {
		d := byQuery[t.Query]
		d.Query = t.Query
		d.After = t
		byQuery[t.Query] = d
	}

	var diffs []TemplateDiff
	for _, d := range byQuery {
		if d.Before.Count != d.After.Count ||
			d.Before.NPlus1Count != d.After.NPlus1Count ||
			d.Before.SlowCount != d.After.SlowCount {
			diffs = append(diffs, d)
		}
	}
	slices.SortFunc(diffs, func(x, y TemplateDiff) int {
		return cmp.Or(
			cmp.Compare(absInt(y.After.Count-y.Before.Count), absInt(x.After.Count-x.Before.Count)),
			cmp.Compare(x.Query, y.Query),
		)
	})
	return diffs
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
