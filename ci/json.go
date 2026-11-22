package ci

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"
)

// SchemaVersion is the version of the JSON report format.
// Bump it on breaking changes so that consumers such as `sql-tap diff` can reject unknown reports.
const SchemaVersion = 1

type jsonReport struct {
	Version      int            `json:"version"`
	TotalQueries int            `json:"total_queries"`
	Statements   int            `json:"statements"`
	TotalMs      float64        `json:"total_ms"`
	Templates    []jsonTemplate `json:"templates"`
	Problems     []jsonProblem  `json:"problems"`
}

type jsonTemplate struct {
	Query       string  `json:"query"`
	Count       int     `json:"count"`
	TotalMs     float64 `json:"total_ms"`
	NPlus1Count int     `json:"n_plus_1_count"`
	SlowCount   int     `json:"slow_count"`
}

type jsonProblem struct {
	Kind  ProblemKind `json:"kind"`
	Query string      `json:"query"`
	Count int         `json:"count"`
	AvgMs float64     `json:"avg_ms,omitempty"`
}

// MarshalJSON encodes the result in the versioned report format.
func (r Result) MarshalJSON() ([]byte, error) {
	rep := jsonReport{
		Version:      SchemaVersion,
		TotalQueries: r.TotalQueries,
		Statements:   r.Statements(),
		TotalMs:      millis(r.TotalDuration),
		Templates:    make([]jsonTemplate, 0, len(r.Templates)),
		Problems:     make([]jsonProblem, 0, len(r.Problems)),
	}
	for _, t := range r.Templates {
		rep.Templates = append(rep.Templates, jsonTemplate{
			Query:       t.Query,
			Count:       t.Count,
			TotalMs:     millis(t.TotalDuration),
			NPlus1Count: t.NPlus1Count,
			SlowCount:   t.SlowCount,
		})
	}
	for _, p := range r.Problems {
		rep.Problems = append(rep.Problems, jsonProblem{
			Kind:  p.Kind,
			Query: p.Query,
			Count: p.Count,
			AvgMs: millis(p.AvgDuration),
		})
	}
	b, err := json.Marshal(rep)
	if err != nil {
		return nil, fmt.Errorf("marshal report: %w", err)
	}
	return b, nil
}

// DecodeReport reads a JSON report written by Result.MarshalJSON.
// Reports with a different SchemaVersion are rejected.
func DecodeReport(r io.Reader) (Result, error) {
	var rep jsonReport
	if err := json.NewDecoder(r).Decode(&rep); err != nil {
		return Result{}, fmt.Errorf("decode report: %w", err)
	}
	if rep.Version != SchemaVersion {
		return Result{}, fmt.Errorf("unsupported report version %d (want %d)", rep.Version, SchemaVersion)
	}

	res := Result{
		TotalQueries:  rep.TotalQueries,
		TotalDuration: fromMillis(rep.TotalMs),
	}
	for _, t := range rep.Templates {
		res.Templates = append(res.Templates, TemplateStat{
			Query:         t.Query,
			Count:         t.Count,
			TotalDuration: fromMillis(t.TotalMs),
			NPlus1Count:   t.NPlus1Count,
			SlowCount:     t.SlowCount,
		})
	}
	for _, p := range rep.Problems {
		res.Problems = append(res.Problems, Problem{
			Kind:        p.Kind,
			Query:       p.Query,
			Count:       p.Count,
			AvgDuration: fromMillis(p.AvgMs),
		})
	}
	return res, nil
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func fromMillis(ms float64) time.Duration {
	return time.Duration(math.Round(ms*1000)) * time.Microsecond
}
