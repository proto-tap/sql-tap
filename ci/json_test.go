package ci_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/proto-tap/sql-tap/ci"
)

func TestResult_MarshalJSON(t *testing.T) {
	t.Parallel()

	r := ci.Result{
		TotalQueries:  3,
		TotalDuration: 1500 * time.Microsecond,
		Templates: []ci.TemplateStat{
			{Query: "SELECT name FROM users WHERE id = ?", Count: 2, TotalDuration: time.Millisecond, NPlus1Count: 2},
			{Query: "SELECT id FROM posts", Count: 1, TotalDuration: 500 * time.Microsecond},
		},
		Problems: []ci.Problem{
			{Kind: ci.ProblemNPlus1, Query: "SELECT name FROM users WHERE id = ?", Count: 2},
			{Kind: ci.ProblemSlowQuery, Query: "SELECT id FROM posts", Count: 1, AvgDuration: 500 * time.Microsecond},
		},
	}

	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,"total_queries":3,"statements":3,"total_ms":1.5,` +
		`"templates":[` +
		`{"query":"SELECT name FROM users WHERE id = ?","count":2,"total_ms":1,"n_plus_1_count":2,"slow_count":0},` +
		`{"query":"SELECT id FROM posts","count":1,"total_ms":0.5,"n_plus_1_count":0,"slow_count":0}],` +
		`"problems":[` +
		`{"kind":"N+1","query":"SELECT name FROM users WHERE id = ?","count":2},` +
		`{"kind":"SLOW","query":"SELECT id FROM posts","count":1,"avg_ms":0.5}]}`
	if string(got) != want {
		t.Errorf("MarshalJSON =\n%s\nwant\n%s", got, want)
	}
}

func TestResult_MarshalJSON_EmptyArrays(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(ci.Result{})
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,"total_queries":0,"statements":0,"total_ms":0,"templates":[],"problems":[]}`
	if string(got) != want {
		t.Errorf("MarshalJSON = %s, want %s", got, want)
	}
}
