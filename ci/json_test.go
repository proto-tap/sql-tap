package ci_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
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

func TestDecodeReport_RoundTrip(t *testing.T) {
	t.Parallel()

	want := ci.Result{
		Command:       []string{"go", "test", "./..."},
		Incomplete:    "events kept arriving until the drain limit",
		DroppedEvents: 7,
		TotalQueries:  4,
		TotalDuration: 1500 * time.Microsecond,
		Templates: []ci.TemplateStat{
			{Query: "SELECT name FROM users WHERE id = ?", Count: 2, TotalDuration: time.Millisecond, NPlus1Count: 2},
			{Query: "SELECT id FROM posts", Count: 1, TotalDuration: 500 * time.Microsecond, SlowCount: 1},
		},
		Problems: []ci.Problem{
			{Kind: ci.ProblemNPlus1, Query: "SELECT name FROM users WHERE id = ?", Count: 2},
			{Kind: ci.ProblemSlowQuery, Query: "SELECT id FROM posts", Count: 1, AvgDuration: 500 * time.Microsecond},
		},
	}

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ci.DecodeReport(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DecodeReport =\n%+v\nwant\n%+v", got, want)
	}
}

func TestDecodeReport_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "invalid json", input: "{", want: "decode report"},
		{name: "missing version", input: `{"total_queries":1}`, want: "unsupported report version 0"},
		{name: "future version", input: `{"version":2}`, want: "unsupported report version 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ci.DecodeReport(strings.NewReader(tt.input))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}
