package ci_test

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/proto-tap/sql-tap/broker"
	"github.com/proto-tap/sql-tap/ci"
	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/proxy"
	"github.com/proto-tap/sql-tap/server"
)

func TestResult_HasProblems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		r    ci.Result
		want bool
	}{
		{
			name: "no problems",
			r:    ci.Result{TotalQueries: 10},
			want: false,
		},
		{
			name: "with problems",
			r: ci.Result{
				TotalQueries: 10,
				Problems: []ci.Problem{
					{Kind: ci.ProblemNPlus1, Query: "SELECT 1", Count: 5},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.r.HasProblems(); got != tt.want {
				t.Errorf("HasProblems() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResult_Report_NoProblems(t *testing.T) {
	t.Parallel()

	r := ci.Result{TotalQueries: 42}
	report := r.Report()

	assertContains(t, report, "Captured: 42 queries")
	assertContains(t, report, "No problems found.")
}

func TestResult_Report_Incomplete(t *testing.T) {
	t.Parallel()

	assertContains(t, ci.Result{Incomplete: "stream ended"}.Report(), "Incomplete: stream ended")
	if report := (ci.Result{}).Report(); strings.Contains(report, "Incomplete:") {
		t.Errorf("complete report should omit the Incomplete line, got:\n%s", report)
	}
}

func TestResult_Report_Dropped(t *testing.T) {
	t.Parallel()

	assertContains(t, ci.Result{DroppedEvents: 3}.Report(), "Dropped:  3 events")
	if report := (ci.Result{}).Report(); strings.Contains(report, "Dropped:") {
		t.Errorf("report without drops should omit the Dropped line, got:\n%s", report)
	}
}

func TestResult_Report_Command(t *testing.T) {
	t.Parallel()

	assertContains(t, ci.Result{Command: []string{"go", "test", "./..."}}.Report(), "Command:  go test ./...")
	if report := (ci.Result{}).Report(); strings.Contains(report, "Command:") {
		t.Errorf("report without command should omit the Command line, got:\n%s", report)
	}
}

func TestResult_Report_WithProblems(t *testing.T) {
	t.Parallel()

	r := ci.Result{
		TotalQueries: 100,
		Problems: []ci.Problem{
			{Kind: ci.ProblemNPlus1, Query: "SELECT * FROM comments WHERE post_id = ?", Count: 12},
			{Kind: ci.ProblemSlowQuery, Query: "SELECT * FROM users JOIN ...", Count: 3, AvgDuration: 523 * time.Millisecond},
		},
	}
	report := r.Report()

	assertContains(t, report, "Captured: 100 queries")
	assertContains(t, report, "[N+1]")
	assertContains(t, report, "detected 12 times")
	assertContains(t, report, "[SLOW]")
	assertContains(t, report, "avg 523ms")
	assertContains(t, report, "2 problems found")
}

func TestAggregate(t *testing.T) {
	t.Parallel()

	events := []*tapv1.QueryEvent{
		{Query: "SELECT 1", NormalizedQuery: "SELECT ?"},
		{Query: "SELECT * FROM users WHERE id = 1", NormalizedQuery: "SELECT * FROM users WHERE id = ?", NPlus_1: true},
		{Query: "SELECT * FROM users WHERE id = 2", NormalizedQuery: "SELECT * FROM users WHERE id = ?", NPlus_1: true},
		{Query: "SELECT * FROM users WHERE id = 3", NormalizedQuery: "SELECT * FROM users WHERE id = ?", NPlus_1: true},
		{
			Query:           "SELECT * FROM posts JOIN comments ON ...",
			NormalizedQuery: "SELECT * FROM posts JOIN comments ON ...",
			SlowQuery:       true,
			Duration:        durationpb.New(200 * time.Millisecond),
		},
		{
			Query:           "SELECT * FROM posts JOIN comments ON ...",
			NormalizedQuery: "SELECT * FROM posts JOIN comments ON ...",
			SlowQuery:       true,
			Duration:        durationpb.New(400 * time.Millisecond),
		},
	}

	result := ci.Aggregate(events)

	if result.TotalQueries != 6 {
		t.Errorf("TotalQueries = %d, want 6", result.TotalQueries)
	}
	if len(result.Problems) != 2 {
		t.Fatalf("len(Problems) = %d, want 2", len(result.Problems))
	}

	// N+1 problems are sorted first (N+1 < SLOW lexically).
	nplus1 := result.Problems[0]
	if nplus1.Kind != ci.ProblemNPlus1 {
		t.Errorf("Problems[0].Kind = %s, want N+1", nplus1.Kind)
	}
	if nplus1.Count != 3 {
		t.Errorf("Problems[0].Count = %d, want 3", nplus1.Count)
	}

	slow := result.Problems[1]
	if slow.Kind != ci.ProblemSlowQuery {
		t.Errorf("Problems[1].Kind = %s, want SLOW", slow.Kind)
	}
	if slow.Count != 2 {
		t.Errorf("Problems[1].Count = %d, want 2", slow.Count)
	}
	if slow.AvgDuration != 300*time.Millisecond {
		t.Errorf("Problems[1].AvgDuration = %s, want 300ms", slow.AvgDuration)
	}
}

func TestAggregate_NoProblemEvents(t *testing.T) {
	t.Parallel()

	events := []*tapv1.QueryEvent{
		{Query: "SELECT 1"},
		{Query: "INSERT INTO users VALUES (1)"},
	}

	result := ci.Aggregate(events)

	if result.TotalQueries != 2 {
		t.Errorf("TotalQueries = %d, want 2", result.TotalQueries)
	}
	if result.HasProblems() {
		t.Error("expected no problems")
	}
}

func TestAggregate_Empty(t *testing.T) {
	t.Parallel()

	result := ci.Aggregate(nil)

	if result.TotalQueries != 0 {
		t.Errorf("TotalQueries = %d, want 0", result.TotalQueries)
	}
	if result.HasProblems() {
		t.Error("expected no problems")
	}
}

func TestAggregate_BothNPlus1AndSlow(t *testing.T) {
	t.Parallel()

	events := []*tapv1.QueryEvent{
		{
			Query:           "SELECT * FROM users WHERE id = ?",
			NormalizedQuery: "SELECT * FROM users WHERE id = ?",
			NPlus_1:         true,
			SlowQuery:       true,
			Duration:        durationpb.New(150 * time.Millisecond),
		},
	}

	result := ci.Aggregate(events)

	if len(result.Problems) != 2 {
		t.Fatalf("len(Problems) = %d, want 2 (one N+1, one SLOW)", len(result.Problems))
	}
}

func TestAggregate_UsesRawQueryWhenNormalizedEmpty(t *testing.T) {
	t.Parallel()

	const rawQuery = "SELECT id, name FROM users"
	events := []*tapv1.QueryEvent{
		{Query: rawQuery, NPlus_1: true},
		{Query: rawQuery, NPlus_1: true},
	}

	result := ci.Aggregate(events)

	if len(result.Problems) != 1 {
		t.Fatalf("len(Problems) = %d, want 1", len(result.Problems))
	}
	if result.Problems[0].Query != rawQuery {
		t.Errorf("Query = %q, want %q", result.Problems[0].Query, rawQuery)
	}
}

func TestAggregate_ProblemsOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	// Same kind and count for every query, so only the query can break the tie.
	queries := []string{"SELECT c FROM t", "SELECT a FROM t", "SELECT b FROM t"}
	events := make([]*tapv1.QueryEvent, 0, len(queries))
	for _, q := range queries {
		events = append(events, &tapv1.QueryEvent{Query: q, NormalizedQuery: q, NPlus_1: true})
	}

	want := []string{"SELECT a FROM t", "SELECT b FROM t", "SELECT c FROM t"}
	for range 20 {
		problems := ci.Aggregate(events).Problems
		got := make([]string, 0, len(problems))
		for _, p := range problems {
			got = append(got, p.Query)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("Problems order = %v, want %v", got, want)
		}
	}
}

func TestAggregate_Templates(t *testing.T) {
	t.Parallel()

	const (
		users = "SELECT name FROM users WHERE id = ?"
		posts = "SELECT id FROM posts"
	)
	events := []*tapv1.QueryEvent{
		{Op: int32(proxy.OpBegin), Query: "BEGIN"},
		{Op: int32(proxy.OpPrepare), Query: users, NormalizedQuery: users},
		{
			Op: int32(proxy.OpExecute), Query: users, NormalizedQuery: users,
			NPlus_1: true, Duration: durationpb.New(10 * time.Millisecond),
		},
		{
			Op: int32(proxy.OpExecute), Query: users, NormalizedQuery: users,
			NPlus_1: true, Duration: durationpb.New(20 * time.Millisecond),
		},
		{
			Op: int32(proxy.OpQuery), Query: posts, NormalizedQuery: posts,
			SlowQuery: true, Duration: durationpb.New(150 * time.Millisecond),
		},
		{Op: int32(proxy.OpCommit), Query: "COMMIT"},
	}

	result := ci.Aggregate(events)

	if result.TotalQueries != 6 {
		t.Errorf("TotalQueries = %d, want 6", result.TotalQueries)
	}
	if got := result.Statements(); got != 3 {
		t.Errorf("Statements() = %d, want 3", got)
	}
	if result.TotalDuration != 180*time.Millisecond {
		t.Errorf("TotalDuration = %s, want 180ms", result.TotalDuration)
	}
	want := []ci.TemplateStat{
		{Query: posts, Count: 1, TotalDuration: 150 * time.Millisecond, SlowCount: 1},
		{Query: users, Count: 2, TotalDuration: 30 * time.Millisecond, NPlus1Count: 2},
	}
	if !slices.Equal(result.Templates, want) {
		t.Errorf("Templates = %+v, want %+v", result.Templates, want)
	}
}

func startServer(t *testing.T, b *broker.Broker) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(b, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func TestRun_AggregatesEventsOnContextCancel(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	addr := startServer(t, b)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan ci.Result, 1)
	errc := make(chan error, 1)
	go func() {
		result, err := ci.Run(ctx, addr)
		if err != nil {
			errc <- err
			return
		}
		done <- result
	}()

	// Wait for subscription to register.
	time.Sleep(50 * time.Millisecond)

	b.Publish(proxy.Event{ID: "1", Op: proxy.OpQuery, Query: "SELECT 1"})
	b.Publish(proxy.Event{
		ID: "2", Op: proxy.OpQuery, Query: "SELECT id FROM users WHERE id = 1",
		NPlus1:          true,
		NormalizedQuery: "SELECT id FROM users WHERE id = ?",
	})
	b.Publish(proxy.Event{
		ID: "3", Op: proxy.OpQuery, Query: "SELECT id FROM users WHERE id = 2",
		NPlus1:          true,
		NormalizedQuery: "SELECT id FROM users WHERE id = ?",
	})

	// Give events time to arrive, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case result := <-done:
		if result.TotalQueries != 3 {
			t.Errorf("TotalQueries = %d, want 3", result.TotalQueries)
		}
		if !result.HasProblems() {
			t.Error("expected problems")
		}
	case err := <-errc:
		t.Fatalf("Run returned error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func TestRun_NoProblemEvents(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	addr := startServer(t, b)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan ci.Result, 1)
	errc := make(chan error, 1)
	go func() {
		result, err := ci.Run(ctx, addr)
		if err != nil {
			errc <- err
			return
		}
		done <- result
	}()

	time.Sleep(50 * time.Millisecond)

	b.Publish(proxy.Event{ID: "1", Op: proxy.OpQuery, Query: "SELECT 1"})
	b.Publish(proxy.Event{ID: "2", Op: proxy.OpExec, Query: "INSERT INTO t VALUES (1)"})

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case result := <-done:
		if result.TotalQueries != 2 {
			t.Errorf("TotalQueries = %d, want 2", result.TotalQueries)
		}
		if result.HasProblems() {
			t.Error("expected no problems")
		}
	case err := <-errc:
		t.Fatalf("Run returned error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to return")
	}
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !contains(s, substr) {
		t.Errorf("expected report to contain %q, got:\n%s", substr, s)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
