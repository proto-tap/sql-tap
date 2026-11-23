package ci

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/proxy"
)

// Result holds the CI run outcome.
type Result struct {
	// Command is the measured command, set by `sql-tap run`. It is empty for -ci.
	Command []string
	// Incomplete is why the measurement may have missed events; empty for a complete one.
	Incomplete string
	// DroppedEvents is how many events sql-tapd dropped because this client did not keep up.
	DroppedEvents uint64
	TotalQueries  int
	// TotalDuration sums the durations of statements counted in Templates.
	TotalDuration time.Duration
	// Templates is sorted by TotalDuration descending.
	Templates []TemplateStat
	Problems  []Problem
}

// TemplateStat aggregates executions of a single normalized statement.
// Transaction control, Prepare and Bind events are excluded.
type TemplateStat struct {
	Query         string
	Count         int
	TotalDuration time.Duration
	NPlus1Count   int
	SlowCount     int
}

// Statements returns the number of statements counted in Templates.
func (r Result) Statements() int {
	n := 0
	for _, t := range r.Templates {
		n += t.Count
	}
	return n
}

// HasProblems reports whether any issues were detected.
func (r Result) HasProblems() bool {
	return len(r.Problems) > 0
}

// ProblemKind categorizes a detected issue.
type ProblemKind string

const (
	ProblemNPlus1    ProblemKind = "N+1"
	ProblemSlowQuery ProblemKind = "SLOW"
)

// Problem describes a single detected issue.
type Problem struct {
	Kind  ProblemKind
	Query string
	Count int
	// AvgDuration is set only for ProblemSlowQuery.
	AvgDuration time.Duration
}

// Report formats the result as a human-readable string.
func (r Result) Report() string {
	var b strings.Builder
	b.WriteString("sql-tap CI Report\n")
	b.WriteString("=================\n")
	if len(r.Command) > 0 {
		fmt.Fprintf(&b, "Command:  %s\n", strings.Join(r.Command, " "))
	}
	if r.Incomplete != "" {
		fmt.Fprintf(&b, "Incomplete: %s\n", r.Incomplete)
	}
	if r.DroppedEvents > 0 {
		fmt.Fprintf(&b, "Dropped:  %d events (sql-tap did not keep up with sql-tapd)\n", r.DroppedEvents)
	}
	fmt.Fprintf(&b, "Captured: %d queries\n", r.TotalQueries)

	if !r.HasProblems() {
		b.WriteString("\nNo problems found.\n")
		return b.String()
	}

	b.WriteString("\nProblems found:\n")
	for _, p := range r.Problems {
		switch p.Kind {
		case ProblemNPlus1:
			fmt.Fprintf(&b, "  [N+1]  %s  (detected %d times)\n", p.Query, p.Count)
		case ProblemSlowQuery:
			avg := p.AvgDuration.Truncate(time.Millisecond)
			fmt.Fprintf(&b, "  [SLOW] %s  (avg %s, %d occurrences)\n", p.Query, avg, p.Count)
		default:
			fmt.Fprintf(&b, "  [%s] %s  (%d occurrences)\n", string(p.Kind), p.Query, p.Count)
		}
	}

	fmt.Fprintf(&b, "\nExit: 1 (%d problems found)\n", len(r.Problems))
	return b.String()
}

// Run connects to the gRPC server at addr, collects query events until ctx is
// cancelled or the server closes the stream, and returns the aggregated result.
func Run(ctx context.Context, addr string) (Result, error) {
	conn, stream, err := watch(ctx, addr)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = conn.Close() }()

	return collect(ctx, stream)
}

func watch(ctx context.Context, addr string) (*grpc.ClientConn, tapv1.TapService_WatchClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	client := tapv1.NewTapServiceClient(conn)
	stream, err := client.Watch(ctx, &tapv1.WatchRequest{})
	if err != nil {
		closeInBackground(conn)
		return nil, nil, fmt.Errorf("watch %s: %w", addr, err)
	}
	return conn, stream, nil
}

// closeInBackground closes conn without waiting:
// Close can block for seconds while a dial to an unreachable address is still in flight.
func closeInBackground(conn *grpc.ClientConn) {
	go func() { _ = conn.Close() }()
}

func collect(ctx context.Context, stream tapv1.TapService_WatchClient) (Result, error) {
	a := newAggregator()

	for {
		resp, err := stream.Recv()
		if err != nil {
			if isStreamDone(ctx, err) {
				break
			}
			return Result{}, fmt.Errorf("recv: %w", err)
		}
		a.add(resp.GetEvent())
		a.observeDropped(resp.GetDroppedEvents())
	}

	return a.result(), nil
}

func isStreamDone(ctx context.Context, err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	if ctx.Err() != nil {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

type problemStats struct {
	nplus1Count int
	slowCount   int
	slowDur     time.Duration
}

type aggregator struct {
	total     int
	dropped   uint64
	problems  map[string]*problemStats
	templates map[string]*TemplateStat
}

func newAggregator() *aggregator {
	return &aggregator{
		problems:  make(map[string]*problemStats),
		templates: make(map[string]*TemplateStat),
	}
}

func (a *aggregator) add(e *tapv1.QueryEvent) {
	a.total++
	q := normalizedOrRaw(e)
	dur := e.GetDuration().AsDuration()

	if isStatement(e) && q != "" {
		t, ok := a.templates[q]
		if !ok {
			t = &TemplateStat{Query: q}
			a.templates[q] = t
		}
		t.Count++
		t.TotalDuration += dur
		if e.GetNPlus_1() {
			t.NPlus1Count++
		}
		if e.GetSlowQuery() {
			t.SlowCount++
		}
	}

	if !e.GetNPlus_1() && !e.GetSlowQuery() {
		return
	}
	s, ok := a.problems[q]
	if !ok {
		s = &problemStats{}
		a.problems[q] = s
	}
	if e.GetNPlus_1() {
		s.nplus1Count++
	}
	if e.GetSlowQuery() {
		s.slowCount++
		s.slowDur += dur
	}
}

func isStatement(e *tapv1.QueryEvent) bool {
	switch proxy.Op(e.GetOp()) {
	case proxy.OpQuery, proxy.OpExec, proxy.OpExecute:
		return true
	case proxy.OpPrepare, proxy.OpBind, proxy.OpBegin, proxy.OpCommit, proxy.OpRollback:
		return false
	}
	return false
}

// observeDropped records the subscription's cumulative drop count carried by a response.
func (a *aggregator) observeDropped(n uint64) {
	a.dropped = max(a.dropped, n)
}

func (a *aggregator) result() Result {
	r := Result{TotalQueries: a.total, DroppedEvents: a.dropped}

	for _, t := range a.templates {
		r.TotalDuration += t.TotalDuration
		r.Templates = append(r.Templates, *t)
	}
	slices.SortFunc(r.Templates, func(x, y TemplateStat) int {
		return cmp.Or(
			cmp.Compare(y.TotalDuration, x.TotalDuration),
			cmp.Compare(x.Query, y.Query),
		)
	})

	for q, s := range a.problems {
		if s.nplus1Count > 0 {
			r.Problems = append(r.Problems, Problem{
				Kind:  ProblemNPlus1,
				Query: q,
				Count: s.nplus1Count,
			})
		}
		if s.slowCount > 0 {
			avg := s.slowDur / time.Duration(s.slowCount)
			r.Problems = append(r.Problems, Problem{
				Kind:        ProblemSlowQuery,
				Query:       q,
				Count:       s.slowCount,
				AvgDuration: avg,
			})
		}
	}

	slices.SortFunc(r.Problems, func(x, y Problem) int {
		return cmp.Or(
			cmp.Compare(x.Kind, y.Kind),
			cmp.Compare(y.Count, x.Count),
			cmp.Compare(x.Query, y.Query),
		)
	})

	return r
}

// Aggregate computes the CI result from the given events.
// Intended for testing; the streaming path uses aggregator directly.
func Aggregate(events []*tapv1.QueryEvent) Result {
	a := newAggregator()
	for _, e := range events {
		a.add(e)
	}
	return a.result()
}

func normalizedOrRaw(e *tapv1.QueryEvent) string {
	if nq := e.GetNormalizedQuery(); nq != "" {
		return nq
	}
	return e.GetQuery()
}
