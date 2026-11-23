package ci_test

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/proto-tap/sql-tap/broker"
	"github.com/proto-tap/sql-tap/ci"
	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/server"
)

// fakeTapService confirms the subscription only when confirm is set,
// then sends count events interval apart. It ends the stream afterwards unless hold is set.
type fakeTapService struct {
	tapv1.UnimplementedTapServiceServer

	confirm  bool
	count    int
	interval time.Duration
	hold     bool
	// dropStep makes the i-th response carry i*dropStep dropped events.
	dropStep uint64
}

func (s fakeTapService) Watch(_ *tapv1.WatchRequest, stream grpc.ServerStreamingServer[tapv1.WatchResponse]) error {
	if !s.confirm {
		<-stream.Context().Done()
		return nil
	}
	if err := stream.SendHeader(metadata.Pairs(server.SubscribedHeader, "true")); err != nil {
		return err //nolint:wrapcheck // test fake
	}
	for i := range s.count {
		ev := &tapv1.QueryEvent{Query: "SELECT 1", NormalizedQuery: "SELECT ?"}
		if err := stream.Send(&tapv1.WatchResponse{Event: ev, DroppedEvents: uint64(i) * s.dropStep}); err != nil {
			return err //nolint:wrapcheck // test fake
		}
		time.Sleep(s.interval)
	}
	if s.hold {
		<-stream.Context().Done()
	}
	return nil
}

func startFakeServer(t *testing.T, svc fakeTapService) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	tapv1.RegisterTapServiceServer(gs, svc)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	return lis.Addr().String()
}

func closed() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestCollect_StreamEnds(t *testing.T) {
	t.Parallel()

	addr := startFakeServer(t, fakeTapService{confirm: true, count: 2})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect(nil, ci.Drain{})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Statements(); got != 2 {
		t.Errorf("Statements() = %d, want 2", got)
	}
}

func TestCollect_StreamEndsBeforeStop(t *testing.T) {
	t.Parallel()

	addr := startFakeServer(t, fakeTapService{confirm: true, count: 2})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect(make(chan struct{}), ci.Drain{Idle: time.Second, Max: 5 * time.Second})
	if !errors.Is(err, ci.ErrStreamEnded) {
		t.Fatalf("err = %v, want ErrStreamEnded", err)
	}
	if got := r.Statements(); got != 2 {
		t.Errorf("Statements() = %d, want the partial result of 2", got)
	}
}

func TestCollectAndRun_DroppedEvents(t *testing.T) {
	t.Parallel()

	// Responses carry 0, 2, 4 dropped events; the result keeps the latest cumulative count.
	addr := startFakeServer(t, fakeTapService{confirm: true, count: 3, dropStep: 2})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect(nil, ci.Drain{})
	if err != nil {
		t.Fatal(err)
	}
	if r.DroppedEvents != 4 {
		t.Errorf("Collect: DroppedEvents = %d, want 4", r.DroppedEvents)
	}

	r, err = ci.Run(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	if r.DroppedEvents != 4 {
		t.Errorf("Run: DroppedEvents = %d, want 4", r.DroppedEvents)
	}
}

func TestCollect_DrainsInFlightEventsAfterStop(t *testing.T) {
	t.Parallel()

	// Events keep arriving 20ms apart after stop; an idle window of 200ms must wait for all of them.
	addr := startFakeServer(t, fakeTapService{confirm: true, count: 5, interval: 20 * time.Millisecond, hold: true})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect(closed(), ci.Drain{Idle: 200 * time.Millisecond, Max: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Statements(); got != 5 {
		t.Errorf("Statements() = %d, want 5", got)
	}
}

func TestCollect_DrainLimit(t *testing.T) {
	t.Parallel()

	// Events never stop, so draining ends at Max.
	addr := startFakeServer(t, fakeTapService{confirm: true, count: 1000, interval: 10 * time.Millisecond})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect(closed(), ci.Drain{Idle: time.Second, Max: 100 * time.Millisecond})
	if !errors.Is(err, ci.ErrDrainLimit) {
		t.Fatalf("err = %v, want ErrDrainLimit", err)
	}
	if r.Statements() == 0 {
		t.Error("expected partial result along with ErrDrainLimit")
	}
}

func TestSubscribe_TimesOutWithoutConfirmation(t *testing.T) {
	t.Parallel()

	addr := startFakeServer(t, fakeTapService{confirm: false})

	_, err := ci.Subscribe(t.Context(), addr, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "older than this sql-tap") {
		t.Fatalf("err = %v, want subscription confirmation error", err)
	}
}

func TestSubscribe_TimeoutCoversConnecting(t *testing.T) {
	t.Parallel()

	// Nothing listens on port 1, so the timeout must also bound connection setup.
	start := time.Now()
	_, err := ci.Subscribe(t.Context(), "localhost:1", 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected an error for an unreachable address")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Subscribe took %s, want it bounded by the timeout", elapsed)
	}
}

func TestSubscribe_ConfirmedByRealServer(t *testing.T) {
	t.Parallel()

	addr := startServer(t, broker.New(8))

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
}
