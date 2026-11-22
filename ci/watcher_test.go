package ci_test

import (
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
// then sends events and ends the stream.
type fakeTapService struct {
	tapv1.UnimplementedTapServiceServer

	confirm bool
	events  []*tapv1.QueryEvent
}

func (s fakeTapService) Watch(_ *tapv1.WatchRequest, stream grpc.ServerStreamingServer[tapv1.WatchResponse]) error {
	if !s.confirm {
		<-stream.Context().Done()
		return nil
	}
	if err := stream.SendHeader(metadata.Pairs(server.SubscribedHeader, "true")); err != nil {
		return err //nolint:wrapcheck // test fake
	}
	for _, e := range s.events {
		if err := stream.Send(&tapv1.WatchResponse{Event: e}); err != nil {
			return err //nolint:wrapcheck // test fake
		}
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

func TestSubscribe_Collect(t *testing.T) {
	t.Parallel()

	addr := startFakeServer(t, fakeTapService{
		confirm: true,
		events: []*tapv1.QueryEvent{
			{Query: "SELECT 1", NormalizedQuery: "SELECT ?"},
			{Query: "SELECT 2", NormalizedQuery: "SELECT ?"},
		},
	})

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Statements(); got != 2 {
		t.Errorf("Statements() = %d, want 2", got)
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

func TestSubscribe_ConfirmedByRealServer(t *testing.T) {
	t.Parallel()

	addr := startServer(t, broker.New(8))

	w, err := ci.Subscribe(t.Context(), addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
}
