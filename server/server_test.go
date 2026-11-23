package server_test

import (
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/proto-tap/sql-tap/broker"
	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/proxy"
	"github.com/proto-tap/sql-tap/server"
)

func startServer(t *testing.T, b *broker.Broker) tapv1.TapServiceClient {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(b, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return tapv1.NewTapServiceClient(conn)
}

func TestWatch(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	client := startServer(t, b)

	ctx := t.Context()
	stream, err := client.Watch(ctx, &tapv1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}

	// Wait briefly for the subscription to be registered.
	time.Sleep(50 * time.Millisecond)

	b.Publish(proxy.Event{
		ID:    "1",
		Op:    proxy.OpQuery,
		Query: "SELECT 1",
	})

	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	ev := resp.GetEvent()
	if ev.GetId() != "1" {
		t.Fatalf("expected id 1, got %q", ev.GetId())
	}
	if ev.GetQuery() != "SELECT 1" {
		t.Fatalf("expected query SELECT 1, got %q", ev.GetQuery())
	}
	if ev.GetOp() != int32(proxy.OpQuery) {
		t.Fatalf("expected op %d, got %d", proxy.OpQuery, ev.GetOp())
	}
}

func TestWatch_SendsSubscribedHeaderBeforeEvents(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	client := startServer(t, b)

	stream, err := client.Watch(t.Context(), &tapv1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}

	md, err := stream.Header()
	if err != nil {
		t.Fatal(err)
	}
	if got := md.Get(server.SubscribedHeader); len(got) != 1 || got[0] != "true" {
		t.Fatalf("header %s = %v, want [true]", server.SubscribedHeader, got)
	}

	// No sleep: once the header arrives, the subscription is registered.
	b.Publish(proxy.Event{ID: "1", Op: proxy.OpQuery, Query: "SELECT 1"})

	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetEvent().GetId(); got != "1" {
		t.Fatalf("expected id 1, got %q", got)
	}
}

// burst subscribes, overflows a 1-event buffer with n events, and returns the responses as they arrive.
func burst(t *testing.T, reportDrops bool, n int) <-chan *tapv1.WatchResponse {
	t.Helper()

	b := broker.New(1)
	client := startServer(t, b)

	stream, err := client.Watch(t.Context(), &tapv1.WatchRequest{ReportDrops: reportDrops})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Header(); err != nil {
		t.Fatal(err)
	}

	responses := make(chan *tapv1.WatchResponse)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case responses <- resp:
			case <-t.Context().Done():
				return
			}
		}
	}()

	for i := range n {
		b.Publish(proxy.Event{ID: strconv.Itoa(i), Op: proxy.OpQuery, Query: "SELECT 1"})
	}
	return responses
}

func TestWatch_ReportsDroppedEvents(t *testing.T) {
	t.Parallel()

	// Every event is either delivered or counted as dropped, and with report_drops the final count
	// always arrives, even for a drop with no event queued after it.
	const n = 10000
	responses := burst(t, true, n)

	var received int
	var dropped uint64
	deadline := time.After(10 * time.Second)
	for uint64(received)+dropped != n {
		select {
		case resp := <-responses:
			if resp.GetEvent() != nil {
				received++
			}
			dropped = max(dropped, resp.GetDroppedEvents())
		case <-deadline:
			t.Fatalf("received %d + dropped %d never reached %d", received, dropped, n)
		}
	}
	t.Logf("received %d, dropped %d", received, dropped)
}

func TestWatch_WithoutReportDropsEveryResponseHasAnEvent(t *testing.T) {
	t.Parallel()

	responses := burst(t, false, 1000)
	for {
		select {
		case resp := <-responses:
			if resp.GetEvent() == nil {
				t.Fatal("got an event-less response without report_drops")
			}
		case <-time.After(500 * time.Millisecond):
			return // stream went quiet without an event-less response
		}
	}
}

func TestWatch_MultipleEvents(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	client := startServer(t, b)

	ctx := t.Context()
	stream, err := client.Watch(ctx, &tapv1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	for i := range 3 {
		b.Publish(proxy.Event{
			ID:    string(rune('A' + i)),
			Op:    proxy.OpQuery,
			Query: "SELECT " + string(rune('A'+i)),
		})
	}

	for range 3 {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetEvent() == nil {
			t.Fatal("expected event, got nil")
		}
	}
}

func TestExplain_NotConfigured(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	client := startServer(t, b) // explainClient is nil

	ctx := t.Context()
	_, err := client.Explain(ctx, &tapv1.ExplainRequest{
		Query: "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", st.Code())
	}
}
