package server_test

import (
	"net"
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
