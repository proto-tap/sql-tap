package ci

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/server"
)

// Watcher is a Watch subscription confirmed by sql-tapd.
// Every event published after Subscribe returns is delivered to Collect.
type Watcher struct {
	conn   *grpc.ClientConn
	stream tapv1.TapService_WatchClient
}

// Subscribe connects to sql-tapd at addr and waits up to timeout for it to confirm the subscription.
// Collecting stops when ctx is cancelled.
func Subscribe(ctx context.Context, addr string, timeout time.Duration) (Watcher, error) {
	conn, stream, err := watch(ctx, addr)
	if err != nil {
		return Watcher{}, err
	}
	if err := waitSubscribed(stream, timeout); err != nil {
		_ = conn.Close()
		return Watcher{}, fmt.Errorf("%s: %w", addr, err)
	}
	return Watcher{conn: conn, stream: stream}, nil
}

// Collect aggregates events until the context given to Subscribe is cancelled or the stream ends,
// then closes the connection.
func (w Watcher) Collect() (Result, error) {
	defer func() { _ = w.conn.Close() }()
	return collect(w.stream.Context(), w.stream)
}

// Close releases the connection without collecting.
func (w Watcher) Close() {
	_ = w.conn.Close()
}

func waitSubscribed(stream tapv1.TapService_WatchClient, timeout time.Duration) error {
	type header struct {
		md  metadata.MD
		err error
	}
	ch := make(chan header, 1)
	go func() {
		md, err := stream.Header()
		ch <- header{md: md, err: err}
	}()

	select {
	case h := <-ch:
		if h.err != nil {
			return fmt.Errorf("wait for subscription: %w", h.err)
		}
		if len(h.md.Get(server.SubscribedHeader)) == 0 {
			return errNotConfirmed
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("no subscription confirmation within %s: %w", timeout, errNotConfirmed)
	}
}

var errNotConfirmed = errors.New("sql-tapd did not confirm the subscription; it may be older than this sql-tap")
