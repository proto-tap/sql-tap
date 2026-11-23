package ci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"

	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
	"github.com/proto-tap/sql-tap/server"
)

// ErrDrainLimit is returned by Collect together with the result when events kept arriving until Drain.Max,
// so the result may be incomplete.
var ErrDrainLimit = errors.New("events kept arriving until the drain limit")

// ErrStreamEnded is returned by Collect together with the result when sql-tapd ended the stream
// before stop was closed, so events after that point were not captured.
var ErrStreamEnded = errors.New("sql-tapd ended the stream before measurement finished")

var errNotConfirmed = errors.New("sql-tapd did not confirm the subscription; it may be older than this sql-tap")

// Drain controls how long Collect keeps receiving after stop is closed,
// so that events still in flight are not lost.
type Drain struct {
	// Idle ends draining once no event has arrived for this long.
	Idle time.Duration
	// Max bounds the whole draining period.
	Max time.Duration
}

// Watcher is a Watch subscription confirmed by sql-tapd.
// Every event published after Subscribe returns is delivered to Collect.
type Watcher struct {
	conn   *grpc.ClientConn
	stream tapv1.TapService_WatchClient
	cancel context.CancelFunc
}

// Subscribe connects to sql-tapd at addr and waits up to timeout for it to confirm the subscription.
func Subscribe(ctx context.Context, addr string, timeout time.Duration) (Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(timeout, cancel)

	conn, stream, err := watch(ctx, addr)
	if err == nil {
		err = waitSubscribed(stream)
	}
	if !timer.Stop() {
		err = fmt.Errorf("no subscription confirmation within %s: %w", timeout, errNotConfirmed)
	}
	if err != nil {
		cancel()
		if conn != nil {
			closeInBackground(conn)
		}
		return Watcher{}, fmt.Errorf("%s: %w", addr, err)
	}
	return Watcher{conn: conn, stream: stream, cancel: cancel}, nil
}

// Collect aggregates events until the stream ends or the context given to Subscribe is cancelled.
// Once stop is closed, it keeps receiving as configured by d, then closes the connection.
// If draining hits d.Max, it returns the result along with ErrDrainLimit.
// If the stream ends while stop is still open, it returns the result along with ErrStreamEnded;
// pass a nil stop to collect until the stream ends.
// On any other receive error it returns the partial result along with the error.
func (w Watcher) Collect(stop <-chan struct{}, d Drain) (Result, error) {
	defer w.Close()
	untilStreamEnds := stop == nil

	recv := make(chan received)
	done := make(chan struct{})
	defer close(done)
	go receive(w.stream, stop, recv, done)

	a := newAggregator()
	var idle *time.Timer
	var idleC, limitC <-chan time.Time
	for {
		select {
		case r := <-recv:
			if r.err != nil {
				switch {
				case errors.Is(r.err, io.EOF):
					return a.result(), r.endErr
				case untilStreamEnds && isStreamDone(w.stream.Context(), r.err):
					return a.result(), nil
				}
				// With a stop channel, only a clean EOF completes the measurement;
				// cancellation or a deadline means it was interrupted.
				return a.result(), fmt.Errorf("recv: %w", r.err)
			}
			a.add(r.ev)
			if idle != nil {
				idle.Reset(d.Idle)
			}
		case <-stop:
			stop = nil
			idle = time.NewTimer(d.Idle)
			defer idle.Stop()
			idleC = idle.C
			limitC = time.After(d.Max)
		case <-idleC:
			return a.result(), nil
		case <-limitC:
			return a.result(), ErrDrainLimit
		}
	}
}

type received struct {
	ev  *tapv1.QueryEvent
	err error
	// endErr classifies an EOF when it was received, not when Collect gets to it:
	// stop may close in between, which must not turn an early end into a clean one.
	endErr error
}

// receive forwards events from stream to out until an error or done.
func receive(stream tapv1.TapService_WatchClient, stop <-chan struct{}, out chan<- received, done <-chan struct{}) {
	for {
		resp, err := stream.Recv()
		r := received{ev: resp.GetEvent(), err: err}
		if errors.Is(err, io.EOF) {
			r.endErr = streamEndErr(stop)
		}
		select {
		case out <- r:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}

// streamEndErr reports whether the stream ended before stop was closed.
func streamEndErr(stop <-chan struct{}) error {
	if stop == nil {
		return nil
	}
	select {
	case <-stop:
		return nil
	default:
		return ErrStreamEnded
	}
}

// Close releases the subscription without collecting.
func (w Watcher) Close() {
	w.cancel()
	_ = w.conn.Close()
}

func waitSubscribed(stream tapv1.TapService_WatchClient) error {
	md, err := stream.Header()
	if err != nil {
		return fmt.Errorf("wait for subscription: %w", err)
	}
	if len(md.Get(server.SubscribedHeader)) == 0 {
		return errNotConfirmed
	}
	return nil
}
