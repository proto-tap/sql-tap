package ci

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tapv1 "github.com/proto-tap/sql-tap/gen/tap/v1"
)

func TestStreamEndErr(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	close(closed)

	tests := []struct {
		name string
		stop <-chan struct{}
		want error
	}{
		{name: "nil stop collects until the stream ends", stop: nil, want: nil},
		{name: "stop closed together with the end of stream", stop: closed, want: nil},
		{name: "stop still open", stop: make(chan struct{}), want: ErrStreamEnded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := streamEndErr(tt.stop); !errors.Is(got, tt.want) || (got == nil) != (tt.want == nil) {
				t.Errorf("streamEndErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

// failingStream delivers events and then fails with err.
type failingStream struct {
	grpc.ClientStream

	events []*tapv1.QueryEvent
	i      *int
	err    error
}

func (s failingStream) Recv() (*tapv1.WatchResponse, error) {
	if *s.i < len(s.events) {
		ev := s.events[*s.i]
		*s.i++
		return &tapv1.WatchResponse{Event: ev}, nil
	}
	return nil, s.err
}

func (failingStream) Context() context.Context {
	return context.Background()
}

func TestCollect_ReceiveErrorKeepsPartialResult(t *testing.T) {
	t.Parallel()

	conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	events := []*tapv1.QueryEvent{
		{Query: "SELECT 1", NormalizedQuery: "SELECT ?"},
		{Query: "SELECT 2", NormalizedQuery: "SELECT ?"},
	}
	stream := failingStream{events: events, i: new(int), err: status.Error(codes.Unavailable, "connection lost")}
	w := Watcher{conn: conn, stream: stream, cancel: func() {}}

	r, err := w.Collect(make(chan struct{}), Drain{Idle: time.Second, Max: 5 * time.Second})
	if status.Code(errors.Unwrap(err)) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	if got := r.Statements(); got != 2 {
		t.Errorf("Statements() = %d, want the partial result of 2", got)
	}
}

func TestCollect_CancellationIsNotACleanEnd(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	close(closed)

	tests := []struct {
		name    string
		stop    <-chan struct{}
		err     error
		wantErr bool
	}{
		{name: "canceled with stop closed", stop: closed, err: status.Error(codes.Canceled, "x"), wantErr: true},
		{name: "deadline with stop closed", stop: closed, err: status.Error(codes.DeadlineExceeded, "x"), wantErr: true},
		{name: "canceled with nil stop", stop: nil, err: status.Error(codes.Canceled, "x"), wantErr: false},
		{name: "EOF with stop closed", stop: closed, err: io.EOF, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			// stream ends immediately, so stop may or may not have been observed by select yet;
			// the outcome must not depend on that.
			w := Watcher{conn: conn, stream: failingStream{i: new(int), err: tt.err}, cancel: func() {}}
			_, err = w.Collect(tt.stop, Drain{Idle: time.Second, Max: 5 * time.Second})
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
