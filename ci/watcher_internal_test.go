package ci

import (
	"errors"
	"testing"
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
