package explain_test

import (
	"testing"

	"github.com/proto-tap/sql-tap/explain"
)

func TestMode_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode explain.Mode
		want string
	}{
		{explain.Explain, "EXPLAIN"},
		{explain.Analyze, "EXPLAIN ANALYZE"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := tt.mode.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
