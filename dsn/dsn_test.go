package dsn_test

import (
	"testing"

	"github.com/proto-tap/sql-tap/dsn"
)

func TestDetectDriver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "postgres URI", raw: "postgres://user:pass@localhost/db", want: "pgx"},     //nolint:gosec // test data
		{name: "postgresql URI", raw: "postgresql://user:pass@localhost/db", want: "pgx"}, //nolint:gosec // test data
		{name: "postgres key=value", raw: "host=localhost dbname=db", want: "pgx"},
		{name: "mysql", raw: "user:pass@tcp(localhost:3306)/db", want: "mysql"},
		{name: "empty", raw: "", wantErr: true},
		{name: "unknown", raw: "foobar", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dsn.DetectDriver(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
