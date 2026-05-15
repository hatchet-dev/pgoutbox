package pgoutbox

import (
	"context"
	"strings"
	"testing"

	"github.com/hatchet-dev/pgoutbox/sqlc"
)

// TestAddMessages_Validation exercises the input-validation branches of
// AddMessages that return before touching the database, so we can run them
// without a real pgx.Tx.
func TestAddMessages_Validation(t *testing.T) {
	t.Parallel()

	o := &outboxImpl{
		queries: sqlc.New(),
		schema:  "outbox",
	}

	cases := []struct {
		name    string
		topic   string
		msgs    []MessageOpts
		wantErr bool
		errSub  string
	}{
		{
			name:    "empty topic",
			topic:   "",
			msgs:    []MessageOpts{{Payload: []byte(`{}`)}},
			wantErr: true,
			errSub:  "topic must not be empty",
		},
		{
			name:    "nil msgs slice is a no-op",
			topic:   "orders",
			msgs:    nil,
			wantErr: false,
		},
		{
			name:    "empty msgs slice is a no-op",
			topic:   "orders",
			msgs:    []MessageOpts{},
			wantErr: false,
		},
		{
			name:    "nil payload at index 0",
			topic:   "orders",
			msgs:    []MessageOpts{{Payload: nil}},
			wantErr: true,
			errSub:  "index 0",
		},
		{
			name:  "empty payload mid-slice reports correct index",
			topic: "orders",
			msgs: []MessageOpts{
				{Payload: []byte(`{"a":1}`)},
				{Payload: []byte{}},
				{Payload: []byte(`{"b":2}`)},
			},
			wantErr: true,
			errSub:  "index 1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := o.AddMessages(context.Background(), nil, tc.topic, tc.msgs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("AddMessages returned nil, want error containing %q", tc.errSub)
				}
				if !strings.Contains(err.Error(), tc.errSub) {
					t.Fatalf("AddMessages error = %q, want substring %q", err.Error(), tc.errSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("AddMessages returned %v, want nil", err)
			}
		})
	}
}
