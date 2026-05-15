package pgoutbox

import "testing"

func TestValidateSchemaName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{name: "simple lowercase", schema: "outbox", wantErr: false},
		{name: "leading underscore", schema: "_outbox", wantErr: false},
		{name: "mixed case", schema: "MyOutbox", wantErr: false},
		{name: "digits after letter", schema: "outbox_v2", wantErr: false},
		{name: "single letter", schema: "a", wantErr: false},
		{name: "max length 63", schema: "a" + repeat("b", 62), wantErr: false},

		{name: "empty", schema: "", wantErr: true},
		{name: "leading digit", schema: "1outbox", wantErr: true},
		{name: "hyphen", schema: "my-outbox", wantErr: true},
		{name: "space", schema: "my outbox", wantErr: true},
		{name: "dot", schema: "my.outbox", wantErr: true},
		{name: "quote", schema: `out"box`, wantErr: true},
		{name: "semicolon injection", schema: "outbox; DROP SCHEMA public CASCADE; --", wantErr: true},
		{name: "embedded quote injection", schema: `outbox"; DROP TABLE x; --`, wantErr: true},
		{name: "trailing whitespace", schema: "outbox ", wantErr: true},
		{name: "leading whitespace", schema: " outbox", wantErr: true},
		{name: "unicode letter", schema: "öutbox", wantErr: true},
		{name: "exceeds 63 chars", schema: repeat("a", 64), wantErr: true},
		{name: "null byte", schema: "outbox\x00", wantErr: true},
		{name: "newline", schema: "outbox\nDROP", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateSchemaName(tc.schema)
			if tc.wantErr && err == nil {
				t.Fatalf("validateSchemaName(%q) = nil, want error", tc.schema)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateSchemaName(%q) = %v, want nil", tc.schema, err)
			}
		})
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
