package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseManifest_ClaimBlocks(t *testing.T) {
	t.Parallel()

	content := `---
service: widget
last_audit_date: 2026-08-01
ops:
  ListWidgets: {wire: ok, errors: ok, state: ok, persist: ok}
gaps:
  - "widget not fixed"
items_still_open:
  - "still open item"
deferred:
  - "out of scope thing"
---

## Notes
body text
`

	tests := []struct {
		name      string
		wantField string
	}{
		{name: "gaps block found", wantField: "gaps"},
		{name: "items_still_open block found", wantField: "items_still_open"},
	}

	m := parseManifest("widget", "services/widget/PARITY.md", content)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var found bool
			for _, c := range m.claims {
				if c.field == tt.wantField {
					found = true
				}
			}
			assert.True(t, found, "expected claim block %q", tt.wantField)
		})
	}

	require.Len(t, m.claims, 2, "deferred is not a claim field")
	assert.Equal(t, 11, m.body.start, "body should start after the closing ---")
}

func TestManifest_BlockEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{
			name:    "empty bracket list",
			content: "---\nservice: x\ngaps: []\n---\n",
			want:    true,
		},
		{
			name:    "populated list",
			content: "---\nservice: x\ngaps:\n  - \"a real gap\"\n---\n",
			want:    false,
		},
		{
			name:    "bare key with nothing following",
			content: "---\nservice: x\ngaps:\nops:\n  Foo: {wire: ok}\n---\n",
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := parseManifest("x", "services/x/PARITY.md", tt.content)
			require.Len(t, m.claims, 1)
			assert.Equal(t, tt.want, m.blockEmpty(m.claims[0]))
		})
	}
}

func TestComplementRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		blocks []claimBlock
		want   []lineRange
		start  int
		end    int
	}{
		{
			name:   "no blocks",
			blocks: nil,
			want:   []lineRange{{start: 0, end: 10}},
			start:  0,
			end:    10,
		},
		{
			name: "block in the middle",
			blocks: []claimBlock{
				{start: 4, end: 6},
			},
			want:  []lineRange{{start: 0, end: 4}, {start: 6, end: 10}},
			start: 0,
			end:   10,
		},
		{
			name: "block touches both edges",
			blocks: []claimBlock{
				{start: 0, end: 3},
				{start: 7, end: 10},
			},
			want:  []lineRange{{start: 3, end: 7}},
			start: 0,
			end:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, complementRanges(tt.start, tt.end, tt.blocks))
		})
	}
}
