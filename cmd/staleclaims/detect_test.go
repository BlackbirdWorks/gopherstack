package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllIndexes_WholeWordOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		text string
		tok  string
		want []int
	}{
		{
			name: "whole word match",
			text: "CreateAccessPoint is real",
			tok:  "CreateAccessPoint",
			want: []int{0},
		},
		{
			name: "substring inside a longer identifier is excluded",
			text: "CreateAccessPointForObjectLambdaResult only",
			tok:  "CreateAccessPoint",
			want: nil,
		},
		{
			name: "two whole-word occurrences",
			text: "Foo bar Foo",
			tok:  "Foo",
			want: []int{0, 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, allIndexes(tt.text, tt.tok))
		})
	}
}

func TestIsCandidateToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tok  string
		want bool
	}{
		{name: "real op name", tok: "DescribeImages", want: true},
		{name: "too short", tok: "Foo", want: false},
		{name: "all caps has no lowercase", tok: "ARNSUFFIX", want: false},
		{name: "stopword capitalized", tok: "Deferred", want: false},
		{name: "stopword lowercase from backtick", tok: "output", want: false},
		{name: "exception suffix excluded", tok: "InvalidRequestException", want: false},
		{name: "generic field name excluded", tok: "Description", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isCandidateToken(tt.tok))
		})
	}
}

func TestIsTooCommon(t *testing.T) {
	t.Parallel()

	over := strings.Repeat("Create ", commonTokenMax+1)

	assert.True(t, isTooCommon(over, "Create"))
	assert.False(t, isTooCommon("Create Create", "Create"))
}

func TestCheckStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{
			name: "genuine stale claim flagged",
			content: `---
service: widget
gaps:
  - "DescribeWidgets is not fixed -- always returns empty"
ops:
  DescribeWidgets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20: now returns real widgets"}
---
`,
			want: 1,
		},
		{
			name: "self-resolved claim not flagged",
			content: `---
service: widget
gaps:
  - "DescribeWidgets was not fixed, but is now fixed 2026-08-20: see ops"
ops:
  DescribeWidgets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20"}
---
`,
			want: 0,
		},
		{
			name: "analogy mention without its own open marker is not flagged",
			content: `---
service: widget
gaps:
  - "OtherThing was tricky. DescribeWidgets, by contrast, turned out to be simple and already correct."
ops:
  DescribeWidgets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20"}
---
`,
			want: 0,
		},
		{
			name: "genuinely still-open claim with no elsewhere fix is not flagged",
			content: `---
service: widget
gaps:
  - "DescribeWidgets is not fixed -- always returns empty"
ops:
  DescribeWidgets: {wire: gap, errors: ok, state: ok, persist: ok, note: "still empty, no fix yet"}
---
`,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := parseManifest("widget", "services/widget/PARITY.md", tt.content)
			require.NotEmpty(t, m.claims)

			got := checkStructured(m)
			assert.Len(t, got, tt.want)
		})
	}
}

func TestCheckProse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{
			name: "cross-section fix flagged",
			content: `---
service: widget
---

## Notes

### 2026-07-01 pass

DescribeWidgets is not fixed this pass -- always returns empty.

### 2026-08-01 pass

DescribeWidgets is now fixed: real widgets are returned.
`,
			want: 1,
		},
		{
			name: "same-section problem-then-fix narrative is not flagged",
			content: `---
service: widget
---

## Notes

### 2026-08-23 pass

Found: DescribeWidgets was not fixed, always returned empty.

Fixed by wiring the real backend call for DescribeWidgets.
`,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := parseManifest("widget", "services/widget/PARITY.md", tt.content)
			got := checkProse(m)
			assert.Len(t, got, tt.want)
		})
	}
}

func TestSplitParagraphs_SectionTracking(t *testing.T) {
	t.Parallel()

	lines := []string{
		"### first",
		"",
		"para one",
		"",
		"### second",
		"",
		"para two",
	}

	paras := splitParagraphs(lines, 0)
	require.Len(t, paras, 2)
	assert.NotEqual(t, paras[0].section, paras[1].section, "different headings should be different sections")
}
