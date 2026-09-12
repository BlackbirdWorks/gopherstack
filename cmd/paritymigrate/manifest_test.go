package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lines     []string
		wantTexts []string
	}{
		{
			name:      "single-line bullets",
			lines:     []string{`  - "first item"`, `  - "second item"`},
			wantTexts: []string{"first item", "second item"},
		},
		{
			name: "wrapped multi-line bullet folds onto one item",
			lines: []string{
				`  - "first line of the item`,
				`    continues here"`,
				`  - "second item"`,
			},
			wantTexts: []string{"first line of the item continues here", "second item"},
		},
		{
			name: "standalone comment never folds into an item",
			lines: []string{
				`  - "an item"`,
				`  # a standalone historical note`,
				`  - "another item"`,
			},
			wantTexts: []string{"an item", "another item"},
		},
		{
			name:      "blank line separates without folding",
			lines:     []string{`  - "an item"`, ``, `  - "another item"`},
			wantTexts: []string{"an item", "another item"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			items := parseItems(tc.lines, 0, len(tc.lines))

			texts := make([]string, len(items))
			for i, it := range items {
				texts[i] = it.text
			}

			assert.Equal(t, tc.wantTexts, texts)
		})
	}
}

func TestParseItems_PreservesCommentLines(t *testing.T) {
	t.Parallel()

	lines := []string{
		`  - "first item"`,
		`  # a comment describing history`,
	}

	items := parseItems(lines, 0, len(lines))

	require.Len(t, items, 1)
	assert.Equal(t, 0, items[0].start)
	assert.Equal(t, 1, items[0].end, "the comment line must never be swallowed into the preceding item's range")
}

func TestFindBlock(t *testing.T) {
	t.Parallel()

	lines := []string{
		"service: example",
		"gaps:",
		`  - "one"`,
		`  - "two"`,
		"deferred: []",
	}

	b := findBlock(lines, 0, len(lines), "gaps")
	require.NotNil(t, b)
	assert.Equal(t, 1, b.keyLine)
	assert.Equal(t, 4, b.end)
	assert.False(t, b.isInline)
	require.Len(t, b.items, 2)
	assert.Equal(t, "one", b.items[0].text)
	assert.Equal(t, "two", b.items[1].text)

	assert.Nil(t, findBlock(lines, 0, len(lines), "residual_gaps"))

	deferredBlock := findBlock(lines, 0, len(lines), "deferred")
	require.NotNil(t, deferredBlock)
	assert.True(t, deferredBlock.isInline)
}
