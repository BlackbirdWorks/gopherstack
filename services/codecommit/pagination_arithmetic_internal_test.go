package codecommit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginateSlice_BoundaryWalk(t *testing.T) {
	t.Parallel()

	items := make([]string, 0, 25)
	for i := range 25 {
		items = append(items, string(rune('a'+i)))
	}

	var collected []string

	token := ""

	for {
		page, next, err := paginateSlice(items, token, 6)
		require.NoError(t, err)

		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, items, collected)
}

// The token is a plain decimal offset; malformed or negative ones are InvalidContinuationToken.
func TestPaginateSlice_Cases(t *testing.T) {
	t.Parallel()

	abcde := []string{"a", "b", "c", "d", "e"}

	tests := []struct {
		name     string
		token    string
		wantNext string
		items    []string
		wantPage []string
		max      int
		wantErr  bool
	}{
		{name: "first_page", items: abcde, max: 2, wantPage: []string{"a", "b"}, wantNext: "2"},
		{name: "exact_division_no_cursor", items: abcde[:4], token: "2", max: 2, wantPage: []string{"c", "d"}},
		{name: "single_page", items: abcde[:2], max: 10, wantPage: []string{"a", "b"}},
		{name: "empty", max: 10, wantPage: nil},
		{name: "plain_offset", items: abcde, token: "2", max: 2, wantPage: []string{"c", "d"}, wantNext: "4"},
		{name: "past_end", items: abcde[:3], token: "100", max: 10, wantPage: []string{}},
		{name: "negative", items: abcde[:3], token: "-5", max: 2, wantErr: true},
		{name: "malformed", items: abcde[:3], token: "not-a-number", max: 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page, next, err := paginateSlice(tt.items, tt.token, tt.max)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidContinuationToken)

				return
			}

			require.NoError(t, err)
			assert.Len(t, page, len(tt.wantPage))
			assert.Equal(t, tt.wantNext, next)
		})
	}
}
