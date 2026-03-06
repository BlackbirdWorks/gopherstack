package page_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	all5 := []string{"a", "b", "c", "d", "e"}

	// pre-compute tokens for chained cases
	p1 := page.New(all5, "", 2, 100)      // ["a","b"], Next -> offset 2
	p2 := page.New(all5, p1.Next, 2, 100) // ["c","d"], Next -> offset 4
	_ = page.New(all5, p2.Next, 2, 100)   // ["e"], Next == ""

	tests := []struct {
		name         string
		token        string
		input        []string
		wantData     []string
		limit        int
		defaultLimit int
		wantHasNext  bool
	}{
		{
			name:         "first_page",
			input:        all5,
			token:        "",
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"a", "b"},
			wantHasNext:  true,
		},
		{
			name:         "second_page",
			input:        all5,
			token:        p1.Next,
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"c", "d"},
			wantHasNext:  true,
		},
		{
			name:         "last_partial_page",
			input:        all5,
			token:        p2.Next,
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"e"},
			wantHasNext:  false,
		},
		{
			name:         "all_fit_in_one_page",
			input:        all5,
			token:        "",
			limit:        10,
			defaultLimit: 100,
			wantData:     all5,
			wantHasNext:  false,
		},
		{
			name:         "uses_default_limit_when_zero",
			input:        all5,
			token:        "",
			limit:        0,
			defaultLimit: 3,
			wantData:     []string{"a", "b", "c"},
			wantHasNext:  true,
		},
		{
			name:         "empty_slice",
			input:        []string{},
			token:        "",
			limit:        10,
			defaultLimit: 100,
			wantData:     []string{},
			wantHasNext:  false,
		},
		{
			name:         "invalid_token_resets_to_start",
			input:        all5,
			token:        "not-valid-base64!!!",
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"a", "b"},
			wantHasNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := page.New(tt.input, tt.token, tt.limit, tt.defaultLimit)

			require.Equal(t, tt.wantData, got.Data)
			assert.Equal(t, tt.wantHasNext, got.Next != "")
		})
	}
}

func TestNewTraversal(t *testing.T) {
	t.Parallel()

	all := make([]int, 25)
	for i := range all {
		all[i] = i
	}

	var collected []int
	token := ""

	for {
		p := page.New(all, token, 10, 10)
		collected = append(collected, p.Data...)

		if p.Next == "" {
			break
		}

		token = p.Next
	}

	require.Equal(t, all, collected)
}
