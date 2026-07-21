package page_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

func TestValidateToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{"empty", "", false},
		{"valid", page.EncodeToken(5), false},
		{"invalid base64", "not-base64-!!!", true},
		{"valid base64 not int", "YWJj", true}, // "abc"
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := page.ValidateToken(tt.token)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewHMAC(t *testing.T) {
	t.Parallel()

	all5 := []string{"a", "b", "c", "d", "e"}
	secret := "test-secret"

	p1 := page.NewHMAC(all5, "", secret, 2, 100)      // ["a","b"]
	p2 := page.NewHMAC(all5, p1.Next, secret, 2, 100) // ["c","d"]
	_ = page.NewHMAC(all5, p2.Next, secret, 2, 100)   // ["e"]

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
			name:         "invalid_token_resets_to_start",
			input:        all5,
			token:        "not-valid",
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"a", "b"},
			wantHasNext:  true,
		},
		{
			name:         "tampered_token_resets_to_start",
			input:        all5,
			token:        page.EncodeHMACToken(2, "wrong-secret"),
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{"a", "b"},
			wantHasNext:  true,
		},
		{
			name:         "start_beyond_length",
			input:        all5,
			token:        page.EncodeHMACToken(10, secret),
			limit:        2,
			defaultLimit: 100,
			wantData:     []string{},
			wantHasNext:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := page.NewHMAC(tt.input, tt.token, secret, tt.limit, tt.defaultLimit)

			require.Equal(t, tt.wantData, got.Data)
			assert.Equal(t, tt.wantHasNext, got.Next != "")
		})
	}
}

func TestDecodeToken(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5, page.DecodeToken(page.EncodeToken(5)))
	assert.Equal(t, 0, page.DecodeToken(""))
	assert.Equal(t, 0, page.DecodeToken("invalid"))
}

func TestDecodeHMACToken(t *testing.T) {
	t.Parallel()

	secret := "secret"
	assert.Equal(t, 5, page.DecodeHMACToken(page.EncodeHMACToken(5, secret), secret))
	assert.Equal(t, 0, page.DecodeHMACToken("", secret))
	assert.Equal(t, 0, page.DecodeHMACToken("invalid", secret))
	// wrong secret
	assert.Equal(t, 0, page.DecodeHMACToken(page.EncodeHMACToken(5, secret), "wrong"))

	// malformed base64url
	assert.Equal(t, 0, page.DecodeHMACToken("YWJj!!", secret))

	// token missing dot
	importBase64URL := "YWJjZGVmZ2hpag==" // random base64
	assert.Equal(t, 0, page.DecodeHMACToken(importBase64URL, secret))
}
