package fis

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginatePage_BoundaryWalk(t *testing.T) {
	t.Parallel()

	items := make([]string, 0, 21)
	for i := range 21 {
		items = append(items, string(rune('a'+i)))
	}

	var collected []string

	token := ""
	for {
		q := url.Values{"maxResults": {"5"}}
		if token != "" {
			q.Set("nextToken", token)
		}

		page, next := paginatePage(items, items, q)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, items, collected)
}

func TestPaginatePage_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c", "d"}

	page1, tok1 := paginatePage(items, items, url.Values{"maxResults": {"2"}})
	require.Equal(t, []string{"a", "b"}, page1)
	require.NotEmpty(t, tok1)

	q2 := url.Values{"maxResults": {"2"}, "nextToken": {tok1}}
	page2, tok2 := paginatePage(items, items, q2)
	require.Equal(t, []string{"c", "d"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginatePage_SinglePage(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b"}

	page, tok := paginatePage(items, items, url.Values{"maxResults": {"10"}})
	require.Equal(t, items, page)
	assert.Empty(t, tok)
}

func TestPaginatePage_Empty(t *testing.T) {
	t.Parallel()

	page, tok := paginatePage([]string{}, []string{}, url.Values{"maxResults": {"10"}})
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginatePage_StaleOrTamperedTokenPastEnd verifies paginatePage does
// not panic when the nextToken decodes to an offset beyond the current item
// count -- e.g. the list shrank between calls (items deleted), or the token
// was tampered with / hand-constructed by a client. Every list handler in
// this service (ListActions, ListExperiments, ListExperimentTemplates,
// ListTargetAccountConfigurations, ...) funnels through this helper or its
// hand-rolled equivalent, so an unguarded offset here is a slice-bounds
// panic reachable from any of them.
func TestPaginatePage_StaleOrTamperedTokenPastEnd(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c"}
	staleToken := encodePageToken(100)

	require.NotPanics(t, func() {
		q := url.Values{"nextToken": {staleToken}}
		page, tok := paginatePage(items, items, q)
		assert.Empty(t, page)
		assert.Empty(t, tok)
	})
}

func TestPaginateWithToken_DefaultAndCapMaxResults(t *testing.T) {
	t.Parallel()

	mr, start := paginateWithToken(nil, url.Values{})
	assert.Equal(t, defaultMaxResults, mr)
	assert.Equal(t, 0, start)

	mr2, _ := paginateWithToken(nil, url.Values{"maxResults": {"9999"}})
	assert.Equal(t, absoluteMaxResults, mr2, "maxResults must be capped at absoluteMaxResults")
}

func TestEncodeDecodePageToken_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, idx := range []int{0, 1, 42, 1000} {
		tok := encodePageToken(idx)
		got, err := decodePageToken(tok)
		require.NoError(t, err)
		assert.Equal(t, idx, got)
	}
}

func TestDecodePageToken_Malformed(t *testing.T) {
	t.Parallel()

	_, err := decodePageToken("not-valid-base64!!!")
	assert.Error(t, err)
}
