package rolesanywhere_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// TestParity_ListTrustAnchors_TokenWalk verifies that pagination emits a working
// NextToken and that walking it visits every item exactly once (no duplicates,
// no skips) — the previous nextTokenFromSlice always returned "".
func TestParity_ListTrustAnchors_TokenWalk(t *testing.T) {
	t.Parallel()

	b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")

	const total = 5
	for i := range total {
		_, err := b.CreateTrustAnchor(
			"anchor-"+string(rune('a'+i)),
			rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"},
			nil,
		)
		require.NoError(t, err)
	}

	seen := make(map[string]int)
	token := ""

	for range total + 2 {
		items, next, err := b.ListTrustAnchors(token, 2)
		require.NoError(t, err)

		for _, ta := range items {
			seen[ta.TrustAnchorID]++
		}

		if next == "" {
			break
		}

		token = next
	}

	assert.Len(t, seen, total, "every trust anchor must be returned exactly once")
	for id, count := range seen {
		assert.Equalf(t, 1, count, "trust anchor %s returned %d times", id, count)
	}
}

// TestParity_ParsePageParams_InvalidMaxResults verifies a non-numeric maxResults
// query param yields a ValidationException rather than silently coercing to 0.
func TestParity_ParsePageParams_InvalidMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{name: "valid_numeric", query: "?maxResults=2", wantStatus: http.StatusOK},
		{name: "non_numeric", query: "?maxResults=abc", wantStatus: http.StatusBadRequest},
		{name: "mixed", query: "?maxResults=1a2", wantStatus: http.StatusBadRequest},
		{name: "empty_ignored", query: "?maxResults=", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
			h := rolesanywhere.NewHandler(b)

			rec := doREST(t, h, http.MethodGet, "/trustanchors"+tt.query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
