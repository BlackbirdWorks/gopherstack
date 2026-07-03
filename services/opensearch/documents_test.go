package opensearch_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// seedIndexDomain is the domain name used by the document tests.
const seedIndexDomain = "dom"

// seedIndex creates the test domain and an index, returning the backend.
func seedIndex(t *testing.T, index string) *opensearch.InMemoryBackend {
	t.Helper()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: seedIndexDomain})
	require.NoError(t, err)
	_, err = b.CreateIndex(seedIndexDomain, index, nil, nil, nil)
	require.NoError(t, err)

	return b
}

// TestIndexDocumentLifecycle covers indexing, counting, fetching and deleting
// real documents, and the accurate document count reflected in GetIndex.
func TestIndexDocumentLifecycle(t *testing.T) {
	t.Parallel()

	b := seedIndex(t, "products")

	// Explicit ID create.
	id, created, err := b.IndexDocument("dom", "products", "p1", map[string]any{
		"name": "widget", "price": float64(10),
	})
	require.NoError(t, err)
	assert.Equal(t, "p1", id)
	assert.True(t, created)

	// Auto-generated ID.
	id2, created2, err := b.IndexDocument("dom", "products", "", map[string]any{"name": "gadget"})
	require.NoError(t, err)
	assert.NotEmpty(t, id2)
	assert.True(t, created2)

	// Count and index metadata reflect the two documents.
	count, err := b.CountDocuments("dom", "products")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	idx, err := b.GetIndex("dom", "products")
	require.NoError(t, err)
	assert.Equal(t, 2, idx.DocumentCount)

	// Domain-wide stats aggregate the count.
	assert.Equal(t, 2, b.DomainDocumentCount("dom"))

	health, err := b.GetDomainHealth("dom")
	require.NoError(t, err)
	assert.Equal(t, 2, health["DocumentCount"])

	// Re-index existing ID updates, does not create.
	_, createdAgain, err := b.IndexDocument("dom", "products", "p1", map[string]any{"name": "widget2"})
	require.NoError(t, err)
	assert.False(t, createdAgain)

	got, err := b.GetDocument("dom", "products", "p1")
	require.NoError(t, err)
	assert.Equal(t, "widget2", got["name"])

	// Delete decrements the count.
	require.NoError(t, b.DeleteDocument("dom", "products", "p1"))
	count, err = b.CountDocuments("dom", "products")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	_, err = b.GetDocument("dom", "products", "p1")
	require.ErrorIs(t, err, opensearch.ErrConnectionNotFound)
}

// TestIndexDocumentErrors covers the not-found error paths.
func TestIndexDocumentErrors(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)

	tests := []struct {
		run  func() error
		name string
	}{
		{
			name: "index_unknown_domain",
			run: func() error {
				_, _, err := b.IndexDocument("nope", "i", "1", map[string]any{})

				return err
			},
		},
		{
			name: "get_unknown_index",
			run: func() error {
				_, err := b.GetDocument("nope", "i", "1")

				return err
			},
		},
		{
			name: "delete_unknown_doc",
			run: func() error {
				return b.DeleteDocument("nope", "i", "1")
			},
		},
		{
			name: "search_unknown_index",
			run: func() error {
				_, err := b.SearchIndex("nope", "i", nil, 10)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.run())
		})
	}
}

// TestSearchIndex covers the bounded match_all / term / match query engine over
// real stored documents.
func TestSearchIndex(t *testing.T) {
	t.Parallel()

	b := seedIndex(t, "books")
	docs := map[string]map[string]any{
		"b1": {"title": "The Go Programming Language", "author": "Donovan", "year": float64(2015)},
		"b2": {"title": "Effective Go", "author": "Team", "year": float64(2020)},
		"b3": {"title": "Rust in Action", "author": "McNamara", "year": float64(2021)},
	}
	for id, doc := range docs {
		_, _, err := b.IndexDocument("dom", "books", id, doc)
		require.NoError(t, err)
	}

	tests := []struct {
		query   map[string]any
		name    string
		wantIDs []string
	}{
		{
			name:    "match_all",
			query:   map[string]any{"match_all": map[string]any{}},
			wantIDs: []string{"b1", "b2", "b3"},
		},
		{
			name:    "empty_query_matches_all",
			query:   nil,
			wantIDs: []string{"b1", "b2", "b3"},
		},
		{
			name:    "term_exact_author",
			query:   map[string]any{"term": map[string]any{"author": "Donovan"}},
			wantIDs: []string{"b1"},
		},
		{
			name:    "term_numeric_year",
			query:   map[string]any{"term": map[string]any{"year": float64(2021)}},
			wantIDs: []string{"b3"},
		},
		{
			name:    "match_substring_case_insensitive",
			query:   map[string]any{"match": map[string]any{"title": "go"}},
			wantIDs: []string{"b1", "b2"},
		},
		{
			name:    "term_no_match",
			query:   map[string]any{"term": map[string]any{"author": "Nobody"}},
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := b.SearchIndex("dom", "books", tt.query, 100)
			require.NoError(t, err)

			var gotIDs []string
			for _, hit := range res.Hits {
				gotIDs = append(gotIDs, hit.ID)
				// Hits must carry the real stored source, never a synthetic value.
				assert.NotNil(t, hit.Source)
			}

			sort.Strings(gotIDs)

			if len(tt.wantIDs) == 0 {
				assert.Empty(t, gotIDs)
			} else {
				assert.Equal(t, tt.wantIDs, gotIDs)
			}

			assert.Equal(t, len(tt.wantIDs), res.Total)
		})
	}
}

// TestSearchIndexSizeBound asserts the size parameter bounds the returned hits
// while Total still reflects the full match count.
func TestSearchIndexSizeBound(t *testing.T) {
	t.Parallel()

	b := seedIndex(t, "big")
	for i := range 25 {
		_, _, err := b.IndexDocument("dom", "big", string(rune('a'+i)), map[string]any{"n": float64(i)})
		require.NoError(t, err)
	}

	res, err := b.SearchIndex("dom", "big", map[string]any{"match_all": map[string]any{}}, 5)
	require.NoError(t, err)
	assert.Len(t, res.Hits, 5, "size must bound returned hits")
	assert.Equal(t, 25, res.Total, "total must reflect the full match count")
}

// TestSearchIndexUnsupportedQuery asserts unsupported clauses return a
// ValidationException rather than silently matching everything.
func TestSearchIndexUnsupportedQuery(t *testing.T) {
	t.Parallel()

	b := seedIndex(t, "q")
	_, _, err := b.IndexDocument("dom", "q", "1", map[string]any{"f": "v"})
	require.NoError(t, err)

	_, err = b.SearchIndex("dom", "q", map[string]any{"range": map[string]any{"f": "v"}}, 10)
	require.ErrorIs(t, err, opensearch.ErrValidation)
}
