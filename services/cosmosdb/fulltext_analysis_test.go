package cosmosdb_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func ftBackend(t *testing.T, lang string, texts ...string) *cosmosdb.InMemoryBackend {
	t.Helper()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("d")
	require.NoError(t, err)

	policy := fmt.Sprintf(`{"defaultLanguage":%q,"fullTextPaths":[{"path":"/text","language":%q}]}`, lang, lang)
	_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{
		ID: "c", PartitionKeyPath: "/pk",
		IndexingPolicy: json.RawMessage(
			`{"indexingMode":"consistent","includedPaths":[{"path":"/*"}],"excludedPaths":[],` +
				`"fullTextIndexes":[{"path":"/text"}]}`),
		FullTextPolicy: json.RawMessage(policy),
	})
	require.NoError(t, err)

	for i, text := range texts {
		_, err = b.CreateDocument("d", "c", map[string]any{"id": fmt.Sprintf("d%d", i), "pk": "k", "text": text}, false)
		require.NoError(t, err)
	}

	return b
}

func ftIDs(t *testing.T, b *cosmosdb.InMemoryBackend, query string) []string {
	t.Helper()

	res, err := b.RunQuery("d", "c", query, nil, "")
	require.NoError(t, err)

	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, r.(string))
	}

	return out
}

func TestFullText_StemmingStopWordsAndFuzzy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		lang  string
		texts []string
		query string
		want  []string
	}{
		{
			name: "stemming matches word forms", lang: "en-US",
			texts: []string{"He was running fast", "A slow walk", "She runs daily"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, "run")`, want: []string{"d0", "d2"},
		},
		{
			name: "the query side is stemmed too", lang: "en-US",
			texts: []string{"bicycles for sale"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, "bicycle")`, want: []string{"d0"},
		},
		{
			name:  "stop words are ignored in the query",
			lang:  "en-US",
			texts: []string{"red bicycle", "blue car"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContainsAll(c.text, "the", "red", "bicycle")`,
			want:  []string{"d0"},
		},
		{
			name: "a query of only stop words matches nothing", lang: "en-US",
			texts: []string{"the cat"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, "the")`, want: []string{},
		},
		{
			name: "fuzzy search within the distance", lang: "en-US",
			texts: []string{"a red bicycle", "a blue car"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, {"term":"bycycle","distance":2})`,
			want:  []string{"d0"},
		},
		{
			name:  "fuzzy search beyond the distance",
			lang:  "en-US",
			texts: []string{"a red bicycle"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, {"term":"bxcxcle","distance":1})`,
			want:  []string{},
		},
		{
			name:  "distance above two is not a valid search",
			lang:  "en-US",
			texts: []string{"a red bicycle"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, {"term":"bicycle","distance":3})`,
			want:  []string{},
		},
		{
			name: "german words are stemmed", lang: "de-DE",
			texts: []string{"Die Häuser sind groß", "Ein Auto"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, "Haus")`, want: []string{"d0"},
		},
		{
			name: "stop words are only removed in english", lang: "de-DE",
			texts: []string{"die katze"},
			query: `SELECT VALUE c.id FROM c WHERE FullTextContains(c.text, "die")`, want: []string{"d0"},
		},
		{
			name:  "score ranks the text with more occurrences first",
			lang:  "en-US",
			texts: []string{"bike", "bikes biking bike rides", "car"},
			query: `SELECT VALUE c.id FROM c ORDER BY RANK FullTextScore(c.text, "bike")`,
			want:  []string{"d1", "d0", "d2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ftBackend(t, tt.lang, tt.texts...)
			got := ftIDs(t, b, tt.query)

			if tt.name == "score ranks the text with more occurrences first" {
				require.NotEmpty(t, got)
				assert.Equal(t, tt.want[0], got[0])

				return
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
