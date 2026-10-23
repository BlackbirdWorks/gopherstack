package cosmosdb_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func searchProperties(vecIndex azcosmos.VectorIndexType, withIndexes bool) azcosmos.ContainerProperties {
	props := azcosmos.ContainerProperties{
		ID:                     "c",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
		VectorEmbeddingPolicy: &azcosmos.VectorEmbeddingPolicy{VectorEmbeddings: []azcosmos.VectorEmbedding{{
			Path: "/vec", DataType: azcosmos.VectorDataTypeFloat32,
			DistanceFunction: azcosmos.VectorDistanceFunctionCosine, Dimensions: 3,
		}}},
		FullTextPolicy: &azcosmos.FullTextPolicy{
			DefaultLanguage: "en-US", FullTextPaths: []azcosmos.FullTextPath{{Path: "/text", Language: "en-US"}},
		},
	}

	if withIndexes {
		props.IndexingPolicy = &azcosmos.IndexingPolicy{
			IndexingMode:    azcosmos.IndexingModeConsistent,
			IncludedPaths:   []azcosmos.IncludedPath{{Path: "/*"}},
			ExcludedPaths:   []azcosmos.ExcludedPath{{Path: "/vec/*"}},
			VectorIndexes:   []azcosmos.VectorIndex{{Path: "/vec", Type: vecIndex}},
			FullTextIndexes: []azcosmos.FullTextIndex{{Path: "/text"}},
		}
	}

	return props
}

func seedSearchContainer(t *testing.T, props azcosmos.ContainerProperties) *azcosmos.ContainerClient {
	t.Helper()

	client := sdkClient(t, false, cosmosdb.DefaultMasterKey)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "d"}, nil)
	require.NoError(t, err)

	db, err := client.NewDatabase("d")
	require.NoError(t, err)

	_, err = db.CreateContainer(ctx, props, nil)
	require.NoError(t, err)

	c, err := client.NewContainer("d", "c")
	require.NoError(t, err)

	docs := []map[string]any{
		{"id": "a", "pk": "p", "vec": []float64{1, 0, 0}, "text": "the quick brown fox jumps over the lazy dog"},
		{"id": "b", "pk": "p", "vec": []float64{0.9, 0.1, 0}, "text": "a fox is a small wild animal, fox fox fox"},
		{"id": "c", "pk": "p", "vec": []float64{0, 1, 0}, "text": "dogs and cats are pets"},
		{"id": "d", "pk": "p", "vec": []float64{0, 0, 1}, "text": "birds fly high"},
	}

	for _, d := range docs {
		body, mErr := json.Marshal(d)
		require.NoError(t, mErr)

		_, err = c.CreateItem(ctx, azcosmos.NewPartitionKeyString("p"), body, nil)
		require.NoError(t, err)
	}

	return c
}

func queryIDs(t *testing.T, c *azcosmos.ContainerClient, query string) ([]string, error) {
	t.Helper()

	pager := c.NewQueryItemsPager(query, azcosmos.NewPartitionKey(), nil)

	var ids []string

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		if err != nil {
			return nil, err
		}

		for _, item := range page.Items {
			var v struct {
				ID string `json:"id"`
			}

			require.NoError(t, json.Unmarshal(item, &v))

			ids = append(ids, v.ID)
		}
	}

	return ids, nil
}

func TestSDK_VectorSearch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		index   azcosmos.VectorIndexType
		query   string
		want    []string
		wantErr bool
	}{
		{
			name: "cosine_flat", index: azcosmos.VectorIndexTypeFlat,
			query: "SELECT TOP 2 c.id FROM c ORDER BY VectorDistance(c.vec, [1,0,0])", want: []string{"a", "b"},
		},
		{
			name: "cosine_quantized", index: azcosmos.VectorIndexTypeQuantizedFlat,
			query: "SELECT TOP 3 c.id FROM c ORDER BY VectorDistance(c.vec, [0,1,0])", want: []string{"c", "b", "a"},
		},
		{
			name: "euclidean_option_ascending", index: azcosmos.VectorIndexTypeDiskANN,
			query: "SELECT TOP 2 c.id FROM c ORDER BY VectorDistance(c.vec, [0,0,1], false, " +
				"{distanceFunction:'euclidean'})",
			want: []string{"d", "b"},
		},
		{
			name: "dotproduct", index: azcosmos.VectorIndexTypeFlat,
			query: "SELECT TOP 1 c.id FROM c ORDER BY VectorDistance(c.vec, [1,0,0], true, " +
				"{distanceFunction:'dotproduct'})",
			want: []string{"a"},
		},
		{
			name:  "where_threshold",
			index: azcosmos.VectorIndexTypeFlat,
			query: "SELECT c.id FROM c WHERE c.pk = 'p' AND VectorDistance(c.vec, [1,0,0]) > 0.9 ORDER BY c.id",
			want:  []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := seedSearchContainer(t, searchProperties(tt.index, true))

			got, err := queryIDs(t, c, tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSDK_VectorSearchNeedsIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "order_without_index",
			query:   "SELECT TOP 1 c.id FROM c ORDER BY VectorDistance(c.vec, [1,0,0])",
			wantErr: true,
		},
		{name: "brute_force", query: "SELECT TOP 1 c.id FROM c ORDER BY VectorDistance(c.vec, [1,0,0], true)"},
		{name: "select_only", query: "SELECT c.id, VectorDistance(c.vec, [1,0,0]) AS s FROM c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := seedSearchContainer(t, searchProperties("", false))

			_, err := queryIDs(t, c, tt.query)
			if tt.wantErr {
				respErr, ok := errAsResponse(err)
				require.True(t, ok)
				assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)

				return
			}

			require.NoError(t, err)
		})
	}
}

func errAsResponse(err error) (*azcore.ResponseError, bool) {
	re, ok := errors.AsType[*azcore.ResponseError](err)

	return re, ok
}

func TestSDK_FullTextSearch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "contains",
			query: "SELECT c.id FROM c WHERE FullTextContains(c.text, 'fox') ORDER BY c.id",
			want:  []string{"a", "b"},
		},
		{
			name:  "contains_all",
			query: "SELECT c.id FROM c WHERE FullTextContainsAll(c.text, 'fox', 'dog') ORDER BY c.id",
			want:  []string{"a"},
		},
		{
			name:  "contains_any",
			query: "SELECT c.id FROM c WHERE FullTextContainsAny(c.text, 'cats', 'birds') ORDER BY c.id",
			want:  []string{"c", "d"},
		},
		{
			name:  "rank_score",
			query: "SELECT TOP 2 c.id FROM c ORDER BY RANK FullTextScore(c.text, 'fox')",
			want:  []string{"b", "a"},
		},
		{
			name:  "rank_array_terms",
			query: "SELECT TOP 1 c.id FROM c ORDER BY RANK FullTextScore(c.text, ['cats', 'pets'])",
			want:  []string{"c"},
		},
		{
			name: "rrf_hybrid",
			query: "SELECT TOP 2 c.id FROM c ORDER BY RANK RRF(FullTextScore(c.text, 'fox'), " +
				"VectorDistance(c.vec, [1,0,0]))",
			want: []string{"a", "b"},
		},
		{
			name: "rrf_weighted",
			query: "SELECT TOP 1 c.id FROM c ORDER BY RANK RRF(FullTextScore(c.text, 'fox'), " +
				"VectorDistance(c.vec, [1,0,0]), [5, 1])",
			want: []string{"b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := seedSearchContainer(t, searchProperties(azcosmos.VectorIndexTypeFlat, true))

			got, err := queryIDs(t, c, tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSDK_FullTextRankNeedsIndex(t *testing.T) {
	t.Parallel()

	c := seedSearchContainer(t, searchProperties("", false))

	_, err := queryIDs(t, c, "SELECT TOP 1 c.id FROM c ORDER BY RANK FullTextScore(c.text, 'fox')")
	respErr, ok := errAsResponse(err)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)

	got, err := queryIDs(t, c, "SELECT c.id FROM c WHERE FullTextContains(c.text, 'birds')")
	require.NoError(t, err)
	assert.Equal(t, []string{"d"}, got)
}

func TestSDK_SearchPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*azcosmos.ContainerProperties)
		name   string
	}{
		{
			name:   "bad_dimensions",
			mutate: func(p *azcosmos.ContainerProperties) { p.VectorEmbeddingPolicy.VectorEmbeddings[0].Dimensions = 0 },
		},
		{
			name:   "huge_dimensions",
			mutate: func(p *azcosmos.ContainerProperties) { p.VectorEmbeddingPolicy.VectorEmbeddings[0].Dimensions = 5000 },
		},
		{name: "bad_function", mutate: func(p *azcosmos.ContainerProperties) {
			p.VectorEmbeddingPolicy.VectorEmbeddings[0].DistanceFunction = "manhattan"
		}},
		{
			name:   "bad_index_type",
			mutate: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.VectorIndexes[0].Type = "hnsw" },
		},
		{
			name:   "index_path_unknown",
			mutate: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.VectorIndexes[0].Path = "/other" },
		},
		{
			name:   "ft_index_path_unknown",
			mutate: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.FullTextIndexes[0].Path = "/other" },
		},
		{
			name:   "bad_language",
			mutate: func(p *azcosmos.ContainerProperties) { p.FullTextPolicy.DefaultLanguage = "english" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := sdkClient(t, false, cosmosdb.DefaultMasterKey)
			_, err := client.CreateDatabase(t.Context(), azcosmos.DatabaseProperties{ID: "d"}, nil)
			require.NoError(t, err)

			db, err := client.NewDatabase("d")
			require.NoError(t, err)

			props := searchProperties(azcosmos.VectorIndexTypeFlat, true)
			tt.mutate(&props)

			_, err = db.CreateContainer(t.Context(), props, nil)
			respErr, ok := errAsResponse(err)
			require.True(t, ok)
			assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
		})
	}
}

func TestSDK_SearchPoliciesImmutable(t *testing.T) {
	t.Parallel()

	c := seedSearchContainer(t, searchProperties(azcosmos.VectorIndexTypeFlat, true))

	props := searchProperties(azcosmos.VectorIndexTypeFlat, true)
	_, err := c.Replace(t.Context(), props, nil)
	require.NoError(t, err)

	props.VectorEmbeddingPolicy.VectorEmbeddings[0].Dimensions = 8
	_, err = c.Replace(t.Context(), props, nil)
	respErr, ok := errAsResponse(err)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
}
