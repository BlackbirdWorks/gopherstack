package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/document"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// Test_SDKRoundTrip_CreateIndex_IndexSchema proves that the real
// CreateIndex request -- a single IndexSchema smithy document
// (api_op_CreateIndex.go:37-60, opensearch@v1.75.4) -- is actually read and
// stored, and that the response carries the required Status field. Before
// the fix, the handler decoded nonexistent top-level Mappings/Settings/
// Aliases fields, so IndexSchema was silently dropped and the response
// never set Status at all (a real client's *CreateIndexOutput.Status stayed
// the empty string).
func Test_SDKRoundTrip_CreateIndex_IndexSchema(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(backend)
	client := newTestOpenSearchClient(t, h)

	_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("index-schema-domain"),
	})
	require.NoError(t, err)

	schema := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"title": map[string]any{"type": "text"},
			},
		},
		"settings": map[string]any{
			"number_of_shards": "1",
		},
	}

	out, err := client.CreateIndex(t.Context(), &opensearchsdk.CreateIndexInput{
		DomainName:  aws.String("index-schema-domain"),
		IndexName:   aws.String("my-index"),
		IndexSchema: document.NewLazyDocument(schema),
	})
	require.NoError(t, err)
	assert.Equal(t, types.IndexStatusCreated, out.Status)

	idx, err := backend.GetIndex("index-schema-domain", "my-index")
	require.NoError(t, err)
	require.NotNil(t, idx.IndexSchema)

	stored, ok := idx.IndexSchema.(map[string]any)
	require.True(t, ok, "IndexSchema must be stored as the decoded document, not dropped")
	mappings, ok := stored["mappings"].(map[string]any)
	require.True(t, ok)
	properties, ok := mappings["properties"].(map[string]any)
	require.True(t, ok)
	title, ok := properties["title"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "text", title["type"])
}

// Test_SDKRoundTrip_UpdateIndex_IndexSchema proves that UpdateIndex's
// IndexSchema (the request's only body member -- api_op_UpdateIndex.go:32-52)
// is read and replaces the stored schema, and that the response Status is
// "UPDATED".
func Test_SDKRoundTrip_UpdateIndex_IndexSchema(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(backend)
	client := newTestOpenSearchClient(t, h)

	_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("index-update-domain"),
	})
	require.NoError(t, err)

	_, err = client.CreateIndex(t.Context(), &opensearchsdk.CreateIndexInput{
		DomainName:  aws.String("index-update-domain"),
		IndexName:   aws.String("my-index"),
		IndexSchema: document.NewLazyDocument(map[string]any{"settings": map[string]any{"number_of_shards": "1"}}),
	})
	require.NoError(t, err)

	updateSchema := map[string]any{
		"settings": map[string]any{"number_of_replicas": "2"},
	}

	out, err := client.UpdateIndex(t.Context(), &opensearchsdk.UpdateIndexInput{
		DomainName:  aws.String("index-update-domain"),
		IndexName:   aws.String("my-index"),
		IndexSchema: document.NewLazyDocument(updateSchema),
	})
	require.NoError(t, err)
	assert.Equal(t, types.IndexStatusUpdated, out.Status)

	idx, err := backend.GetIndex("index-update-domain", "my-index")
	require.NoError(t, err)
	require.NotNil(t, idx.IndexSchema)

	stored, ok := idx.IndexSchema.(map[string]any)
	require.True(t, ok)
	settings, ok := stored["settings"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "2", settings["number_of_replicas"])
}
