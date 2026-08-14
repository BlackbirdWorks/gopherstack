package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/document"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestGetIndex_IndexSchema_RealClient drives GetIndex through the real
// aws-sdk-go-v2 client. GetIndexOutput's sole field is IndexSchema, a
// required smithy document (api_op_GetIndex.go: "The JSON schema of the
// index including mappings, settings, and semantic enrichment
// configuration. This member is required."). The handler previously reused
// the IndexName/IndexStatus/Mappings/Settings/Aliases/DocumentCount shape
// (no real op has that shape), so a real client's IndexSchema always
// decoded nil.
func TestGetIndex_IndexSchema_RealClient(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(backend)
	client := newTestOpenSearchClient(t, h)

	_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("getindex-domain"),
	})
	require.NoError(t, err)

	schema := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"title": map[string]any{"type": "text"},
			},
		},
	}

	_, err = client.CreateIndex(t.Context(), &opensearchsdk.CreateIndexInput{
		DomainName:  aws.String("getindex-domain"),
		IndexName:   aws.String("my-index"),
		IndexSchema: document.NewLazyDocument(schema),
	})
	require.NoError(t, err)

	out, err := client.GetIndex(t.Context(), &opensearchsdk.GetIndexInput{
		DomainName: aws.String("getindex-domain"),
		IndexName:  aws.String("my-index"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.IndexSchema)

	got := decodeDocument(t, out.IndexSchema)
	assert.Contains(t, got, `"title":{"type":"text"}`)
}

// TestVpcEndpointListOps_NextTokenPresent_RealClient drives three
// ListVpcEndpoint* operations through the real client. NextToken is a
// required member of each (api_op_ListVpcEndpoints.go,
// api_op_ListVpcEndpointsForDomain.go, api_op_ListVpcEndpointAccess.go —
// "This member is required."). gopherstack's backend for these ops is
// single-page, so the correct value is always an empty string rather than
// omitted; before the fix the response maps didn't carry the key at all, so
// a real client's *string NextToken always decoded nil.
func TestVpcEndpointListOps_NextTokenPresent_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       func(t *testing.T, client *opensearchsdk.Client, domainName string) *string
		name       string
		domainName string
	}{
		{
			name:       "listvpcendpoints",
			domainName: "nexttoken-endpoints",
			call: func(t *testing.T, client *opensearchsdk.Client, _ string) *string {
				t.Helper()

				out, err := client.ListVpcEndpoints(t.Context(), &opensearchsdk.ListVpcEndpointsInput{})
				require.NoError(t, err)

				return out.NextToken
			},
		},
		{
			name:       "listvpcendpointsfordomain",
			domainName: "nexttoken-fordomain",
			call: func(t *testing.T, client *opensearchsdk.Client, domainName string) *string {
				t.Helper()

				out, err := client.ListVpcEndpointsForDomain(t.Context(), &opensearchsdk.ListVpcEndpointsForDomainInput{
					DomainName: aws.String(domainName),
				})
				require.NoError(t, err)

				return out.NextToken
			},
		},
		{
			name:       "listvpcendpointaccess",
			domainName: "nexttoken-access",
			call: func(t *testing.T, client *opensearchsdk.Client, domainName string) *string {
				t.Helper()

				out, err := client.ListVpcEndpointAccess(t.Context(), &opensearchsdk.ListVpcEndpointAccessInput{
					DomainName: aws.String(domainName),
				})
				require.NoError(t, err)

				return out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := opensearch.NewInMemoryBackend(testAccountID, testRegion)
			h := opensearch.NewHandler(backend)
			client := newTestOpenSearchClient(t, h)

			domainName := tt.domainName

			_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
				DomainName: aws.String(domainName),
			})
			require.NoError(t, err)

			gotNextToken := tt.call(t, client, domainName)

			require.NotNil(t, gotNextToken)
			assert.Empty(t, *gotNextToken)
		})
	}
}
