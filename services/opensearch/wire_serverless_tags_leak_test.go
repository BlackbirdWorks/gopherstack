package opensearch_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// captureTransport records the raw bytes of the last HTTP response, then
// replays them so the real SDK deserializer still sees the full body.
type captureTransport struct {
	body []byte
}

func (c *captureTransport) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	resp.Body.Close()

	c.body = b
	resp.Body = io.NopCloser(bytes.NewReader(b))

	return resp, nil
}

// newCapturingTestServerlessClient is newTestServerlessClient plus a
// transport that stashes each response's raw bytes on capture.body, so a
// test can assert on the wire JSON while also proving the real client
// decodes it.
func newCapturingTestServerlessClient(
	t *testing.T,
	h *opensearch.Handler,
) (*opensearchserverless.Client, *captureTransport) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	capture := &captureTransport{}

	client := opensearchserverless.NewFromConfig(cfg, func(o *opensearchserverless.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// testServerlessHandlerWithDelay is testServerlessHandler with a configured
// processing delay, so collection lifecycle transitions (CREATING/DELETING)
// are observable instead of settling instantly.
func testServerlessHandlerWithDelay(t *testing.T, delay time.Duration) *opensearch.Handler {
	t.Helper()

	bk := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	bk.SetProcessingDelay(delay)

	return opensearch.NewHandler(bk)
}

// TestServerlessCollectionWire_TagsAndStatusUntilStripped covers every AOSS
// wire path that marshals ServerlessCollection directly (gopherstack-7l0v3):
// real CollectionDetail/CollectionSummary/CreateCollectionDetail/
// DeleteCollectionDetail declare neither tags nor statusUntil, so the raw
// body must not carry either key even though the collection has tags and an
// internal lifecycle deadline persisted. Each case also decodes the
// response through the real typed client to prove the strip didn't break
// the shape.
func TestServerlessCollectionWire_TagsAndStatusUntilStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *opensearchserverless.Client)
		name string
	}{
		{
			name: "create_collection",
			run: func(t *testing.T, client *opensearchserverless.Client) {
				t.Helper()

				out, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
					Name: aws.String("wire-tags-create"),
					Type: aosstypes.CollectionTypeVectorsearch,
					Tags: []aosstypes.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
				})
				require.NoError(t, err)
				require.NotNil(t, out.CreateCollectionDetail)
				assert.Equal(t, "wire-tags-create", aws.ToString(out.CreateCollectionDetail.Name))
			},
		},
		{
			name: "delete_collection",
			run: func(t *testing.T, client *opensearchserverless.Client) {
				t.Helper()

				id := createTaggedCollectionForWireTest(t, client, "wire-tags-delete")

				out, err := client.DeleteCollection(t.Context(), &opensearchserverless.DeleteCollectionInput{
					Id: aws.String(id),
				})
				require.NoError(t, err)
				require.NotNil(t, out.DeleteCollectionDetail)
			},
		},
		{
			name: "batch_get_collection_detail",
			run: func(t *testing.T, client *opensearchserverless.Client) {
				t.Helper()

				id := createTaggedCollectionForWireTest(t, client, "wire-tags-batch-detail")

				out, err := client.BatchGetCollection(t.Context(), &opensearchserverless.BatchGetCollectionInput{
					Ids: []string{id},
				})
				require.NoError(t, err)
				require.Len(t, out.CollectionDetails, 1)
			},
		},
		{
			name: "list_collections_summary",
			run: func(t *testing.T, client *opensearchserverless.Client) {
				t.Helper()

				createTaggedCollectionForWireTest(t, client, "wire-tags-list")

				out, err := client.ListCollections(t.Context(), &opensearchserverless.ListCollectionsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.CollectionSummaries)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// A nonzero processing delay keeps the collection observably
			// CREATING/DELETING with a nonzero StatusUntil at marshal time,
			// so the leak assertion below is meaningful (with no delay,
			// StatusUntil settles back to zero before the response is
			// built and omitzero would hide it either way).
			h := testServerlessHandlerWithDelay(t, 5*time.Minute)
			client, capture := newCapturingTestServerlessClient(t, h)

			tt.run(t, client)

			require.NotEmpty(t, capture.body)
			assert.NotContains(t, string(capture.body), `"tags"`,
				"response must not carry the persisted-only tags field on the wire")
			assert.NotContains(t, string(capture.body), `"statusUntil"`,
				"response must not carry the internal lifecycle statusUntil field on the wire")
		})
	}
}

// createTaggedCollectionForWireTest creates a collection with a non-empty
// tags list through the real client, so later reads of it have something to
// leak if the wire strip regresses. Returns the collection id.
func createTaggedCollectionForWireTest(t *testing.T, client *opensearchserverless.Client, name string) string {
	t.Helper()

	out, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String(name),
		Type: aosstypes.CollectionTypeVectorsearch,
		Tags: []aosstypes.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	return aws.ToString(out.CreateCollectionDetail.Id)
}
