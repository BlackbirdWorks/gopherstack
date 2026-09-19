package dynamodbstreams_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dynamodbstreamssdk "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newStreamsClientForBackend wires a real aws-sdk-go-v2 DynamoDB Streams
// client to a handler over an already-populated backend (see
// newTestStreamsClient for the empty-backend variant).
func newStreamsClientForBackend(t *testing.T, backend *ddbbackend.InMemoryDB) *dynamodbstreamssdk.Client {
	t.Helper()

	h := dynamodbstreams.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dynamodbstreamssdk.NewFromConfig(cfg, func(o *dynamodbstreamssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Generic wire-format assertions (headers, content type) that apply to every
// successful DynamoDB Streams response, exercised here via ListStreams.

func TestHandler_WireFormat_CRC32Header(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"),
		"all successful responses must include X-Amz-Crc32 header")
}

func TestHandler_WireFormat_ContentType(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/x-amz-json-1.0",
		w.Header().Get("Content-Type"),
		"DynamoDB Streams responses must use application/x-amz-json-1.0")
}

// TestHandler_WireFormat_NoResultMetadataLeak proves ListStreams and
// GetShardIterator no longer marshal the raw SDK output struct (whose
// exported-but-empty ResultMetadata field leaked "ResultMetadata":{}, a
// member no real AWS response carries) (gopherstack-21my).
func TestHandler_WireFormat_NoResultMetadataLeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		body string
	}{
		{name: "liststreams", op: "ListStreams", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, _ := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			w := doRequest(t, handler, tt.op, tt.body)

			require.Equal(t, http.StatusOK, w.Code)
			assert.NotContains(t, w.Body.String(), "ResultMetadata")
		})
	}

	t.Run("getsharditerator", func(t *testing.T) {
		t.Parallel()

		db, streamARN := newTestBackend(t)
		handler := dynamodbstreams.NewHandler(db)

		descBody := `{"StreamArn":"` + streamARN + `"}`
		descW := doRequest(t, handler, "DescribeStream", descBody)
		require.Equal(t, http.StatusOK, descW.Code)

		var desc struct {
			StreamDescription struct {
				Shards []struct {
					ShardID string `json:"ShardId"` //nolint:tagliatelle // matches wire key exactly
				} `json:"Shards"`
			} `json:"StreamDescription"`
		}
		require.NoError(t, json.Unmarshal(descW.Body.Bytes(), &desc))
		require.NotEmpty(t, desc.StreamDescription.Shards)

		body := `{"StreamArn":"` + streamARN + `","ShardId":"` +
			desc.StreamDescription.Shards[0].ShardID +
			`","ShardIteratorType":"TRIM_HORIZON"}`
		w := doRequest(t, handler, "GetShardIterator", body)

		require.Equal(t, http.StatusOK, w.Code)
		assert.NotContains(t, w.Body.String(), "ResultMetadata")
	})
}

// TestRealClient_ListStreamsAndGetShardIterator drives ListStreams and
// GetShardIterator through the real aws-sdk-go-v2 client, proving the
// per-item fields (Stream.StreamArn/TableName, ShardIterator) round-trip --
// not just that the call succeeds (gopherstack-21my).
func TestRealClient_ListStreamsAndGetShardIterator(t *testing.T) {
	t.Parallel()

	backend, streamARN := newTestBackend(t)
	client := newStreamsClientForBackend(t, backend)
	ctx := t.Context()

	listOut, err := client.ListStreams(ctx, &dynamodbstreamssdk.ListStreamsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.Streams)

	var stream streamstypes.Stream
	for _, s := range listOut.Streams {
		if aws.ToString(s.StreamArn) == streamARN {
			stream = s

			break
		}
	}

	require.NotEmpty(t, aws.ToString(stream.StreamArn))
	assert.Equal(t, "StreamsTestTable", aws.ToString(stream.TableName))

	descOut, err := client.DescribeStream(ctx, &dynamodbstreamssdk.DescribeStreamInput{
		StreamArn: stream.StreamArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.StreamDescription.Shards)

	iterOut, err := client.GetShardIterator(ctx, &dynamodbstreamssdk.GetShardIteratorInput{
		StreamArn:         stream.StreamArn,
		ShardId:           descOut.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(iterOut.ShardIterator))
}
