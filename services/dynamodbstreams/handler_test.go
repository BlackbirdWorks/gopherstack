package dynamodbstreams_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// newTestBackend creates a DynamoDB InMemoryDB with a streams-enabled table
// and one item, returning the backend and the stream ARN.
func newTestBackend(t *testing.T) (*ddbbackend.InMemoryDB, string) {
	t.Helper()

	db := ddbbackend.NewInMemoryDB()
	ctx := context.Background()

	const tableName = "StreamsTestTable"

	_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item-1"},
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	return db, table.StreamARN
}

// doRequest sends a POST request to the DynamoDBStreams handler.
func doRequest(
	t *testing.T,
	handler *dynamodbstreams.Handler,
	action string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810."+action)
	w := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, w)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return w
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches DynamoDBStreams prefix",
			target:    "DynamoDBStreams_20120810.ListStreams",
			wantMatch: true,
		},
		{
			name:      "does not match DynamoDB prefix",
			target:    "DynamoDB_20120810.ListTables",
			wantMatch: false,
		},
		{
			name:      "does not match empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := dynamodbstreams.NewHandler(nil)
			matcher := handler.RouteMatcher()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ListStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		body             string
		wantBodyContains string
		wantStatusCode   int
	}{
		{
			name:             "lists streams for table",
			body:             `{"TableName":"StreamsTestTable"}`,
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "StreamsTestTable",
		},
		{
			name:             "lists all streams with empty body",
			body:             `{}`,
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "Streams",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, _ := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			w := doRequest(t, handler, "ListStreams", tt.body)

			assert.Equal(t, tt.wantStatusCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantBodyContains)
		})
	}
}

func TestHandler_DescribeStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantBodyContains string
		wantStatusCode   int
	}{
		{
			name:             "describes stream by ARN",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "StreamsTestTable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, streamARN := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			body := `{"StreamArn":"` + streamARN + `"}`
			w := doRequest(t, handler, "DescribeStream", body)

			assert.Equal(t, tt.wantStatusCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantBodyContains)
		})
	}
}

func TestHandler_GetShardIterator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		iterType       streamstypes.ShardIteratorType
		wantStatusCode int
	}{
		{
			name:           "gets trim horizon iterator",
			iterType:       streamstypes.ShardIteratorTypeTrimHorizon,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "gets latest iterator",
			iterType:       streamstypes.ShardIteratorTypeLatest,
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, streamARN := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			const sid = "shardId-00000000000000000001-00000001"
			body := `{"StreamArn":"` + streamARN +
				`","ShardId":"` + sid +
				`","ShardIteratorType":"` + string(tt.iterType) + `"}`
			w := doRequest(t, handler, "GetShardIterator", body)

			assert.Equal(t, tt.wantStatusCode, w.Code)
			assert.Contains(t, w.Body.String(), "ShardIterator")
		})
	}
}

func TestHandler_GetRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantBodyContains string
		wantStatusCode   int
	}{
		{
			name:             "returns records for iterator",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "Records",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, streamARN := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			// First get a shard iterator
			const shardID = "shardId-00000000000000000001-00000001"
			iterBody := `{"StreamArn":"` + streamARN +
				`","ShardId":"` + shardID + `","ShardIteratorType":"TRIM_HORIZON"}`
			iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
			require.Equal(t, http.StatusOK, iterResp.Code)

			var iterOut map[string]any
			require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))

			shardIterRaw, ok := iterOut["ShardIterator"]
			require.True(t, ok, "ShardIterator key expected in response")
			shardIter, ok := shardIterRaw.(string)
			require.True(t, ok, "ShardIterator should be a string")

			// Then get records
			recBody := `{"ShardIterator":"` + shardIter + `"}`
			w := doRequest(t, handler, "GetRecords", recBody)

			assert.Equal(t, tt.wantStatusCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantBodyContains)
		})
	}
}

func TestHandler_NilBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
	}{
		{"ListStreams with nil backend", "ListStreams"},
		{"DescribeStream with nil backend", "DescribeStream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := dynamodbstreams.NewHandler(nil)
			w := doRequest(t, handler, tt.operation, `{}`)

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	handler := dynamodbstreams.NewHandler(nil)

	assert.Equal(t, "DynamoDBStreams", handler.Name())
	assert.Equal(t, "dynamodbstreams", handler.ChaosServiceName())
	assert.ElementsMatch(t, []string{
		"DescribeStream",
		"GetRecords",
		"GetShardIterator",
		"ListStreams",
	}, handler.GetSupportedOperations())
}
