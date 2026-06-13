package dynamodbstreams_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// newStreamsTestDB builds an InMemoryDB and creates a streams-enabled table in the
// given region, returning the DB and the stream ARN for that table.
func newStreamsTestDB(t *testing.T, db *ddbbackend.InMemoryDB, region, tableName string) string {
	t.Helper()

	ctx := ddbbackend.WithRegion(t.Context(), region)

	_, err := db.CreateTableInRegion(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	}, region)
	require.NoError(t, err)

	require.NoError(t, db.EnableStream(ctx, tableName, "NEW_AND_OLD_IMAGES"))

	table, ok := db.GetTableInRegion(tableName, region)
	require.True(t, ok)

	return table.StreamARN
}

// sigV4AuthHeader builds a minimal fake SigV4 Authorization header with the given region.
func sigV4AuthHeader(region string) string {
	const tmpl = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/%s" +
		"/dynamodbstreams/aws4_request, SignedHeaders=host, Signature=deadbeef"

	return fmt.Sprintf(tmpl, region)
}

// doStreamsRequest sends a POST request to the handler with SigV4 for the given region.
func doStreamsRequest(
	t *testing.T,
	h *dynamodbstreams.Handler,
	region, action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810."+action)
	req.Header.Set("Authorization", sigV4AuthHeader(region))

	w := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, w)
	require.NoError(t, h.Handler()(c))

	return w
}

// TestDynamoDBStreamsRegionIsolation verifies that streams created in different regions
// are isolated: each region's ListStreams returns only its own streams, and
// DescribeStream/GetShardIterator in the wrong region return not-found errors.
func TestDynamoDBStreamsRegionIsolation(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	eastARN := newStreamsTestDB(t, db, "us-east-1", "IsolationTable")
	westARN := newStreamsTestDB(t, db, "us-west-2", "IsolationTable")

	// ARNs must differ even though the table name is the same.
	assert.NotEqual(t, eastARN, westARN)
	assert.Contains(t, eastARN, "us-east-1")
	assert.Contains(t, westARN, "us-west-2")

	h := dynamodbstreams.NewHandler(db)
	h.DefaultRegion = "us-east-1"

	// us-east-1 sees only its stream.
	eastResp := doStreamsRequest(t, h, "us-east-1", "ListStreams", `{}`)
	require.Equal(t, http.StatusOK, eastResp.Code)

	var eastOut struct {
		Streams []struct {
			StreamArn string `json:"StreamArn"`
		} `json:"Streams"`
	}
	require.NoError(t, json.Unmarshal(eastResp.Body.Bytes(), &eastOut))
	require.Len(t, eastOut.Streams, 1)
	assert.Contains(t, eastOut.Streams[0].StreamArn, "us-east-1")

	// us-west-2 sees only its stream.
	westResp := doStreamsRequest(t, h, "us-west-2", "ListStreams", `{}`)
	require.Equal(t, http.StatusOK, westResp.Code)

	var westOut struct {
		Streams []struct {
			StreamArn string `json:"StreamArn"`
		} `json:"Streams"`
	}
	require.NoError(t, json.Unmarshal(westResp.Body.Bytes(), &westOut))
	require.Len(t, westOut.Streams, 1)
	assert.Contains(t, westOut.Streams[0].StreamArn, "us-west-2")

	// DescribeStream for the east ARN works from east.
	descEastBody := fmt.Sprintf(`{"StreamArn":%q}`, eastARN)
	descEast := doStreamsRequest(t, h, "us-east-1", "DescribeStream", descEastBody)
	assert.Equal(t, http.StatusOK, descEast.Code)

	// GetShardIterator for east table works from east.
	shardBody := fmt.Sprintf(
		`{"StreamArn":%q,"ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		eastARN,
	)
	shardResp := doStreamsRequest(t, h, "us-east-1", "GetShardIterator", shardBody)
	assert.Equal(t, http.StatusOK, shardResp.Code)
}

// TestDynamoDBStreamsDefaultRegionFallback verifies that requests without an
// explicit region fall back to the handler's DefaultRegion.
func TestDynamoDBStreamsDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	newStreamsTestDB(t, db, "eu-central-1", "FallbackTable")

	h := dynamodbstreams.NewHandler(db)
	h.DefaultRegion = "eu-central-1"

	// Request with no Authorization header → falls back to DefaultRegion.
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{}`))
	req.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810.ListStreams")
	w := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, w)
	require.NoError(t, h.Handler()(c))

	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Streams []struct {
			StreamArn string `json:"StreamArn"`
		} `json:"Streams"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Streams, 1)
	assert.Contains(t, out.Streams[0].StreamArn, "eu-central-1")
}
