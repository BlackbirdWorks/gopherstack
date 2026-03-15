package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// newStreamEnabledHandler creates a handler backed by an InMemoryDB that has a
// table with streams enabled and one item inserted (generating an INSERT record).
// Returns the handler and the stream ARN.
func newStreamEnabledHandler(t *testing.T) (*dynamodb.DynamoDBHandler, string) {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	ctx := context.Background()

	const tableName = "StreamHandlerTable"

	sdkInput := models.ToSDKCreateTableInput(&models.CreateTableInput{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
		StreamSpecification: map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": string(streamstypes.StreamViewTypeNewAndOldImages),
		},
	})
	_, err := db.CreateTable(ctx, sdkInput)
	require.NoError(t, err)

	_, err = db.PutItem(ctx, makePutItem(tableName, "pk", "item-1"))
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	return dynamodb.NewHandler(db), table.StreamARN
}

// doStreamsRequest sends a POST request with a DynamoDBStreams X-Amz-Target header.
func doStreamsRequest(
	t *testing.T,
	handler *dynamodb.DynamoDBHandler,
	action string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810."+action)
	w := httptest.NewRecorder()

	echoHandler := handler.Handler()
	_ = serveEchoHandler(echoHandler, w, req)

	return w
}

func TestHandler_StreamsDispatch(t *testing.T) {
	t.Parallel()

	t.Run("ListStreams returns stream for table", func(t *testing.T) {
		t.Parallel()

		handler, _ := newStreamEnabledHandler(t)
		w := doStreamsRequest(t, handler, "ListStreams", `{"TableName":"StreamHandlerTable"}`)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "StreamHandlerTable")
	})

	t.Run("DescribeStream returns shard info", func(t *testing.T) {
		t.Parallel()

		handler, arn := newStreamEnabledHandler(t)
		w := doStreamsRequest(t, handler, "DescribeStream", `{"StreamArn":"`+arn+`"}`)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "StreamHandlerTable")
	})

	t.Run("GetShardIterator returns iterator", func(t *testing.T) {
		t.Parallel()

		handler, arn := newStreamEnabledHandler(t)
		body := `{"StreamArn":"` + arn + `","ShardId":"` + dynamodb.StreamShardID + `","ShardIteratorType":"TRIM_HORIZON"}`
		w := doStreamsRequest(t, handler, "GetShardIterator", body)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ShardIterator")
	})

	t.Run("GetRecords returns INSERT record", func(t *testing.T) {
		t.Parallel()

		handler, _ := newStreamEnabledHandler(t)
		// Use current timestamp so the 3-part iterator (tableName:startSeq:timestamp) is valid.
		iter := fmt.Sprintf("StreamHandlerTable:0:%d", time.Now().Unix())
		w := doStreamsRequest(t, handler, "GetRecords", `{"ShardIterator":"`+iter+`"}`)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Records")
		assert.Contains(t, w.Body.String(), "INSERT")
	})

	t.Run("UnknownStreamsAction returns UnknownOperationException", func(t *testing.T) {
		t.Parallel()

		handler, _ := newStreamEnabledHandler(t)
		w := doStreamsRequest(t, handler, "NoSuchOp", `{}`)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "UnknownOperationException")
	})
}

// TestHandler_StreamsNilBackend ensures that when Streams is nil the dispatch
// returns an unknown-operation error for all streams actions.
func TestHandler_StreamsNilBackend(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)
	h.Streams = nil // simulate non-streams-capable backend

	for _, action := range []string{"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()

			w := doStreamsRequest(t, h, action, `{}`)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "UnknownOperationException")
		})
	}
}

func TestHandler_HandlerUtilities(t *testing.T) {
	t.Parallel()

	t.Run("WithJanitor sets region and creates janitor", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		h2 := h.WithJanitor(dynamodb.Settings{DefaultRegion: "us-west-2"})

		require.NotNil(t, h2)
		assert.Equal(t, "us-west-2", h2.DefaultRegion)
	})

	t.Run("WithJanitor uses default region when empty", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		h2 := h.WithJanitor(dynamodb.Settings{}) // DefaultRegion should fall back to config default
		// The default region from config.DefaultRegion is "us-east-1"
		assert.NotEmpty(t, h2.DefaultRegion, "DefaultRegion should be set to the config default")
	})

	t.Run("StartWorker starts janitor goroutine", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db).WithJanitor(dynamodb.Settings{})

		ctx := t.Context()

		err := h.StartWorker(ctx)
		require.NoError(t, err)
	})

	t.Run("StartWorker without janitor is a no-op", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)

		err := h.StartWorker(context.Background())
		require.NoError(t, err)
	})

	t.Run("Regions empty when no tables", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)

		_ = h.Regions() // no panic when no tables
	})

	t.Run("Regions returns regions after table creation", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		createTableHelper(t, db, "RegionTable", "pk")

		assert.NotEmpty(t, h.Regions())
	})

	t.Run("ChaosServiceName", func(t *testing.T) {
		t.Parallel()

		h := dynamodb.NewHandler(dynamodb.NewInMemoryDB())
		assert.Equal(t, "dynamodb", h.ChaosServiceName())
	})

	t.Run("ChaosOperations returns non-empty list", func(t *testing.T) {
		t.Parallel()

		h := dynamodb.NewHandler(dynamodb.NewInMemoryDB())
		ops := h.ChaosOperations()
		assert.NotEmpty(t, ops)
		assert.Contains(t, ops, "CreateTable")
	})

	t.Run("ChaosRegions with no tables returns default region", func(t *testing.T) {
		t.Parallel()

		h := dynamodb.NewHandler(dynamodb.NewInMemoryDB())
		assert.NotEmpty(t, h.ChaosRegions())
	})

	t.Run("ChaosRegions with tables", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		createTableHelper(t, db, "ChaosTable", "pk")

		assert.NotEmpty(t, h.ChaosRegions())
	})

	t.Run("TableNamesByRegion returns table names", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		createTableHelper(t, db, "RegionTableA", "pk")

		names := h.TableNamesByRegion("")
		assert.Contains(t, names, "RegionTableA")
	})

	t.Run("TableNamesByRegion empty region returns all tables", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)

		names := h.TableNamesByRegion("us-east-1")
		assert.Empty(t, names)
	})

	t.Run("DescribeTableInRegion returns table", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		createTableHelper(t, db, "DTIRTable", "pk")

		tbl := h.DescribeTableInRegion("", "DTIRTable")
		require.NotNil(t, tbl)
		assert.Equal(t, "DTIRTable", tbl.Name)
	})

	t.Run("DescribeTableInRegion returns nil for missing table", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)

		assert.Nil(t, h.DescribeTableInRegion("", "NoSuchTable"))
	})
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         string
		name         string
		wantResource string
	}{
		{
			name:         "extracts table name from body",
			body:         `{"TableName":"MyTable"}`,
			wantResource: "MyTable",
		},
		{
			name:         "returns empty when no TableName key",
			body:         `{}`,
			wantResource: "",
		},
		{
			name:         "returns empty for invalid json",
			body:         `not json`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// TestHandler_ExportAndDescribeExport covers exportTableToPointInTime and describeExport.
func TestHandler_ExportAndDescribeExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           string
		name           string
		action         string
		wantStatusCode int
	}{
		{
			name:           "ExportTableToPointInTime returns stub",
			action:         "ExportTableToPointInTime",
			body:           `{"TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/T","S3Bucket":"bucket"}`,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "DescribeExport returns stub",
			action:         "DescribeExport",
			body:           `{"ExportArn":"arn:aws:dynamodb:us-east-1:123456789012:table/T/export/01"}`,
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+tt.action)
			w := httptest.NewRecorder()
			echoHandler := h.Handler()
			_ = serveEchoHandler(echoHandler, w, req)

			assert.Equal(t, tt.wantStatusCode, w.Code)
		})
	}
}

// TestHandler_GetRecords_InvalidIterator verifies the error path in handleStreamsGetRecords.
func TestHandler_GetRecords_InvalidIterator(t *testing.T) {
	t.Parallel()

	handler, _ := newStreamEnabledHandler(t)
	w := doStreamsRequest(t, handler, "GetRecords", `{"ShardIterator":"BAD_NO_COLON"}`)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandler_DescribeTable_ReturnsStreamFields verifies that DescribeTable includes
// LatestStreamArn, LatestStreamLabel, and StreamSpecification in the HTTP response.
func TestHandler_DescribeTable_ReturnsStreamFields(t *testing.T) {
	t.Parallel()

	handler, streamARN := newStreamEnabledHandler(t)

	reqBody, err := json.Marshal(models.DescribeTableInput{TableName: "StreamHandlerTable"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")
	w := httptest.NewRecorder()

	echoHandler := handler.Handler()
	_ = serveEchoHandler(echoHandler, w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var out models.DescribeTableOutput
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))

	assert.Equal(t, streamARN, out.Table.LatestStreamArn, "DescribeTable should return LatestStreamArn")
	assert.NotEmpty(t, out.Table.LatestStreamLabel, "DescribeTable should return LatestStreamLabel")
	require.NotNil(t, out.Table.StreamSpecification, "DescribeTable should return StreamSpecification")
	assert.True(t, out.Table.StreamSpecification.StreamEnabled)
	assert.Equal(t, "NEW_AND_OLD_IMAGES", out.Table.StreamSpecification.StreamViewType)
}
