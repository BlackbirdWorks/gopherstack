package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// postSearchVectors drives SearchVectors through the real wire path: a raw
// JSON body plus the DynamoDB_20120810.SearchVectors X-Amz-Target header,
// exactly as the real AWS SDK client would send it.
func postSearchVectors(t *testing.T, handler *dynamodb.DynamoDBHandler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.SearchVectors")
	w := httptest.NewRecorder()

	echoHandler := handler.Handler()
	_ = serveEchoHandler(echoHandler, w, req)

	return w
}

// TestSearchVectors covers the honest-gap behavior in search_vectors.go:
// no vector index model, so the named index always reports not found.
func TestSearchVectors(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	backend.SetDefaultRegion("us-east-1")
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "VectorTable", "pk")

	t.Run("missing required fields is a ValidationException", func(t *testing.T) {
		t.Parallel()

		w := postSearchVectors(t, handler, `{}`)
		require.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "ValidationException")
	})

	t.Run("unknown table is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()

		body := mustMarshal(t, models.SearchVectorsInput{
			TableName:    "NoSuchTable",
			IndexName:    "vec-index",
			SearchVector: []any{map[string]any{"N": "0.1"}},
			TopK:         aws.Int32(5),
		})

		w := postSearchVectors(t, handler, body)
		require.Equal(t, http.StatusBadRequest, w.Code)

		var errBody map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
		assert.Contains(t, errBody["__type"], "ResourceNotFoundException")
		assert.Contains(t, errBody["message"], "table not found")
	})

	t.Run("existing table but no vector index is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()

		body := mustMarshal(t, models.SearchVectorsInput{
			TableName:    "VectorTable",
			IndexName:    "vec-index",
			SearchVector: []any{map[string]any{"N": "0.1"}, map[string]any{"N": "0.2"}},
			TopK:         aws.Int32(3),
		})

		w := postSearchVectors(t, handler, body)
		require.Equal(t, http.StatusBadRequest, w.Code)

		var errBody map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
		assert.Contains(t, errBody["__type"], "ResourceNotFoundException")
		assert.Contains(t, errBody["message"], "Index: vec-index not found")
	})
}
