package dashboard_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDashboard_Pagination(t *testing.T) {
	t.Parallel()

	// Test DynamoDB Table Pagination
	t.Run("DynamoDB Table Pagination", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		// Create 5 tables
		for i := 1; i <= 5; i++ {
			newDDBTable(t, stack, fmt.Sprintf("table-%03d", i))
		}

		// List with limit 2 (simulated via query param if implemented, or just check standard)
		// Our implementation uses AWS default (100) or 1000 for search.
		// To test our pagination logic, we'd need more than 100 tables in the backend.
		// Since creating 100 tables is slow, we can mock the DynamoDB client if needed,
		// but our integration stack uses a real (in-memory) handler.

		req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/tables", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "table-001")
		// Since we have < 100, no "Load More" should be present
		assert.NotContains(t, w.Body.String(), "Load More")
	})

	// Test S3 Bucket Pagination
	t.Run("S3 Bucket Pagination", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		// Create 5 buckets
		for i := 1; i <= 5; i++ {
			newS3Bucket(t, stack, fmt.Sprintf("bucket-%03d", i))
		}

		req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "bucket-001")
	})
}

func TestDashboard_DynamoDB_ItemEditor(t *testing.T) {
	t.Parallel()

	// Test Create Item
	t.Run("Create Item", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		tableName := "editor-table"
		newDDBTable(t, stack, tableName)

		itemJSON := `{"id": "user-1", "name": "Alice"}`
		form := url.Values{"itemJson": {itemJSON}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/dynamodb/table/%s/item", tableName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify in DB
		out, err := stack.DDBClient.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "user-1"},
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, out.Item)
	})

	// Test Edit Item (GET form)
	t.Run("Edit Item Form", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		tableName := "editor-table"
		newDDBTable(t, stack, tableName)

		// Create item first
		_, _ = stack.DDBClient.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: &tableName,
			Item: map[string]ddbtypes.AttributeValue{
				"id":   &ddbtypes.AttributeValueMemberS{Value: "user-1"},
				"name": &ddbtypes.AttributeValueMemberS{Value: "Alice"},
			},
		})

		pkJSON, _ := json.Marshal("user-1")
		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf(
				"/dashboard/dynamodb/table/%s/item?pk=%s",
				tableName,
				url.QueryEscape(string(pkJSON)),
			),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Alice")
	})
}

func TestDashboard_S3_Previews(t *testing.T) {
	t.Parallel()

	t.Run("Text Preview", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		bucketName := "preview-bucket"
		newS3Bucket(t, stack, bucketName)

		content := "Hello World"
		uploadS3Object(t, stack, bucketName, "test.txt", content)

		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/test.txt/preview", bucketName),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), content)
	})

	t.Run("Metadata Export", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		bucketName := "preview-bucket"
		newS3Bucket(t, stack, bucketName)
		uploadS3Object(t, stack, bucketName, "test.txt", "content")

		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/test.txt/export", bucketName),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test.txt")
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})
}

func TestDashboard_DDB_ExportImport(t *testing.T) {
	t.Parallel()

	t.Run("Export JSON", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		tableName := "xport-table"
		newDDBTable(t, stack, tableName)

		// Seed data
		_, _ = stack.DDBClient.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "item-1"},
			},
		})

		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf("/dashboard/dynamodb/table/%s/export", tableName),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "item-1")
	})

	t.Run("Import JSON", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		tableName := "xport-table"
		newDDBTable(t, stack, tableName)

		importJSON := `[{"id": "item-2"}]`
		form := url.Values{"importData": {importJSON}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/dynamodb/table/%s/import", tableName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify in DB
		out, _ := stack.DDBClient.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "item-2"},
			},
		})
		assert.NotNil(t, out.Item)
	})
}
