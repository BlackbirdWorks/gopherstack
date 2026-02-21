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

// TestDashboard_Metrics covers getMetricsJSON and metricsIndex.
func TestDashboard_Metrics(t *testing.T) {
	t.Parallel()

	t.Run("getMetricsJSON returns JSON", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/api/metrics", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var result map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	})

	t.Run("metricsIndex renders HTML page", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/metrics", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	})
}

// TestDashboard_GetSupportedOperations covers the GetSupportedOperations method.
func TestDashboard_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	stack := newIntegrationStack(t)
	ops := stack.handler.GetSupportedOperations()

	assert.NotNil(t, ops)
	assert.Empty(t, ops)
}

// TestDashboard_DDB_DeleteItem covers dynamoDBDeleteItem.
func TestDashboard_DDB_DeleteItem(t *testing.T) {
	t.Parallel()

	t.Run("delete existing item succeeds", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)
		tableName := "del-item-table"
		newDDBTable(t, stack, tableName)

		// Seed an item.
		_, err := stack.dyClient.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "to-delete"},
			},
		})
		require.NoError(t, err)

		pkJSON, _ := json.Marshal("to-delete")
		req := httptest.NewRequest(
			http.MethodDelete,
			fmt.Sprintf(
				"/dashboard/dynamodb/table/%s/item?pk=%s",
				tableName,
				url.QueryEscape(string(pkJSON)),
			),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify item is gone.
		out, _ := stack.dyClient.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "to-delete"},
			},
		})
		assert.Empty(t, out.Item)
	})

	t.Run("delete from non-existent table returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)

		pkJSON, _ := json.Marshal("x")
		req := httptest.NewRequest(
			http.MethodDelete,
			fmt.Sprintf(
				"/dashboard/dynamodb/table/no-such-table/item?pk=%s",
				url.QueryEscape(string(pkJSON)),
			),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("wrong method returns 405", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)

		req := httptest.NewRequest(http.MethodPut, "/dashboard/dynamodb/table/t/item", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}

// TestDashboard_S3_TagOperations covers s3UpdateTag, s3DeleteTag, renderTagsList, renderTagItem.
func TestDashboard_S3_TagOperations(t *testing.T) {
	t.Parallel()

	t.Run("add tag to object", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)
		bucketName := "tag-bucket"
		newS3Bucket(t, stack, bucketName)
		uploadS3Object(t, stack, bucketName, "tagged.txt", "data")

		form := url.Values{"key": {"env"}, "value": {"prod"}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/tagged.txt/tag", bucketName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		// renderTagsList/renderTagItem produce HTML with the tag key
		assert.Contains(t, w.Body.String(), "env")
		assert.Contains(t, w.Body.String(), "prod")
	})

	t.Run("update existing tag", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)
		bucketName := "tag-update-bucket"
		newS3Bucket(t, stack, bucketName)
		uploadS3Object(t, stack, bucketName, "obj.txt", "data")

		// Add initial tag.
		form := url.Values{"key": {"color"}, "value": {"red"}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/obj.txt/tag", bucketName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		serveHandler(stack.handler, httptest.NewRecorder(), req)

		// Update it.
		form2 := url.Values{"key": {"color"}, "value": {"blue"}}
		req2 := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/obj.txt/tag", bucketName),
			strings.NewReader(form2.Encode()),
		)
		req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req2)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "blue")
	})

	t.Run("delete tag from object", func(t *testing.T) {
		t.Parallel()
		stack := newIntegrationStack(t)
		bucketName := "tag-del-bucket"
		newS3Bucket(t, stack, bucketName)
		uploadS3Object(t, stack, bucketName, "del.txt", "data")

		// Add two tags first (so after deletion one remains).
		for _, kv := range []struct{ k, v string }{{"env", "prod"}, {"to-remove", "yes"}} {
			form := url.Values{"key": {kv.k}, "value": {kv.v}}
			req := httptest.NewRequest(
				http.MethodPost,
				fmt.Sprintf("/dashboard/s3/bucket/%s/file/del.txt/tag", bucketName),
				strings.NewReader(form.Encode()),
			)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			serveHandler(stack.handler, httptest.NewRecorder(), req)
		}

		// Delete one of the tags.
		req2 := httptest.NewRequest(
			http.MethodDelete,
			fmt.Sprintf(
				"/dashboard/s3/bucket/%s/file/del.txt/tag?key=%s",
				bucketName,
				url.QueryEscape("to-remove"),
			),
			nil,
		)
		w := httptest.NewRecorder()
		serveHandler(stack.handler, w, req2)

		assert.Equal(t, http.StatusOK, w.Code)
		// Deleted tag should not appear in rendered list; kept tag should.
		assert.NotContains(t, w.Body.String(), "to-remove")
		assert.Contains(t, w.Body.String(), "env")
	})
}

// TestDashboard_S3_UpdateMetadata covers s3UpdateMetadata.
func TestDashboard_S3_UpdateMetadata(t *testing.T) {
	t.Parallel()

	stack := newIntegrationStack(t)
	bucketName := "meta-update-bucket"
	newS3Bucket(t, stack, bucketName)
	uploadS3Object(t, stack, bucketName, "doc.txt", "hello")

	form := url.Values{"contentType": {"text/plain"}}
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/dashboard/s3/bucket/%s/file/doc.txt/metadata", bucketName),
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.handler, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "true", w.Header().Get("Hx-Refresh"))
}
