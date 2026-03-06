package dashboard_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestDashboard_Metrics covers getMetricsJSON and metricsIndex.
func TestDashboard_Metrics(t *testing.T) {
	t.Parallel()

	t.Run("getMetricsJSON returns JSON", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/api/metrics", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var result map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	})

	t.Run("metricsIndex renders HTML page", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/metrics", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	})
}

// TestDashboard_GetSupportedOperations covers the GetSupportedOperations method.
func TestDashboard_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	ops := stack.Dashboard.GetSupportedOperations()

	assert.NotNil(t, ops)
	assert.Empty(t, ops)
}

// TestDashboard_DDB_DeleteItem covers dynamoDBDeleteItem.
func TestDashboard_DDB_DeleteItem(t *testing.T) {
	t.Parallel()

	t.Run("delete existing item succeeds", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		tableName := "del-item-table"
		stack.CreateDDBTable(t, tableName)

		// Seed an item.
		_, err := stack.DDBClient.PutItem(t.Context(), &dynamodb.PutItemInput{
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
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify item is gone.
		out, _ := stack.DDBClient.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "to-delete"},
			},
		})
		assert.Empty(t, out.Item)
	})

	t.Run("delete from non-existent table returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

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
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("wrong method returns 405", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPut, "/dashboard/dynamodb/table/t/item", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}

// TestDashboard_S3_TagOperations covers s3UpdateTag, s3DeleteTag, renderTagsList, renderTagItem.
func TestDashboard_S3_TagOperations(t *testing.T) {
	t.Parallel()

	t.Run("add tag to object", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		bucketName := "tag-bucket"
		stack.CreateS3Bucket(t, bucketName)
		uploadS3Object(t, stack, bucketName, "tagged.txt", "data")

		form := url.Values{"key": {"env"}, "value": {"prod"}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/tagged.txt/tag", bucketName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		// renderTagsList/renderTagItem produce HTML with the tag key
		assert.Contains(t, w.Body.String(), "env")
		assert.Contains(t, w.Body.String(), "prod")
	})

	t.Run("update existing tag", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		bucketName := "tag-update-bucket"
		stack.CreateS3Bucket(t, bucketName)
		uploadS3Object(t, stack, bucketName, "obj.txt", "data")

		// Add initial tag.
		form := url.Values{"key": {"color"}, "value": {"red"}}
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/obj.txt/tag", bucketName),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		serveHandler(stack.Dashboard, httptest.NewRecorder(), req)

		// Update it.
		form2 := url.Values{"key": {"color"}, "value": {"blue"}}
		req2 := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/dashboard/s3/bucket/%s/file/obj.txt/tag", bucketName),
			strings.NewReader(form2.Encode()),
		)
		req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req2)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "blue")
	})

	t.Run("delete tag from object", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		bucketName := "tag-del-bucket"
		stack.CreateS3Bucket(t, bucketName)
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
			serveHandler(stack.Dashboard, httptest.NewRecorder(), req)
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
		serveHandler(stack.Dashboard, w, req2)

		assert.Equal(t, http.StatusOK, w.Code)
		// Deleted tag should not appear in rendered list; kept tag should.
		assert.NotContains(t, w.Body.String(), "to-remove")
		assert.Contains(t, w.Body.String(), "env")
	})
}

// TestDashboard_S3_UpdateMetadata covers s3UpdateMetadata.
func TestDashboard_S3_UpdateMetadata(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	bucketName := "meta-update-bucket"
	stack.CreateS3Bucket(t, bucketName)
	uploadS3Object(t, stack, bucketName, "doc.txt", "hello")

	form := url.Values{"contentType": {"text/plain"}}
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/dashboard/s3/bucket/%s/file/doc.txt/metadata", bucketName),
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "true", w.Header().Get("Hx-Refresh"))
}

// TestDashboard_STS covers the STS index page.
func TestDashboard_STS(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	req := httptest.NewRequest(http.MethodGet, "/dashboard/sts", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, w.Body.String(), "STS Security Token Service")
	assert.Contains(t, w.Body.String(), "000000000000")
}

// TestDashboard_Lambda covers Lambda dashboard handlers.
func TestDashboard_Lambda(t *testing.T) {
	t.Parallel()

	t.Run("lambdaIndex nil ops returns 200", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		// LambdaOps is nil in the default test stack

		req := httptest.NewRequest(http.MethodGet, "/dashboard/lambda", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	})

	t.Run("lambdaFunctionDetail redirect when no name", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/lambda/function", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		// Should redirect back to lambda index
		assert.True(t, w.Code == http.StatusFound || w.Code == http.StatusOK)
	})

	t.Run("lambdaInvoke nil ops returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		form := url.Values{"payload": {"{}"}}
		req := httptest.NewRequest(http.MethodPost, "/dashboard/lambda/invoke?name=", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// TestDashboard_DynamoDBPartiQL covers the DynamoDB PartiQL handler.
func TestDashboard_DynamoDBPartiQL(t *testing.T) {
	t.Parallel()

	t.Run("GET partiql form", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a table first
		_, err := stack.DDBClient.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String("partiql-test"),
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			},
			BillingMode: ddbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/table/partiql-test/partiql", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("POST partiql execute", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a table first
		_, err := stack.DDBClient.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String("partiql-exec"),
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			},
			BillingMode: ddbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		form := url.Values{"statement": {`SELECT * FROM "partiql-exec"`}}
		req := httptest.NewRequest(http.MethodPost, "/dashboard/dynamodb/table/partiql-exec/partiql",
			strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("POST partiql missing statement", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/dynamodb/table/any/partiql",
			strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// TestDashboard_CloudWatchLogs covers CloudWatch Logs dashboard handlers including the stream viewer.
func TestDashboard_CloudWatchLogs(t *testing.T) {
	t.Parallel()

	t.Run("index renders log groups", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a log group directly via backend
		_, err := stack.CloudWatchLogsHandler.Backend.CreateLogGroup("/aws/test/group")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/cloudwatchlogs", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
		assert.Contains(t, w.Body.String(), "/aws/test/group")
	})

	t.Run("group detail shows streams and links to viewer", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		_, err := stack.CloudWatchLogsHandler.Backend.CreateLogGroup("/aws/my-group")
		require.NoError(t, err)
		_, err = stack.CloudWatchLogsHandler.Backend.CreateLogStream("/aws/my-group", "my-stream")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/cloudwatchlogs/group?name=%2Faws%2Fmy-group", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "my-stream")
		// Stream name should be a link to the viewer
		assert.Contains(t, w.Body.String(), "/dashboard/cloudwatchlogs/stream")
	})

	t.Run("stream detail shows events", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		_, err := stack.CloudWatchLogsHandler.Backend.CreateLogGroup("/aws/stream-test")
		require.NoError(t, err)
		_, err = stack.CloudWatchLogsHandler.Backend.CreateLogStream("/aws/stream-test", "app-log")
		require.NoError(t, err)
		_, err = stack.CloudWatchLogsHandler.Backend.PutLogEvents("/aws/stream-test", "app-log",
			[]cwlogsbackend.InputLogEvent{
				{Timestamp: 1000, Message: "hello world"},
				{Timestamp: 2000, Message: "ERROR something failed"},
			},
		)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet,
			"/dashboard/cloudwatchlogs/stream?group=%2Faws%2Fstream-test&stream=app-log", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "hello world")
		assert.Contains(t, w.Body.String(), "ERROR something failed")
	})

	t.Run("stream detail with filter", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		_, err := stack.CloudWatchLogsHandler.Backend.CreateLogGroup("/aws/filter-test")
		require.NoError(t, err)
		_, err = stack.CloudWatchLogsHandler.Backend.CreateLogStream("/aws/filter-test", "svc-log")
		require.NoError(t, err)
		_, err = stack.CloudWatchLogsHandler.Backend.PutLogEvents("/aws/filter-test", "svc-log",
			[]cwlogsbackend.InputLogEvent{
				{Timestamp: 1000, Message: "INFO startup"},
				{Timestamp: 2000, Message: "ERROR boom"},
			},
		)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet,
			"/dashboard/cloudwatchlogs/stream?group=%2Faws%2Ffilter-test&stream=svc-log&filter=ERROR", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ERROR boom")
		assert.NotContains(t, w.Body.String(), "INFO startup")
	})

	t.Run("stream detail nil ops returns 503", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.Dashboard.CloudWatchLogsOps = nil

		req := httptest.NewRequest(http.MethodGet,
			"/dashboard/cloudwatchlogs/stream?group=g&stream=s", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// validSFnDef is a minimal valid ASL definition for dashboard tests.
const validSFnDef = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`

// TestDashboard_StepFunctions covers Step Functions dashboard handlers including execution detail.
func TestDashboard_StepFunctions(t *testing.T) {
	t.Parallel()

	t.Run("index renders state machines", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		_, err := stack.StepFunctionsHandler.Backend.CreateStateMachine("my-sm", validSFnDef, "arn:role", "STANDARD")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/stepfunctions", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "my-sm")
	})

	t.Run("statemachine detail shows executions and links to detail", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		sm, err := stack.StepFunctionsHandler.Backend.CreateStateMachine("link-sm", validSFnDef, "arn:role", "STANDARD")
		require.NoError(t, err)
		_, err = stack.StepFunctionsHandler.Backend.StartExecution(sm.StateMachineArn, "exec-1", "{}")
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/dashboard/stepfunctions/statemachine?arn=%s", url.QueryEscape(sm.StateMachineArn)), nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "exec-1")
		// Execution name should be a link to execution detail
		assert.Contains(t, w.Body.String(), "/dashboard/stepfunctions/execution")
	})

	t.Run("execution detail shows history events", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		sm, err := stack.StepFunctionsHandler.Backend.CreateStateMachine("hist-sm", validSFnDef, "arn:role", "STANDARD")
		require.NoError(t, err)
		exec, err := stack.StepFunctionsHandler.Backend.StartExecution(sm.StateMachineArn, "hist-exec", `{"key":"val"}`)
		require.NoError(t, err)

		// Wait for the async execution to complete before checking the rendered page.
		require.Eventually(t, func() bool {
			e, descErr := stack.StepFunctionsHandler.Backend.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && e.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/dashboard/stepfunctions/execution?arn=%s", url.QueryEscape(exec.ExecutionArn)), nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "hist-exec")
		// History events should be visible
		assert.Contains(t, w.Body.String(), "ExecutionStarted")
		assert.Contains(t, w.Body.String(), "ExecutionSucceeded")
	})

	t.Run("execution detail not found returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet,
			"/dashboard/stepfunctions/execution?arn=arn%3Anonexistent", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("execution detail nil ops returns 503", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.Dashboard.StepFunctionsOps = nil

		req := httptest.NewRequest(http.MethodGet, "/dashboard/stepfunctions/execution?arn=x", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}
