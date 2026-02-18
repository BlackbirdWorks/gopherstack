package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/logger"
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDebugLogging verifies that debug logging works correctly with context-based logging.
func TestDebugLogging(t *testing.T) {
	t.Parallel()

	// Create a buffer to capture log output
	var logBuffer bytes.Buffer
	testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create handler with test logger
	handler := dynamodb.NewHandler(testLogger)

	// Wrap handler with logger middleware
	loggerMiddleware := logger.Middleware(testLogger)
	handlerWithLogger := loggerMiddleware(handler)

	// Create a test table
	createTableInput := models.CreateTableInput{
		TableName: "DebugLogTestTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
	}

	body, err := json.Marshal(createTableInput)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rec := httptest.NewRecorder()

	handlerWithLogger.ServeHTTP(rec, req)

	// Verify the request succeeded
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify debug logging was captured
	logOutput := logBuffer.String()
	t.Logf("Log output:\n%s", logOutput)

	// Check for expected debug log messages
	assert.Contains(
		t,
		logOutput,
		"DynamoDB request",
		"Expected to see 'DynamoDB request' in debug logs",
	)
	assert.Contains(
		t,
		logOutput,
		"CreateTable",
		"Expected to see 'CreateTable' action in debug logs",
	)
	assert.Contains(t, logOutput, "handler input", "Expected to see 'handler input' in debug logs")
	assert.Contains(
		t,
		logOutput,
		"handler output",
		"Expected to see 'handler output' in debug logs",
	)
	assert.Contains(t, logOutput, "DebugLogTestTable", "Expected to see table name in debug logs")

	// Verify debug level is logged
	assert.Contains(t, logOutput, "level=DEBUG", "Expected to see DEBUG level in logs")
}

// TestDebugLoggingWithItem verifies debug logging works for item operations.
func TestDebugLoggingWithItem(t *testing.T) {
	t.Parallel()

	// Create a buffer to capture log output
	var logBuffer bytes.Buffer
	testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create handler with test logger
	handler := dynamodb.NewHandler(testLogger)

	// Wrap handler with logger middleware
	loggerMiddleware := logger.Middleware(testLogger)
	handlerWithLogger := loggerMiddleware(handler)

	// First create a table
	createTableInput := models.CreateTableInput{
		TableName: "ItemLogTestTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}

	body, err := json.Marshal(createTableInput)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rec := httptest.NewRecorder()

	handlerWithLogger.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Clear the log buffer
	logBuffer.Reset()

	// Now put an item
	putItemInput := models.PutItemInput{
		TableName: "ItemLogTestTable",
		Item: map[string]any{
			"pk":   map[string]any{"S": "test-key"},
			"data": map[string]any{"S": "test-data"},
		},
	}

	body, err = json.Marshal(putItemInput)
	require.NoError(t, err)

	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	rec = httptest.NewRecorder()

	handlerWithLogger.ServeHTTP(rec, req)

	// Verify the request succeeded
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify debug logging was captured
	logOutput := logBuffer.String()
	t.Logf("PutItem log output:\n%s", logOutput)

	// Check for expected debug log messages
	assert.Contains(
		t,
		logOutput,
		"DynamoDB request",
		"Expected to see 'DynamoDB request' in debug logs",
	)
	assert.Contains(t, logOutput, "PutItem", "Expected to see 'PutItem' action in debug logs")
	assert.Contains(t, logOutput, "handler input", "Expected to see 'handler input' in debug logs")
	assert.Contains(
		t,
		logOutput,
		"handler output",
		"Expected to see 'handler output' in debug logs",
	)
	assert.Contains(t, logOutput, "test-key", "Expected to see item key in debug logs")
	assert.Contains(t, logOutput, "test-data", "Expected to see item data in debug logs")

	// Verify debug level is logged
	assert.Contains(t, logOutput, "level=DEBUG", "Expected to see DEBUG level in logs")
}
