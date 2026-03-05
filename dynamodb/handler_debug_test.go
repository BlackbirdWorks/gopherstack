package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestDebugLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupInput      any
		input           any
		name            string
		setupTarget     string
		target          string
		wantLogContains []string
		wantStatus      int
	}{
		{
			name:   "create_table_logs_debug_info",
			target: "DynamoDB_20120810.CreateTable",
			input: models.CreateTableInput{
				TableName: "DebugLogTestTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "id", KeyType: "HASH"},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "id", AttributeType: "S"},
				},
			},
			wantStatus: http.StatusOK,
			wantLogContains: []string{
				"DynamoDB request",
				"CreateTable",
				"handler input",
				"handler output",
				"DebugLogTestTable",
				"level=DEBUG",
			},
		},
		{
			name:        "put_item_logs_debug_info",
			setupTarget: "DynamoDB_20120810.CreateTable",
			setupInput: models.CreateTableInput{
				TableName: "ItemLogTestTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			},
			target: "DynamoDB_20120810.PutItem",
			input: models.PutItemInput{
				TableName: "ItemLogTestTable",
				Item: map[string]any{
					"pk":   map[string]any{"S": "test-key"},
					"data": map[string]any{"S": "test-data"},
				},
			},
			wantStatus: http.StatusOK,
			wantLogContains: []string{
				"DynamoDB request",
				"PutItem",
				"handler input",
				"handler output",
				"test-key",
				"test-data",
				"level=DEBUG",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logBuffer bytes.Buffer
			testLogger := slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			h := dynamodb.NewHandler(dynamodb.NewInMemoryDB())
			e := echo.New()
			echoHandler := h.Handler()

			if tt.setupTarget != "" {
				setupBody, err := json.Marshal(tt.setupInput)
				require.NoError(t, err)

				setupReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(setupBody))
				setupReq.Header.Set("X-Amz-Target", tt.setupTarget)
				setupRec := httptest.NewRecorder()

				setupCtx := logger.Save(setupReq.Context(), testLogger)
				setupReq = setupReq.WithContext(setupCtx)

				err = echoHandler(e.NewContext(setupReq, setupRec))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, setupRec.Code)

				logBuffer.Reset()
			}

			body, err := json.Marshal(tt.input)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()

			ctx := logger.Save(req.Context(), testLogger)
			req = req.WithContext(ctx)

			err = echoHandler(e.NewContext(req, rec))
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			logOutput := logBuffer.String()
			t.Logf("Log output:\n%s", logOutput)

			for _, want := range tt.wantLogContains {
				assert.Contains(t, logOutput, want)
			}
		})
	}
}
