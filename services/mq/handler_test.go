package mq_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *mq.Handler {
	t.Helper()

	return mq.NewHandler(mq.NewInMemoryBackend(testAccountID, testRegion))
}

func doRequest(t *testing.T, h *mq.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(bodyBytes)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestMQ_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MQ", h.Name())
}

func TestMQ_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateBroker")
	assert.Contains(t, ops, "DescribeBroker")
	assert.Contains(t, ops, "ListBrokers")
	assert.Contains(t, ops, "UpdateBroker")
	assert.Contains(t, ops, "DeleteBroker")
	assert.Contains(t, ops, "RebootBroker")
	assert.Contains(t, ops, "CreateUser")
	assert.Contains(t, ops, "DescribeUser")
	assert.Contains(t, ops, "DeleteUser")
	assert.Contains(t, ops, "ListUsers")
	assert.Contains(t, ops, "CreateConfiguration")
	assert.Contains(t, ops, "DescribeConfiguration")
	assert.Contains(t, ops, "ListConfigurations")
	assert.Contains(t, ops, "ListTags")
}

func TestMQ_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 86, h.MatchPriority())
}

func TestMQ_BrokerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		brokerName string
		engineType string
		wantStatus int
	}{
		{
			name:       "create_activemq",
			brokerName: "my-activemq-broker",
			engineType: "ACTIVEMQ",
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_rabbitmq",
			brokerName: "my-rabbitmq-broker",
			engineType: "RABBITMQ",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create broker.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName":       tt.brokerName,
				"engineType":       tt.engineType,
				"hostInstanceType": "mq.m5.large",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.NotEmpty(t, createResp["brokerId"])
			assert.NotEmpty(t, createResp["brokerArn"])

			brokerID := createResp["brokerId"]

			// Describe broker.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.brokerName, descResp["brokerName"])
			assert.Equal(t, "RUNNING", descResp["brokerState"])

			// List brokers.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			summaries, ok := listResp["brokerSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, 1)

			// Delete broker.
			rec = doRequest(t, h, http.MethodDelete, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deletion.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestMQ_BrokerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "describe_nonexistent",
			method: http.MethodGet,
			path:   "/v1/brokers/nonexistent-id",
		},
		{
			name:   "delete_nonexistent",
			method: http.MethodDelete,
			path:   "/v1/brokers/nonexistent-id",
		},
		{
			name:   "reboot_nonexistent",
			method: http.MethodPost,
			path:   "/v1/brokers/nonexistent-id/reboot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestMQ_CreateBroker_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing_broker_name",
			body:       map[string]any{"engineType": "ACTIVEMQ"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_engine_type",
			body:       map[string]any{"brokerName": "my-broker"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_broker",
			body:       map[string]any{"brokerName": "my-broker", "engineType": "ACTIVEMQ"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_broker" {
				// Create first broker.
				rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
					"brokerName": "my-broker",
					"engineType": "ACTIVEMQ",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMQ_ConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		engineType string
	}{
		{
			name:       "activemq_config",
			configName: "my-activemq-config",
			engineType: "ACTIVEMQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()

			// Create configuration — must use MQ-signed Authorization header.
			body, err := json.Marshal(map[string]any{
				"name":          tt.configName,
				"engineType":    tt.engineType,
				"engineVersion": "5.15.14",
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/v1/configurations", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.NotEmpty(t, createResp["id"])
			assert.NotEmpty(t, createResp["arn"])

			configID := createResp["id"].(string)

			// Describe configuration.
			req = httptest.NewRequest(http.MethodGet, "/v1/configurations/"+configID, nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			rec = httptest.NewRecorder()
			c = e.NewContext(req, rec)

			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.configName, descResp["name"])
			assert.Equal(t, tt.engineType, descResp["engineType"])

			// List configurations.
			req = httptest.NewRequest(http.MethodGet, "/v1/configurations", nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			rec = httptest.NewRecorder()
			c = e.NewContext(req, rec)

			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			configs, ok := listResp["configurations"].([]any)
			require.True(t, ok)
			assert.Len(t, configs, 1)
		})
	}
}

func TestMQ_UserLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
	}{
		{
			name:     "create_and_delete_user",
			username: "testuser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create broker first.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "test-broker",
				"engineType": "ACTIVEMQ",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createBrokerResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBrokerResp))
			brokerID := createBrokerResp["brokerId"]

			// Create user.
			rec = doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/"+tt.username, map[string]any{
				"password":      "password1234",
				"consoleAccess": true,
			})
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Describe user.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var userResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &userResp))
			assert.Equal(t, tt.username, userResp["username"])

			// List users.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			users, ok := listResp["users"].([]any)
			require.True(t, ok)
			assert.Len(t, users, 1)

			// Delete user.
			rec = doRequest(t, h, http.MethodDelete, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deletion.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestMQ_TagsLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tags_on_broker"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			// Create a broker to get an ARN.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "tagged-broker",
				"engineType": "ACTIVEMQ",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			resourceARN := createResp["brokerArn"]

			// Create tags.
			tagBody, _ := json.Marshal(map[string]any{
				"tags": map[string]string{"env": "test", "owner": "alice"},
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/tags/"+resourceARN, bytes.NewReader(tagBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			recW := httptest.NewRecorder()
			c := e.NewContext(req, recW)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, recW.Code)

			// List tags.
			req = httptest.NewRequest(http.MethodGet, "/v1/tags/"+resourceARN, nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			recW = httptest.NewRecorder()
			c = e.NewContext(req, recW)
			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, recW.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(recW.Body.Bytes(), &tagsResp))
			tags, ok := tagsResp["tags"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "alice", tags["owner"])

			// Delete tags.
			req = httptest.NewRequest(http.MethodDelete, "/v1/tags/"+resourceARN+"?tagKeys=env", nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			recW = httptest.NewRecorder()
			c = e.NewContext(req, recW)
			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, recW.Code)
		})
	}
}
