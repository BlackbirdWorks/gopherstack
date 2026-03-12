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

func TestMQ_UpdateBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "update_engine_version",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create broker first.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "update-broker",
				"engineType": "ACTIVEMQ",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			brokerID := createResp["brokerId"]

			// Update broker.
			rec = doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
				"engineVersion": "5.16.7",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var updateResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			assert.Equal(t, brokerID, updateResp["brokerId"])
		})
	}
}

func TestMQ_UpdateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_user_password"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create broker.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "broker-for-user-update",
				"engineType": "ACTIVEMQ",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createBrokerResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBrokerResp))
			brokerID := createBrokerResp["brokerId"]

			// Create user.
			rec = doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
				"password": "oldpassword1234",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Update user (PUT for update).
			rec = doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
				"password": "newpassword1234",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestMQ_UpdateConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_configuration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			// Create configuration.
			body, err := json.Marshal(map[string]any{
				"name":          "updateable-config",
				"engineType":    "ACTIVEMQ",
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
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			configID := createResp["id"].(string)

			// Update configuration.
			updateBody, err := json.Marshal(map[string]any{
				"description": "updated description",
				"data":        "<broker>...</broker>",
			})
			require.NoError(t, err)

			req = httptest.NewRequest(http.MethodPut, "/v1/configurations/"+configID, bytes.NewReader(updateBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			rec = httptest.NewRecorder()
			c = e.NewContext(req, rec)

			err = h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			assert.Equal(t, configID, updateResp["id"])
			assert.NotNil(t, updateResp["latestRevision"])
		})
	}
}

func TestMQ_Introspection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("ChaosServiceName", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "mq", h.ChaosServiceName())
	})

	t.Run("ChaosOperations", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	})

	t.Run("ChaosRegions", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, []string{testRegion}, h.ChaosRegions())
	})

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			method string
			path   string
			wantOp string
		}{
			{
				name:   "list_brokers",
				method: http.MethodGet,
				path:   "/v1/brokers",
				wantOp: "ListBrokers",
			},
			{
				name:   "create_broker",
				method: http.MethodPost,
				path:   "/v1/brokers",
				wantOp: "CreateBroker",
			},
			{
				name:   "describe_broker",
				method: http.MethodGet,
				path:   "/v1/brokers/broker-id",
				wantOp: "DescribeBroker",
			},
			{
				name:   "reboot_broker",
				method: http.MethodPost,
				path:   "/v1/brokers/broker-id/reboot",
				wantOp: "RebootBroker",
			},
			{
				name:   "list_users",
				method: http.MethodGet,
				path:   "/v1/brokers/broker-id/users",
				wantOp: "ListUsers",
			},
			{
				name:   "create_user",
				method: http.MethodPost,
				path:   "/v1/brokers/broker-id/users/admin",
				wantOp: "CreateUser",
			},
			{
				name:   "describe_user",
				method: http.MethodGet,
				path:   "/v1/brokers/broker-id/users/admin",
				wantOp: "DescribeUser",
			},
			{
				name:   "delete_user",
				method: http.MethodDelete,
				path:   "/v1/brokers/broker-id/users/admin",
				wantOp: "DeleteUser",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				e := echo.New()
				req := httptest.NewRequest(tt.method, tt.path, nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				op := h.ExtractOperation(c)
				assert.Equal(t, tt.wantOp, op)
			})
		}
	})

	t.Run("ExtractResource", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			method       string
			path         string
			wantResource string
		}{
			{
				name:         "broker_id_from_describe",
				method:       http.MethodGet,
				path:         "/v1/brokers/my-broker-id",
				wantResource: "my-broker-id",
			},
			{
				name:         "no_resource_from_list",
				method:       http.MethodGet,
				path:         "/v1/brokers",
				wantResource: "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				e := echo.New()
				req := httptest.NewRequest(tt.method, tt.path, nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				resource := h.ExtractResource(c)
				assert.Equal(t, tt.wantResource, resource)
			})
		}
	})

	t.Run("RouteMatcher", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name      string
			method    string
			path      string
			auth      string
			wantMatch bool
		}{
			{
				name:      "brokers_always_match",
				method:    http.MethodGet,
				path:      "/v1/brokers",
				wantMatch: true,
			},
			{
				name:      "configurations_match_with_mq_auth",
				method:    http.MethodGet,
				path:      "/v1/configurations",
				auth:      "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request",
				wantMatch: true,
			},
			{
				name:      "configurations_no_match_without_mq_auth",
				method:    http.MethodGet,
				path:      "/v1/configurations",
				auth:      "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/kafka/aws4_request",
				wantMatch: false,
			},
			{
				name:      "tags_match_with_mq_auth",
				method:    http.MethodGet,
				path:      "/v1/tags/arn:aws:mq:us-east-1:123456789012:broker:my-broker",
				auth:      "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request",
				wantMatch: true,
			},
			{
				name:      "other_path_no_match",
				method:    http.MethodGet,
				path:      "/v2/brokers",
				wantMatch: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				e := echo.New()
				req := httptest.NewRequest(tt.method, tt.path, nil)
				if tt.auth != "" {
					req.Header.Set("Authorization", tt.auth)
				}
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				matcher := h.RouteMatcher()
				assert.Equal(t, tt.wantMatch, matcher(c))
			})
		}
	})
}

func TestMQ_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "update_nonexistent_broker",
			method:     http.MethodPut,
			path:       "/v1/brokers/nonexistent",
			body:       map[string]any{"engineVersion": "5.16.7"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "create_user_on_nonexistent_broker",
			method:     http.MethodPost,
			path:       "/v1/brokers/nonexistent/users/admin",
			body:       map[string]any{"password": "adminpassword1234"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "describe_user_not_found",
			method:     http.MethodGet,
			path:       "/v1/brokers/nonexistent/users/admin",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_user_not_found",
			method:     http.MethodDelete,
			path:       "/v1/brokers/nonexistent/users/admin",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_users_broker_not_found",
			method:     http.MethodGet,
			path:       "/v1/brokers/nonexistent/users",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
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
			tagBody, err := json.Marshal(map[string]any{
				"tags": map[string]string{"env": "test", "owner": "alice"},
			})
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/v1/tags/"+resourceARN, bytes.NewReader(tagBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
			recW := httptest.NewRecorder()
			c := e.NewContext(req, recW)
			err = h.Handler()(c)
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

func TestMQ_AdditionalCoverage(t *testing.T) {
	t.Parallel()

	t.Run("update_user_invalid_body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		// Create broker and user.
		rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
			"brokerName": "test-broker-upd",
			"engineType": "ACTIVEMQ",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var createBrokerResp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBrokerResp))
		brokerID := createBrokerResp["brokerId"]

		rec = doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/admin", map[string]any{
			"password": "password1234",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		// Send invalid JSON body for update.
		req := httptest.NewRequest(
			http.MethodPut,
			"/v1/brokers/"+brokerID+"/users/admin",
			bytes.NewReader([]byte("not-json")),
		)
		req.Header.Set("Content-Type", "application/json")
		recW := httptest.NewRecorder()
		c := e.NewContext(req, recW)
		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, recW.Code)
	})

	t.Run("create_configuration_missing_name", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		body, err := json.Marshal(map[string]any{
			"engineType": "ACTIVEMQ",
		})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/v1/configurations", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("create_configuration_missing_engine_type", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		body, err := json.Marshal(map[string]any{
			"name": "my-config",
		})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/v1/configurations", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("describe_configuration_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		req := httptest.NewRequest(http.MethodGet, "/v1/configurations/nonexistent-id", nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("update_configuration_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		body, err := json.Marshal(map[string]any{"description": "updated"})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPut, "/v1/configurations/nonexistent-id", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("create_tags_on_configuration", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()

		// Create configuration.
		body, err := json.Marshal(map[string]any{
			"name":       "tagged-config",
			"engineType": "ACTIVEMQ",
		})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/v1/configurations", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
		configARN := createResp["arn"].(string)

		// Create tags on configuration.
		tagBody, err := json.Marshal(map[string]any{
			"tags": map[string]string{"tier": "free"},
		})
		require.NoError(t, err)
		req = httptest.NewRequest(http.MethodPost, "/v1/tags/"+configARN, bytes.NewReader(tagBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec = httptest.NewRecorder()
		c = e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Delete tags on configuration.
		req = httptest.NewRequest(http.MethodDelete, "/v1/tags/"+configARN+"?tagKeys=tier", nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
		rec = httptest.NewRecorder()
		c = e.NewContext(req, rec)
		err = h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, rec.Code)
	})

	t.Run("update_user_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		// Create broker without users.
		rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
			"brokerName": "broker-no-users",
			"engineType": "ACTIVEMQ",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var createBrokerResp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBrokerResp))
		brokerID := createBrokerResp["brokerId"]

		// Try to update nonexistent user.
		rec = doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID+"/users/nonexistent", map[string]any{
			"password": "newpassword1234",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}
