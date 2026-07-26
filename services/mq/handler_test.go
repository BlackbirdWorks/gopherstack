package mq_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// newTestBackend returns a fresh in-memory MQ backend for tests.
func newTestBackend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	return mq.NewInMemoryBackend(testAccountID, testRegion)
}

// newTestHandler returns a fresh MQ handler wrapping a fresh backend.
func newTestHandler(t *testing.T) *mq.Handler {
	t.Helper()

	return mq.NewHandler(newTestBackend(t))
}

// doRequest performs an HTTP request against the handler with an MQ-signed
// Authorization header. The header is harmless for /v1/brokers requests
// (RouteMatcher always matches that prefix regardless of Authorization) and
// required for /v1/configurations and /v1/tags requests, so a single helper
// covers every route family.
func doRequest(t *testing.T, h *mq.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var reqBody *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(bodyBytes)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRawJSONRequest performs an HTTP request with a literal request body,
// bypassing JSON marshaling. Used to exercise malformed-body error paths.
func doRawJSONRequest(t *testing.T, h *mq.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// parseResponse decodes a JSON response body into a generic map.
func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// createTestBroker creates a broker via the HTTP handler and returns its ID.
func createTestBroker(t *testing.T, h *mq.Handler, name, engineType string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": name,
		"engineType": engineType,
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateBroker %s failed: %s", name, rec.Body.String())

	return parseResponse(t, rec)["brokerId"].(string)
}

// describeTestBroker fetches a broker's full DescribeBroker response.
func describeTestBroker(t *testing.T, h *mq.Handler, brokerID string) map[string]any {
	t.Helper()

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	return parseResponse(t, rec)
}

// createTestConfig creates a configuration via the HTTP handler and returns its ID.
func createTestConfig(t *testing.T, h *mq.Handler, name, engineType string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       name,
		"engineType": engineType,
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateConfiguration %s failed: %s", name, rec.Body.String())

	return parseResponse(t, rec)["id"].(string)
}

// decodeOpaqueToken decodes a base64 page token back to its integer index.
func decodeOpaqueToken(t *testing.T, token string) int {
	t.Helper()

	b, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "next token must be valid base64")

	n, err := strconv.Atoi(string(b))
	require.NoError(t, err, "decoded token must be an integer offset")

	return n
}

func TestName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MQ", h.Name())
}

func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Len(t, ops, 25)
	assert.Contains(t, ops, "CreateBroker")
	assert.Contains(t, ops, "DescribeSharedResources")
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

func TestMatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 86, h.MatchPriority())
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := mq.NewHandler(b)

	_, err := b.CreateBroker(
		"broker-reset", mq.DeploymentModeSingleInstance,
		mq.EngineTypeRabbitMQ, "3.13.2", "mq.m5.large",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, mq.BrokerCount(b))
}

func TestIntrospection(t *testing.T) {
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
