package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_EndpointCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateEndpoint", map[string]any{
		"Name": "h-endpoint",
		"RoutingConfig": map[string]any{
			"FailoverConfig": map[string]any{
				"Primary":   map[string]string{"HealthCheck": "arn:aws:route53:::healthcheck/abc"},
				"Secondary": map[string]string{"Route": "us-west-2"},
			},
		},
		"EventBuses": []map[string]string{
			{"EventBusArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeEndpoint", map[string]any{"Name": "h-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListEndpoints", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateEndpoint", map[string]any{
		"Name":        "h-endpoint",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteEndpoint", map[string]any{"Name": "h-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_Endpoint_TimestampsAreEpochSeconds proves DescribeEndpoint and
// ListEndpoints emit CreationTime/LastModifiedTime as AWS json-1.1 wire
// format epoch-seconds JSON numbers, not RFC3339 strings -- a raw
// json.Marshal of eventbridge.Endpoint's time.Time fields would produce the
// latter, which a real AWS SDK client's deserializer rejects.
func TestHandler_Endpoint_TimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateEndpoint", map[string]any{"Name": "epoch-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeEndpoint", map[string]any{"Name": "epoch-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	creationRaw, ok := raw["CreationTime"]
	require.True(t, ok, "CreationTime must be present")

	var f float64
	require.NoError(t, json.Unmarshal(creationRaw, &f),
		"CreationTime must be a JSON number (epoch seconds), got: %s", string(creationRaw))
	assert.Greater(t, f, float64(0))
}
