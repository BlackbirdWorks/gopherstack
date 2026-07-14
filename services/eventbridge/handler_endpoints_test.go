package eventbridge_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
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
