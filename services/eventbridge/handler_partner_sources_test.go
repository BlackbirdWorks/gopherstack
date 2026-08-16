package eventbridge_test

import (
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestHandler_PartnerEventSourceCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreatePartnerEventSource", map[string]any{
		"Name":    "aws.partner/example.com/app",
		"Account": "123456789012",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribePartnerEventSource", map[string]any{
		"Name": "aws.partner/example.com/app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListPartnerEventSources", map[string]any{
		"NamePrefix": "aws.partner/",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "aws.partner/example.com/app")

	rec = auditMakeRequest(t, h, e, "DeletePartnerEventSource", map[string]any{
		"Name": "aws.partner/example.com/app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutPartnerEvents(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "PutPartnerEvents", map[string]any{
		"Entries": []map[string]any{
			{
				"Source":     "aws.partner/example.com/app",
				"DetailType": "UserEvent",
				"Detail":     `{"userId":"u1"}`,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
