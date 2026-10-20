package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

func inspectorServer(t *testing.T) (*echo.Echo, *apptrace.Tracer) {
	t.Helper()

	e := echo.New()
	tr := registerAppInspector(context.Background(), e)

	e.Any("/api", func(c *echo.Context) error {
		apptrace.FromContext(c.Request().Context()).SetOperation("sns", "Publish", "topic")

		done := traceSQSDelivery(c.Request().Context(), "arn:aws:sqs:eu-west-1:000000000000:q")
		done(assert.AnError)

		return c.String(http.StatusOK, "ok")
	})

	return e, tr
}

func inspectorDo(e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestAppInspectorEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		total int
	}{
		{"all", "", 2},
		{"service", "?service=sqs", 1},
		{"children only", "?kind=internal", 1},
		{"errors", "?errorsOnly=true", 1},
		{"none", "?service=s3", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e, _ := inspectorServer(t)
			require.Equal(t, http.StatusOK, inspectorDo(e, http.MethodPost, "/api", "x").Code)

			rec := inspectorDo(e, http.MethodGet, appInspectorPath+"/requests"+tc.query, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var res apptrace.ListResult
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
			assert.Equal(t, tc.total, res.Total)
		})
	}
}

func TestAppInspectorDetailTraceAndConfig(t *testing.T) {
	t.Parallel()

	e, tr := inspectorServer(t)

	rec := inspectorDo(e, http.MethodPut, appInspectorPath+"/config", `{"detail":true}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, tr.DetailEnabled())

	inspectorDo(e, http.MethodPost, "/api", `{"Password":"hunter2"}`)

	list := tr.List(apptrace.Filter{Kind: apptrace.KindRequest})
	require.Len(t, list.Items, 1)

	rec = inspectorDo(e, http.MethodGet, appInspectorPath+"/requests/"+list.Items[0].ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var detail appInspectorDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
	require.NotNil(t, detail.Span.Detail)
	assert.NotContains(t, detail.Span.Detail.RequestBody, "hunter2")
	require.Len(t, detail.Trace, 2)

	var child apptrace.Span

	for _, s := range detail.Trace {
		if s.Kind == apptrace.KindInternal {
			child = s
		}
	}

	assert.Equal(t, "DeliveryFailed", child.ErrorCode)
	assert.Equal(t, "eu-west-1", child.Region)
	assert.Equal(t, detail.Span.ID, child.ParentID)

	assert.Equal(t, http.StatusNotFound, inspectorDo(e, http.MethodGet, appInspectorPath+"/requests/nope", "").Code)
	assert.Equal(t, http.StatusNoContent, inspectorDo(e, http.MethodDelete, appInspectorPath+"/requests", "").Code)
	assert.Empty(t, tr.List(apptrace.Filter{}).Items)

	assert.Equal(t, http.StatusOK, inspectorDo(e, http.MethodPut, appInspectorPath+"/config", `{"enabled":false}`).Code)
	assert.False(t, tr.Enabled())
}

func TestAppInspectorConfigFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env  map[string]string
		name string
		want apptrace.Config
	}{
		{map[string]string{}, "defaults", apptrace.Config{Enabled: true}},
		{map[string]string{envAppTrace: "off"}, "off", apptrace.Config{}},
		{
			map[string]string{envAppTraceBodies: "true", envAppTraceCapacity: "50", envAppTraceMaxBody: "99"},
			"bodies", apptrace.Config{Enabled: true, Detail: true, Capacity: 50, MaxBody: 99},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, appInspectorConfig(func(k string) string { return tc.env[k] }))
		})
	}
}
