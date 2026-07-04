package cloudtrail_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// fakeMutatingService is a minimal service.Registerable standing in for a
// real emulator service (e.g. EC2, S3) so the test can drive a request
// through the central service.Registry chokepoint — the same chokepoint
// every real service is wired through in cli.go — without touching any
// individual service package.
type fakeMutatingService struct{}

func (fakeMutatingService) Name() string { return "FakeService" }

func (fakeMutatingService) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"ok": "true"})
	}
}

func (fakeMutatingService) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return c.Request().Header.Get("X-Amz-Target") == "FakeService.CreateWidget"
	}
}

func (fakeMutatingService) GetSupportedOperations() []string {
	return []string{"CreateWidget"}
}

func (fakeMutatingService) ExtractOperation(_ *echo.Context) string {
	return "CreateWidget"
}

func (fakeMutatingService) ExtractResource(_ *echo.Context) string {
	return "widget-1"
}

func (fakeMutatingService) MatchPriority() int {
	return service.PriorityHeaderExact
}

// fakeReadOnlyService is a second stand-in service exercising a read-only
// operation (naming convention "Get*"/"List*"/"Describe*"), which the
// registry's CloudTrail capture must skip by default.
type fakeReadOnlyService struct{}

func (fakeReadOnlyService) Name() string { return "FakeReadOnlyService" }

func (fakeReadOnlyService) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"ok": "true"})
	}
}

func (fakeReadOnlyService) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return c.Request().Header.Get("X-Amz-Target") == "FakeReadOnlyService.ListWidgets"
	}
}

func (fakeReadOnlyService) GetSupportedOperations() []string {
	return []string{"ListWidgets"}
}

func (fakeReadOnlyService) ExtractOperation(_ *echo.Context) string {
	return "ListWidgets"
}

func (fakeReadOnlyService) ExtractResource(_ *echo.Context) string {
	return ""
}

func (fakeReadOnlyService) MatchPriority() int {
	return service.PriorityHeaderExact
}

// newCaptureTestRegistry wires a service.Registry exactly the way cli.go's
// setupRegistry does: it registers the live CloudTrail backend as the
// registry's global CloudTrailRecorder (reusing the same backend the test
// later asserts against, per the "do not construct a second backend" rule),
// then registers the fake services and mounts the router on a fresh Echo
// instance.
func newCaptureTestRegistry(t *testing.T, extra ...service.Registerable) (*echo.Echo, *cloudtrail.InMemoryBackend) {
	t.Helper()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	ctHandler := cloudtrail.NewHandler(backend)

	registry := service.NewRegistry()
	registry.SetCloudTrailRecorder(ctHandler)

	for _, svc := range extra {
		require.NoError(t, registry.Register(svc))
	}
	require.NoError(t, registry.Register(ctHandler))

	router := service.NewServiceRouter(registry)

	e := echo.New()
	e.Use(router.RouteHandler())

	return e, backend
}

// TestCloudTrailCapture_MutatingCallRecordedViaRegistry drives a mutating
// call through the central service.Registry chokepoint (the same one every
// real service is wired through) and asserts LookupEvents subsequently
// returns a real management event for it.
func TestCloudTrailCapture_MutatingCallRecordedViaRegistry(t *testing.T) {
	t.Parallel()

	e, backend := newCaptureTestRegistry(t, fakeMutatingService{})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "FakeService.CreateWidget")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIAFAKEKEY/20260703/us-west-2/fakeservice/aws4_request, "+
			"SignedHeaders=host, Signature=abc",
	)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	out := backend.LookupEvents(cloudtrail.LookupEventsInput{})
	require.Len(t, out.Events, 1)

	ev := out.Events[0]
	assert.Equal(t, "CreateWidget", ev.EventName)
	assert.Equal(t, "fakeservice.amazonaws.com", ev.EventSource)
	assert.Equal(t, "AKIAFAKEKEY", ev.AccessKeyID)
	assert.Equal(t, "AKIAFAKEKEY", ev.Username)
	assert.Equal(t, "false", ev.ReadOnly)
	assert.NotEmpty(t, ev.EventID)
	assert.False(t, ev.EventTime.IsZero())
	assert.NotEmpty(t, ev.CloudTrailEvent)
	require.Len(t, ev.Resources, 1)
	assert.Equal(t, "widget-1", ev.Resources[0].ResourceName)

	// LookupEvents' existing filters must still honor the newly-captured event.
	filtered := backend.LookupEvents(cloudtrail.LookupEventsInput{
		LookupAttributes: []cloudtrail.LookupAttribute{
			{AttributeKey: "EventName", AttributeValue: "CreateWidget"},
		},
	})
	assert.Len(t, filtered.Events, 1)

	noMatch := backend.LookupEvents(cloudtrail.LookupEventsInput{
		LookupAttributes: []cloudtrail.LookupAttribute{
			{AttributeKey: "EventName", AttributeValue: "DeleteWidget"},
		},
	})
	assert.Empty(t, noMatch.Events)
}

// TestCloudTrailCapture_ReadOnlyCallNotRecorded verifies that read-only calls
// (Get/List/Describe/... naming convention) are excluded from capture by
// default, matching CloudTrail trails only logging management (write) events
// unless a read event selector is explicitly configured.
func TestCloudTrailCapture_ReadOnlyCallNotRecorded(t *testing.T) {
	t.Parallel()

	e, backend := newCaptureTestRegistry(t, fakeReadOnlyService{})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "FakeReadOnlyService.ListWidgets")
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	out := backend.LookupEvents(cloudtrail.LookupEventsInput{})
	assert.Empty(t, out.Events)
}
