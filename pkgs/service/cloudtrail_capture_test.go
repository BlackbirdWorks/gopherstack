package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestIsReadOnlyOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		want bool
	}{
		{"Get", "GetObject", true},
		{"List", "ListBuckets", true},
		{"Describe", "DescribeInstances", true},
		{"Head", "HeadObject", true},
		{"UnknownPrefix", "UnknownOperation", false},
		{"Mutate", "PutObject", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, service.IsReadOnlyOperation(tt.op))
		})
	}
}

func TestIsUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		want bool
	}{
		{"Empty", "", true},
		{"UnknownLower", "unknown", true},
		{"UnknownMixed", "UnKnOwN", true},
		{"Valid", "PutObject", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, service.IsUnknownOperation(tt.op))
		})
	}
}

func TestExtractAccessKeyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth string
		want string
	}{
		{"Empty", "", ""},
		{"NoCredential", "AWS4-HMAC-SHA256 Signature=xyz", ""},
		{"MalformedCredential", "AWS4-HMAC-SHA256 Credential=short, Signature=xyz", ""},
		{
			"ValidCredential",
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, Signature=xyz",
			"AKIAIOSFODNN7EXAMPLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			assert.Equal(t, tt.want, service.ExtractAccessKeyID(req))
		})
	}
}

type dummyService struct {
	name             string
	extractOperation string
	extractResource  string
}

func (d *dummyService) Name() string                            { return d.name }
func (d *dummyService) Init(_ context.Context) error            { return nil }
func (d *dummyService) Priority() int                           { return 0 }
func (d *dummyService) RegisterRoutes(_ *echo.Echo)             {}
func (d *dummyService) ExtractOperation(_ *echo.Context) string { return d.extractOperation }
func (d *dummyService) ExtractResource(_ *echo.Context) string  { return d.extractResource }
func (d *dummyService) GetSupportedOperations() []string        { return nil }
func (d *dummyService) Handler() echo.HandlerFunc               { return nil }
func (d *dummyService) RouteMatcher() service.Matcher           { return nil }
func (d *dummyService) MatchPriority() int                      { return 0 }

func TestEventSourceFor(t *testing.T) {
	t.Parallel()

	svc := &dummyService{name: "MyService"}

	tests := []struct {
		name string
		auth string
		want string
	}{
		{
			"from_request",
			"AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, Signature=xyz",
			"s3.amazonaws.com",
		},
		{
			"from_service",
			"",
			"myservice.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			assert.Equal(t, tt.want, service.EventSourceFor(req, svc))
		})
	}
}

type mockRecorder struct {
	events []service.CloudTrailEventInput
}

func (m *mockRecorder) RecordManagementEvent(ev service.CloudTrailEventInput) {
	m.events = append(m.events, ev)
}

func TestWrapCloudTrailCapture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		auth       string
		wantEvents int
	}{
		{"ReadOnly", "GetObject", "", 0},
		{"Unknown", "unknown", "", 0},
		{
			"Mutating",
			"PutObject",
			"AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, Signature=xyz",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &mockRecorder{}
			svc := &dummyService{name: "S3", extractOperation: tt.op, extractResource: "bucket"}

			next := func(_ *echo.Context) error { return nil }
			handler := service.WrapCloudTrailCapture(rec, svc, next)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			c := e.NewContext(req, httptest.NewRecorder())

			err := handler(c)
			require.NoError(t, err)
			assert.Len(t, rec.events, tt.wantEvents)

			if tt.wantEvents > 0 {
				assert.Equal(t, tt.op, rec.events[0].EventName)
				assert.Equal(t, "s3.amazonaws.com", rec.events[0].EventSource)
				assert.Equal(t, "AKID", rec.events[0].AccessKeyID)
			}
		})
	}
}
