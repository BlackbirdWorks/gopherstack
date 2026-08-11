package service_test

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestRecordRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		records []string
	}{
		{name: "records a new region", records: []string{"test-record-basic-1"}, want: "test-record-basic-1"},
		{name: "ignores empty region", records: []string{""}, want: ""},
		{
			name:    "idempotent on repeat",
			records: []string{"test-record-idempotent-1", "test-record-idempotent-1", "test-record-idempotent-1"},
			want:    "test-record-idempotent-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, r := range tt.records {
				service.RecordRegion(r)
			}

			if tt.want == "" {
				assert.NotContains(t, service.KnownRegions(), "")

				return
			}

			assert.Contains(t, service.KnownRegions(), tt.want)
		})
	}
}

func TestSeedRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seed       []string
		wantHas    []string
		wantNotHas []string
	}{
		{
			name:    "seeds multiple new regions",
			seed:    []string{"test-seed-1", "test-seed-2"},
			wantHas: []string{"test-seed-1", "test-seed-2"},
		},
		{
			name:       "ignores empty string in seed list",
			seed:       []string{"test-seed-empty-check", ""},
			wantHas:    []string{"test-seed-empty-check"},
			wantNotHas: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service.SeedRegions(tt.seed...)

			known := service.KnownRegions()
			for _, want := range tt.wantHas {
				assert.Contains(t, known, want)
			}
			for _, notWant := range tt.wantNotHas {
				assert.NotContains(t, known, notWant)
			}
		})
	}
}

func TestKnownRegions_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed []string
	}{
		{name: "reverse order input", seed: []string{"test-sorted-z", "test-sorted-a", "test-sorted-m"}},
		{name: "already sorted input", seed: []string{"test-sorted2-a", "test-sorted2-b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service.SeedRegions(tt.seed...)

			known := service.KnownRegions()
			assert.True(t, sort.StringsAreSorted(known), "expected %v to be sorted", known)
		})
	}
}

func TestRegionTrackingMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		headerRegion  string
		defaultRegion string
		want          string
	}{
		{
			name:          "uses X-Amz-Region header",
			headerRegion:  "test-middleware-header-1",
			defaultRegion: "test-middleware-default-unused",
			want:          "test-middleware-header-1",
		},
		{
			name:          "falls back to default region",
			defaultRegion: "test-middleware-default-1",
			want:          "test-middleware-default-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next := func(c *echo.Context) error { return c.NoContent(http.StatusOK) }
			handler := service.RegionTrackingMiddleware(tt.defaultRegion)(next)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.headerRegion != "" {
				req.Header.Set("X-Amz-Region", tt.headerRegion)
			}
			c := e.NewContext(req, httptest.NewRecorder())

			err := handler(c)
			require.NoError(t, err)
			assert.Contains(t, service.KnownRegions(), tt.want)
		})
	}
}
