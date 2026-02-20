package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3_sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepro_RoutingFix(t *testing.T) {
	backend := s3.NewInMemoryBackend(nil)
	s3Handler := s3.NewHandler(backend, slog.Default())

	e := echo.New()

	// Create bucket named "dashboard" (valid S3 name)
	_, err := backend.CreateBucket(context.Background(), &s3_sdk.CreateBucketInput{
		Bucket: aws.String("dashboard"),
	})
	require.NoError(t, err)

	// Set up registry and router like in main.go
	reg := service.NewRegistry(slog.Default())
	require.NoError(t, reg.Register(s3Handler))

	router := service.NewServiceRouter(reg)
	e.Use(router.RouteHandler())

	t.Run("BucketNamedDashboard_Success", func(t *testing.T) {
		// Request: HEAD /dashboard
		req := httptest.NewRequest(http.MethodHead, "/dashboard", nil)
		rec := httptest.NewRecorder()

		// This should now be handled by S3 handler because RouteMatcher
		// no longer rejects "/dashboard" exactly.
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DashboardUI_StillWorks", func(t *testing.T) {
		// Request: GET /dashboard/s3 (should be handled by someone else or Echo)
		req := httptest.NewRequest(http.MethodGet, "/dashboard/s3", nil)
		rec := httptest.NewRecorder()

		// The service router should evaluate S3 matcher.
		// RouteMatcher should return false for /dashboard/s3 because it has prefix /dashboard/
		e.ServeHTTP(rec, req)

		// It should NOT be handled by S3 handler (which would return 400 for bucket "dashboard")
		// Instead, it falls through.
		assert.NotEqual(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, http.StatusNotFound, rec.Code, "Expected 404 because no dashboard handler is registered in THIS test")
	})
}
