package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_AnalyticsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *s3.InMemoryBackend)
		name      string
		bucket    string
		id        string
		configXML string
		wantErr   error
		wantXML   string
	}{
		{
			name:      "put and get returns stored config",
			bucket:    "bkt",
			id:        "cfg1",
			configXML: "<AnalyticsConfiguration><Id>cfg1</Id></AnalyticsConfiguration>",
			setup:     func(t *testing.T, b *s3.InMemoryBackend) { t.Helper(); mustCreateBucket(t, b, "bkt") },
			wantXML:   "<AnalyticsConfiguration>",
		},
		{
			name:    "get missing id returns ErrNoAnalyticsConfig",
			bucket:  "bkt",
			id:      "nonexistent",
			setup:   func(t *testing.T, b *s3.InMemoryBackend) { t.Helper(); mustCreateBucket(t, b, "bkt") },
			wantErr: s3.ErrNoAnalyticsConfig,
		},
		{
			name:    "get missing bucket returns ErrNoSuchBucket",
			bucket:  "no-bucket",
			id:      "cfg1",
			wantErr: s3.ErrNoSuchBucket,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}

			ctx := t.Context()

			if tt.configXML != "" {
				err := b.PutBucketAnalyticsConfiguration(ctx, tt.bucket, tt.id, tt.configXML)
				require.NoError(t, err)
			}

			got, err := b.GetBucketAnalyticsConfiguration(ctx, tt.bucket, tt.id)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, got, tt.wantXML)
		})
	}
}

func TestInMemoryBackend_AnalyticsConfig_DeleteAndList(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")
	ctx := t.Context()

	const xml1 = "<AnalyticsConfiguration><Id>c1</Id></AnalyticsConfiguration>"
	const xml2 = "<AnalyticsConfiguration><Id>c2</Id></AnalyticsConfiguration>"

	require.NoError(t, b.PutBucketAnalyticsConfiguration(ctx, "bkt", "c1", xml1))
	require.NoError(t, b.PutBucketAnalyticsConfiguration(ctx, "bkt", "c2", xml2))

	list, err := b.ListBucketAnalyticsConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Len(t, list, 2)

	require.NoError(t, b.DeleteBucketAnalyticsConfiguration(ctx, "bkt", "c1"))

	list, err = b.ListBucketAnalyticsConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestInMemoryBackend_IntelligentTieringConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *s3.InMemoryBackend)
		name      string
		bucket    string
		id        string
		configXML string
		wantErr   error
		wantXML   string
	}{
		{
			name:      "put and get returns stored config",
			bucket:    "bkt",
			id:        "tier1",
			configXML: "<IntelligentTieringConfiguration><Id>tier1</Id></IntelligentTieringConfiguration>",
			setup:     func(t *testing.T, b *s3.InMemoryBackend) { t.Helper(); mustCreateBucket(t, b, "bkt") },
			wantXML:   "tier1",
		},
		{
			name:    "get missing id returns ErrNoIntelligentTieringConfig",
			bucket:  "bkt",
			id:      "missing",
			setup:   func(t *testing.T, b *s3.InMemoryBackend) { t.Helper(); mustCreateBucket(t, b, "bkt") },
			wantErr: s3.ErrNoIntelligentTieringConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}

			ctx := t.Context()

			if tt.configXML != "" {
				err := b.PutBucketIntelligentTieringConfiguration(
					ctx,
					tt.bucket,
					tt.id,
					tt.configXML,
				)
				require.NoError(t, err)
			}

			got, err := b.GetBucketIntelligentTieringConfiguration(ctx, tt.bucket, tt.id)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, got, tt.wantXML)
		})
	}
}

func TestInMemoryBackend_InventoryConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")
	ctx := t.Context()

	const xml1 = "<InventoryConfiguration><Id>inv1</Id></InventoryConfiguration>"
	const xml2 = "<InventoryConfiguration><Id>inv2</Id></InventoryConfiguration>"

	require.NoError(t, b.PutBucketInventoryConfiguration(ctx, "bkt", "inv1", xml1))
	require.NoError(t, b.PutBucketInventoryConfiguration(ctx, "bkt", "inv2", xml2))

	got, err := b.GetBucketInventoryConfiguration(ctx, "bkt", "inv1")
	require.NoError(t, err)
	assert.Contains(t, got, "inv1")

	_, err = b.GetBucketInventoryConfiguration(ctx, "bkt", "missing")
	require.ErrorIs(t, err, s3.ErrNoInventoryConfig)

	list, err := b.ListBucketInventoryConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Len(t, list, 2)

	require.NoError(t, b.DeleteBucketInventoryConfiguration(ctx, "bkt", "inv1"))
	list, err = b.ListBucketInventoryConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestInMemoryBackend_MetricsConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")
	ctx := t.Context()

	const xml1 = "<MetricsConfiguration><Id>m1</Id></MetricsConfiguration>"

	require.NoError(t, b.PutBucketMetricsConfiguration(ctx, "bkt", "m1", xml1))

	got, err := b.GetBucketMetricsConfiguration(ctx, "bkt", "m1")
	require.NoError(t, err)
	assert.Contains(t, got, "m1")

	_, err = b.GetBucketMetricsConfiguration(ctx, "bkt", "missing")
	require.ErrorIs(t, err, s3.ErrNoMetricsConfig)

	list, err := b.ListBucketMetricsConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Len(t, list, 1)

	require.NoError(t, b.DeleteBucketMetricsConfiguration(ctx, "bkt", "m1"))
	list, err = b.ListBucketMetricsConfigurations(ctx, "bkt")
	require.NoError(t, err)
	assert.Empty(t, list)
}

func TestS3_BucketAnalyticsConfig(t *testing.T) {
	t.Parallel()

	analyticsXML := `<AnalyticsConfiguration><Id>test-analytics</Id>` +
		`<StorageClassAnalysis></StorageClassAnalysis></AnalyticsConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PutBucketAnalyticsConfiguration stores config",
			method: http.MethodPut,
			path:   "/analytics-bucket?analytics&id=test-analytics",
			body:   analyticsXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "PutBucketAnalyticsConfiguration missing id returns 400",
			method: http.MethodPut,
			path:   "/analytics-bucket?analytics",
			body:   analyticsXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:   "GetBucketAnalyticsConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/analytics-bucket?analytics&id=test-analytics",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/analytics-bucket?analytics&id=test-analytics",
					strings.NewReader(analyticsXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "test-analytics",
		},
		{
			name:   "GetBucketAnalyticsConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/analytics-bucket?analytics&id=nonexistent",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "ListBucketAnalyticsConfigurations returns empty list",
			method: http.MethodGet,
			path:   "/analytics-bucket?analytics",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusOK,
			wantBody:   "IsTruncated",
		},
		{
			name:   "ListBucketAnalyticsConfigurations returns stored configs",
			method: http.MethodGet,
			path:   "/analytics-bucket?analytics",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/analytics-bucket?analytics&id=cfg1",
					strings.NewReader(analyticsXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "AnalyticsConfiguration",
		},
		{
			name:   "DeleteBucketAnalyticsConfiguration succeeds",
			method: http.MethodDelete,
			path:   "/analytics-bucket?analytics&id=test-analytics",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "DeleteBucketAnalyticsConfiguration missing id returns 400",
			method: http.MethodDelete,
			path:   "/analytics-bucket?analytics",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "analytics-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:       "DeleteBucketAnalyticsConfiguration on missing bucket returns 404",
			method:     http.MethodDelete,
			path:       "/no-such-bucket?analytics&id=test",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, bodyReader)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_BucketIntelligentTieringConfig verifies put/get/list/delete bucket Intelligent-Tiering configuration.

func TestS3_BucketIntelligentTieringConfig(t *testing.T) {
	t.Parallel()

	tieringXML := `<IntelligentTieringConfiguration><Id>tier-config</Id>` +
		`<Status>Enabled</Status></IntelligentTieringConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PutBucketIntelligentTieringConfiguration stores config",
			method: http.MethodPut,
			path:   "/it-bucket?intelligent-tiering&id=tier-config",
			body:   tieringXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "PutBucketIntelligentTieringConfiguration missing id returns 400",
			method: http.MethodPut,
			path:   "/it-bucket?intelligent-tiering",
			body:   tieringXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:   "GetBucketIntelligentTieringConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/it-bucket?intelligent-tiering&id=tier-config",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/it-bucket?intelligent-tiering&id=tier-config",
					strings.NewReader(tieringXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "tier-config",
		},
		{
			name:   "GetBucketIntelligentTieringConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/it-bucket?intelligent-tiering&id=nonexistent",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "ListBucketIntelligentTieringConfigurations returns empty list",
			method: http.MethodGet,
			path:   "/it-bucket?intelligent-tiering",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
			},
			wantStatus: http.StatusOK,
			wantBody:   "IsTruncated",
		},
		{
			name:   "DeleteBucketIntelligentTieringConfiguration succeeds",
			method: http.MethodDelete,
			path:   "/it-bucket?intelligent-tiering&id=tier-config",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "it-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DeleteBucketIntelligentTieringConfiguration on missing bucket returns 404",
			method:     http.MethodDelete,
			path:       "/no-such-bucket?intelligent-tiering&id=test",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, bodyReader)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_BucketInventoryConfig verifies put/get/list/delete bucket inventory configuration.

func TestS3_BucketInventoryConfig(t *testing.T) {
	t.Parallel()

	inventoryXML := `<InventoryConfiguration><Id>inv-config</Id>` +
		`<IsEnabled>true</IsEnabled></InventoryConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PutBucketInventoryConfiguration stores config",
			method: http.MethodPut,
			path:   "/inventory-bucket?inventory&id=inv-config",
			body:   inventoryXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "PutBucketInventoryConfiguration missing id returns 400",
			method: http.MethodPut,
			path:   "/inventory-bucket?inventory",
			body:   inventoryXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:   "GetBucketInventoryConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/inventory-bucket?inventory&id=inv-config",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/inventory-bucket?inventory&id=inv-config",
					strings.NewReader(inventoryXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "inv-config",
		},
		{
			name:   "GetBucketInventoryConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/inventory-bucket?inventory&id=nonexistent",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "ListBucketInventoryConfigurations returns stored configs",
			method: http.MethodGet,
			path:   "/inventory-bucket?inventory",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/inventory-bucket?inventory&id=cfg1",
					strings.NewReader(inventoryXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "InventoryConfiguration",
		},
		{
			name:   "DeleteBucketInventoryConfiguration succeeds",
			method: http.MethodDelete,
			path:   "/inventory-bucket?inventory&id=inv-config",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "inventory-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DeleteBucketInventoryConfiguration on missing bucket returns 404",
			method:     http.MethodDelete,
			path:       "/no-such-bucket?inventory&id=test",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, bodyReader)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_DeleteBucketLifecycle verifies the legacy DeleteBucketLifecycle alias.

func TestS3_BucketMetricsConfig(t *testing.T) {
	t.Parallel()

	metricsXML := `<MetricsConfiguration><Id>metrics-config</Id>` +
		`</MetricsConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PutBucketMetricsConfiguration stores config",
			method: http.MethodPut,
			path:   "/metrics-bucket?metrics&id=metrics-config",
			body:   metricsXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "PutBucketMetricsConfiguration missing id returns 400",
			method: http.MethodPut,
			path:   "/metrics-bucket?metrics",
			body:   metricsXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:   "GetBucketMetricsConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/metrics-bucket?metrics&id=metrics-config",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/metrics-bucket?metrics&id=metrics-config",
					strings.NewReader(metricsXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "metrics-config",
		},
		{
			name:   "GetBucketMetricsConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/metrics-bucket?metrics&id=nonexistent",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "ListBucketMetricsConfigurations returns empty list",
			method: http.MethodGet,
			path:   "/metrics-bucket?metrics",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusOK,
			wantBody:   "IsTruncated",
		},
		{
			name:   "DeleteBucketMetricsConfiguration succeeds",
			method: http.MethodDelete,
			path:   "/metrics-bucket?metrics&id=metrics-config",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "DeleteBucketMetricsConfiguration missing id returns 400",
			method: http.MethodDelete,
			path:   "/metrics-bucket?metrics",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metrics-bucket")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "InvalidArgument",
		},
		{
			name:       "DeleteBucketMetricsConfiguration on missing bucket returns 404",
			method:     http.MethodDelete,
			path:       "/no-such-bucket?metrics&id=test",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, bodyReader)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_CreateSession verifies the CreateSession operation.
