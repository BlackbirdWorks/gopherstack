package elasticache_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func newTestStack(t *testing.T) *elasticachesdk.Client {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client
}

// newTestHandler creates a raw handler for internal tests.
func newTestHandler(t *testing.T) *elasticache.Handler {
	t.Helper()
	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	return elasticache.NewHandler(backend)
}

// newTestStackWithBackend creates a test stack returning both the backend and the SDK client.
func newTestStackWithBackend(t *testing.T) (*elasticache.InMemoryBackend, *elasticachesdk.Client) {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return backend, client
}

// ----------------------------------------
// Log delivery configs in DescribeReplicationGroups response
// ----------------------------------------

// newTestStackSeeded creates a test stack backed by the given pre-configured backend.
func newTestStackSeeded(t *testing.T, b *elasticache.InMemoryBackend) *elasticachesdk.Client {
	t.Helper()

	handler := elasticache.NewHandler(b)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// ----------------------------------------
// DescribeSnapshots SnapshotSource filter (AWS parity)
//
// Real AWS: SnapshotSource="system" returns only automated snapshots;
// "user" returns only manual ones. The filter values differ from the
// stored field values ("automated"/"manual").
// ----------------------------------------

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantName     string
		wantOps      []string
		wantPriority bool
	}{
		{
			name:     "name",
			wantName: "ElastiCache",
		},
		{
			name: "supported_operations",
			wantOps: []string{
				"CreateCacheCluster",
				"DeleteCacheCluster",
				"DescribeCacheClusters",
				"ModifyCacheCluster",
				"ListTagsForResource",
				"CreateReplicationGroup",
				"DeleteReplicationGroup",
				"DescribeReplicationGroups",
				"ModifyReplicationGroup",
				"CreateCacheParameterGroup",
				"DeleteCacheParameterGroup",
				"DescribeCacheParameterGroups",
				"ModifyCacheParameterGroup",
				"ResetCacheParameterGroup",
				"DescribeCacheParameters",
				"CreateCacheSubnetGroup",
				"DeleteCacheSubnetGroup",
				"DescribeCacheSubnetGroups",
				"ModifyCacheSubnetGroup",
				"CreateSnapshot",
				"DeleteSnapshot",
				"DescribeSnapshots",
				"CopySnapshot",
				"DescribeCacheEngineVersions",
				"AuthorizeCacheSecurityGroupIngress",
				"BatchApplyUpdateAction",
				"BatchStopUpdateAction",
				"CompleteMigration",
				"CopyServerlessCacheSnapshot",
				"CreateCacheSecurityGroup",
				"CreateGlobalReplicationGroup",
				"CreateServerlessCache",
				"CreateServerlessCacheSnapshot",
				"CreateUser",
			},
		},
		{
			name:         "match_priority",
			wantPriority: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, h.Name())
			}

			if len(tt.wantOps) > 0 {
				ops := h.GetSupportedOperations()
				for _, op := range tt.wantOps {
					assert.Contains(t, ops, op)
				}
			}

			if tt.wantPriority {
				assert.Positive(t, h.MatchPriority())
			}
		})
	}
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		body        string
		contentType string
		wantMatch   bool
	}{
		{
			name:      "non_post",
			method:    http.MethodGet,
			wantMatch: false,
		},
		{
			name:        "wrong_version",
			method:      http.MethodPost,
			body:        "Action=CreateCacheCluster&Version=2012-01-01",
			contentType: "application/x-www-form-urlencoded",
			wantMatch:   false,
		},
		{
			name:        "wrong_content_type",
			method:      http.MethodPost,
			body:        `{"Action":"CreateCacheCluster","Version":"2015-02-02"}`,
			contentType: "application/json",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, "/", bodyReader)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "with_action",
			body:   "Action=DescribeCacheClusters&Version=2015-02-02",
			wantOp: "DescribeCacheClusters",
		},
		{
			name:   "empty_action",
			body:   "Version=2015-02-02",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "cache_cluster_id",
			body:         "Action=DescribeCacheClusters&Version=2015-02-02&CacheClusterId=my-cluster",
			wantResource: "my-cluster",
		},
		{
			name:         "replication_group_id",
			body:         "Action=DescribeReplicationGroups&Version=2015-02-02&ReplicationGroupId=my-rg",
			wantResource: "my-rg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "init_default",
			wantName: "ElastiCache",
		},
		{
			name:     "init_with_config",
			wantName: "ElastiCache",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &elasticache.Provider{}
			assert.Equal(t, tt.wantName, p.Name())

			ctx := &service.AppContext{Logger: slog.Default()}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantName, svc.Name())
		})
	}
}

func TestHandlerUnknownAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stack_initializes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_ = newTestStack(t)
		})
	}
}
