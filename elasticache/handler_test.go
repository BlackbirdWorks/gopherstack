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

	"github.com/blackbirdworks/gopherstack/elasticache"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newTestStack(t *testing.T) *elasticachesdk.Client {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1")
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
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
	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	return elasticache.NewHandler(backend)
}

func TestCreateCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, client *elasticachesdk.Client)
		name         string
		clusterID    string
		engine       string
		nodeType     string
		wantStatus   string
		wantErr      bool
		wantEndpoint bool
	}{
		{
			name:         "success",
			clusterID:    "my-cluster",
			engine:       "redis",
			nodeType:     "cache.t3.micro",
			wantStatus:   "available",
			wantEndpoint: true,
		},
		{
			name:      "already_exists",
			clusterID: "dup",
			engine:    "redis",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("dup"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String(tt.clusterID),
				Engine:         aws.String(tt.engine),
				CacheNodeType:  aws.String(tt.nodeType),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheCluster)
			assert.Equal(t, tt.clusterID, aws.ToString(out.CacheCluster.CacheClusterId))
			assert.Equal(t, tt.wantStatus, aws.ToString(out.CacheCluster.CacheClusterStatus))
			assert.Equal(t, tt.engine, aws.ToString(out.CacheCluster.Engine))

			if tt.wantEndpoint {
				require.NotEmpty(t, out.CacheCluster.CacheNodes)
				ep := out.CacheCluster.CacheNodes[0].Endpoint
				require.NotNil(t, ep)
				assert.Contains(t, aws.ToString(ep.Address), ".cache.amazonaws.com")
				assert.Positive(t, aws.ToInt32(ep.Port))
			}
		})
	}
}

func TestDescribeCacheClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		clusterID string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "describe_specific",
			clusterID: "my-cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantCount: 1,
		},
		{
			name: "describe_all",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, id := range []string{"cluster-a", "cluster-b"} {
					_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
						CacheClusterId: aws.String(id),
						Engine:         aws.String("redis"),
					})
					require.NoError(t, err)
				}
			},
			wantCount: 2,
		},
		{
			name:      "not_found",
			clusterID: "does-not-exist",
			wantErr:   true,
		},
		{
			name:      "not_found_after_delete",
			clusterID: "my-cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.DeleteCacheCluster(t.Context(), &elasticachesdk.DeleteCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			var clusterID *string
			if tt.clusterID != "" {
				clusterID = aws.String(tt.clusterID)
			}

			out, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
				CacheClusterId: clusterID,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.CacheClusters, tt.wantCount)

			if tt.clusterID != "" && tt.wantCount == 1 {
				assert.Equal(t, tt.clusterID, aws.ToString(out.CacheClusters[0].CacheClusterId))
			}
		})
	}
}

func TestDeleteCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, client *elasticachesdk.Client)
		name       string
		clusterID  string
		wantStatus string
		wantErr    bool
	}{
		{
			name:      "success",
			clusterID: "my-cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantStatus: "deleting",
		},
		{
			name:      "not_found",
			clusterID: "does-not-exist",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteCacheCluster(t.Context(), &elasticachesdk.DeleteCacheClusterInput{
				CacheClusterId: aws.String(tt.clusterID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheCluster)
			assert.Equal(t, tt.wantStatus, aws.ToString(out.CacheCluster.CacheClusterStatus))
		})
	}
}

func TestCreateReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		rgID        string
		description string
		wantStatus  string
		wantErr     bool
	}{
		{
			name:        "success",
			rgID:        "my-rg",
			description: "test replication group",
			wantStatus:  "available",
		},
		{
			name:        "already_exists",
			rgID:        "dup-rg",
			description: "duplicate",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("dup-rg"),
					ReplicationGroupDescription: aws.String("first"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String(tt.rgID),
				ReplicationGroupDescription: aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
			assert.Equal(t, tt.wantStatus, aws.ToString(out.ReplicationGroup.Status))
		})
	}
}

func TestDescribeReplicationGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		rgID      string
		wantCount int
		wantErr   bool
	}{
		{
			name: "describe_specific",
			rgID: "my-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("test replication group"),
				})
				require.NoError(t, err)
			},
			wantCount: 1,
		},
		{
			name: "describe_all",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, rg := range []struct{ id, desc string }{{"rg-one", "first"}, {"rg-two", "second"}} {
					_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
						ReplicationGroupId:          aws.String(rg.id),
						ReplicationGroupDescription: aws.String(rg.desc),
					})
					require.NoError(t, err)
				}
			},
			wantCount: 2,
		},
		{
			name:    "not_found",
			rgID:    "does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			var rgID *string
			if tt.rgID != "" {
				rgID = aws.String(tt.rgID)
			}

			out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
				ReplicationGroupId: rgID,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReplicationGroups, tt.wantCount)

			if tt.rgID != "" && tt.wantCount == 1 {
				assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroups[0].ReplicationGroupId))
			}
		})
	}
}

func TestDeleteReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, client *elasticachesdk.Client)
		name       string
		rgID       string
		wantStatus string
		wantErr    bool
	}{
		{
			name: "success",
			rgID: "my-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("test replication group"),
				})
				require.NoError(t, err)
			},
			wantStatus: "deleting",
		},
		{
			name:    "not_found",
			rgID:    "does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteReplicationGroup(t.Context(), &elasticachesdk.DeleteReplicationGroupInput{
				ReplicationGroupId: aws.String(tt.rgID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.wantStatus, aws.ToString(out.ReplicationGroup.Status))
		})
	}
}

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client) string
		name    string
		arn     string
		wantErr bool
	}{
		{
			name: "cluster_no_tags",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("tag-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheCluster.ARN)
			},
		},
		{
			name: "replication_group_no_tags",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-tags"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ReplicationGroup.ARN)
			},
		},
		{
			name:    "not_found",
			arn:     "arn:aws:elasticache:us-east-1:000000000000:cluster:does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			resourceARN := tt.arn
			if tt.setup != nil {
				resourceARN = tt.setup(t, client)
			}

			out, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, out)
			assert.Empty(t, out.TagList)
		})
	}
}

func TestBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		engineMode        string
		clusterEngine     string
		wantFirstEndpoint string
		wantFirstStatus   string
		clusterIDs        []string
		clusterPort       int
		wantCount         int
		wantFirstPort     int
	}{
		{
			name:              "stub_engine_mode",
			engineMode:        elasticache.EngineStub,
			clusterIDs:        []string{"stub-cluster"},
			clusterEngine:     "redis",
			clusterPort:       0,
			wantFirstEndpoint: ".cache.amazonaws.com",
			wantFirstPort:     6379,
			wantFirstStatus:   "available",
		},
		{
			name:          "default_engine",
			engineMode:    "",
			clusterIDs:    []string{"test"},
			clusterEngine: "redis",
			clusterPort:   6379,
			wantCount:     1,
		},
		{
			name:          "list_all",
			engineMode:    elasticache.EngineStub,
			clusterIDs:    []string{"c1", "c2"},
			clusterEngine: "redis",
			wantCount:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(tt.engineMode, "000000000000", "us-east-1")

			var firstCluster *elasticache.Cluster
			for _, id := range tt.clusterIDs {
				cluster, err := backend.CreateCluster(id, tt.clusterEngine, "cache.t3.micro", tt.clusterPort)
				require.NoError(t, err)
				if firstCluster == nil {
					firstCluster = cluster
				}
			}

			if tt.wantCount > 0 {
				assert.Len(t, backend.ListAll(), tt.wantCount)
			}

			if tt.wantFirstEndpoint != "" {
				require.NotNil(t, firstCluster)
				assert.Contains(t, firstCluster.Endpoint, tt.wantFirstEndpoint)
				assert.Equal(t, tt.wantFirstPort, firstCluster.Port)
				assert.Equal(t, tt.wantFirstStatus, firstCluster.Status)
			}
		})
	}
}

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
				"ListTagsForResource",
				"CreateReplicationGroup",
				"DeleteReplicationGroup",
				"DescribeReplicationGroups",
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
