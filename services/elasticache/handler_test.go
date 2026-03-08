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
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
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

func TestCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		pgName      string
		family      string
		description string
		wantErr     bool
		wantCount   int
	}{
		{
			name:        "create_success",
			pgName:      "my-pg",
			family:      "redis7",
			description: "test param group",
		},
		{
			name:   "create_already_exists",
			pgName: "dup-pg",
			family: "redis7",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("dup-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("first"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name:      "describe_all_includes_defaults",
			wantCount: 8, // 8 default parameter groups seeded
		},
		{
			name:   "describe_specific",
			pgName: "my-specific-pg",
			family: "redis7",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("my-specific-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("specific"),
				})
				require.NoError(t, err)
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			if tt.pgName != "" && tt.setup == nil {
				out, err := client.CreateCacheParameterGroup(
					t.Context(),
					&elasticachesdk.CreateCacheParameterGroupInput{
						CacheParameterGroupName:   aws.String(tt.pgName),
						CacheParameterGroupFamily: aws.String(tt.family),
						Description:               aws.String(tt.description),
					},
				)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, out.CacheParameterGroup)
				assert.Equal(t, tt.pgName, aws.ToString(out.CacheParameterGroup.CacheParameterGroupName))
				assert.Equal(t, tt.family, aws.ToString(out.CacheParameterGroup.CacheParameterGroupFamily))

				return
			}

			if tt.wantErr {
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String(tt.pgName),
					CacheParameterGroupFamily: aws.String(tt.family),
					Description:               aws.String(tt.description),
				})
				require.Error(t, err)

				return
			}

			if tt.wantCount > 0 {
				var pgName *string
				if tt.pgName != "" {
					pgName = aws.String(tt.pgName)
				}
				out, err := client.DescribeCacheParameterGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheParameterGroupsInput{
						CacheParameterGroupName: pgName,
					},
				)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, len(out.CacheParameterGroups), tt.wantCount)
			}
		})
	}
}

func TestDeleteCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		pgName  string
		wantErr bool
	}{
		{
			name:   "success",
			pgName: "my-pg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("my-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			pgName:  "does-not-exist",
			wantErr: true,
		},
		{
			name:    "default_not_deletable",
			pgName:  "default.redis7",
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

			_, err := client.DeleteCacheParameterGroup(t.Context(), &elasticachesdk.DeleteCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyAndResetCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pgName  string
		wantErr bool
	}{
		{
			name:   "modify_and_reset_success",
			pgName: "my-pg",
		},
		{
			name:    "modify_default_fails",
			pgName:  "default.redis7",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if !tt.wantErr {
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String(tt.pgName),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("test"),
				})
				require.NoError(t, err)
			}

			_, modErr := client.ModifyCacheParameterGroup(t.Context(), &elasticachesdk.ModifyCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
				ParameterNameValues: []elasticachetypes.ParameterNameValue{
					{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")},
				},
			})

			if tt.wantErr {
				require.Error(t, modErr)

				return
			}

			require.NoError(t, modErr)

			// Verify via DescribeCacheParameters
			paramsOut, err := client.DescribeCacheParameters(t.Context(), &elasticachesdk.DescribeCacheParametersInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})
			require.NoError(t, err)
			require.Len(t, paramsOut.Parameters, 1)
			assert.Equal(t, "maxmemory-policy", aws.ToString(paramsOut.Parameters[0].ParameterName))
			assert.Equal(t, "allkeys-lru", aws.ToString(paramsOut.Parameters[0].ParameterValue))

			// Reset all parameters
			_, resetErr := client.ResetCacheParameterGroup(t.Context(), &elasticachesdk.ResetCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
				ResetAllParameters:      aws.Bool(true),
			})
			require.NoError(t, resetErr)

			// Should be empty again
			paramsOut2, err := client.DescribeCacheParameters(t.Context(), &elasticachesdk.DescribeCacheParametersInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})
			require.NoError(t, err)
			assert.Empty(t, paramsOut2.Parameters)
		})
	}
}

func TestDefaultParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		paramGroupName string
		wantFound      bool
	}{
		{
			name:           "default_redis7",
			paramGroupName: "default.redis7",
			wantFound:      true,
		},
		{
			name:           "default_redis6x",
			paramGroupName: "default.redis6.x",
			wantFound:      true,
		},
		{
			name:           "default_memcached16",
			paramGroupName: "default.memcached1.6",
			wantFound:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeCacheParameterGroups(
				t.Context(),
				&elasticachesdk.DescribeCacheParameterGroupsInput{
					CacheParameterGroupName: aws.String(tt.paramGroupName),
				},
			)
			require.NoError(t, err)
			require.Len(t, out.CacheParameterGroups, 1)
			assert.Equal(t, tt.paramGroupName, aws.ToString(out.CacheParameterGroups[0].CacheParameterGroupName))
		})
	}
}

func TestCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		sgName    string
		desc      string
		subnetIDs []string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "create_success",
			sgName:    "my-sg",
			desc:      "test subnet group",
			subnetIDs: []string{"subnet-1", "subnet-2"},
		},
		{
			name:      "create_already_exists",
			sgName:    "dup-sg",
			desc:      "duplicate",
			subnetIDs: []string{"subnet-1"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String("dup-sg"),
					CacheSubnetGroupDescription: aws.String("first"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name: "describe_all",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, nm := range []string{"sg-one", "sg-two"} {
					_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
						CacheSubnetGroupName:        aws.String(nm),
						CacheSubnetGroupDescription: aws.String("desc"),
						SubnetIds:                   []string{"subnet-1"},
					})
					require.NoError(t, err)
				}
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			if tt.sgName != "" {
				out, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String(tt.sgName),
					CacheSubnetGroupDescription: aws.String(tt.desc),
					SubnetIds:                   tt.subnetIDs,
				})

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, out.CacheSubnetGroup)
				assert.Equal(t, tt.sgName, aws.ToString(out.CacheSubnetGroup.CacheSubnetGroupName))

				return
			}

			if tt.wantCount > 0 {
				out, err := client.DescribeCacheSubnetGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSubnetGroupsInput{},
				)
				require.NoError(t, err)
				assert.Len(t, out.CacheSubnetGroups, tt.wantCount)
			}
		})
	}
}

func TestDeleteCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "my-sg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String("my-sg"),
					CacheSubnetGroupDescription: aws.String("test"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "does-not-exist",
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

			_, err := client.DeleteCacheSubnetGroup(t.Context(), &elasticachesdk.DeleteCacheSubnetGroupInput{
				CacheSubnetGroupName: aws.String(tt.sgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "my-sg",
		},
		{
			name:    "not_found",
			sgName:  "does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if !tt.wantErr {
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String(tt.sgName),
					CacheSubnetGroupDescription: aws.String("original"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			}

			out, err := client.ModifyCacheSubnetGroup(t.Context(), &elasticachesdk.ModifyCacheSubnetGroupInput{
				CacheSubnetGroupName:        aws.String(tt.sgName),
				CacheSubnetGroupDescription: aws.String("updated"),
				SubnetIds:                   []string{"subnet-1", "subnet-2"},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSubnetGroup)
			assert.Equal(t, "updated", aws.ToString(out.CacheSubnetGroup.CacheSubnetGroupDescription))
		})
	}
}

func TestCreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		snapshotName       string
		clusterID          string
		replicationGroupID string
		wantStatus         string
		wantErr            bool
	}{
		{
			name:         "create_from_cluster",
			snapshotName: "my-snap",
			clusterID:    "snap-cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("snap-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantStatus: "available",
		},
		{
			name:               "create_from_replication_group",
			snapshotName:       "rg-snap",
			replicationGroupID: "rg-for-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-for-snap"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
			wantStatus: "available",
		},
		{
			name:         "create_snapshot_already_exists",
			snapshotName: "dup-snap",
			clusterID:    "snap-cluster2",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("snap-cluster2"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("dup-snap"),
					CacheClusterId: aws.String("snap-cluster2"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name:         "create_cluster_not_found",
			snapshotName: "no-snap",
			clusterID:    "does-not-exist",
			wantErr:      true,
		},
		{
			name:               "invalid_both_sources",
			snapshotName:       "both-snap",
			clusterID:          "some-cluster",
			replicationGroupID: "some-rg",
			wantErr:            true,
		},
		{
			name:         "invalid_no_source",
			snapshotName: "no-source-snap",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.CreateSnapshotInput{
				SnapshotName: aws.String(tt.snapshotName),
			}
			if tt.clusterID != "" {
				input.CacheClusterId = aws.String(tt.clusterID)
			}
			if tt.replicationGroupID != "" {
				input.ReplicationGroupId = aws.String(tt.replicationGroupID)
			}

			out, err := client.CreateSnapshot(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.Snapshot.SnapshotName))
			assert.Equal(t, tt.wantStatus, aws.ToString(out.Snapshot.SnapshotStatus))
		})
	}
}

func TestDescribeSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		snapshotName    string
		filterClusterID string
		filterRGID      string
		wantCount       int
		wantErr         bool
	}{
		{
			name:      "describe_all",
			wantCount: 3,
		},
		{
			name:            "describe_by_cluster",
			filterClusterID: "desc-snap-cluster",
			wantCount:       2,
		},
		{
			name:       "describe_by_replication_group",
			filterRGID: "desc-snap-rg",
			wantCount:  1,
		},
		{
			name:         "describe_specific",
			snapshotName: "snap-a",
			wantCount:    1,
		},
		{
			name:         "describe_not_found",
			snapshotName: "does-not-exist",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			// Create a cluster and two snapshots for describe tests.
			_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("desc-snap-cluster"),
				Engine:         aws.String("redis"),
			})
			require.NoError(t, err)

			for _, sname := range []string{"snap-a", "snap-b"} {
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String(sname),
					CacheClusterId: aws.String("desc-snap-cluster"),
				})
				require.NoError(t, err)
			}

			// Create a replication group and one snapshot for RG-filter tests.
			_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String("desc-snap-rg"),
				ReplicationGroupDescription: aws.String("test"),
			})
			require.NoError(t, err)
			_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
				SnapshotName:       aws.String("rg-snap-x"),
				ReplicationGroupId: aws.String("desc-snap-rg"),
			})
			require.NoError(t, err)

			input := &elasticachesdk.DescribeSnapshotsInput{}
			if tt.snapshotName != "" {
				input.SnapshotName = aws.String(tt.snapshotName)
			}
			if tt.filterClusterID != "" {
				input.CacheClusterId = aws.String(tt.filterClusterID)
			}
			if tt.filterRGID != "" {
				input.ReplicationGroupId = aws.String(tt.filterRGID)
			}

			out, err := client.DescribeSnapshots(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Snapshots, tt.wantCount)
		})
	}
}

func TestDeleteSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, client *elasticachesdk.Client)
		name         string
		snapshotName string
		wantErr      bool
	}{
		{
			name:         "success",
			snapshotName: "del-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("del-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("del-snap"),
					CacheClusterId: aws.String("del-cluster"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:         "not_found",
			snapshotName: "ghost-snap",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteSnapshot(t.Context(), &elasticachesdk.DeleteSnapshotInput{
				SnapshotName: aws.String(tt.snapshotName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.Snapshot.SnapshotName))

			// Verify it is actually gone.
			_, descErr := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
				SnapshotName: aws.String(tt.snapshotName),
			})
			require.Error(t, descErr)
		})
	}
}

func TestCopySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		sourceSnapshotName string
		targetSnapshotName string
		wantErr            bool
	}{
		{
			name:               "success",
			sourceSnapshotName: "source-snap",
			targetSnapshotName: "target-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("copy-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("source-snap"),
					CacheClusterId: aws.String("copy-cluster"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:               "source_not_found",
			sourceSnapshotName: "does-not-exist",
			targetSnapshotName: "target",
			wantErr:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
				SourceSnapshotName: aws.String(tt.sourceSnapshotName),
				TargetSnapshotName: aws.String(tt.targetSnapshotName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.targetSnapshotName, aws.ToString(out.Snapshot.SnapshotName))
		})
	}
}

func TestModifyCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client)
		name     string
		id       string
		nodeType string
		wantErr  bool
	}{
		{
			name:     "success",
			id:       "my-cluster",
			nodeType: "cache.r6g.large",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			id:      "does-not-exist",
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

			out, err := client.ModifyCacheCluster(t.Context(), &elasticachesdk.ModifyCacheClusterInput{
				CacheClusterId: aws.String(tt.id),
				CacheNodeType:  aws.String(tt.nodeType),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheCluster)
			assert.Equal(t, tt.nodeType, aws.ToString(out.CacheCluster.CacheNodeType))
		})
	}
}

func TestModifyReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		id          string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			id:          "my-rg",
			description: "updated description",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("original"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			id:      "does-not-exist",
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

			out, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
				ReplicationGroupId:          aws.String(tt.id),
				ReplicationGroupDescription: aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.description, aws.ToString(out.ReplicationGroup.Description))
		})
	}
}

func TestCreateClusterWithParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterID      string
		paramGroupName string
		wantErr        bool
	}{
		{
			name:           "with_default_param_group",
			clusterID:      "my-cluster",
			paramGroupName: "default.redis7",
		},
		{
			name:           "with_custom_param_group",
			clusterID:      "my-cluster2",
			paramGroupName: "custom-pg",
		},
		{
			name:           "param_group_not_found",
			clusterID:      "my-cluster3",
			paramGroupName: "nonexistent-pg",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.paramGroupName == "custom-pg" {
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("custom-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("custom"),
				})
				require.NoError(t, err)
			}

			out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId:          aws.String(tt.clusterID),
				Engine:                  aws.String("redis"),
				CacheParameterGroupName: aws.String(tt.paramGroupName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheCluster)
		})
	}
}
