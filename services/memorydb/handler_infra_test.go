package memorydb_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestProvider_Init tests that the Provider initialises successfully.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &memorydb.Provider{}

	assert.Equal(t, "MemoryDB", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "MemoryDB", svc.Name())
}

// TestBackend_ListClusters tests the ListClusters method.
func TestBackend_ListClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*memorydb.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "multiple clusters",
			setup: func(b *memorydb.InMemoryBackend) {
				for _, clusterName := range []string{"cluster-1", "cluster-2", "cluster-3"} {
					_, err := b.CreateCluster(testRegion, testAccountID, &memorydb.ExportedCreateClusterRequest{
						ClusterName: clusterName,
						NodeType:    "db.r6g.large",
						ACLName:     "open-access",
					})
					require.NoError(t, err)
				}
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			clusters := b.ListClusters()
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

// TestBackend_Purge tests that the Purge method removes old resources.
func TestBackend_Purge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantClusters int
	}{
		{
			name:         "purges old clusters",
			wantClusters: 0,
		},
		{
			name:         "keeps recent clusters",
			wantClusters: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()
			ctx := t.Context()

			_, err := b.CreateCluster(testRegion, testAccountID, &memorydb.ExportedCreateClusterRequest{
				ClusterName: "old-cluster",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			})
			require.NoError(t, err)

			switch tt.name {
			case "purges old clusters":
				// Set cutoff to future - purge everything
				b.Purge(ctx, time.Now().Add(time.Hour))
				clusters := b.ListClusters()
				assert.Len(t, clusters, tt.wantClusters)
			case "keeps recent clusters":
				// Set cutoff to past - keep everything
				b.Purge(ctx, time.Now().Add(-time.Hour))
				clusters := b.ListClusters()
				assert.Len(t, clusters, tt.wantClusters)
			}
		})
	}
}

// TestBackend_Purge_CancelledContext tests that Purge respects context cancellation.
func TestBackend_Purge_CancelledContext(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateCluster(testRegion, testAccountID, &memorydb.ExportedCreateClusterRequest{
		ClusterName: "my-cluster",
		NodeType:    "db.r6g.large",
		ACLName:     "open-access",
	})
	require.NoError(t, err)

	// Purge with cancelled context should be a no-op
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	b.Purge(ctx, time.Now().Add(time.Hour))

	// Cluster should still be there
	clusters := b.ListClusters()
	assert.Len(t, clusters, 1)
}

// TestHandler_Name tests the Name method.
func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MemoryDB", h.Name())
}

// TestHandler_Infrastructure tests routing infrastructure methods.
func TestHandler_Infrastructure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// These infrastructure methods return constant values
	assert.Equal(t, "memorydb", h.ChaosServiceName())
	assert.NotNil(t, h.ChaosOperations())
	assert.NotNil(t, h.ChaosRegions())

	// RouteMatcher
	rm := h.RouteMatcher()
	assert.NotNil(t, rm)
	assert.Equal(t, 87, h.MatchPriority())

	// GetSupportedOperations
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateCluster")
	assert.Contains(t, ops, "BatchUpdateCluster")
	assert.Contains(t, ops, "CopySnapshot")
	assert.Contains(t, ops, "CreateMultiRegionCluster")
	assert.Contains(t, ops, "CreateSnapshot")
	assert.Contains(t, ops, "DescribeEngineVersions")
	assert.Contains(t, ops, "DescribeEvents")
	assert.Contains(t, ops, "DescribeMultiRegionClusters")
	assert.Contains(t, ops, "DescribeMultiRegionParameterGroups")
}

// TestHandler_Purge tests the handler Purge delegation.
func TestHandler_Purge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "purge-test",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	// Purge through handler - should delegate to backend
	h.Purge(t.Context(), time.Now().Add(time.Hour))

	// Cluster should be gone
	rec := doRequest(t, h, "DescribeClusters", map[string]any{
		"ClusterName": "purge-test",
	})
	assert.Equal(t, 404, rec.Code)
}

// TestBackend_DescribeMultiRegionParameterGroups_WithData tests filtering with existing data.
func TestBackend_DescribeMultiRegionParameterGroups_WithData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterName string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "all groups",
			wantCount: 4, // 4 default multi-region parameter groups are seeded on startup
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()
			groups, err := b.DescribeMultiRegionParameterGroups(tt.filterName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

// TestHandler_DescribeACLs_All tests DescribeACLs with no filter returns all ACLs.
func TestHandler_DescribeACLs_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 2 ACLs
	doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-1"})
	doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-2"})

	// Describe with no filter
	rec := doRequest(t, h, "DescribeACLs", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_DescribeSubnetGroups_All tests DescribeSubnetGroups with no filter.
func TestHandler_DescribeSubnetGroups_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "sg-1",
		"SubnetIds":       []string{"subnet-1"},
	})

	rec := doRequest(t, h, "DescribeSubnetGroups", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_DescribeUsers_All tests DescribeUsers with no filter.
func TestHandler_DescribeUsers_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateUser", map[string]any{
		"UserName":     "u1",
		"AccessString": "on ~*",
		"AuthenticationMode": map[string]any{
			"Type":      "password",
			"Passwords": []string{"pass1"},
		},
	})

	rec := doRequest(t, h, "DescribeUsers", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_DescribeParameterGroups_All tests DescribeParameterGroups with no filter.
func TestHandler_DescribeParameterGroups_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "pg-1",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "DescribeParameterGroups", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_DescribeEvents_WithData tests DescribeEvents with actual events.
func TestHandler_DescribeEvents_WithData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name: "filter by source name",
			body: map[string]any{
				"SourceName": "my-cluster",
			},
			wantCount: 1,
		},
		{
			name: "filter by source type",
			body: map[string]any{
				"SourceType": "cluster",
			},
			wantCount: 2,
		},
		{
			name:      "all events",
			body:      map[string]any{},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()

			b.AddEvent(&memorydb.ExportedEvent{
				SourceName: "my-cluster",
				SourceType: "cluster",
				Message:    "event 1",
			})
			b.AddEvent(&memorydb.ExportedEvent{
				SourceName: "other-cluster",
				SourceType: "cluster",
				Message:    "event 2",
			})

			events, err := b.DescribeEvents(&memorydb.ExportedDescribeEventsRequest{
				SourceName: func() string {
					if v, ok := tt.body["SourceName"].(string); ok {
						return v
					}

					return ""
				}(),
				SourceType: func() string {
					if v, ok := tt.body["SourceType"].(string); ok {
						return v
					}

					return ""
				}(),
			})
			require.NoError(t, err)
			assert.Len(t, events, tt.wantCount)
		})
	}
}

// TestHandler_Tags_NotFound tests tag operations on unknown ARN.
func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list tags unknown ARN",
			op:         "ListTags",
			body:       map[string]any{"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such"},
			wantStatus: 404,
		},
		{
			name: "tag resource unknown ARN",
			op:   "TagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such",
				"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
			},
			wantStatus: 404,
		},
		{
			name: "untag resource unknown ARN",
			op:   "UntagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such",
				"TagKeys":     []string{"k"},
			},
			wantStatus: 404,
		},
		{
			name:       "list tags missing ARN",
			op:         "ListTags",
			body:       map[string]any{},
			wantStatus: 400,
		},
		{
			name:       "tag resource missing ARN",
			op:         "TagResource",
			body:       map[string]any{"Tags": []map[string]any{{"Key": "k", "Value": "v"}}},
			wantStatus: 400,
		},
		{
			name:       "untag resource missing ARN",
			op:         "UntagResource",
			body:       map[string]any{"TagKeys": []string{"k"}},
			wantStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ACL_Tags tests that ACL tags are routed correctly.
func TestHandler_ACL_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create ACL with tags
	rec := doRequest(t, h, "CreateACL", map[string]any{
		"ACLName": "tagged-acl",
		"Tags":    []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	require.Equal(t, 200, rec.Code)

	// Get ACL ARN
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	aclMap := aclResp["ACL"].(map[string]any)
	aclARN := aclMap["ARN"].(string)

	// List tags
	tagsRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": aclARN})
	assert.Equal(t, 200, tagsRec.Code)
}
