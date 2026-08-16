package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestEMR_ListInstanceGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns instance groups for cluster with groups",
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
				"Name":         "ig-cluster",
				"ReleaseLabel": "emr-6.0.0",
				"Instances": map[string]any{
					"InstanceGroups": []map[string]any{
						{"InstanceRole": "MASTER", "InstanceType": "m4.large", "InstanceCount": 1},
						{"InstanceRole": "CORE", "InstanceType": "m4.large", "InstanceCount": 2},
					},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			rec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{
				"ClusterId": createOut.JobFlowID,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var out struct {
				InstanceGroups []struct {
					InstanceGroupType string `json:"InstanceGroupType"`
					Status            struct {
						State string `json:"State"`
					} `json:"Status"`
				} `json:"InstanceGroups"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.InstanceGroups, tt.wantCount)

			for _, g := range out.InstanceGroups {
				assert.Equal(t, "RUNNING", g.Status.State)
			}
		})
	}
}

func TestEMR_ListInstanceGroups_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{
		"ClusterId": "j-NOTEXIST",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_AddInstanceGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		jobFlowID string
		wantCode  int
	}{
		{
			name:      "adds groups to existing cluster",
			jobFlowID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "returns error for non-existent cluster",
			jobFlowID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "ig-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			jobFlowID := tt.jobFlowID
			if jobFlowID == "" {
				jobFlowID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "AddInstanceGroups", map[string]any{
				"JobFlowId": jobFlowID,
				"InstanceGroups": []map[string]any{
					{"InstanceRole": "TASK", "InstanceType": "m4.large", "InstanceCount": 1},
				},
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					ClusterArn       string   `json:"ClusterArn"`
					JobFlowID        string   `json:"JobFlowId"`
					InstanceGroupIDs []string `json:"InstanceGroupIds"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.InstanceGroupIDs, 1)
				assert.Equal(t, jobFlowID, out.JobFlowID)
				assert.Contains(t, out.ClusterArn, "elasticmapreduce")
			}
		})
	}
}

func TestPersistence_InstanceGroups(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := src.RunJobFlow(context.Background(), emr.RunJobFlowParams{
		Name:         "persist-ig-cluster",
		ReleaseLabel: "emr-7.3.0",
		Instances: emr.RunJobFlowInstances{
			InstanceGroups: []emr.InstanceGroupSpec{
				{Name: "master", InstanceRole: "MASTER", InstanceType: "m5.xlarge", InstanceCount: 1},
				{Name: "core", InstanceRole: "CORE", InstanceType: "m5.2xlarge", InstanceCount: 2},
			},
		},
	})
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, dst.Restore(t.Context(), snap))

	clusters, _ := dst.ListClusters(context.Background(), emr.ListClustersParams{})
	require.Len(t, clusters, 1)

	groups, err := dst.ListInstanceGroups(context.Background(), clusters[0].ID)
	require.NoError(t, err)
	assert.Len(t, groups, 2)
}

func TestModifyInstanceGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "modify-ig-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	var listOut struct {
		InstanceGroups []struct {
			ID string `json:"Id"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.InstanceGroups)

	modRec := doEMRRequest(t, h, "ModifyInstanceGroups", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceGroups": []map[string]any{
			{"InstanceGroupId": listOut.InstanceGroups[0].ID, "InstanceCount": 5},
		},
	})
	assert.Equal(t, http.StatusOK, modRec.Code)
}

func TestInstanceGroup_BidPrice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "bidprice-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{
					"Name":          "task",
					"InstanceRole":  "TASK",
					"InstanceType":  "m5.xlarge",
					"InstanceCount": 2,
					"Market":        "SPOT",
					"BidPrice":      "0.05",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceGroups []struct {
			Market   string `json:"Market"`
			BidPrice string `json:"BidPrice"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceGroups, 1)
	assert.Equal(t, "SPOT", out.InstanceGroups[0].Market)
	assert.Equal(t, "0.05", out.InstanceGroups[0].BidPrice)
}

// TestNonNilInstanceGroups verifies ListInstanceGroups returns non-nil for no groups.
func TestNonNilInstanceGroups(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "nogroup-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	groups, err := b.ListInstanceGroups(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.NotNil(t, groups)
	assert.Empty(t, groups)
}

// TestAddInstanceGroups_UniqueIDs verifies group IDs are unique across calls.
func TestAddInstanceGroups_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "multi-ig", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	ids1, _, err := b.AddInstanceGroups(context.Background(), cluster.ID, []emr.InstanceGroupSpec{
		{Name: "g1", InstanceRole: "TASK", InstanceType: "m5.xlarge", InstanceCount: 2},
	})
	require.NoError(t, err)

	ids2, _, err := b.AddInstanceGroups(context.Background(), cluster.ID, []emr.InstanceGroupSpec{
		{Name: "g2", InstanceRole: "TASK", InstanceType: "m5.xlarge", InstanceCount: 2},
	})
	require.NoError(t, err)

	assert.NotEqual(t, ids1[0], ids2[0], "consecutive AddInstanceGroups calls must produce unique IDs")
}

// TestBuildInstanceGroups_UniqueIDs verifies consecutive RunJobFlow calls produce unique instance group IDs.
func TestBuildInstanceGroups_UniqueIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groups := []map[string]any{
		{"InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
		{"InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
	}

	rec1 := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":      "c1",
		"Instances": map[string]any{"InstanceGroups": groups},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":      "c2",
		"Instances": map[string]any{"InstanceGroups": groups},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var c1, c2 struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &c1))
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &c2))

	// Gather instance group IDs via ListInstanceGroups for each cluster
	allIDs := make(map[string]bool)
	for _, clusterID := range []string{c1.JobFlowID, c2.JobFlowID} {
		igRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": clusterID})
		require.Equal(t, http.StatusOK, igRec.Code)

		var igOut struct {
			InstanceGroups []struct {
				ID string `json:"Id"`
			} `json:"InstanceGroups"`
		}
		require.NoError(t, json.Unmarshal(igRec.Body.Bytes(), &igOut))

		for _, ig := range igOut.InstanceGroups {
			assert.False(t, allIDs[ig.ID], "duplicate instance group ID %s", ig.ID)
			allIDs[ig.ID] = true
		}
	}
}
