package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_MultiRegionCluster_TLSEnabled verifies TLSEnabled defaults to
// true (matching CreateCluster's TLS-by-default convention) and honors an
// explicit false, matching the real MultiRegionCluster.TLSEnabled wire field
// (confirmed against aws-sdk-go-v2/service/memorydb/types.MultiRegionCluster;
// a prior pass parsed the request's TLSEnabled but never stored or returned
// it).
func TestHandler_MultiRegionCluster_TLSEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantTLS bool
	}{
		{
			name: "defaults to true when omitted",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "tls-default",
				"NodeType":                     "db.r6g.large",
			},
			wantTLS: true,
		},
		{
			name: "explicit true",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "tls-true",
				"NodeType":                     "db.r6g.large",
				"TLSEnabled":                   true,
			},
			wantTLS: true,
		},
		{
			name: "explicit false",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "tls-false",
				"NodeType":                     "db.r6g.large",
				"TLSEnabled":                   false,
			},
			wantTLS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateMultiRegionCluster", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			mrc, _ := resp["MultiRegionCluster"].(map[string]any)
			require.NotNil(t, mrc)
			assert.Equal(t, tt.wantTLS, mrc["TLSEnabled"])
		})
	}
}

// TestHandler_DescribeMultiRegionClusters_ShowClusterDetails verifies that
// DescribeMultiRegionClusters populates MultiRegionCluster.Clusters (the real
// SDK's []RegionalCluster) with the per-Region clusters created against this
// multi-Region cluster only when ShowClusterDetails is true, and omits it
// otherwise -- mirroring DescribeClusters' ShowShardDetails convention. A
// prior pass never modeled the Clusters field at all.
func TestHandler_DescribeMultiRegionClusters_ShowClusterDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createMRC := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "membership",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, createMRC.Code)

	var mrcResp map[string]any
	require.NoError(t, json.Unmarshal(createMRC.Body.Bytes(), &mrcResp))
	mrc, _ := mrcResp["MultiRegionCluster"].(map[string]any)
	mrcName, _ := mrc["MultiRegionClusterName"].(string)
	require.NotEmpty(t, mrcName)

	createCluster := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":            "member-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"MultiRegionClusterName": mrcName,
	})
	require.Equal(t, http.StatusOK, createCluster.Code, "body: %s", createCluster.Body)

	t.Run("without ShowClusterDetails", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "DescribeMultiRegionClusters", map[string]any{
			"MultiRegionClusterName": mrcName,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		mrcs, _ := resp["MultiRegionClusters"].([]any)
		require.Len(t, mrcs, 1)
		got, _ := mrcs[0].(map[string]any)
		assert.Nil(t, got["Clusters"], "Clusters must be omitted when ShowClusterDetails is not set")
	})

	t.Run("with ShowClusterDetails", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "DescribeMultiRegionClusters", map[string]any{
			"MultiRegionClusterName": mrcName,
			"ShowClusterDetails":     true,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		mrcs, _ := resp["MultiRegionClusters"].([]any)
		require.Len(t, mrcs, 1)
		got, _ := mrcs[0].(map[string]any)

		clusters, _ := got["Clusters"].([]any)
		require.Len(t, clusters, 1)

		regional, _ := clusters[0].(map[string]any)
		assert.Equal(t, "member-cluster", regional["ClusterName"])
		assert.NotEmpty(t, regional["ARN"])
		assert.NotEmpty(t, regional["Region"])
		assert.NotEmpty(t, regional["Status"])
	})
}

// TestHandler_CreateCluster_MultiRegionClusterName_NotFound verifies
// CreateCluster rejects a MultiRegionClusterName that does not reference an
// existing multi-Region cluster, matching the FK-validation convention
// already applied to ACLName/SubnetGroupName/ParameterGroupName.
func TestHandler_CreateCluster_MultiRegionClusterName_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":            "orphan-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"MultiRegionClusterName": "virv-does-not-exist",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["__type"], "MultiRegionClusterNotFoundFault")
}

// TestHandler_CreateCluster_MultiRegionClusterName_Linked verifies a cluster
// created with a valid MultiRegionClusterName carries it through to the
// CreateCluster response (Cluster.MultiRegionClusterName was already a wire
// field; this locks the request-to-response link end to end).
func TestHandler_CreateCluster_MultiRegionClusterName_Linked(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createMRC := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "linked",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, createMRC.Code)

	var mrcResp map[string]any
	require.NoError(t, json.Unmarshal(createMRC.Body.Bytes(), &mrcResp))
	mrc, _ := mrcResp["MultiRegionCluster"].(map[string]any)
	mrcName, _ := mrc["MultiRegionClusterName"].(string)
	require.NotEmpty(t, mrcName)

	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":            "linked-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"MultiRegionClusterName": mrcName,
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cl, _ := resp["Cluster"].(map[string]any)
	assert.Equal(t, mrcName, cl["MultiRegionClusterName"])
}
