package kafka_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// ----------------------------------------
// Pagination: ListClusters
// ----------------------------------------

func TestParityC_ListClusters_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		totalClusters int
		maxResults    int
		wantPage1     int
		wantPage2     int
		wantNextPage1 bool
	}{
		{
			name:          "single_page_when_within_limit",
			totalClusters: 3,
			maxResults:    10,
			wantPage1:     3,
			wantNextPage1: false,
		},
		{
			name:          "two_pages",
			totalClusters: 5,
			maxResults:    3,
			wantPage1:     3,
			wantPage2:     2,
			wantNextPage1: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.totalClusters {
				b.AddClusterInternal(fmt.Sprintf("cluster-%02d", i), "3.6.0")
			}

			path1 := fmt.Sprintf("/v1/clusters?maxResults=%d", tt.maxResults)
			rec1 := doKafkaRequest(t, h, http.MethodGet, path1, nil)
			assert.Equal(t, http.StatusOK, rec1.Code)

			var resp1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

			list1, ok := resp1["clusterInfoList"].([]any)
			require.True(t, ok, "clusterInfoList must be an array")
			assert.Len(t, list1, tt.wantPage1)

			nextToken, _ := resp1["nextToken"].(string)
			if tt.wantNextPage1 {
				require.NotEmpty(t, nextToken, "nextToken must be present when more results exist")

				path2 := fmt.Sprintf(
					"/v1/clusters?maxResults=%d&nextToken=%s",
					tt.maxResults,
					url.QueryEscape(nextToken),
				)
				rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

				list2, ok2 := resp2["clusterInfoList"].([]any)
				require.True(t, ok2)
				assert.Len(t, list2, tt.wantPage2)
			} else {
				assert.Empty(t, nextToken, "nextToken must be absent on a complete page")
			}
		})
	}
}

// ----------------------------------------
// Pagination: ListClustersV2
// ----------------------------------------

func TestParityC_ListClustersV2_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 4 {
		b.AddClusterInternal(fmt.Sprintf("v2-cluster-%02d", i), "3.6.0")
	}

	rec1 := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	list1 := resp1["clusterInfoList"].([]any)
	assert.Len(t, list1, 2)
	nextToken, _ := resp1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Decode token: must be base64url JSON {"o":N}
	decoded, err := base64.RawURLEncoding.DecodeString(nextToken)
	require.NoError(t, err)
	var tok struct {
		O int `json:"o"`
	}
	require.NoError(t, json.Unmarshal(decoded, &tok))
	assert.Equal(t, 2, tok.O)

	path2 := "/api/v2/clusters?maxResults=2&nextToken=" + url.QueryEscape(nextToken)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	list2 := resp2["clusterInfoList"].([]any)
	assert.Len(t, list2, 2)
	assert.Empty(t, resp2["nextToken"])
}

// ----------------------------------------
// Pagination: ListConfigurations
// ----------------------------------------

func TestParityC_ListConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		totalConfs  int
		maxResults  int
		wantPage1   int
		wantHasMore bool
	}{
		{
			name:        "all_fit",
			totalConfs:  2,
			maxResults:  5,
			wantPage1:   2,
			wantHasMore: false,
		},
		{
			name:        "two_pages",
			totalConfs:  4,
			maxResults:  2,
			wantPage1:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.totalConfs {
				b.AddConfigurationInternal(fmt.Sprintf("cfg-%02d", i))
			}

			path := fmt.Sprintf("/v1/configurations?maxResults=%d", tt.maxResults)
			rec := doKafkaRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs := resp["configurations"].([]any)
			assert.Len(t, configs, tt.wantPage1)

			nextToken, _ := resp["nextToken"].(string)
			if tt.wantHasMore {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

// ----------------------------------------
// Pagination: ListReplicators
// ----------------------------------------

func TestParityC_ListReplicators_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 3 {
		b.AddReplicatorInternal(fmt.Sprintf("rep-%02d", i))
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	reps := resp["replicators"].([]any)
	assert.Len(t, reps, 2)

	nextToken, _ := resp["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	path2 := "/replication/v1/replicators?maxResults=2&nextToken=" + url.QueryEscape(nextToken)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	reps2 := resp2["replicators"].([]any)
	assert.Len(t, reps2, 1)
	assert.Empty(t, resp2["nextToken"])
}

// ----------------------------------------
// Pagination: ListTopics
// ----------------------------------------

func TestParityC_ListTopics_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	cl := b.AddClusterInternal("tpc-cluster", "3.6.0")
	for i := range 5 {
		b.AddTopicInternal(cl.ClusterArn, fmt.Sprintf("topic-%02d", i))
	}

	encodedArn := url.PathEscape(cl.ClusterArn)
	path1 := fmt.Sprintf("/v1/clusters/%s/topics?maxResults=3", encodedArn)
	rec1 := doKafkaRequest(t, h, http.MethodGet, path1, nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	topics1 := resp1["topics"].([]any)
	assert.Len(t, topics1, 3)

	nextToken, _ := resp1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	path2 := fmt.Sprintf(
		"/v1/clusters/%s/topics?maxResults=3&nextToken=%s",
		encodedArn,
		url.QueryEscape(nextToken),
	)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	topics2 := resp2["topics"].([]any)
	assert.Len(t, topics2, 2)
	assert.Empty(t, resp2["nextToken"])
}

// ----------------------------------------
// CREATING→ACTIVE state transition
// ----------------------------------------

func TestParityC_ClusterCreatingToActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterV2 bool
	}{
		{name: "provisioned_v1", clusterV2: false},
		{name: "provisioned_v2", clusterV2: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var clusterArn string

			if tt.clusterV2 {
				rec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", map[string]any{
					"clusterName": "creating-cluster",
					"provisioned": map[string]any{
						"kafkaVersion":        "3.6.0",
						"numberOfBrokerNodes": 3,
						"brokerNodeGroupInfo": map[string]any{
							"instanceType":  "kafka.m5.large",
							"clientSubnets": []string{"subnet-1"},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				clusterArn = createResp["clusterArn"].(string)
			} else {
				rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
					"clusterName":         "creating-cluster",
					"kafkaVersion":        "3.6.0",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				clusterArn = createResp["clusterArn"].(string)
				assert.Equal(t, kafka.ClusterStateCreating, createResp["state"])
			}

			require.NotEmpty(t, clusterArn)

			// First DescribeCluster call transitions CREATING→ACTIVE.
			encoded := url.PathEscape(clusterArn)
			descRec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			clusterInfo := descResp["clusterInfo"].(map[string]any)
			assert.Equal(t, kafka.ClusterStateActive, clusterInfo["state"])
		})
	}
}

// ----------------------------------------
// O(1) name-uniqueness via clusterNames index
// ----------------------------------------

func TestParityC_ClusterNameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		useSeed bool // true = seed via AddClusterInternal, false = via CreateCluster
	}{
		{name: "duplicate_after_create", useSeed: false},
		{name: "duplicate_after_seed", useSeed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			ctx := context.Background()

			const clName = "unique-cluster"

			if tt.useSeed {
				b.AddClusterInternal(clName, "3.6.0")
			} else {
				_, err := b.CreateCluster(ctx, clName, "3.6.0", 3, kafka.BrokerNodeGroupInfo{
					InstanceType:  "kafka.m5.large",
					ClientSubnets: []string{"subnet-1"},
				}, nil, nil)
				require.NoError(t, err)
			}

			_, err := b.CreateCluster(ctx, clName, "3.6.0", 3, kafka.BrokerNodeGroupInfo{
				InstanceType:  "kafka.m5.large",
				ClientSubnets: []string{"subnet-1"},
			}, nil, nil)
			require.ErrorIs(t, err, kafka.ErrAlreadyExists)
		})
	}
}

// ----------------------------------------
// Growth cap: cluster map bounded at maxClustersPerRegion
// ----------------------------------------

func TestParityC_ClusterGrowthCap(t *testing.T) {
	t.Parallel()

	const maxPerRegion = 500

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	ctx := context.Background()

	// Seed up to the cap via AddClusterInternal (fast path).
	for i := range maxPerRegion {
		b.AddClusterInternal(fmt.Sprintf("seed-%04d", i), "3.6.0")
	}

	require.Equal(t, maxPerRegion, kafka.ClusterCount(b))

	// One more via CreateCluster: should evict one and stay at cap.
	_, err := b.CreateCluster(ctx, "new-cluster", "3.6.0", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, maxPerRegion, kafka.ClusterCount(b))
}

// ----------------------------------------
// Pagination token: invalid token falls back to offset 0
// ----------------------------------------

func TestParityC_InvalidNextTokenFallsBackToStart(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 3 {
		b.AddClusterInternal(fmt.Sprintf("fb-cluster-%02d", i), "3.6.0")
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters?nextToken=not-valid-base64!!!", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list := resp["clusterInfoList"].([]any)
	assert.Len(t, list, 3)
}
