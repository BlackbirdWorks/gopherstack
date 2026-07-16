package kafka_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestKafka_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		wantFields map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"clusterName":         "test-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantStatus: http.StatusOK,
			wantFields: map[string]string{
				"clusterName": "test-cluster",
				"state":       kafka.ClusterStateCreating,
			},
		},
		{
			name:       "invalid_body",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/v1/clusters", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				for k, v := range tt.wantFields {
					assert.Equal(t, v, resp[k])
				}

				clusterArn, ok := resp["clusterArn"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, clusterArn)
			}
		})
	}
}

// ----------------------------------------
// ListClusters handler tests
// ----------------------------------------

func TestKafka_ListClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kafka.Handler)
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			setup:      func(_ *kafka.Handler) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "with_clusters",
			setup: func(h *kafka.Handler) {
				doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
					"clusterName":         "cluster-a",
					"kafkaVersion":        "2.8.0",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				})
				doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
					"clusterName":         "cluster-b",
					"kafkaVersion":        "2.8.0",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				})
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			list, ok := resp["clusterInfoList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeCluster / DeleteCluster tests
// ----------------------------------------

func TestKafka_DescribeAndDeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		useRealArn bool
	}{
		{
			name:       "describe_existing",
			wantStatus: http.StatusOK,
			useRealArn: true,
		},
		{
			name:       "describe_not_found",
			wantStatus: http.StatusNotFound,
			useRealArn: false,
		},
		{
			name:       "delete_existing",
			wantStatus: http.StatusOK,
			useRealArn: true,
		},
		{
			name:       "delete_not_found",
			wantStatus: http.StatusNotFound,
			useRealArn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a cluster to get a real ARN
			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "my-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			var clusterArn string
			if tt.useRealArn {
				clusterArn = createResp["clusterArn"].(string)
			} else {
				clusterArn = "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/bad-uuid"
			}

			encodedArn := url.PathEscape(clusterArn)

			var rec *httptest.ResponseRecorder
			if tt.name == "describe_existing" || tt.name == "describe_not_found" {
				rec = doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encodedArn, nil)
			} else {
				rec = doKafkaRequest(t, h, http.MethodDelete, "/v1/clusters/"+encodedArn, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// GetBootstrapBrokers tests
// ----------------------------------------

func TestKafka_GetBootstrapBrokers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useRealArn bool
		wantStatus int
	}{
		{
			name:       "success",
			useRealArn: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useRealArn: false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "bootstrap-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			var clusterArn string
			if tt.useRealArn {
				clusterArn = createResp["clusterArn"].(string)
			} else {
				clusterArn = "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/bad-uuid"
			}

			encodedArn := url.PathEscape(clusterArn)
			path := fmt.Sprintf("/v1/clusters/%s/bootstrap-brokers", encodedArn)
			rec := doKafkaRequest(t, h, http.MethodGet, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["bootstrapBrokerString"])
				assert.NotEmpty(t, resp["bootstrapBrokerStringTls"])
			}
		})
	}
}

// ----------------------------------------
// Configuration handler tests
// ----------------------------------------

func TestKafka_CreateClusterV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success_provisioned",
			body: map[string]any{
				"clusterName": "v2-cluster",
				"provisioned": map[string]any{
					"kafkaVersion":        "2.8.0",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "v2-cluster", resp["clusterName"])
			assert.Equal(t, "PROVISIONED", resp["clusterType"])
			assert.NotEmpty(t, resp["clusterArn"])
		})
	}
}

func TestKafka_DescribeClusterV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useRealArn bool
		wantStatus int
	}{
		{name: "success", useRealArn: true, wantStatus: http.StatusOK},
		{name: "not_found", useRealArn: false, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", map[string]any{
				"clusterName": "v2-cluster",
				"provisioned": map[string]any{
					"kafkaVersion":        "2.8.0",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			var clusterArn string
			if tt.useRealArn {
				clusterArn = createResp["clusterArn"].(string)
			} else {
				clusterArn = "arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/bad-uuid"
			}

			encodedArn := url.PathEscape(clusterArn)
			rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encodedArn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				clusterInfo, ok := resp["clusterInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "PROVISIONED", clusterInfo["clusterType"])
				assert.Equal(t, "ACTIVE", clusterInfo["state"])
			}
		})
	}
}

// ----------------------------------------
// SCRAM secret handler tests
// ----------------------------------------

func TestKafka_ListClustersV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kafka.Handler)
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			setup:      func(_ *kafka.Handler) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "with_clusters",
			setup: func(h *kafka.Handler) {
				doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", map[string]any{
					"clusterName": "v2-a",
					"provisioned": map[string]any{
						"kafkaVersion":        "2.8.0",
						"numberOfBrokerNodes": 3,
						"brokerNodeGroupInfo": map[string]any{
							"instanceType":  "kafka.m5.large",
							"clientSubnets": []string{"subnet-1"},
						},
					},
				})
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			clusters, ok := resp["clusterInfoList"].([]any)
			require.True(t, ok)
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

func TestCreateClusterNumberOfBrokerNodesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "zero_brokers_rejected",
			body: map[string]any{
				"clusterName":         "zero-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 0,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "negative_brokers_rejected",
			body: map[string]any{
				"clusterName":         "neg-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": -1,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "one_broker_accepted",
			body: map[string]any{
				"clusterName":         "one-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 1,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "three_brokers_accepted",
			body: map[string]any{
				"clusterName":         "three-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"numberOfBrokerNodes=%v", tt.body["numberOfBrokerNodes"])
		})
	}
}

func TestListClustersPagination(t *testing.T) {
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

func TestListClustersV2Pagination(t *testing.T) {
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

func TestClusterCreatingToActive(t *testing.T) {
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

func TestInvalidNextTokenFallsBackToStart(t *testing.T) {
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

func TestCreateClusterV2HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", map[string]any{
		"clusterName": "v2-cluster",
		"provisioned": map[string]any{
			"kafkaVersion":        "3.5.0",
			"numberOfBrokerNodes": 3,
			"brokerNodeGroupInfo": map[string]any{
				"instanceType":  "kafka.m5.large",
				"clientSubnets": []string{"subnet-1"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "v2-cluster", resp["clusterName"])
	assert.Equal(t, "PROVISIONED", resp["clusterType"])
}
