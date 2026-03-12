package kafka_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func newTestHandler(t *testing.T) *kafka.Handler {
	t.Helper()

	backend := kafka.NewInMemoryBackend(testAccountID, testRegion)

	return kafka.NewHandler(backend)
}

func doKafkaRequest(t *testing.T, h *kafka.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	var req *http.Request
	if bodyBytes != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----------------------------------------
// Provider tests
// ----------------------------------------

func TestKafka_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &kafka.Provider{}
	assert.Equal(t, "Kafka", p.Name())
}

func TestKafka_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &kafka.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Kafka", svc.Name())
}

// ----------------------------------------
// Handler metadata tests
// ----------------------------------------

func TestKafka_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Kafka", h.Name())
}

func TestKafka_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCluster")
	assert.Contains(t, ops, "DescribeCluster")
	assert.Contains(t, ops, "DeleteCluster")
	assert.Contains(t, ops, "GetBootstrapBrokers")
	assert.Contains(t, ops, "CreateConfiguration")
	assert.Contains(t, ops, "TagResource")
}

func TestKafka_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestKafka_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "v1_clusters", path: "/v1/clusters", want: true},
		{name: "v2_clusters", path: "/api/v2/clusters", want: true},
		{name: "v1_configurations", path: "/v1/configurations", want: true},
		{
			name: "v1_tags_kafka_arn",
			path: "/v1/tags/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Ftest%2Fabc",
			want: true,
		},
		{name: "v1_tags_non_kafka_arn", path: "/v1/tags/some-arn", want: false},
		{name: "other_path", path: "/v1/other", want: false},
		{name: "s3_path", path: "/my-bucket", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ----------------------------------------
// CreateCluster handler tests
// ----------------------------------------

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
				"state":       kafka.ClusterStateActive,
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

func TestKafka_CreateAndDescribeConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		confName   string
		wantStatus int
	}{
		{
			name:       "success",
			confName:   "my-config",
			wantStatus: http.StatusOK,
		},
		{
			name:       "duplicate",
			confName:   "my-config",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"name":             "my-config",
				"kafkaVersions":    []string{"2.8.0"},
				"serverProperties": "auto.create.topics.enable=false",
			}

			if tt.name == "duplicate" {
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", body)
			}

			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-config", resp["name"])
				assert.NotEmpty(t, resp["arn"])
			}
		})
	}
}

func TestKafka_ListConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*kafka.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *kafka.Handler) {},
			wantCount: 0,
		},
		{
			name: "with_configurations",
			setup: func(h *kafka.Handler) {
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
					"name": "config-a", "kafkaVersions": []string{"2.8.0"}, "serverProperties": "",
				})
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
					"name": "config-b", "kafkaVersions": []string{"2.8.0"}, "serverProperties": "",
				})
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/configurations", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs, ok := resp["configurations"].([]any)
			require.True(t, ok)
			assert.Len(t, configs, tt.wantCount)
		})
	}
}

// ----------------------------------------
// Tag handler tests
// ----------------------------------------

func TestKafka_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{name: "list_tags", op: "list", wantStatus: http.StatusOK},
		{name: "tag_resource", op: "tag", wantStatus: http.StatusOK},
		{name: "untag_resource", op: "untag", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "tagged-cluster",
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
			clusterArn := createResp["clusterArn"].(string)
			encodedArn := url.PathEscape(clusterArn)
			tagPath := "/v1/tags/" + encodedArn

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "list":
				rec = doKafkaRequest(t, h, http.MethodGet, tagPath, nil)
			case "tag":
				rec = doKafkaRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"tags": map[string]string{"env": "prod"},
				})
			case "untag":
				// First add a tag, then remove it
				doKafkaRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"tags": map[string]string{"env": "prod"},
				})

				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, tagPath+"?tagKeys=env", http.NoBody)
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// V2 API tests
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
			}
		})
	}
}
