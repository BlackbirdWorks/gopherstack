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

func newTestHandlerWithBackend(t *testing.T) (*kafka.Handler, *kafka.InMemoryBackend) {
	t.Helper()

	backend := kafka.NewInMemoryBackend(testAccountID, testRegion)

	return kafka.NewHandler(backend), backend
}

func newTestHandler(t *testing.T) *kafka.Handler {
	t.Helper()

	h, _ := newTestHandlerWithBackend(t)

	return h
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
				assert.Equal(t, "ACTIVE", clusterInfo["state"])
			}
		})
	}
}

// ----------------------------------------
// SCRAM secret handler tests
// ----------------------------------------

func TestKafka_BatchAssociateScramSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"},
			},
			wantStatus: http.StatusOK,
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

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "scram-cluster",
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

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/scram-secrets",
				bytes.NewReader(bodyBytes),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "unprocessedScramSecrets")
			}
		})
	}
}

func TestKafka_BatchDisassociateScramSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "success", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "scram-cluster",
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

			secretArn := "arn:aws:secretsmanager:us-east-1:000000000000:secret/my-secret"

			// Associate first
			bodyBytes, _ := json.Marshal(map[string]any{"secretArnList": []string{secretArn}})
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/scram-secrets",
				bytes.NewReader(bodyBytes),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)

			// Disassociate
			bodyBytes2, _ := json.Marshal(map[string]any{"secretArnList": []string{secretArn}})
			req2 := httptest.NewRequest(
				http.MethodPatch,
				"/v1/clusters/"+encodedArn+"/scram-secrets",
				bytes.NewReader(bodyBytes2),
			)
			req2.Header.Set("Content-Type", "application/json")
			rec2 := httptest.NewRecorder()
			c2 := e.NewContext(req2, rec2)
			err2 := h.Handler()(c2)
			require.NoError(t, err2)

			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// ----------------------------------------
// Replicator handler tests
// ----------------------------------------

func TestKafka_CreateReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"replicatorName":          "my-replicator",
				"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
			},
			wantStatus: http.StatusOK,
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
			req := httptest.NewRequest(http.MethodPost, "/replication/v1/replicators", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-replicator", resp["replicatorName"])
				assert.NotEmpty(t, resp["replicatorArn"])
				assert.Equal(t, kafka.ReplicatorStateRunning, resp["replicatorState"])
			}
		})
	}
}

func TestKafka_DeleteReplicator(t *testing.T) {
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

			e := echo.New()
			bodyBytes, _ := json.Marshal(map[string]any{
				"replicatorName":          "my-replicator",
				"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
			})
			req := httptest.NewRequest(http.MethodPost, "/replication/v1/replicators", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			var replicatorArn string
			if tt.useRealArn {
				replicatorArn = createResp["replicatorArn"].(string)
			} else {
				replicatorArn = "arn:aws:kafka:us-east-1:000000000000:replicator/nonexistent/uuid"
			}

			encodedArn := url.PathEscape(replicatorArn)
			req2 := httptest.NewRequest(http.MethodDelete, "/replication/v1/replicators/"+encodedArn, http.NoBody)
			rec2 := httptest.NewRecorder()
			c2 := e.NewContext(req2, rec2)
			err2 := h.Handler()(c2)
			require.NoError(t, err2)

			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// ----------------------------------------
// Topic handler tests
// ----------------------------------------

func TestKafka_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"topicName":         "my-topic",
				"replicationFactor": 1,
				"numPartitions":     3,
			},
			wantStatus: http.StatusOK,
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

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "topic-cluster",
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

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/topics",
				bytes.NewReader(bodyBytes),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-topic", resp["topicName"])
			}
		})
	}
}

func TestKafka_DeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topicName  string
		wantStatus int
	}{
		{name: "success", topicName: "my-topic", wantStatus: http.StatusOK},
		{name: "not_found", topicName: "nonexistent-topic", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "topic-cluster",
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

			// Create topic
			e := echo.New()
			topicBody, _ := json.Marshal(
				map[string]any{"topicName": "my-topic", "replicationFactor": 1, "numPartitions": 3},
			)
			reqCreate := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/topics",
				bytes.NewReader(topicBody),
			)
			reqCreate.Header.Set("Content-Type", "application/json")
			recCreate := httptest.NewRecorder()
			cCreate := e.NewContext(reqCreate, recCreate)
			require.NoError(t, h.Handler()(cCreate))
			require.Equal(t, http.StatusOK, recCreate.Code)

			// Delete topic
			reqDel := httptest.NewRequest(
				http.MethodDelete,
				"/v1/clusters/"+encodedArn+"/topics/"+tt.topicName,
				http.NoBody,
			)
			recDel := httptest.NewRecorder()
			cDel := e.NewContext(reqDel, recDel)
			require.NoError(t, h.Handler()(cDel))

			assert.Equal(t, tt.wantStatus, recDel.Code)
		})
	}
}

// ----------------------------------------
// VPC connection handler tests
// ----------------------------------------

func TestKafka_CreateVpcConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"targetClusterArn": "",
				"vpcId":            "vpc-12345",
				"authentication":   "SASL_IAM",
			},
			wantStatus: http.StatusOK,
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

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "vpc-cluster",
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

			body := tt.body
			if body != nil && body["targetClusterArn"] == "" {
				body["targetClusterArn"] = clusterArn
			}

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/v1/vpc-connection", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["vpcConnectionArn"])
				assert.Equal(t, kafka.VpcConnectionStateAvailable, resp["state"])
			}
		})
	}
}

func TestKafka_DeleteVpcConnection(t *testing.T) {
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

			// Create cluster
			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "vpc-cluster",
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

			// Create VPC connection
			e := echo.New()
			vpcBody, _ := json.Marshal(map[string]any{
				"targetClusterArn": clusterArn,
				"vpcId":            "vpc-12345",
				"authentication":   "SASL_IAM",
			})
			reqVpc := httptest.NewRequest(http.MethodPost, "/v1/vpc-connection", bytes.NewReader(vpcBody))
			reqVpc.Header.Set("Content-Type", "application/json")
			recVpc := httptest.NewRecorder()
			cVpc := e.NewContext(reqVpc, recVpc)
			require.NoError(t, h.Handler()(cVpc))
			require.Equal(t, http.StatusOK, recVpc.Code)

			var vpcResp map[string]any
			require.NoError(t, json.Unmarshal(recVpc.Body.Bytes(), &vpcResp))

			var vpcConnectionArn string
			if tt.useRealArn {
				vpcConnectionArn = vpcResp["vpcConnectionArn"].(string)
			} else {
				vpcConnectionArn = "arn:aws:kafka:us-east-1:000000000000:vpc-connection/nonexistent"
			}

			encodedArn := url.PathEscape(vpcConnectionArn)
			reqDel := httptest.NewRequest(http.MethodDelete, "/v1/vpc-connection/"+encodedArn, http.NoBody)
			recDel := httptest.NewRecorder()
			cDel := e.NewContext(reqDel, recDel)
			require.NoError(t, h.Handler()(cDel))

			assert.Equal(t, tt.wantStatus, recDel.Code)
		})
	}
}

// ----------------------------------------
// Cluster policy handler tests
// ----------------------------------------

func TestKafka_DeleteClusterPolicy(t *testing.T) {
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

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "policy-cluster",
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
			e := echo.New()
			req := httptest.NewRequest(http.MethodDelete, "/v1/clusters/"+encodedArn+"/policy", http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// Cluster operation handler tests
// ----------------------------------------

func TestKafka_DescribeClusterOperation(t *testing.T) {
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

			h, backend := newTestHandlerWithBackend(t)

			// Create a cluster and an operation via internal helper
			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "op-cluster",
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

			op := backend.AddClusterOperationInternal(clusterArn, "UPDATE_BROKER_COUNT")

			var operationArn string
			if tt.useRealArn {
				operationArn = op.ClusterOperationArn
			} else {
				operationArn = "arn:aws:kafka:us-east-1:000000000000:cluster-operation/nonexistent/uuid"
			}

			encodedArn := url.PathEscape(operationArn)
			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/operations/"+encodedArn, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				opInfo, ok := resp["clusterOperationInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, kafka.ClusterOperationStateUpdateComplete, opInfo["operationState"])
			}
		})
	}
}

// ----------------------------------------
// Path parsing tests for new operations
// ----------------------------------------

func TestParseKafkaPath_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:         "batch_associate_scram",
			method:       http.MethodPost,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/scram-secrets",
			wantOp:       "BatchAssociateScramSecret",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "batch_disassociate_scram",
			method:       http.MethodPatch,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/scram-secrets",
			wantOp:       "BatchDisassociateScramSecret",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "create_topic",
			method:       http.MethodPost,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/topics",
			wantOp:       "CreateTopic",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "delete_topic",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/topics/my-topic",
			wantOp:       "DeleteTopic",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1|my-topic",
		},
		{
			name:         "delete_cluster_policy",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/policy",
			wantOp:       "DeleteClusterPolicy",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "describe_cluster_operation",
			method:       http.MethodGet,
			path:         "/v1/operations/arn:aws:kafka:us-east-1:000000000000:cluster-operation/uuid-1",
			wantOp:       "DescribeClusterOperation",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster-operation/uuid-1",
		},
		{
			name:   "create_replicator",
			method: http.MethodPost,
			path:   "/replication/v1/replicators",
			wantOp: "CreateReplicator",
		},
		{
			name:         "delete_replicator",
			method:       http.MethodDelete,
			path:         "/replication/v1/replicators/arn:aws:kafka:us-east-1:000000000000:replicator/test/uuid-1",
			wantOp:       "DeleteReplicator",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:replicator/test/uuid-1",
		},
		{
			name:   "create_vpc_connection",
			method: http.MethodPost,
			path:   "/v1/vpc-connection",
			wantOp: "CreateVpcConnection",
		},
		{
			name:         "delete_vpc_connection",
			method:       http.MethodDelete,
			path:         "/v1/vpc-connection/arn:aws:kafka:us-east-1:000000000000:vpc-connection/uuid-1",
			wantOp:       "DeleteVpcConnection",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:vpc-connection/uuid-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, resource := kafka.ParseKafkaPathForTest(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// ----------------------------------------
// Additional tests to improve coverage
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

func TestKafka_DescribeAndDeleteConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		useRealArn bool
		wantStatus int
	}{
		{name: "describe_success", op: "describe", useRealArn: true, wantStatus: http.StatusOK},
		{name: "describe_not_found", op: "describe", useRealArn: false, wantStatus: http.StatusNotFound},
		{name: "delete_success", op: "delete", useRealArn: true, wantStatus: http.StatusOK},
		{name: "delete_not_found", op: "delete", useRealArn: false, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
				"name":             "cfg-test",
				"kafkaVersions":    []string{"2.8.0"},
				"serverProperties": "auto.create.topics.enable=false",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			var cfgArn string
			if tt.useRealArn {
				cfgArn = createResp["arn"].(string)
			} else {
				cfgArn = "arn:aws:kafka:us-east-1:000000000000:configuration/nonexistent/bad-uuid"
			}

			encodedArn := url.PathEscape(cfgArn)

			var rec *httptest.ResponseRecorder
			switch tt.op {
			case "describe":
				rec = doKafkaRequest(t, h, http.MethodGet, "/v1/configurations/"+encodedArn, nil)
			default:
				rec = doKafkaRequest(t, h, http.MethodDelete, "/v1/configurations/"+encodedArn, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKafka_ExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:   "list_clusters",
			method: http.MethodGet,
			path:   "/v1/clusters",
			wantOp: "ListClusters",
		},
		{
			name:         "describe_cluster",
			method:       http.MethodGet,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "DescribeCluster",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			resource := h.ExtractResource(c)

			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestKafka_ChaosOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "kafka", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{testRegion}, h.ChaosRegions())
}

func TestKafka_RouteMatcher_NewPaths(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "operations", path: "/v1/operations/some-arn", want: true},
		{name: "replicators_root", path: "/replication/v1/replicators", want: true},
		{name: "replicators_resource", path: "/replication/v1/replicators/some-arn", want: true},
		{name: "vpc_connection_root", path: "/v1/vpc-connection", want: true},
		{name: "vpc_connection_resource", path: "/v1/vpc-connection/some-arn", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
