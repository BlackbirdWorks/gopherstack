package kafka_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// decodeJSONResponse decodes a recorded HTTP response body as a generic JSON map.
func decodeJSONResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// doKafkaRequestJSON issues an HTTP request through the handler and decodes the
// response body as JSON, returning the decoded map (nil if the body is empty)
// alongside the status code.
func doKafkaRequestJSON(
	t *testing.T,
	h *kafka.Handler,
	method, path string,
	body any,
) (map[string]any, int) {
	t.Helper()

	rec := doKafkaRequest(t, h, method, path, body)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}

	return resp, rec.Code
}

// createTestCluster creates a 3-broker cluster with no explicit storage
// configuration via the HTTP API and returns its ARN.
func createTestCluster(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create cluster: %s", rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createTestClusterWithStorage creates a 3-broker cluster with EBS storage
// configuration via the HTTP API and returns its ARN.
func createTestClusterWithStorage(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
			"storageInfo": map[string]any{
				"ebsStorageInfo": map[string]any{"volumeSize": 100},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create cluster: %s", rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createTestClusterOneBroker creates a single-broker cluster with EBS storage
// configuration via the HTTP API and returns its ARN.
func createTestClusterOneBroker(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 1,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-abc"},
			"storageInfo": map[string]any{
				"ebsStorageInfo": map[string]any{"volumeSize": 100},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeJSONResponse(t, rec)
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createTestConfig creates a configuration via the HTTP API and returns its ARN.
func createTestConfig(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":             name,
		"kafkaVersions":    []string{"2.8.0"},
		"serverProperties": "auto.create.topics.enable=false",
	})
	require.Equal(t, http.StatusOK, rec.Code, "create config: %s", rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	arn, _ := resp["arn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createTestReplicator creates a replicator via the HTTP API and returns its ARN.
func createTestReplicator(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators", map[string]any{
		"replicatorName":          name,
		"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeJSONResponse(t, rec)
	arn, _ := resp["replicatorArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

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

func TestProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &kafka.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, kafka.ErrNilAppContext)
}

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

func TestGetSupportedOperationsAllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"BatchAssociateScramSecret",
		"BatchDisassociateScramSecret",
		"CreateCluster",
		"CreateClusterV2",
		"CreateConfiguration",
		"CreateReplicator",
		"CreateTopic",
		"CreateVpcConnection",
		"DeleteCluster",
		"DeleteClusterPolicy",
		"DeleteConfiguration",
		"DeleteReplicator",
		"DeleteTopic",
		"DeleteVpcConnection",
		"DescribeCluster",
		"DescribeClusterOperation",
		"DescribeClusterV2",
		"DescribeConfiguration",
		"GetBootstrapBrokers",
		"ListClusters",
		"ListClustersV2",
		"ListConfigurations",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	backend.AddClusterInternal("c1", "2.8.0")
	require.Equal(t, 1, kafka.ClusterCount(backend))

	h.Reset()

	assert.Equal(t, 0, kafka.ClusterCount(backend))
}

func TestKafka_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Kafka must be evaluated strictly before same-tier services (e.g. AppSync,
	// also at PriorityPathVersioned) whose broader "/v1/tags" matcher would
	// otherwise steal Kafka's "/v1/tags/{arn}" tag-operation requests.
	assert.Greater(t, h.MatchPriority(), service.PriorityPathVersioned)
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
