package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

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

func TestVpcConnectionsLifecycleViaBackend(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createTestClusterOneBroker(t, h, "vpc-cluster")

	// CreateVpcConnection
	conn, err := be.CreateVpcConnection(context.Background(), clusterArn, "vpc-abc", "PLAINTEXT", nil)
	require.NoError(t, err)
	connArn := conn.VpcConnectionArn

	// DescribeVpcConnection
	c, err := be.DescribeVpcConnection(context.Background(), connArn)
	require.NoError(t, err)
	assert.Equal(t, connArn, c.VpcConnectionArn)

	// ListVpcConnections
	conns := be.ListVpcConnections(context.Background())
	assert.NotEmpty(t, conns)

	// ListClientVpcConnections
	clientConns, err := be.ListClientVpcConnections(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, clientConns)

	// RejectClientVpcConnection
	err = be.RejectClientVpcConnection(context.Background(), connArn)
	require.NoError(t, err)
}

func TestVpcConnection_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "vpc-lifecycle-cluster")

	// CreateVpcConnection.
	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-abc123",
		"authentication":   "SASL_IAM",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vpcConnArn, _ := createResp["vpcConnectionArn"].(string)
	require.NotEmpty(t, vpcConnArn)
	assert.Equal(t, clusterArn, createResp["targetClusterArn"])
	assert.Equal(t, "vpc-abc123", createResp["vpcId"])
	assert.NotEmpty(t, createResp["state"])

	encodedVpc := url.PathEscape(vpcConnArn)

	// DescribeVpcConnection.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection/"+encodedVpc, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, vpcConnArn, descResp["vpcConnectionArn"])
	assert.Equal(t, clusterArn, descResp["targetClusterArn"])

	// ListVpcConnections — our connection is present. Note the plural path:
	// AWS models Create/Describe/Delete under the singular /v1/vpc-connection
	// root but List under the distinct plural /v1/vpc-connections.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connections", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	conns, _ := listResp["vpcConnections"].([]any)
	require.Len(t, conns, 1)

	// DeleteVpcConnection.
	delRec := doKafkaRequest(t, h, http.MethodDelete, "/v1/vpc-connection/"+encodedVpc, nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// DescribeVpcConnection after delete → 404.
	descRec2 := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection/"+encodedVpc, nil)
	assert.Equal(t, http.StatusNotFound, descRec2.Code)
}

func TestVpcConnection_ListClientVpcConnections(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "client-vpc-cluster")
	encodedCluster := url.PathEscape(clusterArn)

	// Create two VPC connections targeting this cluster.
	for i, vpcID := range []string{"vpc-111", "vpc-222"} {
		rec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
			"targetClusterArn": clusterArn,
			"vpcId":            vpcID,
		})
		require.Equal(t, http.StatusOK, rec.Code, "create vpc conn %d", i)
	}

	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encodedCluster+"/client-vpc-connections", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	conns, _ := listResp["vpcConnections"].([]any)
	assert.Len(t, conns, 2, "should list both VPC connections for this cluster")
}

func TestVpcConnection_RejectClientVpcConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "reject-vpc-cluster")

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-to-reject",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vpcConnArn, _ := createResp["vpcConnectionArn"].(string)
	encodedCluster := url.PathEscape(clusterArn)

	// The real MSK API carries the VPC connection ARN in the JSON body
	// (vpcConnectionArn), not the path -- the path only identifies the cluster.
	rejectRec := doKafkaRequest(t, h, http.MethodPut,
		"/v1/clusters/"+encodedCluster+"/client-vpc-connection",
		map[string]any{"vpcConnectionArn": vpcConnArn})
	assert.Equal(t, http.StatusOK, rejectRec.Code)
}

func TestVpcConnection_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, code := doKafkaRequestJSON(t, h, http.MethodGet, "/v1/vpc-connection/arn%3Anone", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Configuration revision lifecycle
// ----------------------------------------
