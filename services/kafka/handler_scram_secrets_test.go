package kafka_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestListScramSecrets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "scram-list-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/scram-secrets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestKafkaCoverage2_ListNodes covers the ListNodes handler.

func TestScramSecret_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "scram-lifecycle")
	encoded := url.PathEscape(clusterArn)
	secretArn := "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-kafka-secret"

	// BatchAssociateScramSecret.
	assocRec := doKafkaRequest(t, h, http.MethodPost,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": []string{secretArn}})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	unprocessed, _ := assocResp["unprocessedScramSecrets"].([]any)
	assert.Empty(t, unprocessed, "no secrets should be unprocessed")

	// ListScramSecrets — should include the associated secret.
	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	secretList, _ := listResp["secretArnList"].([]any)
	require.Len(t, secretList, 1)
	assert.Equal(t, secretArn, secretList[0])

	// BatchDisassociateScramSecret.
	disassocRec := doKafkaRequest(t, h, http.MethodPatch,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": []string{secretArn}})
	require.Equal(t, http.StatusOK, disassocRec.Code)

	var disassocResp map[string]any
	require.NoError(t, json.Unmarshal(disassocRec.Body.Bytes(), &disassocResp))
	unprocessed2, _ := disassocResp["unprocessedScramSecrets"].([]any)
	assert.Empty(t, unprocessed2, "no secrets should be unprocessed after disassociate")

	// ListScramSecrets — should be empty again.
	listRec2 := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	secretList2, _ := listResp2["secretArnList"].([]any)
	assert.Empty(t, secretList2, "list should be empty after disassociate")
}

func TestScramSecret_AssociateMultiple(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "scram-multi")
	encoded := url.PathEscape(clusterArn)

	secrets := []string{
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-1",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-2",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-3",
	}

	assocRec := doKafkaRequest(t, h, http.MethodPost,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": secrets})
	require.Equal(t, http.StatusOK, assocRec.Code)

	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	secretList, _ := listResp["secretArnList"].([]any)
	assert.Len(t, secretList, 3)
}

func TestScramSecret_NotFound(t *testing.T) {
	t.Parallel()

	missingARN := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1"
	h := newTestHandler(t)

	// Associate on missing cluster.
	rec := doKafkaRequest(
		t,
		h,
		http.MethodPost,
		"/v1/clusters/"+missingARN+"/scram-secrets",
		map[string]any{
			"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret:s1"},
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List on missing cluster.
	rec2 := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+missingARN+"/scram-secrets", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ----------------------------------------
// VPC connection lifecycle
// ----------------------------------------

func TestBatchAssociateThenDisassociate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName": "scram-cluster", "kafkaVersion": "2.8.0", "numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{"instanceType": "kafka.m5.large", "clientSubnets": []string{"subnet-1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	clusterArn := cr["clusterArn"].(string)

	encoded := url.PathEscape(clusterArn)

	assocRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters/"+encoded+"/scram-secrets", map[string]any{
		"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000:secret:s1"},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	disassocRec := doKafkaRequest(t, h, http.MethodPatch, "/v1/clusters/"+encoded+"/scram-secrets", map[string]any{
		"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000:secret:s1"},
	})
	assert.Equal(t, http.StatusOK, disassocRec.Code)
}
