package kafka_test

import (
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

func TestClusterPolicyViaBackend(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createTestClusterOneBroker(t, h, "policy-cluster")

	policy := `{"Version":"2012-10-17","Statement":[]}`

	err := be.PutClusterPolicy(context.Background(), clusterArn, policy)
	require.NoError(t, err)

	p, err := be.GetClusterPolicy(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.Equal(t, policy, p)
}

func TestGetClusterPolicy_NotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *kafka.InMemoryBackend, clusterArn string)
		name      string
		wantErr   bool
		wantFound bool
	}{
		{
			name:      "no_policy_set",
			setup:     func(_ *kafka.InMemoryBackend, _ string) {},
			wantErr:   true,
			wantFound: false,
		},
		{
			name: "policy_set",
			setup: func(b *kafka.InMemoryBackend, clusterArn string) {
				err := b.PutClusterPolicy(context.Background(), clusterArn, `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
			wantErr:   false,
			wantFound: true,
		},
		{
			name: "policy_put_then_deleted",
			setup: func(b *kafka.InMemoryBackend, clusterArn string) {
				_ = b.PutClusterPolicy(context.Background(), clusterArn, `{"Version":"2012-10-17"}`)
				_ = b.DeleteClusterPolicy(context.Background(), clusterArn)
			},
			wantErr:   true,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("policy-cl", "3.5.1")

			tt.setup(b, cl.ClusterArn)

			_, err := b.GetClusterPolicy(context.Background(), cl.ClusterArn)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, kafka.ErrNotFound)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantFound, kafka.HasClusterPolicy(b, cl.ClusterArn))
		})
	}
}

// TestRefinement2_GetClusterPolicy_HTTP verifies HTTP NotFoundException when no policy.

func TestGetClusterPolicy_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kafka.Handler, encoded string)
		name       string
		wantStatus int
	}{
		{
			name:       "no_policy_returns_404",
			setup:      func(_ *kafka.Handler, _ string) {},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "policy_set_returns_200",
			setup: func(h *kafka.Handler, encoded string) {
				rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encoded+"/policy",
					map[string]any{"policy": `{"Version":"2012-10-17"}`})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandlerWithBackend(t)
			cl := backend.AddClusterInternal("pol-http", "3.5.1")
			encoded := url.PathEscape(cl.ClusterArn)

			tt.setup(h, encoded)

			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/policy", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_UpdateClusterConfiguration_PersistsConfig verifies config is stored on cluster.
