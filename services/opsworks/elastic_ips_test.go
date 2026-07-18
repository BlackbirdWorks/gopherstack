package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestElasticIps verifies elastic IP lifecycle operations.
func TestElasticIps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterElasticIp returns the IP",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "RegisterElasticIp", map[string]any{
					"ElasticIp": "1.2.3.4",
					"Region":    "us-east-1",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, "1.2.3.4", resp["ElasticIp"])
			},
		},
		{
			name: "AssociateElasticIp links IP to instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "2.3.4.5"})
				rec := doTarget(t, h, "AssociateElasticIp", map[string]any{
					"ElasticIp":  "2.3.4.5",
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticIps", map[string]any{
					"InstanceId": instanceID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				eips := parseJSON(t, rec.Body.Bytes())["ElasticIps"].([]any)
				require.Len(t, eips, 1)
				assert.Equal(t, "2.3.4.5", eips[0].(map[string]any)["Ip"])
			},
		},
		{
			name: "DisassociateElasticIp clears instance link",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "3.4.5.6"})
				doTarget(t, h, "AssociateElasticIp", map[string]any{
					"ElasticIp":  "3.4.5.6",
					"InstanceId": instanceID,
				})
				rec := doTarget(t, h, "DisassociateElasticIp", map[string]any{"ElasticIp": "3.4.5.6"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UpdateElasticIp changes name",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "4.5.6.7"})
				rec := doTarget(t, h, "UpdateElasticIp", map[string]any{
					"ElasticIp": "4.5.6.7",
					"Name":      "my-eip",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeregisterElasticIp removes registration",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "5.6.7.8"})
				rec := doTarget(t, h, "DeregisterElasticIp", map[string]any{"ElasticIp": "5.6.7.8"})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticIps", map[string]any{
					"Ips": []string{"5.6.7.8"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
