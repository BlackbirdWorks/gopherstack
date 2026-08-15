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
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterElasticIp", map[string]any{
					"ElasticIp": "1.2.3.4",
					"StackId":   stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, "1.2.3.4", resp["ElasticIp"])
			},
		},
		{
			// RegisterElasticIpInput.StackId is "This member is required"
			// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
			// api_op_RegisterElasticIp.go) -- there is no Region member on
			// the real input at all.
			name: "RegisterElasticIp without StackId returns 400",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "RegisterElasticIp", map[string]any{
					"ElasticIp": "1.2.3.9",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "AssociateElasticIp links IP to instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "2.3.4.5", "StackId": stackID})
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
			name: "DescribeElasticIps filters by StackId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackA := createTestStack(t, h)
				stackB := createTestStack(t, h)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "6.7.8.9", "StackId": stackA})
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "6.7.8.10", "StackId": stackB})

				rec := doTarget(t, h, "DescribeElasticIps", map[string]any{"StackId": stackA})
				require.Equal(t, http.StatusOK, rec.Code)
				eips := parseJSON(t, rec.Body.Bytes())["ElasticIps"].([]any)
				require.Len(t, eips, 1)
				assert.Equal(t, "6.7.8.9", eips[0].(map[string]any)["Ip"])
			},
		},
		{
			name: "DisassociateElasticIp clears instance link",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "3.4.5.6", "StackId": stackID})
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
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "4.5.6.7", "StackId": stackID})
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
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterElasticIp", map[string]any{"ElasticIp": "5.6.7.8", "StackId": stackID})
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
