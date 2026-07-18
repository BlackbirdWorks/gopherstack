package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestElasticLoadBalancers verifies ELB attach/detach/describe.
func TestElasticLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "AttachElasticLoadBalancer returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				rec := doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DescribeElasticLoadBalancers returns attached ELB",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})

				rec := doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				elbs := resp["ElasticLoadBalancers"].([]any)
				require.Len(t, elbs, 1)
				elb := elbs[0].(map[string]any)
				assert.Equal(t, "my-elb", elb["ElasticLoadBalancerName"])
				assert.NotEmpty(t, elb["DnsName"])
			},
		},
		{
			name: "DetachElasticLoadBalancer removes ELB",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				rec := doTarget(t, h, "DetachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "my-elb",
					"LayerId":                 layerID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{})
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Empty(t, resp["ElasticLoadBalancers"].([]any))
			},
		},
		{
			name: "AttachElasticLoadBalancer to nonexistent layer returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "elb",
					"LayerId":                 "nonexistent",
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
