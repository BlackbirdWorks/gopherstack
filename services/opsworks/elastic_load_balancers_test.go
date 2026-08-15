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
			// DescribeElasticLoadBalancersInput.LayerIds is a real, plural
			// filter member (confirmed against
			// aws-sdk-go-v2/service/opsworks@v1.31.0's
			// api_op_DescribeElasticLoadBalancers.go) -- a previous version
			// of the backend discarded this filter entirely.
			name: "DescribeElasticLoadBalancers filters by LayerIds",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layer1 := createTestLayer(t, h, stackID)
				layer2 := createTestLayer(t, h, stackID)
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "elb-one",
					"LayerId":                 layer1,
				})
				doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
					"ElasticLoadBalancerName": "elb-two",
					"LayerId":                 layer2,
				})

				rec := doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{
					"LayerIds": []string{layer2},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				elbs := parseJSON(t, rec.Body.Bytes())["ElasticLoadBalancers"].([]any)
				require.Len(t, elbs, 1)
				assert.Equal(t, "elb-two", elbs[0].(map[string]any)["ElasticLoadBalancerName"])
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
