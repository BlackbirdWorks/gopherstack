package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticLoadBalancerDerivedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instances  []map[string]any
		wantAZs    []any
		wantSubnet []any
	}{
		{name: "no instances"},
		{
			name: "instance placement",
			instances: []map[string]any{
				{"InstanceType": "t2.micro", "SubnetId": "subnet-b", "AvailabilityZone": "us-east-1b"},
				{"InstanceType": "t2.micro", "SubnetId": "subnet-a", "AvailabilityZone": "us-east-1a"},
				{"InstanceType": "t2.micro", "SubnetId": "subnet-a"},
			},
			wantAZs:    []any{"us-east-1a", "us-east-1b"},
			wantSubnet: []any{"subnet-a", "subnet-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, "CreateStack", map[string]any{
				"Name":                      "s",
				"Region":                    "us-east-1",
				"VpcId":                     "vpc-1",
				"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
				"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			stackID := parseJSON(t, rec.Body.Bytes())["StackId"].(string)
			layerID := createTestLayer(t, h, stackID)
			otherLayer := createTestLayer(t, h, stackID)

			for _, in := range tt.instances {
				in["StackId"] = stackID
				in["LayerIds"] = []string{layerID}
				require.Equal(t, http.StatusOK, doTarget(t, h, "CreateInstance", in).Code)
			}
			require.Equal(t, http.StatusOK, doTarget(t, h, "CreateInstance", map[string]any{
				"StackId": stackID, "LayerIds": []string{otherLayer},
				"InstanceType": "t2.micro", "SubnetId": "subnet-other",
			}).Code)

			require.Equal(t, http.StatusOK, doTarget(t, h, "AttachElasticLoadBalancer", map[string]any{
				"ElasticLoadBalancerName": "elb", "LayerId": layerID,
			}).Code)

			rec = doTarget(t, h, "DescribeElasticLoadBalancers", map[string]any{"StackId": stackID})
			elbs := parseJSON(t, rec.Body.Bytes())["ElasticLoadBalancers"].([]any)
			require.Len(t, elbs, 1)
			elb := elbs[0].(map[string]any)
			assert.Equal(t, "vpc-1", elb["VpcId"])
			if tt.wantAZs == nil {
				assert.NotContains(t, elb, "AvailabilityZones")
				assert.NotContains(t, elb, "SubnetIds")

				return
			}
			assert.Equal(t, tt.wantAZs, elb["AvailabilityZones"])
			assert.Equal(t, tt.wantSubnet, elb["SubnetIds"])
		})
	}
}

func TestErrorContentType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "DescribeStacks", map[string]any{"StackIds": []string{"missing"}})
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/x-amz-json-1.1", rec.Header().Get("Content-Type"))
}
