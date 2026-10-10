package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeSSLPolicies_LoadBalancerType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		lbType string
		want   int
	}{
		{name: "all", lbType: "", want: 45},
		{name: "application", lbType: "application", want: 44},
		{name: "network", lbType: "network", want: 45},
		{name: "gateway", lbType: "gateway", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestELBv2Backend(t)
			in := &elbv2sdk.DescribeSSLPoliciesInput{PageSize: aws.Int32(100)}

			if tt.lbType != "" {
				in.LoadBalancerType = elbtypes.LoadBalancerTypeEnum(tt.lbType)
			}

			out, err := client.DescribeSSLPolicies(t.Context(), in)
			require.NoError(t, err)
			assert.Len(t, out.SslPolicies, tt.want)
		})
	}
}

func TestDescribeSSLPolicies_CatalogShape(t *testing.T) {
	t.Parallel()

	client := newTestELBv2Backend(t)

	out, err := client.DescribeSSLPolicies(t.Context(), &elbv2sdk.DescribeSSLPoliciesInput{
		Names: []string{
			"ELBSecurityPolicy-2016-08",
			"ELBSecurityPolicy-2015-05",
			"ELBSecurityPolicy-TLS13-1-3-2021-06",
		},
	})
	require.NoError(t, err)
	require.Len(t, out.SslPolicies, 3)

	byName := map[string]int{}
	for i, p := range out.SslPolicies {
		byName[aws.ToString(p.Name)] = i
	}

	p2016 := out.SslPolicies[byName["ELBSecurityPolicy-2016-08"]]
	assert.Len(t, p2016.Ciphers, 18)
	assert.Equal(t, []string{"application", "network"}, p2016.SupportedLoadBalancerTypes)

	p2015 := out.SslPolicies[byName["ELBSecurityPolicy-2015-05"]]
	assert.Equal(t, []string{"network"}, p2015.SupportedLoadBalancerTypes)

	p13 := out.SslPolicies[byName["ELBSecurityPolicy-TLS13-1-3-2021-06"]]
	assert.Equal(t, []string{"TLSv1.3"}, p13.SslProtocols)
}
