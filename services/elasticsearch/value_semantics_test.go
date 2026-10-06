package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateDomainConfig_PartialMembersKeepStoredValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update func(in *elasticsearchsdk.UpdateElasticsearchDomainConfigInput)
		check  func(t *testing.T, out *elasticsearchsdk.DescribeElasticsearchDomainConfigOutput)
		name   string
	}{
		{
			name: "cluster instance count only",
			update: func(in *elasticsearchsdk.UpdateElasticsearchDomainConfigInput) {
				in.ElasticsearchClusterConfig = &types.ElasticsearchClusterConfig{InstanceCount: aws.Int32(3)}
			},
			check: func(t *testing.T, out *elasticsearchsdk.DescribeElasticsearchDomainConfigOutput) {
				t.Helper()

				c := out.DomainConfig.ElasticsearchClusterConfig.Options
				assert.Equal(t, int32(3), aws.ToInt32(c.InstanceCount))
				assert.Equal(t, types.ESPartitionInstanceTypeM5LargeElasticsearch, c.InstanceType)
			},
		},
		{
			name: "ebs volume size only",
			update: func(in *elasticsearchsdk.UpdateElasticsearchDomainConfigInput) {
				in.EBSOptions = &types.EBSOptions{VolumeSize: aws.Int32(50)}
			},
			check: func(t *testing.T, out *elasticsearchsdk.DescribeElasticsearchDomainConfigOutput) {
				t.Helper()

				o := out.DomainConfig.EBSOptions.Options
				assert.Equal(t, int32(50), aws.ToInt32(o.VolumeSize))
				assert.True(t, aws.ToBool(o.EBSEnabled))
				assert.Equal(t, types.VolumeTypeGp2, o.VolumeType)
			},
		},
		{
			name: "endpoint enforce https only",
			update: func(in *elasticsearchsdk.UpdateElasticsearchDomainConfigInput) {
				in.DomainEndpointOptions = &types.DomainEndpointOptions{EnforceHTTPS: aws.Bool(false)}
			},
			check: func(t *testing.T, out *elasticsearchsdk.DescribeElasticsearchDomainConfigOutput) {
				t.Helper()

				o := out.DomainConfig.DomainEndpointOptions.Options
				assert.False(t, aws.ToBool(o.EnforceHTTPS))
				assert.Equal(t, types.TLSSecurityPolicyPolicyMinTls12201907, o.TLSSecurityPolicy)
			},
		},
		{
			name: "endpoint tls policy only",
			update: func(in *elasticsearchsdk.UpdateElasticsearchDomainConfigInput) {
				in.DomainEndpointOptions = &types.DomainEndpointOptions{
					TLSSecurityPolicy: types.TLSSecurityPolicyPolicyMinTls10201907,
				}
			},
			check: func(t *testing.T, out *elasticsearchsdk.DescribeElasticsearchDomainConfigOutput) {
				t.Helper()

				o := out.DomainConfig.DomainEndpointOptions.Options
				assert.True(t, aws.ToBool(o.EnforceHTTPS))
				assert.Equal(t, types.TLSSecurityPolicyPolicyMinTls10201907, o.TLSSecurityPolicy)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestElasticsearchClient(t, newTestHandler())
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("vs-domain"),
				ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{
					InstanceType:  types.ESPartitionInstanceTypeM5LargeElasticsearch,
					InstanceCount: aws.Int32(1),
				},
				EBSOptions: &types.EBSOptions{
					EBSEnabled: aws.Bool(true), VolumeType: types.VolumeTypeGp2, VolumeSize: aws.Int32(10),
				},
				DomainEndpointOptions: &types.DomainEndpointOptions{
					EnforceHTTPS:      aws.Bool(true),
					TLSSecurityPolicy: types.TLSSecurityPolicyPolicyMinTls12201907,
				},
			})
			require.NoError(t, err)

			in := &elasticsearchsdk.UpdateElasticsearchDomainConfigInput{DomainName: aws.String("vs-domain")}
			tc.update(in)

			_, err = client.UpdateElasticsearchDomainConfig(ctx, in)
			require.NoError(t, err)

			got, err := client.DescribeElasticsearchDomainConfig(
				ctx,
				&elasticsearchsdk.DescribeElasticsearchDomainConfigInput{DomainName: aws.String("vs-domain")},
			)
			require.NoError(t, err)

			tc.check(t, got)
		})
	}
}
