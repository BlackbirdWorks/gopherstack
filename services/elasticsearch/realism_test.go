package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealism_CreateDomainValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *elasticsearchsdk.CreateElasticsearchDomainInput
		name string
	}{
		{name: "instance type", in: &elasticsearchsdk.CreateElasticsearchDomainInput{
			ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{InstanceType: "foo"},
		}},
		{name: "instance count", in: &elasticsearchsdk.CreateElasticsearchDomainInput{
			ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{
				InstanceType: "m5.large.elasticsearch", InstanceCount: aws.Int32(81),
			},
		}},
		{name: "dedicated master count", in: &elasticsearchsdk.CreateElasticsearchDomainInput{
			ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{
				DedicatedMasterEnabled: aws.Bool(true), DedicatedMasterCount: aws.Int32(2),
				DedicatedMasterType: "m5.large.elasticsearch",
			},
		}},
		{name: "ebs volume type", in: &elasticsearchsdk.CreateElasticsearchDomainInput{
			EBSOptions: &types.EBSOptions{EBSEnabled: aws.Bool(true), VolumeType: "gp9", VolumeSize: aws.Int32(20)},
		}},
		{name: "ebs volume size", in: &elasticsearchsdk.CreateElasticsearchDomainInput{
			EBSOptions: &types.EBSOptions{EBSEnabled: aws.Bool(true), VolumeType: "gp2", VolumeSize: aws.Int32(1)},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestElasticsearchClient(t, newTestHandler())
			tc.in.DomainName = aws.String("realism-dom")

			_, err := client.CreateElasticsearchDomain(t.Context(), tc.in)

			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "ValidationException", ae.ErrorCode())
		})
	}
}

func TestRealism_ErrorMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	client := newTestElasticsearchClient(t, newTestHandler())

	_, err := client.DescribeElasticsearchDomain(t.Context(), &elasticsearchsdk.DescribeElasticsearchDomainInput{
		DomainName: aws.String("missing-dom"),
	})

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "Domain not found: missing-dom", aws.ToString(nf.Message))
}

func TestRealism_UpgradeTargetMustBeNewer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{name: "same version", target: "7.10", wantErr: true},
		{name: "older version", target: "6.8", wantErr: true},
		{name: "unknown version", target: "7.11", wantErr: true},
		{name: "newer version", target: "7.10", wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestElasticsearchClient(t, newTestHandler())
			current := "7.10"
			if !tc.wantErr {
				current = "7.4"
			}

			_, err := client.CreateElasticsearchDomain(t.Context(), &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("up-dom"), ElasticsearchVersion: aws.String(current),
			})
			require.NoError(t, err)

			_, err = client.UpgradeElasticsearchDomain(t.Context(), &elasticsearchsdk.UpgradeElasticsearchDomainInput{
				DomainName: aws.String("up-dom"), TargetVersion: aws.String(tc.target),
			})
			if tc.wantErr {
				var ve *types.ValidationException
				require.ErrorAs(t, err, &ve)

				return
			}

			require.NoError(t, err)
		})
	}
}
