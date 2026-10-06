package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateInferenceExperiment_DescriptionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc *string
		want string
	}{
		{name: "description_set", desc: aws.String("compare v1 and v2"), want: "compare v1 and v2"},
		{name: "description_omitted", desc: nil, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
				Name:         aws.String("exp"),
				Type:         smtypes.InferenceExperimentTypeShadowMode,
				EndpointName: aws.String("exp-endpoint"),
				RoleArn:      aws.String("arn:aws:iam::000000000000:role/ExpRole"),
				Description:  tt.desc,
				ModelVariants: []smtypes.ModelVariantConfig{{
					ModelName:   aws.String("m1"),
					VariantName: aws.String("v1"),
					InfrastructureConfig: &smtypes.ModelInfrastructureConfig{
						InfrastructureType: smtypes.ModelInfrastructureTypeRealTimeInference,
						RealTimeInferenceConfig: &smtypes.RealTimeInferenceConfig{
							InstanceType:  smtypes.ProductionVariantInstanceTypeMlM5Large,
							InstanceCount: aws.Int32(1),
						},
					},
				}},
				ShadowModeConfig: &smtypes.ShadowModeConfig{
					SourceModelVariantName: aws.String("v1"),
					ShadowModelVariants:    []smtypes.ShadowModelVariantConfig{},
				},
			})
			require.NoError(t, err)

			out, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
				Name: aws.String("exp"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.Description))
		})
	}
}

func TestUpdateCluster_ClusterRole(t *testing.T) {
	t.Parallel()

	const (
		oldRole = "arn:aws:iam::000000000000:role/Old"
		newRole = "arn:aws:iam::000000000000:role/New"
	)

	tests := []struct {
		name   string
		update *string
		want   string
	}{
		{name: "role_replaced", update: aws.String(newRole), want: newRole},
		{name: "role_unchanged_when_omitted", update: nil, want: oldRole},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
				ClusterName: aws.String("c"),
				ClusterRole: aws.String(oldRole),
			})
			require.NoError(t, err)

			_, err = client.UpdateCluster(t.Context(), &sagemakersdk.UpdateClusterInput{
				ClusterName: aws.String("c"),
				ClusterRole: tt.update,
			})
			require.NoError(t, err)

			out, err := client.DescribeCluster(
				t.Context(),
				&sagemakersdk.DescribeClusterInput{ClusterName: aws.String("c")},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.ClusterRole))
		})
	}
}
