package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type userContextPair struct{ created, modified *smtypes.UserContext }

func TestCallerIdentity_LineageAndPipelineResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *sagemakersdk.Client) userContextPair
		name string
	}{
		{
			name: "experiment",
			run: func(t *testing.T, c *sagemakersdk.Client) userContextPair {
				t.Helper()

				_, err := c.CreateExperiment(
					t.Context(),
					&sagemakersdk.CreateExperimentInput{ExperimentName: aws.String("e")},
				)
				require.NoError(t, err)

				out, err := c.DescribeExperiment(
					t.Context(),
					&sagemakersdk.DescribeExperimentInput{ExperimentName: aws.String("e")},
				)
				require.NoError(t, err)

				return userContextPair{out.CreatedBy, out.LastModifiedBy}
			},
		},
		{
			name: "trial",
			run: func(t *testing.T, c *sagemakersdk.Client) userContextPair {
				t.Helper()

				_, err := c.CreateExperiment(
					t.Context(),
					&sagemakersdk.CreateExperimentInput{ExperimentName: aws.String("e")},
				)
				require.NoError(t, err)

				_, err = c.CreateTrial(t.Context(), &sagemakersdk.CreateTrialInput{
					TrialName: aws.String("t"), ExperimentName: aws.String("e"),
				})
				require.NoError(t, err)

				out, err := c.DescribeTrial(t.Context(), &sagemakersdk.DescribeTrialInput{TrialName: aws.String("t")})
				require.NoError(t, err)

				return userContextPair{out.CreatedBy, out.LastModifiedBy}
			},
		},
		{
			name: "trial_component",
			run: func(t *testing.T, c *sagemakersdk.Client) userContextPair {
				t.Helper()

				_, err := c.CreateTrialComponent(t.Context(), &sagemakersdk.CreateTrialComponentInput{
					TrialComponentName: aws.String("tc"),
				})
				require.NoError(t, err)

				out, err := c.DescribeTrialComponent(t.Context(), &sagemakersdk.DescribeTrialComponentInput{
					TrialComponentName: aws.String("tc"),
				})
				require.NoError(t, err)

				return userContextPair{out.CreatedBy, out.LastModifiedBy}
			},
		},
		{
			name: "pipeline",
			run: func(t *testing.T, c *sagemakersdk.Client) userContextPair {
				t.Helper()

				_, err := c.CreatePipeline(t.Context(), &sagemakersdk.CreatePipelineInput{
					PipelineName:       aws.String("p"),
					RoleArn:            aws.String("arn:aws:iam::123456789012:role/r"),
					PipelineDefinition: aws.String(`{"Version":"2020-12-01","Steps":[]}`),
					ClientRequestToken: aws.String("tok-1"),
				})
				require.NoError(t, err)

				out, err := c.DescribePipeline(
					t.Context(),
					&sagemakersdk.DescribePipelineInput{PipelineName: aws.String("p")},
				)
				require.NoError(t, err)

				return userContextPair{out.CreatedBy, out.LastModifiedBy}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_caller_resolved", func(t *testing.T) {
			t.Parallel()

			got := tt.run(t, newCallerIdentityClient(t, callerIdentityARN))
			require.NotNil(t, got.created)
			require.NotNil(t, got.created.IamIdentity)
			assert.Equal(t, callerIdentityARN, aws.ToString(got.created.IamIdentity.Arn))
			require.NotNil(t, got.modified)
			assert.Equal(t, callerIdentityARN, aws.ToString(got.modified.IamIdentity.Arn))
		})
	}
}

func TestCallerIdentity_AssociationCreatedBy(t *testing.T) {
	t.Parallel()

	c := newCallerIdentityClient(t, callerIdentityARN)

	arns := make([]string, 0, 2)

	for _, name := range []string{"a1", "a2"} {
		out, err := c.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
			ArtifactName: aws.String(name),
			ArtifactType: aws.String("Dataset"),
			Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://b/" + name)},
		})
		require.NoError(t, err)

		arns = append(arns, aws.ToString(out.ArtifactArn))
	}

	_, err := c.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn: aws.String(arns[0]), DestinationArn: aws.String(arns[1]),
	})
	require.NoError(t, err)

	list, err := c.ListAssociations(t.Context(), &sagemakersdk.ListAssociationsInput{})
	require.NoError(t, err)
	require.Len(t, list.AssociationSummaries, 1)
	require.NotNil(t, list.AssociationSummaries[0].CreatedBy)
	assert.Equal(t, callerIdentityARN, aws.ToString(list.AssociationSummaries[0].CreatedBy.IamIdentity.Arn))
}

func TestCallerIdentity_UnresolvedCallerOmitsUserContext(t *testing.T) {
	t.Parallel()

	c := newCallerIdentityClient(t, "")

	_, err := c.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{ExperimentName: aws.String("e")})
	require.NoError(t, err)

	out, err := c.DescribeExperiment(
		t.Context(),
		&sagemakersdk.DescribeExperimentInput{ExperimentName: aws.String("e")},
	)
	require.NoError(t, err)
	assert.Nil(t, out.CreatedBy)
	assert.Nil(t, out.LastModifiedBy)
}
