package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelPackage_VersionedByGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		versions int
	}{
		{name: "single_version", versions: 1},
		{name: "three_versions", versions: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newRealClient(t)

			_, err := client.CreateModelPackageGroup(ctx, &sagemakersdk.CreateModelPackageGroupInput{
				ModelPackageGroupName: aws.String("grp"),
			})
			require.NoError(t, err)

			var lastArn string

			for i := 1; i <= tt.versions; i++ {
				out, cerr := client.CreateModelPackage(ctx, &sagemakersdk.CreateModelPackageInput{
					ModelPackageGroupName: aws.String("grp"),
				})
				require.NoError(t, cerr)
				assert.Contains(t, aws.ToString(out.ModelPackageArn), "model-package/grp/")

				lastArn = aws.ToString(out.ModelPackageArn)
			}

			got, err := client.DescribeModelPackage(ctx, &sagemakersdk.DescribeModelPackageInput{
				ModelPackageName: aws.String(lastArn),
			})
			require.NoError(t, err)
			assert.EqualValues(t, tt.versions, aws.ToInt32(got.ModelPackageVersion))
			assert.Equal(t, "grp", aws.ToString(got.ModelPackageGroupName))

			list, err := client.ListModelPackages(ctx, &sagemakersdk.ListModelPackagesInput{
				ModelPackageGroupName: aws.String("grp"),
			})
			require.NoError(t, err)
			require.Len(t, list.ModelPackageSummaryList, tt.versions)

			versions := make([]int32, 0, tt.versions)
			for _, s := range list.ModelPackageSummaryList {
				versions = append(versions, aws.ToInt32(s.ModelPackageVersion))
			}

			assert.ElementsMatch(t, seq(tt.versions), versions)
		})
	}
}

func seq(n int) []int32 {
	out := make([]int32, 0, n)

	var v int32

	for range n {
		v++
		out = append(out, v)
	}

	return out
}

func TestModelPackage_NeitherNameNorGroupRejected(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	_, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{})
	require.Error(t, err)
}

func TestAutoMLJobV1_ProblemTypeAndConfigRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newRealClient(t)

	_, err := client.CreateAutoMLJob(ctx, &sagemakersdk.CreateAutoMLJobInput{
		AutoMLJobName: aws.String("aml-v1"),
		RoleArn:       aws.String("arn:aws:iam::000000000000:role/r"),
		InputDataConfig: []smtypes.AutoMLChannel{{
			TargetAttributeName: aws.String("y"),
			DataSource: &smtypes.AutoMLDataSource{S3DataSource: &smtypes.AutoMLS3DataSource{
				S3DataType: smtypes.AutoMLS3DataTypeS3Prefix, S3Uri: aws.String("s3://b/in"),
			}},
		}},
		OutputDataConfig:                 &smtypes.AutoMLOutputDataConfig{S3OutputPath: aws.String("s3://b/out")},
		ProblemType:                      smtypes.ProblemTypeBinaryClassification,
		GenerateCandidateDefinitionsOnly: aws.Bool(true),
		AutoMLJobConfig: &smtypes.AutoMLJobConfig{
			Mode: smtypes.AutoMLModeEnsembling,
			CompletionCriteria: &smtypes.AutoMLJobCompletionCriteria{
				MaxCandidates: aws.Int32(7),
			},
			CandidateGenerationConfig: &smtypes.AutoMLCandidateGenerationConfig{
				AlgorithmsConfig: []smtypes.AutoMLAlgorithmConfig{{
					AutoMLAlgorithms: []smtypes.AutoMLAlgorithm{smtypes.AutoMLAlgorithmXgboost},
				}},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeAutoMLJob(ctx, &sagemakersdk.DescribeAutoMLJobInput{AutoMLJobName: aws.String("aml-v1")})
	require.NoError(t, err)

	assert.Equal(t, smtypes.ProblemTypeBinaryClassification, got.ProblemType)
	assert.True(t, aws.ToBool(got.GenerateCandidateDefinitionsOnly))
	require.NotNil(t, got.AutoMLJobConfig)
	assert.Equal(t, smtypes.AutoMLModeEnsembling, got.AutoMLJobConfig.Mode)
	require.NotNil(t, got.AutoMLJobConfig.CompletionCriteria)
	assert.EqualValues(t, 7, aws.ToInt32(got.AutoMLJobConfig.CompletionCriteria.MaxCandidates))
	require.NotNil(t, got.AutoMLJobConfig.CandidateGenerationConfig)
	require.Len(t, got.AutoMLJobConfig.CandidateGenerationConfig.AlgorithmsConfig, 1)
}

func TestDomain_HomeEfsFileSystemKmsKeyIDRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newRealClient(t)

	created, err := client.CreateDomain(ctx, &sagemakersdk.CreateDomainInput{
		DomainName: aws.String("kms-domain"), AuthMode: smtypes.AuthModeIam,
		DefaultUserSettings:       &smtypes.UserSettings{},
		HomeEfsFileSystemKmsKeyId: aws.String("key-123"), //nolint:staticcheck // deprecated-but-real member under test
	})
	require.NoError(t, err)

	got, err := client.DescribeDomain(ctx, &sagemakersdk.DescribeDomainInput{DomainId: created.DomainId})
	require.NoError(t, err)
	gotKey := got.HomeEfsFileSystemKmsKeyId //nolint:staticcheck // deprecated-but-real member under test
	assert.Equal(t, "key-123", aws.ToString(gotKey))
}

func TestPipeline_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	const def = `{"Version":"2020-12-01","Steps":[]}`

	tests := []struct {
		name        string
		secondToken string
		wantSame    bool
		wantErr     bool
	}{
		{name: "same_token_replays", secondToken: "tok-1", wantSame: true},
		{name: "different_token_conflicts", secondToken: "tok-2", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newRealClient(t)

			create := func(token string) (string, error) {
				out, err := client.CreatePipeline(ctx, &sagemakersdk.CreatePipelineInput{
					PipelineName: aws.String("pl"), ClientRequestToken: aws.String(token),
					RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), PipelineDefinition: aws.String(def),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.PipelineArn), nil
			}

			first, err := create("tok-1")
			require.NoError(t, err)

			second, err := create(tt.secondToken)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, first == second)

			start := func(token string) string {
				out, serr := client.StartPipelineExecution(ctx, &sagemakersdk.StartPipelineExecutionInput{
					PipelineName: aws.String("pl"), ClientRequestToken: aws.String(token),
				})
				require.NoError(t, serr)

				return aws.ToString(out.PipelineExecutionArn)
			}

			e1, e2, e3 := start("run-1"), start("run-1"), start("run-2")
			assert.Equal(t, e1, e2)
			assert.NotEqual(t, e1, e3)
		})
	}
}
