package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func TestFunctionStage_LiveSnapshotSurvivesUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stage     types.FunctionStage
		wantCode  string
		wantStage types.FunctionStage
	}{
		{name: "live", stage: types.FunctionStageLive, wantCode: "live-v1", wantStage: types.FunctionStageLive},
		{
			name: "development", stage: types.FunctionStageDevelopment,
			wantCode: "dev-v2", wantStage: types.FunctionStageDevelopment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			t.Cleanup(backend.Close)
			client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

			_, err := backend.CreateFunction("fn", "c", "cloudfront-js-2.0", "live-v1", nil)
			require.NoError(t, err)
			_, err = backend.PublishFunction("fn")
			require.NoError(t, err)
			_, err = backend.UpdateFunction("fn", "c2", "cloudfront-js-2.0", "dev-v2")
			require.NoError(t, err)

			got, err := client.GetFunction(
				t.Context(),
				&cfsdk.GetFunctionInput{Name: aws.String("fn"), Stage: tt.stage},
			)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, string(got.FunctionCode))

			desc, err := client.DescribeFunction(
				t.Context(), &cfsdk.DescribeFunctionInput{Name: aws.String("fn"), Stage: tt.stage},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStage, desc.FunctionSummary.FunctionMetadata.Stage)
		})
	}
}

func TestFunctionStage_LiveMissingBeforePublish(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(backend.Close)
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	_, err := backend.CreateFunction("fn", "c", "cloudfront-js-2.0", "x", nil)
	require.NoError(t, err)

	_, err = client.DescribeFunction(
		t.Context(), &cfsdk.DescribeFunctionInput{Name: aws.String("fn"), Stage: types.FunctionStageLive},
	)
	require.Error(t, err)

	var nf *types.NoSuchFunctionExists
	assert.ErrorAs(t, err, &nf)
}

func TestConnectionFunctionStage_LiveSnapshotSurvivesUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stage     types.FunctionStage
		wantComm  string
		wantStage types.FunctionStage
	}{
		{name: "live", stage: types.FunctionStageLive, wantComm: "v1", wantStage: types.FunctionStageLive},
		{
			name:      "development",
			stage:     types.FunctionStageDevelopment,
			wantComm:  "v2",
			wantStage: types.FunctionStageDevelopment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			t.Cleanup(backend.Close)
			client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

			_, err := backend.CreateConnectionFunction("cfn", "v1")
			require.NoError(t, err)
			_, err = backend.PublishConnectionFunction("cfn")
			require.NoError(t, err)
			_, err = backend.UpdateConnectionFunction("cfn", "v2", "cloudfront-js-2.0", []byte("new"))
			require.NoError(t, err)

			desc, err := client.DescribeConnectionFunction(
				t.Context(), &cfsdk.DescribeConnectionFunctionInput{Identifier: aws.String("cfn"), Stage: tt.stage},
			)
			require.NoError(t, err)

			sum := desc.ConnectionFunctionSummary
			assert.Equal(t, tt.wantStage, sum.Stage)
			assert.Equal(t, tt.wantComm, aws.ToString(sum.ConnectionFunctionConfig.Comment))
		})
	}
}
