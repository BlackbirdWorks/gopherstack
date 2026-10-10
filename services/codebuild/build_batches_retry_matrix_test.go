package codebuild_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

const buildMatrixSpec = `version: 0.2
batch:
  build-matrix:
    static:
      ignore-failure: true
      env:
        privileged-mode: true
    dynamic:
      env:
        image:
          - aws/codebuild/standard:6.0
          - aws/codebuild/standard:7.0
        variables:
          FLAVOR:
            - a
            - b
            - c
`

func TestRetryBuildBatch_Semantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		retryType   types.RetryBuildBatchType
		wantErrCode string
		wantCarried int
		stopFirst   bool
	}{
		{name: "succeeded batch rejected", wantErrCode: "InvalidInputException"},
		{name: "retry all", stopFirst: true, retryType: types.RetryBuildBatchTypeRetryAllBuilds, wantCarried: 0},
		{name: "retry failed", stopFirst: true, retryType: types.RetryBuildBatchTypeRetryFailedBuilds, wantCarried: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
			createGraphTestProject(t, client, "retry-proj", buildListTwoNodesSpec)

			start, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
				ProjectName: aws.String("retry-proj"),
			})
			require.NoError(t, err)

			batchID := aws.ToString(start.BuildBatch.Id)
			var failedID string

			if tt.stopFirst {
				arn := aws.ToString(start.BuildBatch.BuildGroups[0].CurrentBuildSummary.Arn)
				failedID = aws.ToString(start.BuildBatch.BuildGroups[0].Identifier)
				_, buildID, _ := strings.Cut(arn, ":build/")
				_, err = client.StopBuild(t.Context(), &codebuildsdk.StopBuildInput{Id: aws.String(buildID)})
				require.NoError(t, err)
			}

			first := runJanitorUntilBatchTerminal(t, client, backend, batchID)

			retry, err := client.RetryBuildBatch(t.Context(), &codebuildsdk.RetryBuildBatchInput{
				Id: aws.String(batchID), RetryType: tt.retryType,
			})
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, awsErrorCode(t, err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, types.StatusTypeFailed, first.BuildBatchStatus)

			carried := 0
			oldArns := map[string]string{}

			for _, g := range first.BuildGroups {
				oldArns[aws.ToString(g.Identifier)] = aws.ToString(g.CurrentBuildSummary.Arn)
			}

			for _, g := range retry.BuildBatch.BuildGroups {
				id := aws.ToString(g.Identifier)
				if aws.ToString(g.CurrentBuildSummary.Arn) == oldArns[id] {
					carried++

					assert.NotEqual(t, failedID, id)
					assert.Empty(t, g.PriorBuildSummaryList)

					continue
				}

				require.Len(t, g.PriorBuildSummaryList, 1)
				assert.Equal(t, oldArns[id], aws.ToString(g.PriorBuildSummaryList[0].Arn))
			}

			assert.Equal(t, tt.wantCarried, carried)
		})
	}
}

func TestStartBuildBatch_BuildMatrix(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	createGraphTestProject(t, client, "matrix-proj", buildMatrixSpec)

	start, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
		ProjectName: aws.String("matrix-proj"),
	})
	require.NoError(t, err)
	require.Len(t, start.BuildBatch.BuildGroups, 6)

	final := runJanitorUntilBatchTerminal(t, client, backend, aws.ToString(start.BuildBatch.Id))
	assert.Equal(t, types.StatusTypeSucceeded, final.BuildBatchStatus)

	seen := map[string]bool{}

	for _, g := range final.BuildGroups {
		assert.True(t, g.IgnoreFailure)

		out, getErr := client.BatchGetBuilds(t.Context(), &codebuildsdk.BatchGetBuildsInput{
			Ids: []string{aws.ToString(g.CurrentBuildSummary.Arn)},
		})
		require.NoError(t, getErr)
		require.Len(t, out.Builds, 1)

		env := out.Builds[0].Environment
		assert.True(t, aws.ToBool(env.PrivilegedMode))

		var flavor string

		for _, v := range env.EnvironmentVariables {
			if aws.ToString(v.Name) == "FLAVOR" {
				flavor = aws.ToString(v.Value)
			}
		}

		seen[aws.ToString(env.Image)+"|"+flavor] = true
	}

	assert.Len(t, seen, 6, "every image x FLAVOR combination must be a distinct build")
}
