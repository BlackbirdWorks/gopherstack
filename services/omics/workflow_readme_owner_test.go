package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateWorkflow/CreateWorkflowVersion README and WorkflowBucketOwnerId members (api_op_CreateWorkflow.go).
func TestWorkflow_ReadmeAndBucketOwner(t *testing.T) {
	t.Parallel()

	t.Run("workflow-readme-roundtrip", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)
		wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name: aws.String("wf-readme"), Engine: types.WorkflowEngineWdl, RequestId: aws.String(uuid.NewString()),
			ReadmeMarkdown: aws.String("# Hello"), ReadmePath: aws.String("docs/README.md"),
		})
		require.NoError(t, err)

		got, err := client.GetWorkflow(t.Context(), &omicssdk.GetWorkflowInput{Id: wf.Id})
		require.NoError(t, err)
		assert.Equal(t, "# Hello", aws.ToString(got.Readme))
		assert.Equal(t, "docs/README.md", aws.ToString(got.ReadmePath))
	})

	t.Run("version-readme-and-owner-roundtrip", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)
		wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name: aws.String("wf-ver"), Engine: types.WorkflowEngineWdl, RequestId: aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		_, err = client.CreateWorkflowVersion(t.Context(), &omicssdk.CreateWorkflowVersionInput{
			WorkflowId: wf.Id, VersionName: aws.String("v1"), RequestId: aws.String(uuid.NewString()),
			ReadmeMarkdown: aws.String("# V1"), ReadmePath: aws.String("README.md"),
			WorkflowBucketOwnerId: aws.String("000000000000"), DefinitionUri: aws.String("s3://bucket/wf.zip"),
		})
		require.NoError(t, err)

		got, err := client.GetWorkflowVersion(t.Context(), &omicssdk.GetWorkflowVersionInput{
			WorkflowId: wf.Id, VersionName: aws.String("v1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "# V1", aws.ToString(got.Readme))
		assert.Equal(t, "README.md", aws.ToString(got.ReadmePath))
		assert.Equal(t, "000000000000", aws.ToString(got.WorkflowBucketOwnerId))
	})

	tests := []struct {
		name    string
		uri     string
		owner   string
		wantErr bool
	}{
		{name: "matching-owner", uri: "s3://b/wf.zip", owner: "000000000000"},
		{name: "mismatched-owner", uri: "s3://b/wf.zip", owner: "111111111111", wantErr: true},
		{name: "owner-without-s3-uri", uri: "", owner: "111111111111"},
		{name: "no-owner", uri: "s3://b/wf.zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			in := &omicssdk.CreateWorkflowInput{
				Name: aws.String("wf-owner"), Engine: types.WorkflowEngineWdl, RequestId: aws.String(uuid.NewString()),
			}
			if tt.uri != "" {
				in.DefinitionUri = aws.String(tt.uri)
			}

			if tt.owner != "" {
				in.WorkflowBucketOwnerId = aws.String(tt.owner)
			}

			_, err := client.CreateWorkflow(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}

			require.NoError(t, err)
		})
	}
}
