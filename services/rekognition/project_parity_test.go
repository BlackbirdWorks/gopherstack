package rekognition_test

import (
	"testing"

	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	rekognitiontypes "github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateProject_StatusIsCreatedImmediately covers the fix in projects.go:
// CreateProject is synchronous on real AWS, so a project must already be
// CREATED by the time DescribeProjects is called -- not left CREATING, which
// wedges a caller (e.g. Terraform's aws_rekognition_project) that polls for
// CREATED with no async worker here to ever transition it.
func TestCreateProject_StatusIsCreatedImmediately(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		project string
	}{
		{name: "custom labels project", project: "mega-batch-52-project"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			_, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
				ProjectName: &tc.project,
				Feature:     rekognitiontypes.CustomizationFeatureCustomLabels,
			})
			require.NoError(t, err)

			out, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{
				ProjectNames: []string{tc.project},
			})
			require.NoError(t, err)
			require.Len(t, out.ProjectDescriptions, 1)
			assert.Equal(t, rekognitiontypes.ProjectStatusCreated, out.ProjectDescriptions[0].Status)
		})
	}
}

// TestListTagsForResource_ProjectARN covers the fix in tags.go: resourceExists
// must recognize a project ARN (via b.projects.Has), not just collections,
// stream processors, and Custom Labels model (project version) ARNs --
// CreateProjectInput.Tags accepts tags at creation time, so a project must
// be taggable/listable afterward too, and Terraform's aws_rekognition_project
// calls ListTagsForResource with the project ARN on every read.
func TestListTagsForResource_ProjectARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "single tag", tags: map[string]string{"env": "prod"}},
		{name: "no tags", tags: map[string]string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			projectName := "tag-test-project"
			createOut, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
				ProjectName: &projectName,
				Feature:     rekognitiontypes.CustomizationFeatureCustomLabels,
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.ProjectArn)

			if len(tc.tags) > 0 {
				_, err = client.TagResource(t.Context(), &rekognitionsdk.TagResourceInput{
					ResourceArn: createOut.ProjectArn,
					Tags:        tc.tags,
				})
				require.NoError(t, err)
			}

			listOut, err := client.ListTagsForResource(t.Context(), &rekognitionsdk.ListTagsForResourceInput{
				ResourceArn: createOut.ProjectArn,
			})
			require.NoError(t, err, "ListTagsForResource must recognize a project ARN")

			if len(tc.tags) == 0 {
				assert.Empty(t, listOut.Tags)
			} else {
				assert.Equal(t, tc.tags, listOut.Tags)
			}
		})
	}
}
