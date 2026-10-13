package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMLTaskRuns_Properties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start    func(t *testing.T, c *gluesdk.Client, id *string)
		check    func(t *testing.T, p *types.TaskRunProperties)
		name     string
		taskType types.TaskType
	}{
		{
			name:     "export",
			taskType: types.TaskTypeExportLabels,
			start: func(t *testing.T, c *gluesdk.Client, id *string) {
				t.Helper()

				_, err := c.StartExportLabelsTaskRun(t.Context(), &gluesdk.StartExportLabelsTaskRunInput{
					TransformId: id, OutputS3Path: aws.String("s3://b/out"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p *types.TaskRunProperties) {
				t.Helper()
				require.NotNil(t, p.ExportLabelsTaskRunProperties)
				assert.Equal(t, "s3://b/out", aws.ToString(p.ExportLabelsTaskRunProperties.OutputS3Path))
				assert.Nil(t, p.ImportLabelsTaskRunProperties)
			},
		},
		{
			name:     "import",
			taskType: types.TaskTypeImportLabels,
			start: func(t *testing.T, c *gluesdk.Client, id *string) {
				t.Helper()

				_, err := c.StartImportLabelsTaskRun(t.Context(), &gluesdk.StartImportLabelsTaskRunInput{
					TransformId: id, InputS3Path: aws.String("s3://b/in"), ReplaceAllLabels: true,
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p *types.TaskRunProperties) {
				t.Helper()
				require.NotNil(t, p.ImportLabelsTaskRunProperties)
				assert.Equal(t, "s3://b/in", aws.ToString(p.ImportLabelsTaskRunProperties.InputS3Path))
				assert.True(t, p.ImportLabelsTaskRunProperties.Replace)
			},
		},
		{
			name:     "labeling_set",
			taskType: types.TaskTypeLabelingSetGeneration,
			start: func(t *testing.T, c *gluesdk.Client, id *string) {
				t.Helper()

				_, err := c.StartMLLabelingSetGenerationTaskRun(
					t.Context(),
					&gluesdk.StartMLLabelingSetGenerationTaskRunInput{
						TransformId: id, OutputS3Path: aws.String("s3://b/labels"),
					},
				)
				require.NoError(t, err)
			},
			check: func(t *testing.T, p *types.TaskRunProperties) {
				t.Helper()
				require.NotNil(t, p.LabelingSetGenerationTaskRunProperties)
				assert.Equal(t, "s3://b/labels", aws.ToString(p.LabelingSetGenerationTaskRunProperties.OutputS3Path))
			},
		},
		{
			name:     "evaluation",
			taskType: types.TaskTypeEvaluation,
			start: func(t *testing.T, c *gluesdk.Client, id *string) {
				t.Helper()

				_, err := c.StartMLEvaluationTaskRun(
					t.Context(),
					&gluesdk.StartMLEvaluationTaskRunInput{TransformId: id},
				)
				require.NoError(t, err)
			},
			check: func(t *testing.T, p *types.TaskRunProperties) {
				t.Helper()
				assert.Nil(t, p.ExportLabelsTaskRunProperties)
				assert.Nil(t, p.ImportLabelsTaskRunProperties)
				assert.Nil(t, p.LabelingSetGenerationTaskRunProperties)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("mldb")},
			})
			require.NoError(t, err)
			_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
				DatabaseName: aws.String("mldb"), TableInput: &types.TableInput{Name: aws.String("mltbl")},
			})
			require.NoError(t, err)

			created, err := client.CreateMLTransform(ctx, &gluesdk.CreateMLTransformInput{
				Name: aws.String("mlt1"),
				Role: aws.String("r"),
				InputRecordTables: []types.GlueTable{
					{DatabaseName: aws.String("mldb"), TableName: aws.String("mltbl")},
				},
				Parameters: &types.TransformParameters{TransformType: types.TransformTypeFindMatches},
			})
			require.NoError(t, err)

			tt.start(t, client, created.TransformId)

			runs, err := client.GetMLTaskRuns(ctx, &gluesdk.GetMLTaskRunsInput{TransformId: created.TransformId})
			require.NoError(t, err)
			require.Len(t, runs.TaskRuns, 1)

			props := runs.TaskRuns[0].Properties
			require.NotNil(t, props)
			assert.Equal(t, tt.taskType, props.TaskType)
			tt.check(t, props)
		})
	}
}
