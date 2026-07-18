package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_BatchInferenceJob(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateBatchInferenceJob", map[string]any{
		"jobName":            "batch-inf-1",
		"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1",
		"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
		"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/input"}},
		"jobOutput":          map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/output"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := personalizeUnmarshal(t, rec)["batchInferenceJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = personalizeDo(t, h, "DescribeBatchInferenceJob", map[string]any{"batchInferenceJobArn": jobArn})
	job := personalizeUnmarshal(t, rec)["batchInferenceJob"].(map[string]any)
	assert.Equal(t, "batch-inf-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])
}

func TestPersonalize_BatchSegmentJob(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateBatchSegmentJob", map[string]any{
		"jobName":            "seg-job-1",
		"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1",
		"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
		"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/input"}},
		"jobOutput":          map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/output"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := personalizeUnmarshal(t, rec)["batchSegmentJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = personalizeDo(t, h, "ListBatchSegmentJobs", map[string]any{})
	listed := personalizeUnmarshal(t, rec)
	assert.Len(t, listed["batchSegmentJobs"].([]any), 1)
}

func TestPersonalize_DataDeletionJob(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateDataDeletionJob", map[string]any{
		"jobName":         "delete-job-1",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"roleArn":         "arn:aws:iam::000000000000:role/PersonalizeRole",
		"dataSource":      map[string]any{"dataLocation": "s3://bucket/users-to-delete.csv"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := personalizeUnmarshal(t, rec)["dataDeletionJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = personalizeDo(t, h, "DescribeDataDeletionJob", map[string]any{"dataDeletionJobArn": jobArn})
	job := personalizeUnmarshal(t, rec)["dataDeletionJob"].(map[string]any)
	assert.Equal(t, "delete-job-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])
	assert.NotNil(t, job["numDeleted"])
}

// --- Recipe ---
