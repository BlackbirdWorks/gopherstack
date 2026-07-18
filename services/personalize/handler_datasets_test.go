package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Dataset_FieldRetention(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "interaction-ds",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"datasetType":     "INTERACTIONS",
		"schemaArn":       "arn:aws:personalize:us-east-1:000000000000:schema/my-schema",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	created := personalizeUnmarshal(t, rec)
	dsArn := created["datasetArn"].(string)

	rec = personalizeDo(t, h, "DescribeDataset", map[string]any{"datasetArn": dsArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	ds := m["dataset"].(map[string]any)
	assert.Equal(t, "interaction-ds", ds["name"])
	assert.Equal(t, "INTERACTIONS", ds["datasetType"])
	assert.Equal(t, "arn:aws:personalize:us-east-1:000000000000:schema/my-schema", ds["schemaArn"])
	assert.Equal(t, "ACTIVE", ds["status"])
}

func TestPersonalize_Dataset_Update(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":        "update-ds",
		"datasetType": "ITEMS",
		"schemaArn":   "arn:aws:personalize:us-east-1:000000000000:schema/old-schema",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dsArn := personalizeUnmarshal(t, rec)["datasetArn"].(string)

	rec = personalizeDo(t, h, "UpdateDataset", map[string]any{
		"datasetArn": dsArn,
		"schemaArn":  "arn:aws:personalize:us-east-1:000000000000:schema/new-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeDataset", map[string]any{"datasetArn": dsArn})
	ds := personalizeUnmarshal(t, rec)["dataset"].(map[string]any)
	assert.Equal(t, "arn:aws:personalize:us-east-1:000000000000:schema/new-schema", ds["schemaArn"])
}

// --- Schema ---

func TestPersonalize_DatasetImportJob(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateDatasetImportJob", map[string]any{
		"jobName":    "import-job-1",
		"datasetArn": "arn:aws:personalize:us-east-1:000000000000:dataset/my-ds",
		"roleArn":    "arn:aws:iam::000000000000:role/PersonalizeRole",
		"dataSource": map[string]any{"dataLocation": "s3://my-bucket/data.csv"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := personalizeUnmarshal(t, rec)["datasetImportJobArn"].(string)
	assert.NotEmpty(t, jobArn)
	assert.Contains(t, jobArn, "arn:aws:personalize:us-east-1:000000000000:")

	rec = personalizeDo(t, h, "DescribeDatasetImportJob", map[string]any{"datasetImportJobArn": jobArn})
	require.Equal(t, http.StatusOK, rec.Code)
	job := personalizeUnmarshal(t, rec)["datasetImportJob"].(map[string]any)
	assert.Equal(t, "import-job-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])

	rec = personalizeDo(t, h, "ListDatasetImportJobs", map[string]any{})
	listed := personalizeUnmarshal(t, rec)
	assert.Len(t, listed["datasetImportJobs"].([]any), 1)
}
