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
	dgArn := personalizeCreateDatasetGroup(t, h, "interaction-ds-dg")
	schemaArn := personalizeCreateSchema(t, h, "my-schema")

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "interaction-ds",
		"datasetGroupArn": dgArn,
		"datasetType":     "INTERACTIONS",
		"schemaArn":       schemaArn,
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
	assert.Equal(t, schemaArn, ds["schemaArn"])
	assert.Equal(t, "ACTIVE", ds["status"])
}

func TestPersonalize_Dataset_Update(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "update-ds-dg")
	oldSchemaArn := personalizeCreateSchema(t, h, "old-schema")

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "update-ds",
		"datasetGroupArn": dgArn,
		"datasetType":     "ITEMS",
		"schemaArn":       oldSchemaArn,
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

// TestPersonalize_Dataset_InvalidDatasetType locks that CreateDataset
// rejects a datasetType outside the documented set (INTERACTIONS/ITEMS/
// USERS/ACTIONS/ACTION_INTERACTIONS) -- real AWS accepts these
// case-insensitively and rejects anything else, even though the SDK models
// DatasetType as a plain *string rather than a typed enum.
func TestPersonalize_Dataset_InvalidDatasetType(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "bad-type-dg")
	schemaArn := personalizeCreateSchema(t, h, "bad-type-schema")

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "bad-type-ds",
		"datasetGroupArn": dgArn,
		"datasetType":     "NOT_A_REAL_TYPE",
		"schemaArn":       schemaArn,
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "InvalidInputException", m["__type"])
}

// TestPersonalize_Dataset_DatasetTypeCaseInsensitive locks that datasetType
// validation is case-insensitive, matching the real API's documented
// "Interactions | Items | Users | Actions | Action_Interactions" acceptance.
func TestPersonalize_Dataset_DatasetTypeCaseInsensitive(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "mixed-case-dg")
	schemaArn := personalizeCreateSchema(t, h, "mixed-case-schema")

	rec := personalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "mixed-case-ds",
		"datasetGroupArn": dgArn,
		"datasetType":     "Action_Interactions",
		"schemaArn":       schemaArn,
	})

	require.Equal(t, http.StatusOK, rec.Code)
}

// --- Schema ---

func TestPersonalize_DatasetImportJob(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dsArn := personalizeCreateDataset(t, h, "my-ds")

	rec := personalizeDo(t, h, "CreateDatasetImportJob", map[string]any{
		"jobName":    "import-job-1",
		"datasetArn": dsArn,
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
