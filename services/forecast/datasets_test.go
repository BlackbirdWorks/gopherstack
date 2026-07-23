package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatasets_ImportCreateFailure verifies that a DatasetImportJob missing a
// valid S3 path is created in CREATE_FAILED status.
func TestDatasets_ImportCreateFailure(t *testing.T) {
	t.Parallel()

	h := newHandler()
	datasetARN := createDataset(t, h)
	_, created := request(t, h, "CreateDatasetImportJob", map[string]any{
		"DatasetImportJobName": "broken-import",
		"DatasetArn":           datasetARN,
		"DataSource":           map[string]any{"S3Config": map[string]any{}},
	})
	_, described := request(t, h, "DescribeDatasetImportJob", map[string]any{
		"DatasetImportJobArn": created["DatasetImportJobArn"],
	})
	assert.Equal(t, "CREATE_FAILED", described["Status"])
}

// TestDatasets_FieldRetention verifies that Dataset fields (Schema,
// DataFrequency, Domain, DatasetType) round-trip through Create/Describe.
func TestDatasets_FieldRetention(t *testing.T) {
	t.Parallel()

	h := newHandler()

	schema := map[string]any{
		"Attributes": []any{
			map[string]any{"AttributeName": "item_id", "AttributeType": "string"},
			map[string]any{"AttributeName": "demand", "AttributeType": "float"},
		},
	}
	code, created := request(t, h, "CreateDataset", map[string]any{
		"DatasetName":   "schema-ds",
		"Domain":        "RETAIL",
		"DatasetType":   "TARGET_TIME_SERIES",
		"DataFrequency": "W",
		"Schema":        schema,
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["DatasetArn"].(string)

	// Advance to ACTIVE
	request(t, h, "DescribeDataset", map[string]any{"DatasetArn": arn})
	code, m := request(t, h, "DescribeDataset", map[string]any{"DatasetArn": arn})
	require.Equal(t, http.StatusOK, code)

	assert.Equal(t, "schema-ds", m["DatasetName"])
	assert.Equal(t, "RETAIL", m["Domain"])
	assert.Equal(t, "TARGET_TIME_SERIES", m["DatasetType"])
	assert.Equal(t, "W", m["DataFrequency"])
	assert.Equal(t, schema, m["Schema"])
	assert.Equal(t, "ACTIVE", m["Status"])
}

// TestDatasetImportJobs_S3Validation verifies that CreateDatasetImportJob
// requires a non-empty DataSource.S3Config.Path: a valid path is accepted
// (CREATE_PENDING) and a missing one fails the job (CREATE_FAILED).
func TestDatasetImportJobs_S3Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus string
	}{
		{
			name: "valid_s3_path_create_pending",
			body: map[string]any{
				"DatasetImportJobName": "valid-import",
				"DatasetArn":           "arn:aws:forecast:us-east-1:000000000000:dataset/sales",
				"DataSource": map[string]any{
					"S3Config": map[string]any{"Path": "s3://bucket/data.csv"},
				},
			},
			wantStatus: "CREATE_PENDING",
		},
		{
			name: "missing_s3_path_create_failed",
			body: map[string]any{
				"DatasetImportJobName": "broken-import",
				"DatasetArn":           "arn:aws:forecast:us-east-1:000000000000:dataset/sales",
				"DataSource":           map[string]any{"S3Config": map[string]any{}},
			},
			wantStatus: "CREATE_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			// The DatasetImportJob's DatasetArn (below) must resolve to a real
			// Dataset now that CreateDatasetImportJob validates the reference;
			// this creates one named "sales" so its deterministically-built ARN
			// matches the literal "dataset/sales" ARN hardcoded in tt.body.
			request(t, h, "CreateDataset", map[string]any{
				"DatasetName": "sales", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
				"DataFrequency": "D", "Schema": map[string]any{"Attributes": []any{}},
			})

			code, created := request(t, h, "CreateDatasetImportJob", tt.body)
			require.Equal(t, http.StatusOK, code)
			arn := created["DatasetImportJobArn"].(string)

			code, m := request(t, h, "DescribeDatasetImportJob", map[string]any{"DatasetImportJobArn": arn})
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, tt.wantStatus, m["Status"])
		})
	}
}
