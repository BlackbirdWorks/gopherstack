package rds_test

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestStartExportTask_RequiredWireFields proves StartExportTask reads
// IamRoleArn and KmsKeyId from the raw form request (both required per
// rds@v1.124.1 api_op_StartExportTask.go:57-59,90-98) instead of silently
// dropping them, and that omitting either is rejected rather than accepted
// with the value discarded.
func TestStartExportTask_RequiredWireFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraVals url.Values
		name      string
		wantIAM   string
		wantKMS   string
		wantCode  int
	}{
		{
			name: "iam role and kms key echoed",
			extraVals: url.Values{
				"IamRoleArn": {"arn:aws:iam::000000000000:role/export-role"},
				"KmsKeyId":   {"arn:aws:kms:us-east-1:000000000000:key/test-key"},
			},
			wantCode: http.StatusOK,
			wantIAM:  "arn:aws:iam::000000000000:role/export-role",
			wantKMS:  "arn:aws:kms:us-east-1:000000000000:key/test-key",
		},
		{
			name: "missing iam role rejected",
			extraVals: url.Values{
				"KmsKeyId": {"arn:aws:kms:us-east-1:000000000000:key/test-key"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing kms key rejected",
			extraVals: url.Values{
				"IamRoleArn": {"arn:aws:iam::000000000000:role/export-role"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			vals := url.Values{
				"Action":               {"StartExportTask"},
				"Version":              {"2014-10-31"},
				"ExportTaskIdentifier": {"my-task"},
				"SourceArn":            {"arn:aws:rds:us-east-1:000000000000:snapshot:s1"},
				"S3BucketName":         {"my-bucket"},
			}
			maps.Copy(vals, tt.extraVals)

			rec := doAccuracyRDS(t, h, vals)
			require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantCode != http.StatusOK {
				return
			}

			body := rec.Body.String()
			assert.Contains(t, body, tt.wantIAM)
			assert.Contains(t, body, tt.wantKMS)
		})
	}
}

func TestRDSBackend_CancelExportTask_RemovesFromMap(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.StartExportTask("my-task", "arn:aws:rds:us-east-1:000000000000:snapshot:s1", "my-bucket",
		"arn:aws:iam::000000000000:role/export-role", "arn:aws:kms:us-east-1:000000000000:key/test-key")
	require.NoError(t, err)

	task, err := b.CancelExportTask("my-task")
	require.NoError(t, err)
	assert.Equal(t, "canceled", task.Status)

	// Task should no longer be in the map.
	_, err = b.DescribeExportTasks("my-task")
	require.Error(t, err)
}
