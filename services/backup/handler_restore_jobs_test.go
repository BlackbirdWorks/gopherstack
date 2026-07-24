package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartRestoreJobHTTP(t *testing.T) {
	t.Parallel()

	t.Run("success round-trips through Describe", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()

		startRec := doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
			"RecoveryPointArn": "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-1",
			"IamRoleArn":       "arn:aws:iam::123456789012:role/restore-role",
			"ResourceType":     "EBS",
			"Metadata":         map[string]any{"newVolumeAvailabilityZone": "us-east-1a"},
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		started := parseResp(t, startRec)
		jobID, ok := started["RestoreJobId"].(string)
		require.True(t, ok)
		require.NotEmpty(t, jobID)

		describeRec := doREST(t, h, http.MethodGet, "/restore-jobs/"+jobID, nil)
		require.Equal(t, http.StatusOK, describeRec.Code)
		described := parseResp(t, describeRec)
		assert.Equal(t, jobID, described["RestoreJobId"])
		assert.Equal(t, "COMPLETED", described["Status"])
		assert.NotEmpty(t, described["CreatedResourceArn"])
		assert.Equal(t, "123456789012", described["AccountId"])
	})

	t.Run("missing Metadata is MissingParameterValueException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
			"RecoveryPointArn": "arn:rp",
			"IamRoleArn":       "arn:role",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "MissingParameterValueException")
	})

	t.Run("describe unknown job is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodGet, "/restore-jobs/nonexistent", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	})
}

func TestListRestoreJobsHTTP(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerAndBackend()

	doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
		"RecoveryPointArn": "arn:rp-1",
		"IamRoleArn":       "arn:role",
		"Metadata":         map[string]any{"k": "v"},
	})
	doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
		"RecoveryPointArn": "arn:rp-2",
		"IamRoleArn":       "arn:role",
		"Metadata":         map[string]any{"k": "v"},
	})

	rec := doREST(t, h, http.MethodGet, "/restore-jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	items, ok := resp["RestoreJobs"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
}

func TestGetRestoreJobMetadataHTTP(t *testing.T) {
	t.Parallel()

	t.Run("returns the stored metadata map", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		startRec := doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
			"RecoveryPointArn": "arn:rp",
			"IamRoleArn":       "arn:role",
			"Metadata":         map[string]any{"newVolumeName": "restored"},
		})
		started := parseResp(t, startRec)
		jobID, ok := started["RestoreJobId"].(string)
		require.True(t, ok)

		rec := doREST(t, h, http.MethodGet, "/restore-jobs/"+jobID+"/metadata", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseResp(t, rec)
		metadata, ok := resp["Metadata"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "restored", metadata["newVolumeName"])
	})

	t.Run("unknown job is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodGet, "/restore-jobs/nonexistent/metadata", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	})
}

func TestPutRestoreValidationResultHTTP(t *testing.T) {
	t.Parallel()

	t.Run("real AWS responseCode 204, reflected in subsequent Describe", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		startRec := doREST(t, h, http.MethodPut, "/restore-jobs", map[string]any{
			"RecoveryPointArn": "arn:rp",
			"IamRoleArn":       "arn:role",
			"Metadata":         map[string]any{"k": "v"},
		})
		started := parseResp(t, startRec)
		jobID, ok := started["RestoreJobId"].(string)
		require.True(t, ok)

		rec := doREST(t, h, http.MethodPut, "/restore-jobs/"+jobID+"/validations", map[string]any{
			"RestoreJobId":            jobID,
			"ValidationStatus":        "SUCCESSFUL",
			"ValidationStatusMessage": "all good",
		})
		assert.Equal(t, http.StatusNoContent, rec.Code)

		describeRec := doREST(t, h, http.MethodGet, "/restore-jobs/"+jobID, nil)
		described := parseResp(t, describeRec)
		assert.Equal(t, "SUCCESSFUL", described["ValidationStatus"])
		assert.Equal(t, "all good", described["ValidationStatusMessage"])
	})

	t.Run("unknown job is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodPut, "/restore-jobs/nonexistent/validations", map[string]any{
			"RestoreJobId":     "nonexistent",
			"ValidationStatus": "SUCCESSFUL",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	})
}
