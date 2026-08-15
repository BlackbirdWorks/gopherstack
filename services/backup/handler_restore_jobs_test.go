package backup_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
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

// TestSDKRoundTrip_RestoreJobSourceResourceArn proves DescribeRestoreJob and
// ListRestoreJobs both emit SourceResourceArn, the real member name on
// types.RestoreJobsListMember and DescribeRestoreJobOutput
// (backup@v1.59.4 types/types.go:2109-2196,
// api_op_DescribeRestoreJob.go:39-124) -- restoreJobToJSON previously wrote
// "ResourceArn" instead, a name neither type declares. A raw-body assertion
// is weak here: it would only show a key present under the wrong name, not
// prove a real client actually loses the value. Driving the real
// aws-sdk-go-v2 client is what proves it: with the wrong key, the
// deserializer silently discards it and SourceResourceArn decodes as nil
// even though the call succeeds.
func TestSDKRoundTrip_RestoreJobSourceResourceArn(t *testing.T) {
	t.Parallel()

	h, backend := newHandlerAndBackend()
	client := newTestBackupClient(t, h)

	rpArn := seedVaultAndRP(t, h, backend, "restore-vault")

	startOut, err := client.StartRestoreJob(t.Context(), &backupsdk.StartRestoreJobInput{
		RecoveryPointArn: aws.String(rpArn),
		IamRoleArn:       aws.String("arn:aws:iam::123456789012:role/RestoreRole"),
		ResourceType:     aws.String("EC2"),
		Metadata:         map[string]string{"newVolumeAvailabilityZone": "us-east-1a"},
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.RestoreJobId)

	describeOut, err := client.DescribeRestoreJob(t.Context(), &backupsdk.DescribeRestoreJobInput{
		RestoreJobId: startOut.RestoreJobId,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:instance/i-test", aws.ToString(describeOut.SourceResourceArn))

	listOut, err := client.ListRestoreJobs(t.Context(), &backupsdk.ListRestoreJobsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.RestoreJobs, 1)
	assert.Equal(
		t, "arn:aws:ec2:us-east-1:123456789012:instance/i-test", aws.ToString(listOut.RestoreJobs[0].SourceResourceArn),
	)
}
