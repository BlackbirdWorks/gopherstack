package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestGetFlowLogsIntegrationTemplateHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	fls, err := h.Backend.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest", nil)
	require.NoError(t, err)
	require.Len(t, fls, 1)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                         {"GetFlowLogsIntegrationTemplate"},
		"FlowLogId":                      {fls[0].FlowLogID},
		"ConfigDeliveryS3DestinationArn": {"arn:aws:s3:::cfn-bucket"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetFlowLogsIntegrationTemplateResponse>")
	assert.Contains(t, resp, fls[0].FlowLogID)
}

// TestHandlerDeleteFlowLogs covers handleDeleteFlowLogs.
func TestHandlerDeleteFlowLogs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create flow logs first via backend.
	vpc, err := b.CreateVpc("10.8.0.0/16", "default")
	require.NoError(t, err)

	logs, err := b.CreateFlowLogs([]string{vpc.ID}, "ALL", "cloud-watch-logs", "/aws/vpc/flow", nil)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	logID := logs[0].FlowLogID

	rec := postForm(t, h, "Action=DeleteFlowLogs&Version=2016-11-15&FlowLogId.1="+logID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteFlowLogsResponse")
}

// TestHandlerLaunchTemplateVersions covers handleCreateLaunchTemplateVersion,
// handleDeleteLaunchTemplateVersions, handleGetLaunchTemplateData,
// handleDescribeLaunchTemplateVersions.
