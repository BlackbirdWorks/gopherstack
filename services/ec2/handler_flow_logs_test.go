package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestCreateFlowLogs_LogFormatAndMaxAggregationInterval_RealClient covers
// gopherstack-xhu2t: LogFormat and MaxAggregationInterval were never read,
// so DescribeFlowLogs always echoed them empty/zero regardless of the request.
func TestCreateFlowLogs_LogFormatAndMaxAggregationInterval_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestEC2Client(t, h)

	created, err := client.CreateFlowLogs(t.Context(), &ec2sdk.CreateFlowLogsInput{
		ResourceIds:            []string{"vpc-default"},
		ResourceType:           "VPC",
		TrafficType:            "ALL",
		LogDestinationType:     "s3",
		LogDestination:         aws.String("arn:aws:s3:::dest"),
		LogFormat:              aws.String("${srcaddr} ${dstaddr}"),
		MaxAggregationInterval: aws.Int32(60),
	})
	require.NoError(t, err)
	require.Len(t, created.FlowLogIds, 1)

	desc, err := client.DescribeFlowLogs(t.Context(), &ec2sdk.DescribeFlowLogsInput{
		FlowLogIds: created.FlowLogIds,
	})
	require.NoError(t, err)
	require.Len(t, desc.FlowLogs, 1)
	assert.Equal(t, "${srcaddr} ${dstaddr}", aws.ToString(desc.FlowLogs[0].LogFormat),
		"LogFormat dropped - CreateFlowLogs never read it")
	assert.Equal(t, int32(60), aws.ToInt32(desc.FlowLogs[0].MaxAggregationInterval),
		"MaxAggregationInterval dropped - CreateFlowLogs never read it")
}

// TestCreateFlowLogs_MaxAggregationInterval_OmittedDefaultsTo600 pins the
// documented default (600 seconds when omitted).
func TestCreateFlowLogs_MaxAggregationInterval_OmittedDefaultsTo600(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestEC2Client(t, h)

	created, err := client.CreateFlowLogs(t.Context(), &ec2sdk.CreateFlowLogsInput{
		ResourceIds:        []string{"vpc-default"},
		ResourceType:       "VPC",
		TrafficType:        "ALL",
		LogDestinationType: "s3",
		LogDestination:     aws.String("arn:aws:s3:::dest"),
	})
	require.NoError(t, err)
	require.Len(t, created.FlowLogIds, 1)

	desc, err := client.DescribeFlowLogs(t.Context(), &ec2sdk.DescribeFlowLogsInput{
		FlowLogIds: created.FlowLogIds,
	})
	require.NoError(t, err)
	require.Len(t, desc.FlowLogs, 1)
	assert.Equal(t, int32(600), aws.ToInt32(desc.FlowLogs[0].MaxAggregationInterval))
}

func TestGetFlowLogsIntegrationTemplateHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	fls, err := h.Backend.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest", "", 0, nil)
	require.NoError(t, err)
	require.Len(t, fls, 1)

	// Wire key is "IntegrateService" (singular) at the top level -- see
	// GetFlowLogsIntegrationTemplate's own doc comment (flow_logs.go),
	// verified against ec2@v1.329.0 serializers.go:86507-86511.
	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                         {"GetFlowLogsIntegrationTemplate"},
		"FlowLogId":                      {fls[0].FlowLogID},
		"ConfigDeliveryS3DestinationArn": {"arn:aws:s3:::cfn-bucket"},
		"IntegrateService.AthenaIntegration.1.IntegrationResultS3DestinationArn": {"arn:aws:s3:::athena-results"},
		"IntegrateService.AthenaIntegration.1.PartitionLoadFrequency":            {"hourly"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetFlowLogsIntegrationTemplateResponse>")
	assert.Contains(t, resp, fls[0].FlowLogID)
}

// TestGetFlowLogsIntegrationTemplateHTTP_IntegrateServicesRequired covers
// IntegrateServices (api_op_GetFlowLogsIntegrationTemplate.go: "This member
// is required"). Before the fix the handler never read it at all.
func TestGetFlowLogsIntegrationTemplateHTTP_IntegrateServicesRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	fls, err := h.Backend.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest", "", 0, nil)
	require.NoError(t, err)
	require.Len(t, fls, 1)

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                         {"GetFlowLogsIntegrationTemplate"},
		"FlowLogId":                      {fls[0].FlowLogID},
		"ConfigDeliveryS3DestinationArn": {"arn:aws:s3:::cfn-bucket"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterValue")
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

	logs, err := b.CreateFlowLogs([]string{vpc.ID}, "ALL", "cloud-watch-logs", "/aws/vpc/flow", "", 0, nil)
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
