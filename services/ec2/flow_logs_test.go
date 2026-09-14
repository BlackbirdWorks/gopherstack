package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestGetFlowLogsIntegrationTemplate covers ConfigDeliveryS3DestinationArn
// plus IntegrateServices.AthenaIntegrations[].IntegrationResultS3DestinationArn/
// PartitionLoadFrequency (api_op_GetFlowLogsIntegrationTemplate.go /
// types.AthenaIntegration: all "This member is required"). Before the fix
// IntegrateServices was never read at all, and the Athena WorkGroup's
// OutputLocation was wired from ConfigDeliveryS3DestinationArn -- the wrong
// field per the real serializer, which sources it from
// IntegrationResultS3DestinationArn instead.
func TestGetFlowLogsIntegrationTemplate(t *testing.T) {
	t.Parallel()

	t.Run("valid request renders the athena result location, not the config delivery arn", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket", nil)
		require.NoError(t, err)
		require.Len(t, fls, 1)

		tmpl, err := b.GetFlowLogsIntegrationTemplate(
			fls[0].FlowLogID, "arn:aws:s3:::my-cfn-bucket", "arn:aws:s3:::athena-results", "hourly",
		)
		require.NoError(t, err)
		assert.Contains(t, tmpl, fls[0].FlowLogID)
		assert.Contains(t, tmpl, "arn:aws:s3:::athena-results")
		assert.NotContains(t, tmpl, "arn:aws:s3:::my-cfn-bucket")
		assert.Contains(t, tmpl, "AWSTemplateFormatVersion")
	})

	t.Run("unknown flow log id", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		_, err := b.GetFlowLogsIntegrationTemplate("fl-missing", "arn:aws:s3:::x", "arn:aws:s3:::y", "hourly")
		require.ErrorIs(t, err, ec2.ErrFlowLogNotFound)
	})

	t.Run("missing flow log id", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		_, err := b.GetFlowLogsIntegrationTemplate("", "arn:aws:s3:::x", "arn:aws:s3:::y", "hourly")
		require.ErrorIs(t, err, ec2.ErrInvalidParameter)
	})

	t.Run("missing config delivery s3 destination arn", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket", nil)
		require.NoError(t, err)

		_, err = b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "", "arn:aws:s3:::y", "hourly")
		require.ErrorIs(t, err, ec2.ErrInvalidParameter)
	})

	t.Run("missing athena integration result s3 destination arn", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket", nil)
		require.NoError(t, err)

		_, err = b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "arn:aws:s3:::x", "", "hourly")
		require.ErrorIs(t, err, ec2.ErrInvalidParameter)
	})

	t.Run("missing partition load frequency", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket", nil)
		require.NoError(t, err)

		_, err = b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "arn:aws:s3:::x", "arn:aws:s3:::y", "")
		require.ErrorIs(t, err, ec2.ErrInvalidParameter)
	})
}

// ---- Misc singletons ----
