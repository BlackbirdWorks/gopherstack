package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFlowLogsIntegrationTemplate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket")
	require.NoError(t, err)
	require.Len(t, fls, 1)

	tmpl, err := b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "arn:aws:s3:::my-cfn-bucket")
	require.NoError(t, err)
	assert.Contains(t, tmpl, fls[0].FlowLogID)
	assert.Contains(t, tmpl, "arn:aws:s3:::my-cfn-bucket")
	assert.Contains(t, tmpl, "AWSTemplateFormatVersion")

	_, err = b.GetFlowLogsIntegrationTemplate("fl-missing", "arn:aws:s3:::x")
	require.ErrorIs(t, err, ec2.ErrFlowLogNotFound)

	_, err = b.GetFlowLogsIntegrationTemplate("", "arn:aws:s3:::x")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Misc singletons ----
