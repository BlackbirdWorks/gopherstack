package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobTemplate_RequiredZeroValues_RealClient drives CreateJobTemplate
// then DescribeJobTemplate through the real SDK client with legitimate
// zero-ish values for members the pinned SDK's client-side validators only
// nil-check, not zero-check: AbortConfig.CriteriaList as an empty (non-nil)
// list, and AbortCriteria/MaintenanceWindow members set to their zero value
// (iot@v1.83.0 validators.go:5106-5136, 5584-5591, 6401-6411). The pre-fix
// domain structs tagged these `omitempty`, so a legitimate zero value was
// silently dropped from the response instead of round-tripping --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestJobTemplate_RequiredZeroValues_RealClient(t *testing.T) {
	t.Parallel()

	client, _ := newIoTSDKClient(t)
	ctx := t.Context()

	_, err := client.CreateJobTemplate(ctx, &iotsdk.CreateJobTemplateInput{
		JobTemplateId: aws.String("zero-value-template"),
		Description:   aws.String("zero value round trip"),
		Document:      aws.String(`{"foo":"bar"}`),
		AbortConfig: &iottypes.AbortConfig{
			CriteriaList: []iottypes.AbortCriteria{},
		},
		JobExecutionsRetryConfig: &iottypes.JobExecutionsRetryConfig{
			CriteriaList: []iottypes.RetryCriteria{
				{FailureType: iottypes.RetryableFailureTypeFailed, NumberOfRetries: aws.Int32(0)},
			},
		},
		MaintenanceWindows: []iottypes.MaintenanceWindow{
			{StartTime: aws.String("cron(0 9 * * ? *)"), DurationInMinutes: aws.Int32(0)},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeJobTemplate(ctx, &iotsdk.DescribeJobTemplateInput{
		JobTemplateId: aws.String("zero-value-template"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.AbortConfig, "AbortConfig itself must survive")
	assert.NotNil(t, out.AbortConfig.CriteriaList,
		"an empty (non-nil) criteriaList must round-trip as [], not vanish")
	assert.Empty(t, out.AbortConfig.CriteriaList)

	require.NotNil(t, out.JobExecutionsRetryConfig)
	require.Len(t, out.JobExecutionsRetryConfig.CriteriaList, 1)
	require.NotNil(t, out.JobExecutionsRetryConfig.CriteriaList[0].NumberOfRetries,
		"NumberOfRetries=0 is a legitimate value, not an absent one")
	assert.EqualValues(t, 0, *out.JobExecutionsRetryConfig.CriteriaList[0].NumberOfRetries)

	require.Len(t, out.MaintenanceWindows, 1)
	require.NotNil(t, out.MaintenanceWindows[0].DurationInMinutes,
		"DurationInMinutes=0 is a legitimate value, not an absent one")
	assert.EqualValues(t, 0, *out.MaintenanceWindows[0].DurationInMinutes)
	assert.Equal(t, "cron(0 9 * * ? *)", aws.ToString(out.MaintenanceWindows[0].StartTime))
}
