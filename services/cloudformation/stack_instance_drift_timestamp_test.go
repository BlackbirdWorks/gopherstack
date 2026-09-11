package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStackInstance_LastDriftCheckTimestamp_RealClient drives
// DetectStackSetDrift through the real client (gopherstack-eamp).
// types.StackInstanceSummary/types.StackInstance both carry
// LastDriftCheckTimestamp ("NULL for any stack instance that drift
// detection hasn't yet been performed on", cloudformation@v1.76.1
// types/types.go:2058-2061 and :1858-1861). detectStackInstanceDrift
// (stack_sets.go) now sets it on every instance it actually compares.
func TestStackInstance_LastDriftCheckTimestamp_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	stackSetName := "eamp-drift-timestamp-stackset"
	templateBody := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String(stackSetName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(stackSetName),
		Accounts:     []string{"123456789012"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)

	preList, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)
	require.Len(t, preList.Summaries, 1)
	assert.Nil(t, preList.Summaries[0].LastDriftCheckTimestamp,
		"LastDriftCheckTimestamp must be nil before any drift check")

	_, err = client.DetectStackSetDrift(t.Context(), &cfnsdk.DetectStackSetDriftInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)

	postList, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)
	require.Len(t, postList.Summaries, 1)
	require.NotNil(t, postList.Summaries[0].LastDriftCheckTimestamp,
		"ListStackInstances: LastDriftCheckTimestamp still nil after DetectStackSetDrift")

	descOut, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String(stackSetName),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.StackInstance)
	require.NotNil(t, descOut.StackInstance.LastDriftCheckTimestamp,
		"DescribeStackInstance: LastDriftCheckTimestamp still nil after DetectStackSetDrift")
}
