package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Support_CaseLifecycle exercises the core AWS Support
// workflow via the AWS SDK v2: create a case, describe it, then resolve.
// Primary integration coverage for the Support JSON-RPC handler.
func TestIntegration_Support_CaseLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSupportClient(t)
	ctx := t.Context()

	// CreateCase.
	createOut, err := client.CreateCase(ctx, &supportsdk.CreateCaseInput{
		Subject:           aws.String("integration test case"),
		ServiceCode:       aws.String("general-info"),
		CategoryCode:      aws.String("using-aws"),
		SeverityCode:      aws.String("low"),
		CommunicationBody: aws.String("integration test communication body"),
	})
	require.NoError(t, err)
	caseID := aws.ToString(createOut.CaseId)
	require.NotEmpty(t, caseID)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.ResolveCase(cleanupCtx, &supportsdk.ResolveCaseInput{
			CaseId: aws.String(caseID),
		})
	})

	// DescribeCases.
	descOut, err := client.DescribeCases(ctx, &supportsdk.DescribeCasesInput{
		CaseIdList: []string{caseID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Cases, 1)
	assert.Equal(t, caseID, aws.ToString(descOut.Cases[0].CaseId))
	assert.Equal(t, "integration test case", aws.ToString(descOut.Cases[0].Subject))

	// AddCommunicationToCase.
	_, err = client.AddCommunicationToCase(ctx, &supportsdk.AddCommunicationToCaseInput{
		CaseId:            aws.String(caseID),
		CommunicationBody: aws.String("follow-up communication"),
	})
	require.NoError(t, err)

	// ResolveCase.
	_, err = client.ResolveCase(ctx, &supportsdk.ResolveCaseInput{
		CaseId: aws.String(caseID),
	})
	require.NoError(t, err)
}
