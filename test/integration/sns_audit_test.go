package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SNS_ListOriginationNumbersEmpty verifies that ListOriginationNumbers returns
// an empty list for a fresh account. AWS SNS exposes no public "create origination number" API
// (numbers are provisioned via Pinpoint / AWS End User Messaging), so an empty list is the
// AWS-accurate default. There is no SDK call to create one, which is why population is exercised
// by the unit tests via the internal SeedOriginationNumber helper.
func TestIntegration_SNS_ListOriginationNumbersEmpty(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	ctx := t.Context()

	out, err := snsClient.ListOriginationNumbers(ctx, &sns.ListOriginationNumbersInput{})
	require.NoError(t, err)
	assert.Empty(t, out.PhoneNumbers)
	assert.Nil(t, out.NextToken)
}
