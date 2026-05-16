package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	shieldsdk "github.com/aws/aws-sdk-go-v2/service/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Shield_SubscriptionAndProtectionLifecycle exercises the
// core AWS Shield workflow via the AWS SDK v2: create a subscription,
// describe subscription, create a protection on a resource, describe/list it,
// then delete protection and subscription. Primary integration coverage for
// the Shield AWSShield_20160616. JSON-RPC handler.
func TestIntegration_Shield_SubscriptionAndProtectionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createShieldClient(t)
	ctx := t.Context()

	const (
		protectionName = "it-shield-protection"
		resourceArn    = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/it-shield/abc"
	)

	// CreateSubscription.
	_, err := client.CreateSubscription(ctx, &shieldsdk.CreateSubscriptionInput{})
	require.NoError(t, err)

	// DescribeSubscription.
	descSubOut, err := client.DescribeSubscription(ctx, &shieldsdk.DescribeSubscriptionInput{})
	require.NoError(t, err)
	require.NotNil(t, descSubOut.Subscription)

	// CreateProtection.
	createOut, err := client.CreateProtection(ctx, &shieldsdk.CreateProtectionInput{
		Name:        aws.String(protectionName),
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	protectionID := aws.ToString(createOut.ProtectionId)
	require.NotEmpty(t, protectionID)

	t.Cleanup(func() {
		_, _ = client.DeleteProtection(ctx, &shieldsdk.DeleteProtectionInput{
			ProtectionId: aws.String(protectionID),
		})
	})

	// DescribeProtection by ID.
	descProtOut, err := client.DescribeProtection(ctx, &shieldsdk.DescribeProtectionInput{
		ProtectionId: aws.String(protectionID),
	})
	require.NoError(t, err)
	require.NotNil(t, descProtOut.Protection)
	assert.Equal(t, protectionName, aws.ToString(descProtOut.Protection.Name))
	assert.Equal(t, resourceArn, aws.ToString(descProtOut.Protection.ResourceArn))

	// ListProtections should contain the new protection.
	listOut, err := client.ListProtections(ctx, &shieldsdk.ListProtectionsInput{})
	require.NoError(t, err)

	foundProtection := false

	for _, p := range listOut.Protections {
		if aws.ToString(p.Id) == protectionID {
			foundProtection = true

			break
		}
	}

	assert.True(t, foundProtection, "newly created protection should be listed")
}
