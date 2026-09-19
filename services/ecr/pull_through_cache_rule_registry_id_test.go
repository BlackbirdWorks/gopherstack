package ecr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPullThroughCacheRuleRegistryID proves CreatePullThroughCacheRuleInput/
// DescribePullThroughCacheRulesInput's RegistryId (dropped from the wire
// entirely before this fix) is read and applied: an explicit RegistryId
// becomes the rule's own RegistryId (matching real AWS's documented default-
// registry behavior), and DescribePullThroughCacheRules' RegistryId narrows
// results to rules created under that registry.
func TestPullThroughCacheRuleRegistryID(t *testing.T) {
	t.Parallel()

	client := newTestECRClient(t, newTestHandler(t))
	ctx := t.Context()

	const otherRegistryID = "111111111111"

	createOut, err := client.CreatePullThroughCacheRule(ctx, &ecrsdk.CreatePullThroughCacheRuleInput{
		EcrRepositoryPrefix: aws.String("s18-ptc-explicit-registry"),
		UpstreamRegistryUrl: aws.String("public.ecr.aws"),
		RegistryId:          aws.String(otherRegistryID),
	})
	require.NoError(t, err)
	assert.Equal(t, otherRegistryID, aws.ToString(createOut.RegistryId),
		"an explicit RegistryId must become the rule's own registry, not the backend's default account")

	defaultOut, err := client.CreatePullThroughCacheRule(ctx, &ecrsdk.CreatePullThroughCacheRuleInput{
		EcrRepositoryPrefix: aws.String("s18-ptc-default-registry"),
		UpstreamRegistryUrl: aws.String("public.ecr.aws"),
	})
	require.NoError(t, err)
	assert.Equal(t, testAccountID, aws.ToString(defaultOut.RegistryId),
		"an omitted RegistryId must default to the backend's own account")

	scopedOut, err := client.DescribePullThroughCacheRules(ctx, &ecrsdk.DescribePullThroughCacheRulesInput{
		RegistryId: aws.String(otherRegistryID),
	})
	require.NoError(t, err)
	require.Len(t, scopedOut.PullThroughCacheRules, 1,
		"RegistryId must narrow the list to rules created under that registry")
	assert.Equal(t, "s18-ptc-explicit-registry",
		aws.ToString(scopedOut.PullThroughCacheRules[0].EcrRepositoryPrefix))

	_, err = client.DescribePullThroughCacheRules(ctx, &ecrsdk.DescribePullThroughCacheRulesInput{
		EcrRepositoryPrefixes: []string{"s18-ptc-default-registry"},
		RegistryId:            aws.String(otherRegistryID),
	})
	require.Error(t, err, "an explicit prefix under the wrong RegistryId must not resolve")
}
