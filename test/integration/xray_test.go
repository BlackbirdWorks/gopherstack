package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_XRay_GroupAndSamplingRuleLifecycle exercises the AWS X-Ray
// core control-plane workflow via the AWS SDK v2: create a group, get/list
// groups, create a sampling rule, list sampling rules, then get/put the
// encryption configuration. Primary integration coverage for the X-Ray REST
// handler (path-based dispatch).
func TestIntegration_XRay_GroupAndSamplingRuleLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createXRayClient(t)
	ctx := t.Context()

	const (
		groupName        = "it-xray-group"
		filterExpression = "service(\"api\") {fault}"
		ruleName         = "it-xray-rule"
		serviceName      = "api"
	)

	// CreateGroup.
	createGroupOut, err := client.CreateGroup(ctx, &xraysdk.CreateGroupInput{
		GroupName:        aws.String(groupName),
		FilterExpression: aws.String(filterExpression),
	})
	require.NoError(t, err)
	require.NotNil(t, createGroupOut.Group)
	assert.Equal(t, groupName, aws.ToString(createGroupOut.Group.GroupName))
	assert.Equal(t, filterExpression, aws.ToString(createGroupOut.Group.FilterExpression))

	t.Cleanup(func() {
		_, _ = client.DeleteGroup(ctx, &xraysdk.DeleteGroupInput{GroupName: aws.String(groupName)})
	})

	// GetGroup.
	getGroupOut, err := client.GetGroup(ctx, &xraysdk.GetGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)
	require.NotNil(t, getGroupOut.Group)
	assert.Equal(t, groupName, aws.ToString(getGroupOut.Group.GroupName))

	// GetGroups should include the new group.
	getGroupsOut, err := client.GetGroups(ctx, &xraysdk.GetGroupsInput{})
	require.NoError(t, err)

	foundGroup := false

	for _, g := range getGroupsOut.Groups {
		if aws.ToString(g.GroupName) == groupName {
			foundGroup = true

			break
		}
	}

	assert.True(t, foundGroup, "newly created group should be listed")

	// CreateSamplingRule.
	createRuleOut, err := client.CreateSamplingRule(ctx, &xraysdk.CreateSamplingRuleInput{
		SamplingRule: &xraytypes.SamplingRule{
			RuleName:      aws.String(ruleName),
			ResourceARN:   aws.String("*"),
			Priority:      aws.Int32(1000),
			FixedRate:     0.05,
			ReservoirSize: 1,
			ServiceName:   aws.String(serviceName),
			ServiceType:   aws.String("*"),
			Host:          aws.String("*"),
			HTTPMethod:    aws.String("*"),
			URLPath:       aws.String("*"),
			Version:       aws.Int32(1),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createRuleOut.SamplingRuleRecord)
	require.NotNil(t, createRuleOut.SamplingRuleRecord.SamplingRule)
	assert.Equal(t, ruleName, aws.ToString(createRuleOut.SamplingRuleRecord.SamplingRule.RuleName))

	t.Cleanup(func() {
		_, _ = client.DeleteSamplingRule(ctx, &xraysdk.DeleteSamplingRuleInput{RuleName: aws.String(ruleName)})
	})

	// GetSamplingRules should include the new rule.
	getRulesOut, err := client.GetSamplingRules(ctx, &xraysdk.GetSamplingRulesInput{})
	require.NoError(t, err)

	foundRule := false

	for _, r := range getRulesOut.SamplingRuleRecords {
		if r.SamplingRule != nil && aws.ToString(r.SamplingRule.RuleName) == ruleName {
			foundRule = true

			break
		}
	}

	assert.True(t, foundRule, "newly created sampling rule should be listed")

	// GetEncryptionConfig - read-only sanity check.
	getEncOut, err := client.GetEncryptionConfig(ctx, &xraysdk.GetEncryptionConfigInput{})
	require.NoError(t, err)
	require.NotNil(t, getEncOut.EncryptionConfig)

	// PutEncryptionConfig - exercises the POST /PutEncryptionConfig wire path,
	// which is distinct from GetEncryptionConfig's POST /EncryptionConfig path.
	putEncOut, err := client.PutEncryptionConfig(ctx, &xraysdk.PutEncryptionConfigInput{
		Type: xraytypes.EncryptionType("NONE"),
	})
	require.NoError(t, err)
	require.NotNil(t, putEncOut.EncryptionConfig)
}
