package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestResourcePolicy_RealClientRoundTrip drives PutResourcePolicy through the
// real aws-sdk-go-v2 client and asserts ResourceArn/PolicyScope/RevisionId/
// LastUpdatedTime decode -- all four are real types.ResourcePolicy members
// (deserializers.go:awsAwsjson11_deserializeDocumentResourcePolicy) that a
// previous revision of this backend's ResourcePolicy struct had no Go field
// for at all, so a real client's fields always deserialized zero-valued
// regardless of what PutResourcePolicy/DescribeResourcePolicies returned.
func TestResourcePolicy_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	putOut, err := client.PutResourcePolicy(t.Context(), &cwlsdk.PutResourcePolicyInput{
		PolicyName:     aws.String("route53-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.ResourcePolicy)
	assert.Equal(t, "route53-policy", aws.ToString(putOut.ResourcePolicy.PolicyName))
	assert.Equal(t, cwltypes.PolicyScopeAccount, putOut.ResourcePolicy.PolicyScope)
	require.NotNil(t, putOut.ResourcePolicy.RevisionId)
	assert.Equal(t, "1", aws.ToString(putOut.ResourcePolicy.RevisionId))
	require.NotNil(t, putOut.ResourcePolicy.LastUpdatedTime)
	assert.NotZero(t, aws.ToInt64(putOut.ResourcePolicy.LastUpdatedTime))
	assert.Equal(t, aws.ToString(putOut.ResourcePolicy.RevisionId), aws.ToString(putOut.RevisionId))

	descOut, err := client.DescribeResourcePolicies(t.Context(), &cwlsdk.DescribeResourcePoliciesInput{})
	require.NoError(t, err)
	require.Len(t, descOut.ResourcePolicies, 1)
	assert.Equal(t, cwltypes.PolicyScopeAccount, descOut.ResourcePolicies[0].PolicyScope)
	assert.Equal(t, "1", aws.ToString(descOut.ResourcePolicies[0].RevisionId))
}

// TestResourcePolicy_ResourceScoped drives a resource-scoped (resourceArn
// present) PutResourcePolicy through the real client: DescribeResourcePolicies
// defaults to ACCOUNT scope per its own doc comment ("When not specified,
// defaults to ACCOUNT"), so the resource-scoped policy must NOT appear in an
// unfiltered describe call, only when resourceArn is supplied.
func TestResourcePolicy_ResourceScoped(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	const logGroupArn = "arn:aws:logs:us-east-1:000000000000:log-group:/my/group"

	putOut, err := client.PutResourcePolicy(t.Context(), &cwlsdk.PutResourcePolicyInput{
		PolicyName:     aws.String("cross-account"),
		PolicyDocument: aws.String(`{}`),
		ResourceArn:    aws.String(logGroupArn),
	})
	require.NoError(t, err)
	assert.Equal(t, cwltypes.PolicyScopeResource, putOut.ResourcePolicy.PolicyScope)
	assert.Equal(t, logGroupArn, aws.ToString(putOut.ResourcePolicy.ResourceArn))

	accountScoped, err := client.DescribeResourcePolicies(t.Context(), &cwlsdk.DescribeResourcePoliciesInput{})
	require.NoError(t, err)
	assert.Empty(t, accountScoped.ResourcePolicies)

	resourceScoped, err := client.DescribeResourcePolicies(t.Context(), &cwlsdk.DescribeResourcePoliciesInput{
		ResourceArn: aws.String(logGroupArn),
	})
	require.NoError(t, err)
	require.Len(t, resourceScoped.ResourcePolicies, 1)
	assert.Equal(t, "cross-account", aws.ToString(resourceScoped.ResourcePolicies[0].PolicyName))
}

// TestResourcePolicy_ExpectedRevisionMismatchRejected drives a stale
// ExpectedRevisionId through DeleteResourcePolicy via the real client and
// asserts it is rejected -- the optimistic-concurrency check
// DeleteResourcePolicyInput.ExpectedRevisionId exists for.
func TestResourcePolicy_ExpectedRevisionMismatchRejected(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.PutResourcePolicy(t.Context(), &cwlsdk.PutResourcePolicyInput{
		PolicyName:     aws.String("p"),
		PolicyDocument: aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteResourcePolicy(t.Context(), &cwlsdk.DeleteResourcePolicyInput{
		PolicyName:         aws.String("p"),
		ExpectedRevisionId: aws.String("stale-revision"),
	})
	require.Error(t, err)
}

// TestIndexPolicy_RealClientRoundTrip drives PutIndexPolicy through the real
// client and asserts Source decodes as LOG_GROUP -- a real, always-present
// types.IndexPolicy member (deserializers.go) this backend's IndexPolicy
// struct previously had no Go field for at all.
func TestIndexPolicy_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	putOut, err := client.PutIndexPolicy(t.Context(), &cwlsdk.PutIndexPolicyInput{
		LogGroupIdentifier: aws.String("/aws/lambda/fn"),
		PolicyDocument:     aws.String(`{"Fields":["@message"]}`),
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.IndexPolicy)
	assert.Equal(t, cwltypes.IndexSourceLogGroup, putOut.IndexPolicy.Source)
	require.NotNil(t, putOut.IndexPolicy.LastUpdateTime)
	assert.NotZero(t, aws.ToInt64(putOut.IndexPolicy.LastUpdateTime))

	descOut, err := client.DescribeIndexPolicies(t.Context(), &cwlsdk.DescribeIndexPoliciesInput{
		LogGroupIdentifiers: []string{"/aws/lambda/fn"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.IndexPolicies, 1)
	assert.Equal(t, cwltypes.IndexSourceLogGroup, descOut.IndexPolicies[0].Source)
}

// TestQueryDefinition_RealClientRoundTrip drives PutQueryDefinition through
// the real client with Parameters set -- a real, accepted
// PutQueryDefinitionInput member (api_op_PutQueryDefinition.go) this
// backend previously dropped entirely -- and asserts DescribeQueryDefinitions
// echoes both Parameters and the QueryLanguage default.
func TestQueryDefinition_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.PutQueryDefinition(t.Context(), &cwlsdk.PutQueryDefinitionInput{
		Name:        aws.String("parameterized-query"),
		QueryString: aws.String("fields @message | filter @message like /{{errorCode}}/"),
		Parameters: []cwltypes.QueryParameter{
			{Name: aws.String("errorCode"), DefaultValue: aws.String("500"), Description: aws.String("HTTP status")},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeQueryDefinitions(t.Context(), &cwlsdk.DescribeQueryDefinitionsInput{
		QueryDefinitionNamePrefix: aws.String("parameterized-query"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.QueryDefinitions, 1)

	got := descOut.QueryDefinitions[0]
	assert.Equal(t, cwltypes.QueryLanguageCwli, got.QueryLanguage)
	require.Len(t, got.Parameters, 1)
	assert.Equal(t, "errorCode", aws.ToString(got.Parameters[0].Name))
	assert.Equal(t, "500", aws.ToString(got.Parameters[0].DefaultValue))
	assert.Equal(t, "HTTP status", aws.ToString(got.Parameters[0].Description))
}

// TestDataProtectionPolicy_LastUpdatedTime drives GetDataProtectionPolicy
// through the real client and asserts LastUpdatedTime decodes -- a real
// GetDataProtectionPolicyOutput member this backend previously never
// included in its response at all.
func TestDataProtectionPolicy_LastUpdatedTime(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.PutDataProtectionPolicy(t.Context(), &cwlsdk.PutDataProtectionPolicyInput{
		LogGroupIdentifier: aws.String("/my/group"),
		PolicyDocument:     aws.String(`{"Name":"protect"}`),
	})
	require.NoError(t, err)

	getOut, err := client.GetDataProtectionPolicy(t.Context(), &cwlsdk.GetDataProtectionPolicyInput{
		LogGroupIdentifier: aws.String("/my/group"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.LastUpdatedTime)
	assert.NotZero(t, aws.ToInt64(getOut.LastUpdatedTime))
}

// TestS3TableIntegration_DisassociateActuallyRemoves drives Associate then
// Disassociate through the real client. The pre-fix handler never read the
// request body at all, so the association was never actually removed and
// the response never carried the required Identifier member.
func TestS3TableIntegration_DisassociateActuallyRemoves(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	assocOut, err := client.AssociateSourceToS3TableIntegration(
		t.Context(), &cwlsdk.AssociateSourceToS3TableIntegrationInput{
			IntegrationArn: aws.String("arn:aws:logs::123456789012:integration:my-integration"),
			DataSource:     &cwltypes.DataSource{Name: aws.String("my-source"), Type: aws.String("S3")},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, assocOut.Identifier)

	disOut, err := client.DisassociateSourceFromS3TableIntegration(
		t.Context(), &cwlsdk.DisassociateSourceFromS3TableIntegrationInput{
			Identifier: assocOut.Identifier,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(assocOut.Identifier), aws.ToString(disOut.Identifier))

	_, err = client.DisassociateSourceFromS3TableIntegration(
		t.Context(), &cwlsdk.DisassociateSourceFromS3TableIntegrationInput{
			Identifier: assocOut.Identifier,
		},
	)
	require.Error(t, err, "the association should actually have been removed")
}
