package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerless_RealSDKClient_LifecyclePolicy_Lifecycle drives
// CreateLifecyclePolicy -> ListLifecyclePolicies -> BatchGetLifecyclePolicy
// -> UpdateLifecyclePolicy (stale version -> ConflictException, then the
// real version -> success) -> DeleteLifecyclePolicy -> BatchGetLifecyclePolicy
// (not found) through the real opensearchserverless client.
func TestServerless_RealSDKClient_LifecyclePolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.CreateLifecyclePolicyInput{
			Name: aws.String("retain-30d"),
			Type: aosstypes.LifecyclePolicyTypeRetention,
			Policy: aws.String(
				`{"Rules":[{"ResourceType":"index","Resource":["index/logs/*"],"MinIndexRetention":"30d"}]}`,
			),
			Description: aws.String("30 day retention"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.LifecyclePolicyDetail)
	assert.Equal(t, "retain-30d", aws.ToString(created.LifecyclePolicyDetail.Name))
	version := aws.ToString(created.LifecyclePolicyDetail.PolicyVersion)
	require.NotEmpty(t, version)

	listed, err := client.ListLifecyclePolicies(
		t.Context(),
		&opensearchserverless.ListLifecyclePoliciesInput{
			Type: aosstypes.LifecyclePolicyTypeRetention,
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.LifecyclePolicySummaries, 1)
	assert.Equal(t, "retain-30d", aws.ToString(listed.LifecyclePolicySummaries[0].Name))

	got, err := client.BatchGetLifecyclePolicy(
		t.Context(),
		&opensearchserverless.BatchGetLifecyclePolicyInput{
			Identifiers: []aosstypes.LifecyclePolicyIdentifier{
				{Name: aws.String("retain-30d"), Type: aosstypes.LifecyclePolicyTypeRetention},
				{Name: aws.String("ghost"), Type: aosstypes.LifecyclePolicyTypeRetention},
			},
		},
	)
	require.NoError(t, err)
	assert.Len(t, got.LifecyclePolicyDetails, 1)
	assert.Len(t, got.LifecyclePolicyErrorDetails, 1)
	assert.Equal(t, "ghost", aws.ToString(got.LifecyclePolicyErrorDetails[0].Name))

	_, err = client.UpdateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.UpdateLifecyclePolicyInput{
			Name:          aws.String("retain-30d"),
			Type:          aosstypes.LifecyclePolicyTypeRetention,
			PolicyVersion: aws.String("stale-version"),
			Description:   aws.String("should not apply"),
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())

	updated, err := client.UpdateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.UpdateLifecyclePolicyInput{
			Name:          aws.String("retain-30d"),
			Type:          aosstypes.LifecyclePolicyTypeRetention,
			PolicyVersion: aws.String(version),
			Description:   aws.String("60 day retention now"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "60 day retention now", aws.ToString(updated.LifecyclePolicyDetail.Description))
	assert.NotEqual(t, version, aws.ToString(updated.LifecyclePolicyDetail.PolicyVersion))

	_, err = client.DeleteLifecyclePolicy(
		t.Context(),
		&opensearchserverless.DeleteLifecyclePolicyInput{
			Name: aws.String("retain-30d"),
			Type: aosstypes.LifecyclePolicyTypeRetention,
		},
	)
	require.NoError(t, err)

	afterDelete, err := client.BatchGetLifecyclePolicy(
		t.Context(),
		&opensearchserverless.BatchGetLifecyclePolicyInput{
			Identifiers: []aosstypes.LifecyclePolicyIdentifier{
				{Name: aws.String("retain-30d"), Type: aosstypes.LifecyclePolicyTypeRetention},
			},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, afterDelete.LifecyclePolicyDetails)
	assert.Len(t, afterDelete.LifecyclePolicyErrorDetails, 1)
}

// TestServerless_RealSDKClient_EffectiveLifecyclePolicy_MostSpecificWins
// creates two retention policies with overlapping resource patterns and
// confirms BatchGetEffectiveLifecyclePolicy resolves the most specific one.
func TestServerless_RealSDKClient_EffectiveLifecyclePolicy_MostSpecificWins(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	_, err := client.CreateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.CreateLifecyclePolicyInput{
			Name: aws.String("broad"),
			Type: aosstypes.LifecyclePolicyTypeRetention,
			Policy: aws.String(
				`{"Rules":[{"ResourceType":"index","Resource":["index/logs/*"],"MinIndexRetention":"7d"}]}`,
			),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.CreateLifecyclePolicyInput{
			Name: aws.String("specific"),
			Type: aosstypes.LifecyclePolicyTypeRetention,
			Policy: aws.String(
				`{"Rules":[{"ResourceType":"index","Resource":["index/logs/errors-*"],"MinIndexRetention":"90d"}]}`,
			),
		},
	)
	require.NoError(t, err)

	effective, err := client.BatchGetEffectiveLifecyclePolicy(
		t.Context(), &opensearchserverless.BatchGetEffectiveLifecyclePolicyInput{
			ResourceIdentifiers: []aosstypes.LifecyclePolicyResourceIdentifier{
				{
					Resource: aws.String("index/logs/errors-2024"),
					Type:     aosstypes.LifecyclePolicyTypeRetention,
				},
				{
					Resource: aws.String("index/logs/access-2024"),
					Type:     aosstypes.LifecyclePolicyTypeRetention,
				},
				{
					Resource: aws.String("index/other/thing"),
					Type:     aosstypes.LifecyclePolicyTypeRetention,
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, effective.EffectiveLifecyclePolicyDetails, 2)
	require.Len(t, effective.EffectiveLifecyclePolicyErrorDetails, 1)

	byResource := make(
		map[string]aosstypes.EffectiveLifecyclePolicyDetail,
		len(effective.EffectiveLifecyclePolicyDetails),
	)
	for _, d := range effective.EffectiveLifecyclePolicyDetails {
		byResource[aws.ToString(d.Resource)] = d
	}

	errors := aws.ToString(effective.EffectiveLifecyclePolicyErrorDetails[0].Resource)
	assert.Equal(t, "index/other/thing", errors)

	specific := byResource["index/logs/errors-2024"]
	assert.Equal(t, "specific", aws.ToString(specific.PolicyName))
	assert.Equal(t, "90d", aws.ToString(specific.RetentionPeriod))

	broad := byResource["index/logs/access-2024"]
	assert.Equal(t, "broad", aws.ToString(broad.PolicyName))
	assert.Equal(t, "7d", aws.ToString(broad.RetentionPeriod))
}

// TestServerless_RealSDKClient_CollectionGroup_Lifecycle drives
// CreateCollectionGroup -> ListCollectionGroups -> BatchGetCollectionGroup
// -> UpdateCollectionGroup -> DeleteCollectionGroup -> BatchGetCollectionGroup
// (not found) through the real opensearchserverless client.
func TestServerless_RealSDKClient_CollectionGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateCollectionGroup(
		t.Context(),
		&opensearchserverless.CreateCollectionGroupInput{
			Name:            aws.String("group-1"),
			StandbyReplicas: aosstypes.StandbyReplicasDisabled,
			Description:     aws.String("test group"),
			CapacityLimits: &aosstypes.CollectionGroupCapacityLimits{
				MaxIndexingCapacityInOCU: aws.Float32(16),
				MaxSearchCapacityInOCU:   aws.Float32(16),
			},
			Tags: []aosstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.CreateCollectionGroupDetail)
	id := aws.ToString(created.CreateCollectionGroupDetail.Id)
	require.NotEmpty(t, id)
	assert.Equal(t, "CLASSIC", string(created.CreateCollectionGroupDetail.Generation))

	listed, err := client.ListCollectionGroups(
		t.Context(),
		&opensearchserverless.ListCollectionGroupsInput{},
	)
	require.NoError(t, err)
	require.Len(t, listed.CollectionGroupSummaries, 1)
	assert.Equal(t, "group-1", aws.ToString(listed.CollectionGroupSummaries[0].Name))

	got, err := client.BatchGetCollectionGroup(
		t.Context(),
		&opensearchserverless.BatchGetCollectionGroupInput{
			Ids: []string{id, "ghost"},
		},
	)
	require.NoError(t, err)
	require.Len(t, got.CollectionGroupDetails, 1)
	require.Len(t, got.CollectionGroupErrorDetails, 1)
	assert.Equal(t, "ghost", aws.ToString(got.CollectionGroupErrorDetails[0].Id))

	updated, err := client.UpdateCollectionGroup(
		t.Context(),
		&opensearchserverless.UpdateCollectionGroupInput{
			Id:          aws.String(id),
			Description: aws.String("updated description"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"updated description",
		aws.ToString(updated.UpdateCollectionGroupDetail.Description),
	)

	_, err = client.DeleteCollectionGroup(
		t.Context(),
		&opensearchserverless.DeleteCollectionGroupInput{
			Id: aws.String(id),
		},
	)
	require.NoError(t, err)

	afterDelete, err := client.BatchGetCollectionGroup(
		t.Context(),
		&opensearchserverless.BatchGetCollectionGroupInput{
			Ids: []string{id},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, afterDelete.CollectionGroupDetails)
	assert.Len(t, afterDelete.CollectionGroupErrorDetails, 1)
}

// TestServerless_RealSDKClient_AccountSettings confirms GetAccountSettings
// reports the documented default (10/10 OCU) before any update, that
// UpdateAccountSettings changes only the field given, and that an
// out-of-range value is rejected as ValidationException.
func TestServerless_RealSDKClient_AccountSettings(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	got, err := client.GetAccountSettings(
		t.Context(),
		&opensearchserverless.GetAccountSettingsInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, got.AccountSettingsDetail.CapacityLimits)
	assert.EqualValues(
		t,
		10,
		aws.ToInt32(got.AccountSettingsDetail.CapacityLimits.MaxIndexingCapacityInOCU),
	)
	assert.EqualValues(
		t,
		10,
		aws.ToInt32(got.AccountSettingsDetail.CapacityLimits.MaxSearchCapacityInOCU),
	)

	updated, err := client.UpdateAccountSettings(
		t.Context(),
		&opensearchserverless.UpdateAccountSettingsInput{
			CapacityLimits: &aosstypes.CapacityLimits{MaxIndexingCapacityInOCU: aws.Int32(40)},
		},
	)
	require.NoError(t, err)
	assert.EqualValues(
		t,
		40,
		aws.ToInt32(updated.AccountSettingsDetail.CapacityLimits.MaxIndexingCapacityInOCU),
	)
	assert.EqualValues(
		t,
		10,
		aws.ToInt32(updated.AccountSettingsDetail.CapacityLimits.MaxSearchCapacityInOCU),
	)

	_, err = client.UpdateAccountSettings(
		t.Context(),
		&opensearchserverless.UpdateAccountSettingsInput{
			CapacityLimits: &aosstypes.CapacityLimits{MaxIndexingCapacityInOCU: aws.Int32(1)},
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
}

// TestServerless_RealSDKClient_GetPoliciesStats confirms each policy family
// is counted independently, and TotalPolicyCount sums them.
func TestServerless_RealSDKClient_GetPoliciesStats(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	_, err := client.CreateLifecyclePolicy(
		t.Context(),
		&opensearchserverless.CreateLifecyclePolicyInput{
			Name: aws.String("lp-1"), Type: aosstypes.LifecyclePolicyTypeRetention,
			Policy: aws.String(`{"Rules":[]}`),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateAccessPolicy(t.Context(), &opensearchserverless.CreateAccessPolicyInput{
		Name: aws.String("ap-1"), Type: aosstypes.AccessPolicyTypeData,
		Policy: aws.String(`[]`),
	})
	require.NoError(t, err)

	stats, err := client.GetPoliciesStats(
		t.Context(),
		&opensearchserverless.GetPoliciesStatsInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, stats.LifecyclePolicyStats)
	require.NotNil(t, stats.AccessPolicyStats)
	assert.EqualValues(t, 1, aws.ToInt64(stats.LifecyclePolicyStats.RetentionPolicyCount))
	assert.EqualValues(t, 1, aws.ToInt64(stats.AccessPolicyStats.DataPolicyCount))
	assert.EqualValues(t, 2, aws.ToInt64(stats.TotalPolicyCount))
}

// TestServerless_RealSDKClient_BatchGetVpcEndpoint resolves BatchGetVpcEndpoint
// against a VPC endpoint created through the classic-domain backend method
// (there is no AOSS-surface Create op for this yet), and confirms an unknown
// ID reports as a VpcEndpointErrorDetail rather than failing the whole call.
func TestServerless_RealSDKClient_BatchGetVpcEndpoint(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	ep, err := h.Backend.CreateVpcEndpoint(
		"arn:aws:es:us-east-1:000000000000:domain/dom",
		map[string]any{
			"SecurityGroupIds": []string{"sg-1"},
			"SubnetIds":        []string{"subnet-1"},
		},
	)
	require.NoError(t, err)

	got, err := client.BatchGetVpcEndpoint(
		t.Context(),
		&opensearchserverless.BatchGetVpcEndpointInput{
			Ids: []string{ep.VpcEndpointID, "vpce-ghost"},
		},
	)
	require.NoError(t, err)
	require.Len(t, got.VpcEndpointDetails, 1)
	require.Len(t, got.VpcEndpointErrorDetails, 1)

	detail := got.VpcEndpointDetails[0]
	assert.Equal(t, ep.VpcEndpointID, aws.ToString(detail.Id))
	assert.Equal(t, aosstypes.VpcEndpointStatusActive, detail.Status)
	assert.Equal(t, []string{"sg-1"}, detail.SecurityGroupIds)
	assert.Equal(t, []string{"subnet-1"}, detail.SubnetIds)
	assert.Equal(t, "vpce-ghost", aws.ToString(got.VpcEndpointErrorDetails[0].Id))
}
