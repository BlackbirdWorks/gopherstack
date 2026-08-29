package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestListClusters_IncludeFilter covers ListClustersInput.Include
// (api_op_ListClusters.go): blank returns only standard EKS clusters; "all"
// also returns clusters registered via RegisterCluster (connected/external
// clusters). Previously ignored -- ListClusters always returned every
// cluster regardless of Include.
func TestListClusters_IncludeFilter(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("standard-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)

	// RegisterCluster's own wire response has an unrelated pre-existing
	// timestamp bug (deserialization failure on a real client), so the
	// connected-cluster fixture is created directly on the backend.
	_, err = backend.RegisterCluster(
		"connected-cluster", "EKS_ANYWHERE", "arn:aws:iam::123456789012:role/connector-role", nil,
	)
	require.NoError(t, err)

	def, err := client.ListClusters(ctx, &ekssdk.ListClustersInput{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"standard-cluster"}, def.Clusters,
		"blank Include must exclude connected/external clusters")

	all, err := client.ListClusters(ctx, &ekssdk.ListClustersInput{Include: []string{"all"}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"standard-cluster", "connected-cluster"}, all.Clusters,
		"Include=[all] must include connected/external clusters")
}

// TestListEksAnywhereSubscriptions_IncludeStatusFilter covers
// ListEksAnywhereSubscriptionsInput.IncludeStatus
// (api_op_ListEksAnywhereSubscriptions.go): filters returned subscriptions to
// the given statuses. Previously ignored -- every subscription was returned
// regardless of IncludeStatus.
func TestListEksAnywhereSubscriptions_IncludeStatusFilter(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	for _, name := range []string{"sub-a", "sub-b"} {
		_, err := client.CreateEksAnywhereSubscription(ctx, &ekssdk.CreateEksAnywhereSubscriptionInput{
			Name: aws.String(name),
			Term: &ekstypes.EksAnywhereSubscriptionTerm{
				Duration: 12,
				Unit:     ekstypes.EksAnywhereSubscriptionTermUnitMonths,
			},
		})
		require.NoError(t, err)
	}

	active, err := client.ListEksAnywhereSubscriptions(ctx, &ekssdk.ListEksAnywhereSubscriptionsInput{
		IncludeStatus: []ekstypes.EksAnywhereSubscriptionStatus{ekstypes.EksAnywhereSubscriptionStatusActive},
	})
	require.NoError(t, err)
	assert.Len(t, active.Subscriptions, 2, "IncludeStatus=[ACTIVE] must return the two ACTIVE subscriptions")

	expired, err := client.ListEksAnywhereSubscriptions(ctx, &ekssdk.ListEksAnywhereSubscriptionsInput{
		IncludeStatus: []ekstypes.EksAnywhereSubscriptionStatus{ekstypes.EksAnywhereSubscriptionStatusExpired},
	})
	require.NoError(t, err)
	assert.Empty(t, expired.Subscriptions, "IncludeStatus=[EXPIRED] must exclude ACTIVE subscriptions")

	unfiltered, err := client.ListEksAnywhereSubscriptions(ctx, &ekssdk.ListEksAnywhereSubscriptionsInput{})
	require.NoError(t, err)
	assert.Len(t, unfiltered.Subscriptions, 2, "omitting IncludeStatus must return every subscription")
}

// TestListPodIdentityAssociations_NamespaceAndServiceAccountFilters covers
// ListPodIdentityAssociationsInput.Namespace/ServiceAccount
// (api_op_ListPodIdentityAssociations.go). Previously ignored -- every
// association for the cluster was returned regardless of these filters.
func TestListPodIdentityAssociations_NamespaceAndServiceAccountFilters(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("pi-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)

	assocs := []struct {
		namespace string
		sa        string
	}{
		{"team-a", "svc-a"},
		{"team-b", "svc-b"},
	}
	for _, a := range assocs {
		_, createErr := client.CreatePodIdentityAssociation(ctx, &ekssdk.CreatePodIdentityAssociationInput{
			ClusterName:    aws.String("pi-cluster"),
			Namespace:      aws.String(a.namespace),
			ServiceAccount: aws.String(a.sa),
			RoleArn:        aws.String("arn:aws:iam::123456789012:role/pod-role"),
		})
		require.NoError(t, createErr)
	}

	byNamespace, err := client.ListPodIdentityAssociations(ctx, &ekssdk.ListPodIdentityAssociationsInput{
		ClusterName: aws.String("pi-cluster"),
		Namespace:   aws.String("team-a"),
	})
	require.NoError(t, err)
	require.Len(t, byNamespace.Associations, 1, "Namespace filter must exclude the other namespace's association")
	assert.Equal(t, "team-a", aws.ToString(byNamespace.Associations[0].Namespace))

	bySA, err := client.ListPodIdentityAssociations(ctx, &ekssdk.ListPodIdentityAssociationsInput{
		ClusterName:    aws.String("pi-cluster"),
		ServiceAccount: aws.String("svc-b"),
	})
	require.NoError(t, err)
	require.Len(t, bySA.Associations, 1, "ServiceAccount filter must exclude the other association")
	assert.Equal(t, "svc-b", aws.ToString(bySA.Associations[0].ServiceAccount))
}

// TestListAccessEntries_AssociatedPolicyArnFilter covers
// ListAccessEntriesInput.AssociatedPolicyArn (api_op_ListAccessEntries.go):
// "When you specify an access policy ARN, only the access entries associated
// to that access policy are returned." Previously ignored -- every access
// entry in the cluster was returned regardless of AssociatedPolicyArn.
func TestListAccessEntries_AssociatedPolicyArnFilter(t *testing.T) {
	t.Parallel()

	const adminPolicy = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy"
	const viewPolicy = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy"

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ae-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)

	principals := []string{
		"arn:aws:iam::123456789012:role/admin-user",
		"arn:aws:iam::123456789012:role/view-user",
	}
	for _, p := range principals {
		_, createErr := client.CreateAccessEntry(ctx, &ekssdk.CreateAccessEntryInput{
			ClusterName:  aws.String("ae-cluster"),
			PrincipalArn: aws.String(p),
		})
		require.NoError(t, createErr)
	}

	_, err = client.AssociateAccessPolicy(ctx, &ekssdk.AssociateAccessPolicyInput{
		ClusterName:  aws.String("ae-cluster"),
		PrincipalArn: aws.String(principals[0]),
		PolicyArn:    aws.String(adminPolicy),
		AccessScope:  &ekstypes.AccessScope{Type: ekstypes.AccessScopeTypeCluster},
	})
	require.NoError(t, err)

	_, err = client.AssociateAccessPolicy(ctx, &ekssdk.AssociateAccessPolicyInput{
		ClusterName:  aws.String("ae-cluster"),
		PrincipalArn: aws.String(principals[1]),
		PolicyArn:    aws.String(viewPolicy),
		AccessScope:  &ekstypes.AccessScope{Type: ekstypes.AccessScopeTypeCluster},
	})
	require.NoError(t, err)

	filtered, err := client.ListAccessEntries(ctx, &ekssdk.ListAccessEntriesInput{
		ClusterName:         aws.String("ae-cluster"),
		AssociatedPolicyArn: aws.String(adminPolicy),
	})
	require.NoError(t, err)
	assert.Equal(t, principals[:1], filtered.AccessEntries,
		"AssociatedPolicyArn must exclude entries not associated to that policy")

	unfiltered, err := client.ListAccessEntries(ctx, &ekssdk.ListAccessEntriesInput{
		ClusterName: aws.String("ae-cluster"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, principals, unfiltered.AccessEntries)
}

// TestListUpdates_NodegroupNameFilter covers ListUpdatesInput.NodegroupName
// (api_op_ListUpdates.go): "The name of the Amazon EKS managed node group to
// list updates for." Previously ignored -- every update in the cluster
// (including cluster-level updates unrelated to any node group) was
// returned regardless of NodegroupName.
func TestListUpdates_NodegroupNameFilter(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("upd-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
		Version:            aws.String("1.31"),
	})
	require.NoError(t, err)

	_, err = client.CreateNodegroup(ctx, &ekssdk.CreateNodegroupInput{
		ClusterName:   aws.String("upd-cluster"),
		NodegroupName: aws.String("ng-1"),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/node-role"),
		Subnets:       []string{"subnet-abc123"},
	})
	require.NoError(t, err)

	_, err = client.UpdateClusterVersion(ctx, &ekssdk.UpdateClusterVersionInput{
		Name:    aws.String("upd-cluster"),
		Version: aws.String("1.32"),
	})
	require.NoError(t, err)

	_, err = client.UpdateNodegroupVersion(ctx, &ekssdk.UpdateNodegroupVersionInput{
		ClusterName:   aws.String("upd-cluster"),
		NodegroupName: aws.String("ng-1"),
		Version:       aws.String("1.32"),
	})
	require.NoError(t, err)

	unfiltered, err := client.ListUpdates(ctx, &ekssdk.ListUpdatesInput{Name: aws.String("upd-cluster")})
	require.NoError(t, err)
	require.Len(t, unfiltered.UpdateIds, 2, "sanity: one cluster-level and one nodegroup-level update")

	byNodegroup, err := client.ListUpdates(ctx, &ekssdk.ListUpdatesInput{
		Name:          aws.String("upd-cluster"),
		NodegroupName: aws.String("ng-1"),
	})
	require.NoError(t, err)
	require.Len(t, byNodegroup.UpdateIds, 1, "NodegroupName filter must exclude the cluster-level update")

	described, err := client.DescribeUpdate(ctx, &ekssdk.DescribeUpdateInput{
		Name:     aws.String("upd-cluster"),
		UpdateId: aws.String(byNodegroup.UpdateIds[0]),
	})
	require.NoError(t, err)
	assert.Equal(t, ekstypes.UpdateTypeVersionUpdate, described.Update.Type)
}

// TestListInsights_FilterByCategoryAndStatus covers ListInsightsInput.Filter
// (api_op_ListInsights.go, InsightsFilter.Categories/Statuses). Previously
// ignored entirely -- the request body's "filter" key was never even parsed.
func TestListInsights_FilterByCategoryAndStatus(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("insights-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)

	unfiltered, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{
		ClusterName: aws.String("insights-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, unfiltered.Insights, 2, "sanity: two synthetic insights, both UPGRADE_READINESS/PASSING")

	misconfig, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{
		ClusterName: aws.String("insights-cluster"),
		Filter: &ekstypes.InsightsFilter{
			Categories: []ekstypes.Category{ekstypes.CategoryMisconfiguration},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, misconfig.Insights, "Categories=[MISCONFIGURATION] must exclude the UPGRADE_READINESS insights")

	failing, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{
		ClusterName: aws.String("insights-cluster"),
		Filter: &ekstypes.InsightsFilter{
			Statuses: []ekstypes.InsightStatusValue{ekstypes.InsightStatusValueError},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, failing.Insights, "Statuses=[ERROR] must exclude the PASSING insights")
}
