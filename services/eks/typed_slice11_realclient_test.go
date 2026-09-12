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

func newSlice11TestHandler(t *testing.T) *eks.Handler {
	t.Helper()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	return eks.NewHandler(backend)
}

func createSlice11Cluster(t *testing.T, client *ekssdk.Client, name string) {
	t.Helper()

	_, err := client.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
		Name:               aws.String(name),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)
}

// TestTypedSlice11RealClient drives eks's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("access entries", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-ae-cluster")

		principalArn := "arn:aws:iam::123456789012:role/s11-principal"
		_, err := client.CreateAccessEntry(ctx, &ekssdk.CreateAccessEntryInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeAccessEntry(ctx, &ekssdk.DescribeAccessEntryInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)
		assert.Equal(t, principalArn, aws.ToString(descOut.AccessEntry.PrincipalArn))

		updOut, err := client.UpdateAccessEntry(ctx, &ekssdk.UpdateAccessEntryInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
			Username:     aws.String("s11-user"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-user", aws.ToString(updOut.AccessEntry.Username))

		policiesOut, err := client.ListAccessPolicies(ctx, &ekssdk.ListAccessPoliciesInput{})
		require.NoError(t, err)
		require.NotEmpty(t, policiesOut.AccessPolicies)
		policyArn := aws.ToString(policiesOut.AccessPolicies[0].Arn)

		_, err = client.AssociateAccessPolicy(ctx, &ekssdk.AssociateAccessPolicyInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
			PolicyArn:    aws.String(policyArn),
			AccessScope:  &ekstypes.AccessScope{Type: ekstypes.AccessScopeTypeCluster},
		})
		require.NoError(t, err)

		listAssocOut, err := client.ListAssociatedAccessPolicies(ctx, &ekssdk.ListAssociatedAccessPoliciesInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)
		require.Len(t, listAssocOut.AssociatedAccessPolicies, 1)
		assert.Equal(t, policyArn, aws.ToString(listAssocOut.AssociatedAccessPolicies[0].PolicyArn))

		_, err = client.DisassociateAccessPolicy(ctx, &ekssdk.DisassociateAccessPolicyInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
			PolicyArn:    aws.String(policyArn),
		})
		require.NoError(t, err)

		afterDisassoc, err := client.ListAssociatedAccessPolicies(ctx, &ekssdk.ListAssociatedAccessPoliciesInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)
		assert.Empty(t, afterDisassoc.AssociatedAccessPolicies)

		_, err = client.DeleteAccessEntry(ctx, &ekssdk.DeleteAccessEntryInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)

		_, err = client.DescribeAccessEntry(ctx, &ekssdk.DescribeAccessEntryInput{
			ClusterName:  aws.String("s11-ae-cluster"),
			PrincipalArn: aws.String(principalArn),
		})
		require.Error(t, err, "DescribeAccessEntry after delete must fail")
	})

	t.Run("pod identity association", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-pi-cluster")

		createOut, err := client.CreatePodIdentityAssociation(ctx, &ekssdk.CreatePodIdentityAssociationInput{
			ClusterName:    aws.String("s11-pi-cluster"),
			Namespace:      aws.String("default"),
			ServiceAccount: aws.String("s11-sa"),
			RoleArn:        aws.String("arn:aws:iam::123456789012:role/s11-pod-role"),
		})
		require.NoError(t, err)
		assocID := aws.ToString(createOut.Association.AssociationId)

		descOut, err := client.DescribePodIdentityAssociation(ctx, &ekssdk.DescribePodIdentityAssociationInput{
			ClusterName:   aws.String("s11-pi-cluster"),
			AssociationId: aws.String(assocID),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-sa", aws.ToString(descOut.Association.ServiceAccount))

		updOut, err := client.UpdatePodIdentityAssociation(ctx, &ekssdk.UpdatePodIdentityAssociationInput{
			ClusterName:   aws.String("s11-pi-cluster"),
			AssociationId: aws.String(assocID),
			RoleArn:       aws.String("arn:aws:iam::123456789012:role/s11-pod-role-2"),
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::123456789012:role/s11-pod-role-2", aws.ToString(updOut.Association.RoleArn))

		_, err = client.DeletePodIdentityAssociation(ctx, &ekssdk.DeletePodIdentityAssociationInput{
			ClusterName:   aws.String("s11-pi-cluster"),
			AssociationId: aws.String(assocID),
		})
		require.NoError(t, err)

		_, err = client.DescribePodIdentityAssociation(ctx, &ekssdk.DescribePodIdentityAssociationInput{
			ClusterName:   aws.String("s11-pi-cluster"),
			AssociationId: aws.String(assocID),
		})
		require.Error(t, err, "DescribePodIdentityAssociation after delete must fail")
	})

	t.Run("identity provider config", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-idp-cluster")

		_, err := client.AssociateIdentityProviderConfig(ctx, &ekssdk.AssociateIdentityProviderConfigInput{
			ClusterName: aws.String("s11-idp-cluster"),
			Oidc: &ekstypes.OidcIdentityProviderConfigRequest{
				IdentityProviderConfigName: aws.String("s11-oidc"),
				IssuerUrl:                  aws.String("https://s11-issuer.example.com"),
				ClientId:                   aws.String("s11-client-id"),
			},
		})
		require.NoError(t, err)

		listOut, err := client.ListIdentityProviderConfigs(ctx, &ekssdk.ListIdentityProviderConfigsInput{
			ClusterName: aws.String("s11-idp-cluster"),
		})
		require.NoError(t, err)
		var found bool
		for _, c := range listOut.IdentityProviderConfigs {
			if aws.ToString(c.Name) == "s11-oidc" {
				found = true
			}
		}
		assert.True(t, found)

		descOut, err := client.DescribeIdentityProviderConfig(ctx, &ekssdk.DescribeIdentityProviderConfigInput{
			ClusterName: aws.String("s11-idp-cluster"),
			IdentityProviderConfig: &ekstypes.IdentityProviderConfig{
				Name: aws.String("s11-oidc"),
				Type: aws.String("oidc"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, descOut.IdentityProviderConfig.Oidc)
		assert.Equal(t, "https://s11-issuer.example.com", aws.ToString(descOut.IdentityProviderConfig.Oidc.IssuerUrl))

		_, err = client.DisassociateIdentityProviderConfig(ctx, &ekssdk.DisassociateIdentityProviderConfigInput{
			ClusterName: aws.String("s11-idp-cluster"),
			IdentityProviderConfig: &ekstypes.IdentityProviderConfig{
				Name: aws.String("s11-oidc"),
				Type: aws.String("oidc"),
			},
		})
		require.NoError(t, err)

		afterOut, err := client.ListIdentityProviderConfigs(ctx, &ekssdk.ListIdentityProviderConfigsInput{
			ClusterName: aws.String("s11-idp-cluster"),
		})
		require.NoError(t, err)
		for _, c := range afterOut.IdentityProviderConfigs {
			assert.NotEqual(t, "s11-oidc", aws.ToString(c.Name))
		}
	})

	t.Run("addons", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-addon-cluster")

		versionsOut, err := client.DescribeAddonVersions(ctx, &ekssdk.DescribeAddonVersionsInput{
			AddonName: aws.String("vpc-cni"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, versionsOut.Addons)

		_, err = client.CreateAddon(ctx, &ekssdk.CreateAddonInput{
			ClusterName: aws.String("s11-addon-cluster"),
			AddonName:   aws.String("vpc-cni"),
		})
		require.NoError(t, err)

		listOut, err := client.ListAddons(ctx, &ekssdk.ListAddonsInput{
			ClusterName: aws.String("s11-addon-cluster"),
		})
		require.NoError(t, err)
		assert.Contains(t, listOut.Addons, "vpc-cni")

		updOut, err := client.UpdateAddon(ctx, &ekssdk.UpdateAddonInput{
			ClusterName:      aws.String("s11-addon-cluster"),
			AddonName:        aws.String("vpc-cni"),
			ResolveConflicts: ekstypes.ResolveConflictsOverwrite,
		})
		require.NoError(t, err)
		require.NotNil(t, updOut.Update)

		_, err = client.DeleteAddon(ctx, &ekssdk.DeleteAddonInput{
			ClusterName: aws.String("s11-addon-cluster"),
			AddonName:   aws.String("vpc-cni"),
		})
		require.NoError(t, err)

		afterOut, err := client.ListAddons(ctx, &ekssdk.ListAddonsInput{
			ClusterName: aws.String("s11-addon-cluster"),
		})
		require.NoError(t, err)
		assert.NotContains(t, afterOut.Addons, "vpc-cni")
	})

	t.Run("capabilities", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-capa-cluster")

		_, err := client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
			ClusterName:             aws.String("s11-capa-cluster"),
			CapabilityName:          aws.String("s11-ack"),
			Type:                    ekstypes.CapabilityTypeAck,
			RoleArn:                 aws.String("arn:aws:iam::123456789012:role/s11-capability"),
			DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
		})
		require.NoError(t, err)

		listOut, err := client.ListCapabilities(ctx, &ekssdk.ListCapabilitiesInput{
			ClusterName: aws.String("s11-capa-cluster"),
		})
		require.NoError(t, err)
		var found bool
		for _, c := range listOut.Capabilities {
			if aws.ToString(c.CapabilityName) == "s11-ack" {
				found = true
			}
		}
		assert.True(t, found)

		_, err = client.DeleteCapability(ctx, &ekssdk.DeleteCapabilityInput{
			ClusterName:    aws.String("s11-capa-cluster"),
			CapabilityName: aws.String("s11-ack"),
		})
		require.NoError(t, err)

		afterOut, err := client.ListCapabilities(ctx, &ekssdk.ListCapabilitiesInput{
			ClusterName: aws.String("s11-capa-cluster"),
		})
		require.NoError(t, err)
		for _, c := range afterOut.Capabilities {
			assert.NotEqual(t, "s11-ack", aws.ToString(c.CapabilityName))
		}
	})

	t.Run("fargate profile", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-fp-cluster")

		_, err := client.CreateFargateProfile(ctx, &ekssdk.CreateFargateProfileInput{
			ClusterName:         aws.String("s11-fp-cluster"),
			FargateProfileName:  aws.String("s11-fp"),
			PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/s11-fargate"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeFargateProfile(ctx, &ekssdk.DescribeFargateProfileInput{
			ClusterName:        aws.String("s11-fp-cluster"),
			FargateProfileName: aws.String("s11-fp"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-fp", aws.ToString(descOut.FargateProfile.FargateProfileName))

		delOut, err := client.DeleteFargateProfile(ctx, &ekssdk.DeleteFargateProfileInput{
			ClusterName:        aws.String("s11-fp-cluster"),
			FargateProfileName: aws.String("s11-fp"),
		})
		require.NoError(t, err)
		require.NotNil(t, delOut.FargateProfile)
		assert.Equal(t, ekstypes.FargateProfileStatusDeleting, delOut.FargateProfile.Status)

		_, err = client.DescribeFargateProfile(ctx, &ekssdk.DescribeFargateProfileInput{
			ClusterName:        aws.String("s11-fp-cluster"),
			FargateProfileName: aws.String("s11-fp"),
		})
		require.Error(t, err, "DescribeFargateProfile after delete must fail: the profile is removed immediately")
	})

	t.Run("eks anywhere subscription", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()

		createOut, err := client.CreateEksAnywhereSubscription(ctx, &ekssdk.CreateEksAnywhereSubscriptionInput{
			Name: aws.String("s11-anywhere-sub"),
			Term: &ekstypes.EksAnywhereSubscriptionTerm{
				Duration: 12,
				Unit:     ekstypes.EksAnywhereSubscriptionTermUnitMonths,
			},
		})
		require.NoError(t, err)
		subID := aws.ToString(createOut.Subscription.Id)

		descOut, err := client.DescribeEksAnywhereSubscription(ctx, &ekssdk.DescribeEksAnywhereSubscriptionInput{
			Id: aws.String(subID),
		})
		require.NoError(t, err)
		// The real EksAnywhereSubscription type (eks@v1.90.4 types/types.go)
		// has no Name field at all -- only Id/Arn/Status/etc -- so identity
		// is asserted via Id, not a name gopherstack tracks internally but
		// the real wire type has nowhere to carry.
		assert.Equal(t, subID, aws.ToString(descOut.Subscription.Id))
		assert.Equal(t, string(ekstypes.EksAnywhereSubscriptionStatusActive), aws.ToString(descOut.Subscription.Status))

		updOut, err := client.UpdateEksAnywhereSubscription(ctx, &ekssdk.UpdateEksAnywhereSubscriptionInput{
			Id:                 aws.String(subID),
			AutoRenew:          true,
			ClientRequestToken: aws.String("s11-token"),
		})
		require.NoError(t, err)
		assert.True(t, updOut.Subscription.AutoRenew)

		delOut, err := client.DeleteEksAnywhereSubscription(ctx, &ekssdk.DeleteEksAnywhereSubscriptionInput{
			Id: aws.String(subID),
		})
		require.NoError(t, err)
		require.NotNil(t, delOut.Subscription)
		assert.Equal(t, string(ekstypes.EksAnywhereSubscriptionStatusDeleting),
			aws.ToString(delOut.Subscription.Status))

		_, err = client.DescribeEksAnywhereSubscription(ctx, &ekssdk.DescribeEksAnywhereSubscriptionInput{
			Id: aws.String(subID),
		})
		require.Error(t, err, "DescribeEksAnywhereSubscription after delete must fail: it is removed immediately")
	})

	t.Run("insights", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-insights-cluster")

		listOut, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{
			ClusterName: aws.String("s11-insights-cluster"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.Insights)
		insightID := aws.ToString(listOut.Insights[0].Id)

		descOut, err := client.DescribeInsight(ctx, &ekssdk.DescribeInsightInput{
			ClusterName: aws.String("s11-insights-cluster"),
			Id:          aws.String(insightID),
		})
		require.NoError(t, err)
		assert.Equal(t, insightID, aws.ToString(descOut.Insight.Id))

		// Real StartInsightsRefreshOutput/DescribeInsightsRefreshOutput carry
		// no ClusterName field at all (eks@v1.90.4 api_op_*.go) -- only
		// Status/Message/timestamps.
		startOut, err := client.StartInsightsRefresh(ctx, &ekssdk.StartInsightsRefreshInput{
			ClusterName: aws.String("s11-insights-cluster"),
		})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.InsightsRefreshStatusCompleted, startOut.Status)

		refreshOut, err := client.DescribeInsightsRefresh(ctx, &ekssdk.DescribeInsightsRefreshInput{
			ClusterName: aws.String("s11-insights-cluster"),
		})
		require.NoError(t, err)
		assert.Equal(t, ekstypes.InsightsRefreshStatusCompleted, refreshOut.Status)
	})

	t.Run("cluster config, nodegroup config, update cancel, deregister", func(t *testing.T) {
		t.Parallel()

		client := newTestEKSClient(t, newSlice11TestHandler(t))
		ctx := t.Context()
		createSlice11Cluster(t, client, "s11-update-cluster")

		updOut, err := client.UpdateClusterConfig(ctx, &ekssdk.UpdateClusterConfigInput{
			Name: aws.String("s11-update-cluster"),
			Logging: &ekstypes.Logging{
				ClusterLogging: []ekstypes.LogSetup{
					{Types: []ekstypes.LogType{ekstypes.LogTypeApi}, Enabled: aws.Bool(true)},
				},
			},
		})
		require.NoError(t, err)
		updateID := aws.ToString(updOut.Update.Id)

		// This backend only supports cancellation for VersionRollback
		// updates (matching real EKS, whose CancelUpdate doc says
		// cancellation is only performed for that update type); a
		// ConfigUpdate must honestly reject cancellation, not fabricate
		// success.
		_, err = client.CancelUpdate(ctx, &ekssdk.CancelUpdateInput{
			Name:     aws.String("s11-update-cluster"),
			UpdateId: aws.String(updateID),
		})
		require.Error(t, err, "CancelUpdate on a non-VersionRollback update must fail")

		_, err = client.CreateNodegroup(ctx, &ekssdk.CreateNodegroupInput{
			ClusterName:   aws.String("s11-update-cluster"),
			NodegroupName: aws.String("s11-ng"),
			NodeRole:      aws.String("arn:aws:iam::123456789012:role/s11-node"),
			Subnets:       []string{"subnet-abc123"},
		})
		require.NoError(t, err)

		ngOut, err := client.UpdateNodegroupConfig(ctx, &ekssdk.UpdateNodegroupConfigInput{
			ClusterName:   aws.String("s11-update-cluster"),
			NodegroupName: aws.String("s11-ng"),
			ScalingConfig: &ekstypes.NodegroupScalingConfig{
				DesiredSize: aws.Int32(2),
				MinSize:     aws.Int32(1),
				MaxSize:     aws.Int32(3),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, ngOut.Update)

		descNg, err := client.DescribeNodegroup(ctx, &ekssdk.DescribeNodegroupInput{
			ClusterName:   aws.String("s11-update-cluster"),
			NodegroupName: aws.String("s11-ng"),
		})
		require.NoError(t, err)
		require.NotNil(t, descNg.Nodegroup.ScalingConfig)
		assert.Equal(t, int32(2), aws.ToInt32(descNg.Nodegroup.ScalingConfig.DesiredSize))

		_, err = client.DeleteNodegroup(ctx, &ekssdk.DeleteNodegroupInput{
			ClusterName:   aws.String("s11-update-cluster"),
			NodegroupName: aws.String("s11-ng"),
		})
		require.NoError(t, err)

		deregOut, err := client.DeregisterCluster(ctx, &ekssdk.DeregisterClusterInput{
			Name: aws.String("s11-update-cluster"),
		})
		require.NoError(t, err)
		require.NotNil(t, deregOut.Cluster)
		assert.Equal(t, ekstypes.ClusterStatusDeleting, deregOut.Cluster.Status)

		_, err = client.DescribeCluster(ctx, &ekssdk.DescribeClusterInput{
			Name: aws.String("s11-update-cluster"),
		})
		require.Error(t, err, "DescribeCluster after DeregisterCluster must fail: the cluster is removed immediately")
	})
}
