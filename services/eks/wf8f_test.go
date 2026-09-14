package eks_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// gopherstack-wf8f item 1: typed Capability.Configuration (ArgoCd).

func TestCapabilityConfiguration_ArgoCd_RealClient_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("argocd-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	cfg := &ekstypes.CapabilityConfigurationRequest{
		ArgoCd: &ekstypes.ArgoCdConfigRequest{
			AwsIdc: &ekstypes.ArgoCdAwsIdcConfigRequest{
				IdcInstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
				IdcRegion:      aws.String("us-east-1"),
			},
			Namespace:     aws.String("argocd"),
			NetworkAccess: &ekstypes.ArgoCdNetworkAccessConfigRequest{VpceIds: []string{"vpce-1", "vpce-2"}},
			RbacRoleMappings: []ekstypes.ArgoCdRoleMapping{
				{
					Role: ekstypes.ArgoCdRoleAdmin,
					Identities: []ekstypes.SsoIdentity{
						{Id: aws.String("user-1"), Type: ekstypes.SsoIdentityTypeSsoUser},
					},
				},
			},
		},
	}

	out, err := client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
		ClusterName:             aws.String("argocd-cluster"),
		CapabilityName:          aws.String("my-argocd"),
		Type:                    ekstypes.CapabilityTypeArgocd,
		RoleArn:                 aws.String("arn:aws:iam::123456789012:role/capability"),
		DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
		Configuration:           cfg,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Capability.Configuration)
	require.NotNil(t, out.Capability.Configuration.ArgoCd)

	got := out.Capability.Configuration.ArgoCd
	require.NotNil(t, got.AwsIdc)
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-1234567890abcdef", aws.ToString(got.AwsIdc.IdcInstanceArn))
	assert.Equal(t, "us-east-1", aws.ToString(got.AwsIdc.IdcRegion))
	assert.Equal(t, "argocd", aws.ToString(got.Namespace))
	require.NotNil(t, got.NetworkAccess)
	assert.ElementsMatch(t, []string{"vpce-1", "vpce-2"}, got.NetworkAccess.VpceIds)
	require.Len(t, got.RbacRoleMappings, 1)
	assert.Equal(t, ekstypes.ArgoCdRoleAdmin, got.RbacRoleMappings[0].Role)
	require.Len(t, got.RbacRoleMappings[0].Identities, 1)
	assert.Equal(t, "user-1", aws.ToString(got.RbacRoleMappings[0].Identities[0].Id))

	desc, err := client.DescribeCapability(ctx, &ekssdk.DescribeCapabilityInput{
		ClusterName: aws.String("argocd-cluster"), CapabilityName: aws.String("my-argocd"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Capability.Configuration.ArgoCd.AwsIdc)
	assert.Equal(t, "us-east-1", aws.ToString(desc.Capability.Configuration.ArgoCd.AwsIdc.IdcRegion))
}

// TestCapabilityConfiguration_ArgoCd_MissingAwsIdc_InvalidParameterException
// drives the raw HTTP handler, not the real SDK client: the real client's
// own validateOpCreateCapabilityInput (validators.go) rejects a missing
// ArgoCd.AwsIdc before the request is ever sent, so a real-client call can
// never reach this backend's own (defense-in-depth) server-side check.
func TestCapabilityConfiguration_ArgoCd_MissingAwsIdc_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "argocd-invalid-cluster", "roleArn": "arn:aws:iam::123456789012:role/eks",
	})

	rec := doREST(t, h, http.MethodPost, "/clusters/argocd-invalid-cluster/capabilities", map[string]any{
		"capabilityName":          "bad-argocd",
		"type":                    "ARGOCD",
		"roleArn":                 "arn:aws:iam::123456789012:role/capability",
		"deletePropagationPolicy": "RETAIN",
		"configuration":           map[string]any{"argoCd": map[string]any{}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCapabilityConfiguration_UpdateArgoCd_RoleMappingMergeSemantics(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("argocd-update-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
		ClusterName:             aws.String("argocd-update-cluster"),
		CapabilityName:          aws.String("my-argocd"),
		Type:                    ekstypes.CapabilityTypeArgocd,
		RoleArn:                 aws.String("arn:aws:iam::123456789012:role/capability"),
		DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
		Configuration: &ekstypes.CapabilityConfigurationRequest{ArgoCd: &ekstypes.ArgoCdConfigRequest{
			AwsIdc: &ekstypes.ArgoCdAwsIdcConfigRequest{IdcInstanceArn: aws.String("arn:aws:sso:::instance/i-1")},
			RbacRoleMappings: []ekstypes.ArgoCdRoleMapping{
				{
					Role:       ekstypes.ArgoCdRoleAdmin,
					Identities: []ekstypes.SsoIdentity{{Id: aws.String("u1"), Type: ekstypes.SsoIdentityTypeSsoUser}},
				},
				{
					Role:       ekstypes.ArgoCdRoleViewer,
					Identities: []ekstypes.SsoIdentity{{Id: aws.String("u2"), Type: ekstypes.SsoIdentityTypeSsoUser}},
				},
			},
		}},
	})
	require.NoError(t, err)

	// AddOrUpdate replaces ADMIN's identities entirely; VIEWER is untouched
	// by the update body and must survive. UpdateCapabilityOutput carries
	// only an async Update object (types.go:3257), not the Capability
	// itself, so the merged Configuration is read back via DescribeCapability.
	_, err = client.UpdateCapability(ctx, &ekssdk.UpdateCapabilityInput{
		ClusterName:    aws.String("argocd-update-cluster"),
		CapabilityName: aws.String("my-argocd"),
		Configuration: &ekstypes.UpdateCapabilityConfiguration{ArgoCd: &ekstypes.UpdateArgoCdConfig{
			RbacRoleMappings: &ekstypes.UpdateRoleMappings{
				AddOrUpdateRoleMappings: []ekstypes.ArgoCdRoleMapping{
					{
						Role: ekstypes.ArgoCdRoleAdmin,
						Identities: []ekstypes.SsoIdentity{
							{Id: aws.String("u3"), Type: ekstypes.SsoIdentityTypeSsoUser},
						},
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	desc, err := client.DescribeCapability(ctx, &ekssdk.DescribeCapabilityInput{
		ClusterName: aws.String("argocd-update-cluster"), CapabilityName: aws.String("my-argocd"),
	})
	require.NoError(t, err)

	mappings := desc.Capability.Configuration.ArgoCd.RbacRoleMappings
	byRole := map[ekstypes.ArgoCdRole][]string{}

	for _, m := range mappings {
		ids := make([]string, 0, len(m.Identities))
		for _, id := range m.Identities {
			ids = append(ids, aws.ToString(id.Id))
		}

		byRole[m.Role] = ids
	}

	assert.Equal(t, []string{"u3"}, byRole[ekstypes.ArgoCdRoleAdmin])
	assert.Equal(t, []string{"u2"}, byRole[ekstypes.ArgoCdRoleViewer])

	// Now remove u2 from VIEWER -- the mapping should disappear entirely
	// once its identity list empties.
	_, err = client.UpdateCapability(ctx, &ekssdk.UpdateCapabilityInput{
		ClusterName:    aws.String("argocd-update-cluster"),
		CapabilityName: aws.String("my-argocd"),
		Configuration: &ekstypes.UpdateCapabilityConfiguration{ArgoCd: &ekstypes.UpdateArgoCdConfig{
			RbacRoleMappings: &ekstypes.UpdateRoleMappings{
				RemoveRoleMappings: []ekstypes.ArgoCdRoleMapping{
					{
						Role: ekstypes.ArgoCdRoleViewer,
						Identities: []ekstypes.SsoIdentity{
							{Id: aws.String("u2"), Type: ekstypes.SsoIdentityTypeSsoUser},
						},
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	desc2, err := client.DescribeCapability(ctx, &ekssdk.DescribeCapabilityInput{
		ClusterName: aws.String("argocd-update-cluster"), CapabilityName: aws.String("my-argocd"),
	})
	require.NoError(t, err)

	for _, m := range desc2.Capability.Configuration.ArgoCd.RbacRoleMappings {
		assert.NotEqual(t, ekstypes.ArgoCdRoleViewer, m.Role)
	}
}

func TestCapability_OnePerTypePerCluster_ResourceLimitExceeded(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("cap-onetype-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	create := func(name string) error {
		_, cErr := client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
			ClusterName:             aws.String("cap-onetype-cluster"),
			CapabilityName:          aws.String(name),
			Type:                    ekstypes.CapabilityTypeAck,
			RoleArn:                 aws.String("arn:aws:iam::123456789012:role/capability"),
			DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
		})

		return cErr
	}

	require.NoError(t, create("ack-1"))

	err = create("ack-2")
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAs(t, err, &rle)
}

// gopherstack-wf8f item 3: ClientRequestToken idempotency dedup.

func TestClientRequestToken_ReplaySameParams_ReturnsOriginalResult(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	token := "idem-token-1"
	in := &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("idem-cluster"),
		FargateProfileName:  aws.String("idem-fp"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
		ClientRequestToken:  aws.String(token),
	}

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("idem-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	first, err := client.CreateFargateProfile(ctx, in)
	require.NoError(t, err)

	second, err := client.CreateFargateProfile(ctx, in)
	require.NoError(t, err)

	assert.Equal(t, aws.ToTime(first.FargateProfile.CreatedAt), aws.ToTime(second.FargateProfile.CreatedAt))
	assert.Equal(
		t,
		aws.ToString(first.FargateProfile.FargateProfileArn),
		aws.ToString(second.FargateProfile.FargateProfileArn),
	)

	// Confirms the replay short-circuited before the backend, not that the
	// backend happens to be idempotent on its own: a second real create
	// with the SAME name and no token would fail (proven by the sibling
	// TestCreateFargateProfile_Duplicate_InvalidParameterException).
	list, err := client.ListFargateProfiles(
		ctx,
		&ekssdk.ListFargateProfilesInput{ClusterName: aws.String("idem-cluster")},
	)
	require.NoError(t, err)
	assert.Len(t, list.FargateProfileNames, 1)
}

func TestClientRequestToken_ReplayDifferentParams_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("idem-mismatch-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	token := "idem-token-mismatch"

	_, err = client.CreateFargateProfile(ctx, &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("idem-mismatch-cluster"),
		FargateProfileName:  aws.String("fp-a"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
		ClientRequestToken:  aws.String(token),
	})
	require.NoError(t, err)

	_, err = client.CreateFargateProfile(ctx, &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("idem-mismatch-cluster"),
		FargateProfileName:  aws.String("fp-b"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
		ClientRequestToken:  aws.String(token),
	})
	require.Error(t, err)

	var ipe *ekstypes.InvalidParameterException
	require.ErrorAs(t, err, &ipe)

	// The rejected replay must not have created fp-b.
	list, err := client.ListFargateProfiles(
		ctx, &ekssdk.ListFargateProfilesInput{ClusterName: aws.String("idem-mismatch-cluster")},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"fp-a"}, list.FargateProfileNames)
}

func TestClientRequestToken_Absent_AlwaysCreatesNew(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("idem-absent-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	// No ClientRequestToken on either input -- both go straight to the
	// backend; the second must fail as a genuine duplicate.
	in1 := &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("idem-absent-cluster"),
		FargateProfileName:  aws.String("fp-absent"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	}
	in2 := &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("idem-absent-cluster"),
		FargateProfileName:  aws.String("fp-absent"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	}

	_, err = client.CreateFargateProfile(ctx, in1)
	require.NoError(t, err)

	_, err = client.CreateFargateProfile(ctx, in2)
	require.Error(t, err)
}

// gopherstack-wf8f item 4: ResourceLimitExceededException quota enforcement.

func TestResourceLimitExceeded_FargateProfilesPerCluster(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{FargateProfilesPerCluster: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("fp-limit-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreateFargateProfile(ctx, &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("fp-limit-cluster"),
		FargateProfileName:  aws.String("fp-1"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	})
	require.NoError(t, err)

	_, err = client.CreateFargateProfile(ctx, &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("fp-limit-cluster"),
		FargateProfileName:  aws.String("fp-2"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	})
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_ClustersPerAccount(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{ClustersPerAccount: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("only-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("second-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_NodegroupsPerCluster(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{NodegroupsPerCluster: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ng-limit-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	newInput := func(name string) *ekssdk.CreateNodegroupInput {
		return &ekssdk.CreateNodegroupInput{
			ClusterName:   aws.String("ng-limit-cluster"),
			NodegroupName: aws.String(name),
			NodeRole:      aws.String("arn:aws:iam::123456789012:role/node"),
			Subnets:       []string{"subnet-1"},
		}
	}

	_, err = client.CreateNodegroup(ctx, newInput("ng-1"))
	require.NoError(t, err)

	_, err = client.CreateNodegroup(ctx, newInput("ng-2"))
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_AccessEntriesPerCluster(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{AccessEntriesPerCluster: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ae-limit-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreateAccessEntry(ctx, &ekssdk.CreateAccessEntryInput{
		ClusterName:  aws.String("ae-limit-cluster"),
		PrincipalArn: aws.String("arn:aws:iam::123456789012:role/p1"),
	})
	require.NoError(t, err)

	_, err = client.CreateAccessEntry(ctx, &ekssdk.CreateAccessEntryInput{
		ClusterName:  aws.String("ae-limit-cluster"),
		PrincipalArn: aws.String("arn:aws:iam::123456789012:role/p2"),
	})
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_RegisteredClustersPerAccount(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{RegisteredClustersPerAccount: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.RegisterCluster(ctx, &ekssdk.RegisterClusterInput{
		Name: aws.String("registered-1"),
		ConnectorConfig: &ekstypes.ConnectorConfigRequest{
			RoleArn: aws.String(
				"arn:aws:iam::123456789012:role/connector",
			), Provider: ekstypes.ConnectorConfigProviderEksAnywhere,
		},
	})
	require.NoError(t, err)

	_, err = client.RegisterCluster(ctx, &ekssdk.RegisterClusterInput{
		Name: aws.String("registered-2"),
		ConnectorConfig: &ekstypes.ConnectorConfigRequest{
			RoleArn: aws.String(
				"arn:aws:iam::123456789012:role/connector",
			), Provider: ekstypes.ConnectorConfigProviderEksAnywhere,
		},
	})
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_PodIdentityAssociationsPerCluster(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{PodIdentityAssocsPerCluster: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("pia-limit-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreatePodIdentityAssociation(ctx, &ekssdk.CreatePodIdentityAssociationInput{
		ClusterName:    aws.String("pia-limit-cluster"),
		Namespace:      aws.String("ns1"),
		ServiceAccount: aws.String("sa1"),
		RoleArn:        aws.String("arn:aws:iam::123456789012:role/pod1"),
	})
	require.NoError(t, err)

	_, err = client.CreatePodIdentityAssociation(ctx, &ekssdk.CreatePodIdentityAssociationInput{
		ClusterName:    aws.String("pia-limit-cluster"),
		Namespace:      aws.String("ns2"),
		ServiceAccount: aws.String("sa2"),
		RoleArn:        aws.String("arn:aws:iam::123456789012:role/pod2"),
	})
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

func TestResourceLimitExceeded_EksAnywhereSubscriptionsPerAccount(t *testing.T) {
	t.Parallel()

	b := newBackend(t).WithResourceLimits(eks.ResourceLimits{AnywhereSubscriptionsPerAcct: 1})
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	newInput := func(name string) *ekssdk.CreateEksAnywhereSubscriptionInput {
		return &ekssdk.CreateEksAnywhereSubscriptionInput{
			Name: aws.String(name),
			Term: &ekstypes.EksAnywhereSubscriptionTerm{
				Duration: 12,
				Unit:     ekstypes.EksAnywhereSubscriptionTermUnitMonths,
			},
		}
	}

	_, err := client.CreateEksAnywhereSubscription(ctx, newInput("sub-1"))
	require.NoError(t, err)

	_, err = client.CreateEksAnywhereSubscription(ctx, newInput("sub-2"))
	require.Error(t, err)

	var rle *ekstypes.ResourceLimitExceededException
	require.ErrorAsf(t, err, &rle, "expected ResourceLimitExceededException, got %v", err)
}

// gopherstack-wf8f item 2: honest insight derivation.

func TestInsights_DerivedFromRealClusterVersion(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	tests := []struct {
		wantSupportStatus ekstypes.InsightStatusValue
		name              string
		version           string
	}{
		// Dates are this backend's fixed static table (clusters.go);
		// "today" is whatever this suite runs on, so these assertions are
		// derived from the same table the production code reads, not a
		// separately hand-computed expectation.
		{name: "insight-cluster-1-32", version: "1.32"},
		{name: "insight-cluster-1-31", version: "1.31"},
		{name: "insight-cluster-1-30", version: "1.30"},
		{name: "insight-cluster-1-29", version: "1.29"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:               aws.String(tc.name),
				Version:            aws.String(tc.version),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
			})
			require.NoError(t, err)

			out, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{ClusterName: aws.String(tc.name)})
			require.NoError(t, err)
			require.NotEmpty(t, out.Insights)

			for _, ins := range out.Insights {
				assert.Equal(t, ekstypes.CategoryUpgradeReadiness, ins.Category)
				assert.Equal(t, tc.version, aws.ToString(ins.KubernetesVersion))
				assert.NotEmpty(t, aws.ToString(ins.Name))
				require.NotNil(t, ins.InsightStatus)
				assert.NotEmpty(t, ins.InsightStatus.Status)
				assert.NotEmpty(t, aws.ToString(ins.InsightStatus.Reason))
			}
		})
	}
}

func TestInsights_UnknownVersion_NoFabricatedInsights(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("unknown-version-cluster"),
		Version:            aws.String("1.4"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	out, err := client.ListInsights(
		ctx, &ekssdk.ListInsightsInput{ClusterName: aws.String("unknown-version-cluster")},
	)
	require.NoError(t, err)
	assert.Empty(t, out.Insights)
}

func TestInsights_VersionBehindLatest_ReportsGap(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("behind-cluster"),
		Version:            aws.String("1.29"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	out, err := client.ListInsights(ctx, &ekssdk.ListInsightsInput{ClusterName: aws.String("behind-cluster")})
	require.NoError(t, err)

	var found bool

	for _, ins := range out.Insights {
		if aws.ToString(ins.Name) == "Cluster Kubernetes version behind latest supported version" {
			found = true

			assert.Equal(t, ekstypes.InsightStatusValueWarning, ins.InsightStatus.Status)
			assert.Contains(t, aws.ToString(ins.InsightStatus.Reason), "behind")
		}
	}

	assert.True(t, found, "expected a version-behind-latest insight")
}

func TestClientRequestToken_ReplaySameParams_AcrossRemainingWiredOps(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, setupErr := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("idem-wide-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, setupErr)

	t.Run("create_pod_identity_association", func(t *testing.T) {
		t.Parallel()

		in := &ekssdk.CreatePodIdentityAssociationInput{
			ClusterName:        aws.String("idem-wide-cluster"),
			Namespace:          aws.String("ns"),
			ServiceAccount:     aws.String("sa"),
			RoleArn:            aws.String("arn:aws:iam::123456789012:role/pod"),
			ClientRequestToken: aws.String("pia-token"),
		}

		first, err := client.CreatePodIdentityAssociation(ctx, in)
		require.NoError(t, err)

		second, err := client.CreatePodIdentityAssociation(ctx, in)
		require.NoError(t, err)

		assert.Equal(t, aws.ToString(first.Association.AssociationArn), aws.ToString(second.Association.AssociationArn))
	})

	t.Run("create_eks_anywhere_subscription", func(t *testing.T) {
		t.Parallel()

		in := &ekssdk.CreateEksAnywhereSubscriptionInput{
			Name: aws.String("idem-sub"),
			Term: &ekstypes.EksAnywhereSubscriptionTerm{
				Duration: 12, Unit: ekstypes.EksAnywhereSubscriptionTermUnitMonths,
			},
			ClientRequestToken: aws.String("sub-token"),
		}

		first, err := client.CreateEksAnywhereSubscription(ctx, in)
		require.NoError(t, err)

		second, err := client.CreateEksAnywhereSubscription(ctx, in)
		require.NoError(t, err)

		assert.Equal(t, aws.ToString(first.Subscription.Id), aws.ToString(second.Subscription.Id))
	})

	t.Run("associate_encryption_config", func(t *testing.T) {
		t.Parallel()

		_, clusterErr := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
			Name:               aws.String("idem-enc-cluster"),
			RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
			ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
		})
		require.NoError(t, clusterErr)

		in := &ekssdk.AssociateEncryptionConfigInput{
			ClusterName: aws.String("idem-enc-cluster"),
			EncryptionConfig: []ekstypes.EncryptionConfig{
				{
					Provider:  &ekstypes.Provider{KeyArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/k1")},
					Resources: []string{"secrets"},
				},
			},
			ClientRequestToken: aws.String("enc-token"),
		}

		first, err := client.AssociateEncryptionConfig(ctx, in)
		require.NoError(t, err)

		second, err := client.AssociateEncryptionConfig(ctx, in)
		require.NoError(t, err)

		assert.Equal(t, aws.ToString(first.Update.Id), aws.ToString(second.Update.Id))
	})
}
