package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func newSlice32OrgsClient(t *testing.T) (*organizationssdk.Client, *organizationssdk.CreateOrganizationOutput) {
	t.Helper()

	backend := organizations.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestOrganizationsClient(t, organizations.NewHandler(backend))

	org, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{})
	require.NoError(t, err)

	return client, org
}

// TestSlice32Orgs_AccountLifecycle drives CreateAccount, DescribeCreateAccountStatus,
// ListCreateAccountStatus, MoveAccount, ListAccountsForParent and CloseAccount.
func TestSlice32Orgs_AccountLifecycle(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	created, err := client.CreateAccount(ctx, &organizationssdk.CreateAccountInput{
		AccountName: aws.String("member-account"),
		Email:       aws.String("member@example.com"),
	})
	require.NoError(t, err)
	acctID := aws.ToString(created.CreateAccountStatus.AccountId)
	require.NotEmpty(t, acctID)

	status, err := client.DescribeCreateAccountStatus(ctx, &organizationssdk.DescribeCreateAccountStatusInput{
		CreateAccountRequestId: created.CreateAccountStatus.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.CreateAccountStateSucceeded, status.CreateAccountStatus.State)
	assert.Equal(t, acctID, aws.ToString(status.CreateAccountStatus.AccountId))

	listed, err := client.ListCreateAccountStatus(ctx, &organizationssdk.ListCreateAccountStatusInput{})
	require.NoError(t, err)
	require.Len(t, listed.CreateAccountStatuses, 1)

	roots, err := client.ListRoots(ctx, &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, roots.Roots)
	rootID := roots.Roots[0].Id

	ouOut, err := client.CreateOrganizationalUnit(ctx, &organizationssdk.CreateOrganizationalUnitInput{
		ParentId: rootID,
		Name:     aws.String("wrong-parent"),
	})
	require.NoError(t, err)

	_, err = client.MoveAccount(ctx, &organizationssdk.MoveAccountInput{
		AccountId:           aws.String(acctID),
		SourceParentId:      rootID,
		DestinationParentId: ouOut.OrganizationalUnit.Id,
	})
	require.NoError(t, err)

	forParent, err := client.ListAccountsForParent(ctx, &organizationssdk.ListAccountsForParentInput{
		ParentId: ouOut.OrganizationalUnit.Id,
	})
	require.NoError(t, err)
	require.Len(t, forParent.Accounts, 1)
	assert.Equal(t, acctID, aws.ToString(forParent.Accounts[0].Id))

	_, err = client.CloseAccount(ctx, &organizationssdk.CloseAccountInput{AccountId: aws.String(acctID)})
	require.NoError(t, err)

	desc, err := client.DescribeAccount(ctx, &organizationssdk.DescribeAccountInput{AccountId: aws.String(acctID)})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.AccountStatusPendingClosure, desc.Account.Status)
}

// TestSlice32Orgs_RemoveAccountFromOrganization drives RemoveAccountFromOrganization.
func TestSlice32Orgs_RemoveAccountFromOrganization(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	created, err := client.CreateAccount(ctx, &organizationssdk.CreateAccountInput{
		AccountName: aws.String("removable-account"),
		Email:       aws.String("removable@example.com"),
	})
	require.NoError(t, err)
	acctID := aws.ToString(created.CreateAccountStatus.AccountId)

	_, err = client.RemoveAccountFromOrganization(ctx, &organizationssdk.RemoveAccountFromOrganizationInput{
		AccountId: aws.String(acctID),
	})
	require.NoError(t, err)

	_, err = client.DescribeAccount(ctx, &organizationssdk.DescribeAccountInput{AccountId: aws.String(acctID)})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "AccountNotFoundException", apiErr.ErrorCode())
}

// TestSlice32Orgs_CreateGovCloudAccount drives CreateGovCloudAccount.
func TestSlice32Orgs_CreateGovCloudAccount(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	out, err := client.CreateGovCloudAccount(ctx, &organizationssdk.CreateGovCloudAccountInput{
		AccountName: aws.String("govcloud-account"),
		Email:       aws.String("govcloud@example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.CreateAccountStatus)
	assert.NotEmpty(t, aws.ToString(out.CreateAccountStatus.AccountId))
	assert.NotEmpty(t, aws.ToString(out.CreateAccountStatus.GovCloudAccountId))
	assert.Equal(t, organizationstypes.CreateAccountStateSucceeded, out.CreateAccountStatus.State)
}

// TestSlice32Orgs_OrganizationalUnitLifecycle drives ListChildren, ListParents
// and UpdateOrganizationalUnit.
func TestSlice32Orgs_OrganizationalUnitLifecycle(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	roots, err := client.ListRoots(ctx, &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, roots.Roots)
	rootID := roots.Roots[0].Id

	ou, err := client.CreateOrganizationalUnit(ctx, &organizationssdk.CreateOrganizationalUnitInput{
		ParentId: rootID,
		Name:     aws.String("engineering"),
	})
	require.NoError(t, err)

	children, err := client.ListChildren(ctx, &organizationssdk.ListChildrenInput{
		ParentId:  rootID,
		ChildType: organizationstypes.ChildTypeOrganizationalUnit,
	})
	require.NoError(t, err)
	require.Len(t, children.Children, 1)
	assert.Equal(t, aws.ToString(ou.OrganizationalUnit.Id), aws.ToString(children.Children[0].Id))

	parents, err := client.ListParents(ctx, &organizationssdk.ListParentsInput{
		ChildId: ou.OrganizationalUnit.Id,
	})
	require.NoError(t, err)
	require.Len(t, parents.Parents, 1)
	assert.Equal(t, aws.ToString(rootID), aws.ToString(parents.Parents[0].Id))
	assert.Equal(t, organizationstypes.ParentTypeRoot, parents.Parents[0].Type)

	updated, err := client.UpdateOrganizationalUnit(ctx, &organizationssdk.UpdateOrganizationalUnitInput{
		OrganizationalUnitId: ou.OrganizationalUnit.Id,
		Name:                 aws.String("platform-engineering"),
	})
	require.NoError(t, err)
	assert.Equal(t, "platform-engineering", aws.ToString(updated.OrganizationalUnit.Name))
}

// TestSlice32Orgs_PolicyLifecycle drives Describe/Update/DeletePolicy,
// Attach/DetachPolicy, ListPolicies, ListPoliciesForTarget and
// ListTargetsForPolicy.
func TestSlice32Orgs_PolicyLifecycle(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	roots, err := client.ListRoots(ctx, &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	rootID := roots.Roots[0].Id

	created, err := client.CreatePolicy(ctx, &organizationssdk.CreatePolicyInput{
		Name:        aws.String("deny-all"),
		Description: aws.String("initial"),
		Type:        organizationstypes.PolicyTypeServiceControlPolicy,
		Content: aws.String(
			`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
		),
	})
	require.NoError(t, err)
	policyID := created.Policy.PolicySummary.Id

	got, err := client.DescribePolicy(ctx, &organizationssdk.DescribePolicyInput{PolicyId: policyID})
	require.NoError(t, err)
	assert.Equal(t, "initial", aws.ToString(got.Policy.PolicySummary.Description))

	updated, err := client.UpdatePolicy(ctx, &organizationssdk.UpdatePolicyInput{
		PolicyId:    policyID,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updated.Policy.PolicySummary.Description))

	_, err = client.AttachPolicy(ctx, &organizationssdk.AttachPolicyInput{
		PolicyId: policyID,
		TargetId: rootID,
	})
	require.NoError(t, err)

	forTarget, err := client.ListPoliciesForTarget(ctx, &organizationssdk.ListPoliciesForTargetInput{
		TargetId: rootID,
		Filter:   organizationstypes.PolicyTypeServiceControlPolicy,
	})
	require.NoError(t, err)

	found := false

	for _, p := range forTarget.Policies {
		if aws.ToString(p.Id) == aws.ToString(policyID) {
			found = true
		}
	}

	assert.True(t, found, "AttachPolicy must make the policy visible via ListPoliciesForTarget")

	targets, err := client.ListTargetsForPolicy(ctx, &organizationssdk.ListTargetsForPolicyInput{
		PolicyId: policyID,
	})
	require.NoError(t, err)
	require.Len(t, targets.Targets, 1)
	assert.Equal(t, aws.ToString(rootID), aws.ToString(targets.Targets[0].TargetId))
	assert.Equal(t, organizationstypes.TargetTypeRoot, targets.Targets[0].Type)

	allSCPs, err := client.ListPolicies(ctx, &organizationssdk.ListPoliciesInput{
		Filter: organizationstypes.PolicyTypeServiceControlPolicy,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allSCPs.Policies), 2, "must include both FullAWSAccess and the new policy")

	_, err = client.DetachPolicy(ctx, &organizationssdk.DetachPolicyInput{
		PolicyId: policyID,
		TargetId: rootID,
	})
	require.NoError(t, err)

	_, err = client.DeletePolicy(ctx, &organizationssdk.DeletePolicyInput{PolicyId: policyID})
	require.NoError(t, err)

	_, err = client.DescribePolicy(ctx, &organizationssdk.DescribePolicyInput{PolicyId: policyID})
	require.Error(t, err)
}

// TestSlice32Orgs_PolicyTypeToggle drives EnablePolicyType and DisablePolicyType.
func TestSlice32Orgs_PolicyTypeToggle(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	roots, err := client.ListRoots(ctx, &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	rootID := roots.Roots[0].Id

	enabled, err := client.EnablePolicyType(ctx, &organizationssdk.EnablePolicyTypeInput{
		RootId:     rootID,
		PolicyType: organizationstypes.PolicyTypeResourceControlPolicy,
	})
	require.NoError(t, err)

	foundEnabled := false

	for _, pt := range enabled.Root.PolicyTypes {
		if pt.Type == organizationstypes.PolicyTypeResourceControlPolicy {
			foundEnabled = true

			assert.Equal(t, organizationstypes.PolicyTypeStatusEnabled, pt.Status)
		}
	}

	assert.True(t, foundEnabled)

	disabled, err := client.DisablePolicyType(ctx, &organizationssdk.DisablePolicyTypeInput{
		RootId:     rootID,
		PolicyType: organizationstypes.PolicyTypeResourceControlPolicy,
	})
	require.NoError(t, err)

	for _, pt := range disabled.Root.PolicyTypes {
		assert.NotEqual(t, organizationstypes.PolicyTypeResourceControlPolicy, pt.Type,
			"disabled policy type must no longer appear on the root")
	}
}

// TestSlice32Orgs_ServiceAccessAndDelegatedAdmin drives EnableAWSServiceAccess,
// ListAWSServiceAccessForOrganization, RegisterDelegatedAdministrator,
// ListDelegatedAdministrators, ListDelegatedServicesForAccount,
// DeregisterDelegatedAdministrator and DisableAWSServiceAccess.
func TestSlice32Orgs_ServiceAccessAndDelegatedAdmin(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	const servicePrincipal = "config.amazonaws.com"

	_, err := client.EnableAWSServiceAccess(ctx, &organizationssdk.EnableAWSServiceAccessInput{
		ServicePrincipal: aws.String(servicePrincipal),
	})
	require.NoError(t, err)

	access, err := client.ListAWSServiceAccessForOrganization(
		ctx,
		&organizationssdk.ListAWSServiceAccessForOrganizationInput{},
	)
	require.NoError(t, err)
	require.Len(t, access.EnabledServicePrincipals, 1)
	assert.Equal(t, servicePrincipal, aws.ToString(access.EnabledServicePrincipals[0].ServicePrincipal))

	created, err := client.CreateAccount(ctx, &organizationssdk.CreateAccountInput{
		AccountName: aws.String("delegate-account"),
		Email:       aws.String("delegate@example.com"),
	})
	require.NoError(t, err)
	acctID := aws.ToString(created.CreateAccountStatus.AccountId)

	_, err = client.RegisterDelegatedAdministrator(ctx, &organizationssdk.RegisterDelegatedAdministratorInput{
		AccountId:        aws.String(acctID),
		ServicePrincipal: aws.String(servicePrincipal),
	})
	require.NoError(t, err)

	admins, err := client.ListDelegatedAdministrators(ctx, &organizationssdk.ListDelegatedAdministratorsInput{
		ServicePrincipal: aws.String(servicePrincipal),
	})
	require.NoError(t, err)
	require.Len(t, admins.DelegatedAdministrators, 1)
	assert.Equal(t, acctID, aws.ToString(admins.DelegatedAdministrators[0].Id))

	services, err := client.ListDelegatedServicesForAccount(
		ctx,
		&organizationssdk.ListDelegatedServicesForAccountInput{AccountId: aws.String(acctID)},
	)
	require.NoError(t, err)
	require.Len(t, services.DelegatedServices, 1)
	assert.Equal(t, servicePrincipal, aws.ToString(services.DelegatedServices[0].ServicePrincipal))

	_, err = client.DeregisterDelegatedAdministrator(ctx, &organizationssdk.DeregisterDelegatedAdministratorInput{
		AccountId:        aws.String(acctID),
		ServicePrincipal: aws.String(servicePrincipal),
	})
	require.NoError(t, err)

	_, err = client.DisableAWSServiceAccess(ctx, &organizationssdk.DisableAWSServiceAccessInput{
		ServicePrincipal: aws.String(servicePrincipal),
	})
	require.NoError(t, err)

	access, err = client.ListAWSServiceAccessForOrganization(
		ctx,
		&organizationssdk.ListAWSServiceAccessForOrganizationInput{},
	)
	require.NoError(t, err)
	assert.Empty(t, access.EnabledServicePrincipals)
}

// TestSlice32Orgs_HandshakeLifecycle drives DescribeHandshake, CancelHandshake,
// DeclineHandshake, ListHandshakesForAccount and ListHandshakesForOrganization.
func TestSlice32Orgs_HandshakeLifecycle(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	invite1, err := client.InviteAccountToOrganization(ctx, &organizationssdk.InviteAccountToOrganizationInput{
		Target: &organizationstypes.HandshakeParty{
			Id:   aws.String("invitee1@example.com"),
			Type: organizationstypes.HandshakePartyTypeEmail,
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeHandshake(ctx, &organizationssdk.DescribeHandshakeInput{
		HandshakeId: invite1.Handshake.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.HandshakeStateOpen, got.Handshake.State)

	canceled, err := client.CancelHandshake(ctx, &organizationssdk.CancelHandshakeInput{
		HandshakeId: invite1.Handshake.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.HandshakeStateCanceled, canceled.Handshake.State)

	invite2, err := client.InviteAccountToOrganization(ctx, &organizationssdk.InviteAccountToOrganizationInput{
		Target: &organizationstypes.HandshakeParty{
			Id:   aws.String("invitee2@example.com"),
			Type: organizationstypes.HandshakePartyTypeEmail,
		},
	})
	require.NoError(t, err)

	declined, err := client.DeclineHandshake(ctx, &organizationssdk.DeclineHandshakeInput{
		HandshakeId: invite2.Handshake.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, organizationstypes.HandshakeStateDeclined, declined.Handshake.State)

	forAccount, err := client.ListHandshakesForAccount(ctx, &organizationssdk.ListHandshakesForAccountInput{})
	require.NoError(t, err)
	assert.Len(t, forAccount.Handshakes, 2)

	forOrg, err := client.ListHandshakesForOrganization(
		ctx,
		&organizationssdk.ListHandshakesForOrganizationInput{},
	)
	require.NoError(t, err)
	assert.Len(t, forOrg.Handshakes, 2)
}

// TestSlice32Orgs_LeaveOrganization proves LeaveOrganization fails from the
// management account, the only caller identity this single-account backend
// can ever have (organization.go: the org's MasterAccountId equals
// b.accountID).
func TestSlice32Orgs_LeaveOrganization(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)

	_, err := client.LeaveOrganization(t.Context(), &organizationssdk.LeaveOrganizationInput{})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "MasterCannotLeaveOrganizationException", apiErr.ErrorCode())
}

// TestSlice32Orgs_EnableAllFeatures drives EnableAllFeatures.
func TestSlice32Orgs_EnableAllFeatures(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	out, err := client.EnableAllFeatures(ctx, &organizationssdk.EnableAllFeaturesInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Handshake)
	assert.Equal(t, organizationstypes.ActionTypeEnableAllFeatures, out.Handshake.Action)
	assert.Equal(t, organizationstypes.HandshakeStateOpen, out.Handshake.State)
}

// TestSlice32Orgs_ResourcePolicy drives Put/Describe/DeleteResourcePolicy.
func TestSlice32Orgs_ResourcePolicy(t *testing.T) {
	t.Parallel()

	client, _ := newSlice32OrgsClient(t)
	ctx := t.Context()

	content := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`

	put, err := client.PutResourcePolicy(ctx, &organizationssdk.PutResourcePolicyInput{
		Content: aws.String(content),
	})
	require.NoError(t, err)
	assert.Equal(t, content, aws.ToString(put.ResourcePolicy.Content))

	got, err := client.DescribeResourcePolicy(ctx, &organizationssdk.DescribeResourcePolicyInput{})
	require.NoError(t, err)
	assert.Equal(t, content, aws.ToString(got.ResourcePolicy.Content))

	_, err = client.DeleteResourcePolicy(ctx, &organizationssdk.DeleteResourcePolicyInput{})
	require.NoError(t, err)

	_, err = client.DescribeResourcePolicy(ctx, &organizationssdk.DescribeResourcePolicyInput{})
	require.Error(t, err)
}

// TestSlice32Orgs_EffectivePolicy drives DescribeEffectivePolicy and
// ListAccountsWithInvalidEffectivePolicy.
func TestSlice32Orgs_EffectivePolicy(t *testing.T) {
	t.Parallel()

	client, org := newSlice32OrgsClient(t)
	ctx := t.Context()

	roots, err := client.ListRoots(ctx, &organizationssdk.ListRootsInput{})
	require.NoError(t, err)
	rootID := roots.Roots[0].Id

	_, err = client.EnablePolicyType(ctx, &organizationssdk.EnablePolicyTypeInput{
		RootId:     rootID,
		PolicyType: organizationstypes.PolicyTypeTagPolicy,
	})
	require.NoError(t, err)

	policy, err := client.CreatePolicy(ctx, &organizationssdk.CreatePolicyInput{
		Name:        aws.String("tag-policy"),
		Description: aws.String("cost center tag policy"),
		Type:        organizationstypes.PolicyTypeTagPolicy,
		Content:     aws.String(`{"tags":{"CostCenter":{"tag_key":{"@@assign":"CostCenter"}}}}`),
	})
	require.NoError(t, err)

	_, err = client.AttachPolicy(ctx, &organizationssdk.AttachPolicyInput{
		PolicyId: policy.Policy.PolicySummary.Id,
		TargetId: rootID,
	})
	require.NoError(t, err)

	effective, err := client.DescribeEffectivePolicy(ctx, &organizationssdk.DescribeEffectivePolicyInput{
		PolicyType: organizationstypes.EffectivePolicyTypeTagPolicy,
		TargetId:   org.Organization.MasterAccountId,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(effective.EffectivePolicy.PolicyContent), "CostCenter")

	invalid, err := client.ListAccountsWithInvalidEffectivePolicy(
		ctx,
		&organizationssdk.ListAccountsWithInvalidEffectivePolicyInput{
			PolicyType: organizationstypes.EffectivePolicyTypeTagPolicy,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, invalid.Accounts)
}

// TestSlice32Orgs_ResourceTags drives TagResource and UntagResource against a
// real account resource ID.
func TestSlice32Orgs_ResourceTags(t *testing.T) {
	t.Parallel()

	client, org := newSlice32OrgsClient(t)
	ctx := t.Context()

	_, err := client.TagResource(ctx, &organizationssdk.TagResourceInput{
		ResourceId: org.Organization.MasterAccountId,
		Tags:       []organizationstypes.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(ctx, &organizationssdk.ListTagsForResourceInput{
		ResourceId: org.Organization.MasterAccountId,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "team", aws.ToString(got.Tags[0].Key))

	_, err = client.UntagResource(ctx, &organizationssdk.UntagResourceInput{
		ResourceId: org.Organization.MasterAccountId,
		TagKeys:    []string{"team"},
	})
	require.NoError(t, err)

	got, err = client.ListTagsForResource(ctx, &organizationssdk.ListTagsForResourceInput{
		ResourceId: org.Organization.MasterAccountId,
	})
	require.NoError(t, err)
	assert.Empty(t, got.Tags)
}
