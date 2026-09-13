package ram_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// newTestRAMClient31 mirrors permission_version_shape_test.go's newTestRAMClient
// (kept package-local rather than reused, since that helper is unexported to its
// own file and this slice's tests live alongside it in the same package).
func newTestRAMClient31(t *testing.T, h *ram.Handler) *ramsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ramsdk.NewFromConfig(cfg, func(o *ramsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_ResourceShareDeleteLifecycle drives CreateResourceShare,
// DisassociateResourceShare and DeleteResourceShare through the real typed
// client.
func Test_SDKRoundTrip_ResourceShareDeleteLifecycle(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	subnetARN := "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-31del"

	created, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name:         aws.String("share-31-delete"),
		ResourceArns: []string{subnetARN},
	})
	require.NoError(t, err)
	require.NotNil(t, created.ResourceShare)
	shareARN := aws.ToString(created.ResourceShare.ResourceShareArn)
	require.NotEmpty(t, shareARN)

	disassoc, err := client.DisassociateResourceShare(ctx, &ramsdk.DisassociateResourceShareInput{
		ResourceShareArn: aws.String(shareARN),
		ResourceArns:     []string{subnetARN},
	})
	require.NoError(t, err)
	require.Len(t, disassoc.ResourceShareAssociations, 1)
	assert.Equal(t, subnetARN, aws.ToString(disassoc.ResourceShareAssociations[0].AssociatedEntity))
	assert.Equal(
		t,
		ramtypes.ResourceShareAssociationStatusDisassociated,
		disassoc.ResourceShareAssociations[0].Status,
	)

	deleted, err := client.DeleteResourceShare(ctx, &ramsdk.DeleteResourceShareInput{
		ResourceShareArn: aws.String(shareARN),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(deleted.ReturnValue))
}

// Test_SDKRoundTrip_ExternalPrincipalInvitationAccept drives
// CreateResourceShare with an external principal (which auto-generates a
// pending invitation), then GetResourceShareInvitations,
// ListPendingInvitationResources, GetResourceShareAssociations,
// ListPrincipals, ListResources, GetResourcePolicies, and
// AcceptResourceShareInvitation, all through the real typed client.
func Test_SDKRoundTrip_ExternalPrincipalInvitationAccept(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	subnetARN := "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-31inv"
	const externalAccountID = "999999999999"

	created, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name:                    aws.String("share-31-invite"),
		AllowExternalPrincipals: aws.Bool(true),
		Principals:              []string{externalAccountID},
		ResourceArns:            []string{subnetARN},
	})
	require.NoError(t, err)
	shareARN := aws.ToString(created.ResourceShare.ResourceShareArn)
	require.NotEmpty(t, shareARN)

	invs, err := client.GetResourceShareInvitations(ctx, &ramsdk.GetResourceShareInvitationsInput{
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, invs.ResourceShareInvitations, 1)
	inv := invs.ResourceShareInvitations[0]
	assert.Equal(t, ramtypes.ResourceShareInvitationStatusPending, inv.Status)
	assert.Equal(t, externalAccountID, aws.ToString(inv.ReceiverAccountId))
	invARN := aws.ToString(inv.ResourceShareInvitationArn)
	require.NotEmpty(t, invARN)

	pending, err := client.ListPendingInvitationResources(ctx, &ramsdk.ListPendingInvitationResourcesInput{
		ResourceShareInvitationArn: aws.String(invARN),
	})
	require.NoError(t, err)
	require.Len(t, pending.Resources, 1)
	assert.Equal(t, subnetARN, aws.ToString(pending.Resources[0].Arn))

	principalAssocs, err := client.GetResourceShareAssociations(ctx, &ramsdk.GetResourceShareAssociationsInput{
		AssociationType:   ramtypes.ResourceShareAssociationTypePrincipal,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, principalAssocs.ResourceShareAssociations, 1)
	assert.Equal(
		t,
		externalAccountID,
		aws.ToString(principalAssocs.ResourceShareAssociations[0].AssociatedEntity),
	)

	resourceAssocs, err := client.GetResourceShareAssociations(ctx, &ramsdk.GetResourceShareAssociationsInput{
		AssociationType:   ramtypes.ResourceShareAssociationTypeResource,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, resourceAssocs.ResourceShareAssociations, 1)
	assert.Equal(t, subnetARN, aws.ToString(resourceAssocs.ResourceShareAssociations[0].AssociatedEntity))

	principals, err := client.ListPrincipals(ctx, &ramsdk.ListPrincipalsInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, principals.Principals, 1)
	assert.Equal(t, externalAccountID, aws.ToString(principals.Principals[0].Id))

	resources, err := client.ListResources(ctx, &ramsdk.ListResourcesInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, resources.Resources, 1)
	assert.Equal(t, subnetARN, aws.ToString(resources.Resources[0].Arn))

	policies, err := client.GetResourcePolicies(ctx, &ramsdk.GetResourcePoliciesInput{
		ResourceArns: []string{subnetARN},
	})
	require.NoError(t, err)
	require.Len(t, policies.Policies, 1)
	assert.Contains(t, policies.Policies[0], "2012-10-17")

	accepted, err := client.AcceptResourceShareInvitation(ctx, &ramsdk.AcceptResourceShareInvitationInput{
		ResourceShareInvitationArn: aws.String(invARN),
	})
	require.NoError(t, err)
	require.NotNil(t, accepted.ResourceShareInvitation)
	assert.Equal(t, ramtypes.ResourceShareInvitationStatusAccepted, accepted.ResourceShareInvitation.Status)
}

// Test_SDKRoundTrip_RejectResourceShareInvitation drives an external-principal
// invitation through RejectResourceShareInvitation.
func Test_SDKRoundTrip_RejectResourceShareInvitation(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	created, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name:                    aws.String("share-31-reject"),
		AllowExternalPrincipals: aws.Bool(true),
		Principals:              []string{"888888888888"},
	})
	require.NoError(t, err)
	shareARN := aws.ToString(created.ResourceShare.ResourceShareArn)

	invs, err := client.GetResourceShareInvitations(ctx, &ramsdk.GetResourceShareInvitationsInput{
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, invs.ResourceShareInvitations, 1)
	invARN := aws.ToString(invs.ResourceShareInvitations[0].ResourceShareInvitationArn)

	rejected, err := client.RejectResourceShareInvitation(ctx, &ramsdk.RejectResourceShareInvitationInput{
		ResourceShareInvitationArn: aws.String(invARN),
	})
	require.NoError(t, err)
	require.NotNil(t, rejected.ResourceShareInvitation)
	assert.Equal(t, ramtypes.ResourceShareInvitationStatusRejected, rejected.ResourceShareInvitation.Status)
}

// Test_SDKRoundTrip_PermissionVersionLifecycle drives GetPermission,
// ListPermissions, ListPermissionVersions, and SetDefaultPermissionVersion
// through the real typed client.
func Test_SDKRoundTrip_PermissionVersionLifecycle(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	created, err := client.CreatePermission(ctx, &ramsdk.CreatePermissionInput{
		Name:           aws.String("perm-31"),
		ResourceType:   aws.String("ec2:Subnet"),
		PolicyTemplate: aws.String(`{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`),
	})
	require.NoError(t, err)
	permARN := aws.ToString(created.Permission.Arn)
	require.NotEmpty(t, permARN)

	got, err := client.GetPermission(ctx, &ramsdk.GetPermissionInput{PermissionArn: aws.String(permARN)})
	require.NoError(t, err)
	require.NotNil(t, got.Permission)
	assert.Equal(t, "perm-31", aws.ToString(got.Permission.Name))
	assert.Equal(t, "ec2:Subnet", aws.ToString(got.Permission.ResourceType))

	list, err := client.ListPermissions(ctx, &ramsdk.ListPermissionsInput{
		ResourceType: aws.String("ec2:Subnet"),
	})
	require.NoError(t, err)
	var foundInList bool
	for _, p := range list.Permissions {
		if aws.ToString(p.Arn) == permARN {
			foundInList = true
		}
	}
	assert.True(t, foundInList, "ListPermissions must include the newly created permission")

	_, err = client.CreatePermissionVersion(ctx, &ramsdk.CreatePermissionVersionInput{
		PermissionArn:  aws.String(permARN),
		PolicyTemplate: aws.String(`{"Effect":"Allow","Action":["ec2:DescribeSubnets","ec2:DescribeVpcs"]}`),
	})
	require.NoError(t, err)

	versions, err := client.ListPermissionVersions(ctx, &ramsdk.ListPermissionVersionsInput{
		PermissionArn: aws.String(permARN),
	})
	require.NoError(t, err)
	require.Len(t, versions.Permissions, 2)

	setDefault, err := client.SetDefaultPermissionVersion(ctx, &ramsdk.SetDefaultPermissionVersionInput{
		PermissionArn:     aws.String(permARN),
		PermissionVersion: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(setDefault.ReturnValue))

	gotAfter, err := client.GetPermission(ctx, &ramsdk.GetPermissionInput{PermissionArn: aws.String(permARN)})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(gotAfter.Permission.Version))
	assert.True(t, aws.ToBool(gotAfter.Permission.DefaultVersion))
}

// Test_SDKRoundTrip_SharePermissionAssociationLifecycle drives
// ListResourceSharePermissions, DisassociateResourceSharePermission,
// ReplacePermissionAssociations, and ListReplacePermissionAssociationsWork
// through the real typed client.
func Test_SDKRoundTrip_SharePermissionAssociationLifecycle(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	permA, err := client.CreatePermission(ctx, &ramsdk.CreatePermissionInput{
		Name:           aws.String("perm-31-a"),
		ResourceType:   aws.String("ec2:Subnet"),
		PolicyTemplate: aws.String(`{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`),
	})
	require.NoError(t, err)
	permAARN := aws.ToString(permA.Permission.Arn)

	permB, err := client.CreatePermission(ctx, &ramsdk.CreatePermissionInput{
		Name:           aws.String("perm-31-b"),
		ResourceType:   aws.String("ec2:Subnet"),
		PolicyTemplate: aws.String(`{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`),
	})
	require.NoError(t, err)
	permBARN := aws.ToString(permB.Permission.Arn)

	share, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name:           aws.String("share-31-perm"),
		PermissionArns: []string{permAARN},
	})
	require.NoError(t, err)
	shareARN := aws.ToString(share.ResourceShare.ResourceShareArn)

	perms, err := client.ListResourceSharePermissions(ctx, &ramsdk.ListResourceSharePermissionsInput{
		ResourceShareArn: aws.String(shareARN),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)
	assert.Equal(t, permAARN, aws.ToString(perms.Permissions[0].Arn))

	disassoc, err := client.DisassociateResourceSharePermission(
		ctx,
		&ramsdk.DisassociateResourceSharePermissionInput{
			ResourceShareArn: aws.String(shareARN),
			PermissionArn:    aws.String(permAARN),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disassoc.ReturnValue))

	permsAfter, err := client.ListResourceSharePermissions(ctx, &ramsdk.ListResourceSharePermissionsInput{
		ResourceShareArn: aws.String(shareARN),
	})
	require.NoError(t, err)
	assert.Empty(t, permsAfter.Permissions)

	_, err = client.AssociateResourceSharePermission(ctx, &ramsdk.AssociateResourceSharePermissionInput{
		ResourceShareArn: aws.String(shareARN),
		PermissionArn:    aws.String(permBARN),
	})
	require.NoError(t, err)

	replaced, err := client.ReplacePermissionAssociations(ctx, &ramsdk.ReplacePermissionAssociationsInput{
		FromPermissionArn: aws.String(permBARN),
		ToPermissionArn:   aws.String(permAARN),
	})
	require.NoError(t, err)
	require.NotNil(t, replaced.ReplacePermissionAssociationsWork)
	work := replaced.ReplacePermissionAssociationsWork
	assert.Equal(t, permBARN, aws.ToString(work.FromPermissionArn))
	assert.Equal(t, permAARN, aws.ToString(work.ToPermissionArn))
	assert.Equal(t, ramtypes.ReplacePermissionAssociationsWorkStatusCompleted, work.Status)
	workID := aws.ToString(work.Id)
	require.NotEmpty(t, workID)

	works, err := client.ListReplacePermissionAssociationsWork(
		ctx,
		&ramsdk.ListReplacePermissionAssociationsWorkInput{},
	)
	require.NoError(t, err)
	var found bool
	for _, w := range works.ReplacePermissionAssociationsWorks {
		if aws.ToString(w.Id) == workID {
			found = true
			assert.Equal(t, ramtypes.ReplacePermissionAssociationsWorkStatusCompleted, w.Status)
		}
	}
	assert.True(t, found, "ListReplacePermissionAssociationsWork must include the new work item")
}

// Test_SDKRoundTrip_PromotePermissionCreatedFromPolicy drives
// PromotePermissionCreatedFromPolicy on a CREATED_FROM_POLICY permission
// produced by the RAM<-Glue policy-sharing seam (PutPolicyBasedShare -- there
// is no RAM SDK operation that creates this state directly; real AWS creates
// it the same indirect way, by attaching a cross-account resource-based
// policy through another service).
func Test_SDKRoundTrip_PromotePermissionCreatedFromPolicy(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	resourceARN := "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-31policy"
	require.NoError(t, backend.PutPolicyBasedShare(
		resourceARN,
		[]string{"777777777777"},
		[]string{"ec2:DescribeSubnets"},
	))

	list, err := client.ListPermissions(ctx, &ramsdk.ListPermissionsInput{ResourceType: aws.String("ec2:Subnet")})
	require.NoError(t, err)

	var permARN string
	for _, p := range list.Permissions {
		if string(p.PermissionType) == "CREATED_FROM_POLICY" {
			permARN = aws.ToString(p.Arn)
		}
	}
	require.NotEmpty(t, permARN, "PutPolicyBasedShare must have derived a CREATED_FROM_POLICY permission")

	promoted, err := client.PromotePermissionCreatedFromPolicy(ctx, &ramsdk.PromotePermissionCreatedFromPolicyInput{
		PermissionArn: aws.String(permARN),
		Name:          aws.String("promoted-perm-31"),
	})
	require.NoError(t, err)
	require.NotNil(t, promoted.Permission)
	assert.Equal(t, "promoted-perm-31", aws.ToString(promoted.Permission.Name))
	assert.Equal(t, ramtypes.PermissionTypeCustomerManaged, promoted.Permission.PermissionType)
}

// Test_SDKRoundTrip_MiscOps drives ListResourceTypes, ListSourceAssociations,
// EnableSharingWithAwsOrganization, TagResource, and UntagResource through
// the real typed client.
func Test_SDKRoundTrip_MiscOps(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient31(t, h)
	ctx := t.Context()

	types, err := client.ListResourceTypes(ctx, &ramsdk.ListResourceTypesInput{})
	require.NoError(t, err)
	var hasEC2Subnet bool
	for _, rt := range types.ResourceTypes {
		if aws.ToString(rt.ResourceType) == "ec2:Subnet" {
			hasEC2Subnet = true
		}
	}
	assert.True(t, hasEC2Subnet)

	sources, err := client.ListSourceAssociations(ctx, &ramsdk.ListSourceAssociationsInput{})
	require.NoError(t, err)
	assert.Empty(t, sources.SourceAssociations)

	enabled, err := client.EnableSharingWithAwsOrganization(
		ctx,
		&ramsdk.EnableSharingWithAwsOrganizationInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enabled.ReturnValue))

	share, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name: aws.String("share-31-tags"),
	})
	require.NoError(t, err)
	shareARN := aws.ToString(share.ResourceShare.ResourceShareArn)

	_, err = client.TagResource(ctx, &ramsdk.TagResourceInput{
		ResourceShareArn: aws.String(shareARN),
		Tags:             []ramtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	// RAM has no ListTagsForResource operation (confirmed: no api_op_ListTagsForResource.go
	// in aws-sdk-go-v2/service/ram@v1.39.4) -- a real client reads a resource share's
	// tags back via GetResourceShares' ResourceShare.Tags field instead.
	tagged, err := client.GetResourceShares(ctx, &ramsdk.GetResourceSharesInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, tagged.ResourceShares, 1)
	require.Len(t, tagged.ResourceShares[0].Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagged.ResourceShares[0].Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(tagged.ResourceShares[0].Tags[0].Value))

	_, err = client.UntagResource(ctx, &ramsdk.UntagResourceInput{
		ResourceShareArn: aws.String(shareARN),
		TagKeys:          []string{"env"},
	})
	require.NoError(t, err)

	taggedAfter, err := client.GetResourceShares(ctx, &ramsdk.GetResourceSharesInput{
		ResourceOwner:     ramtypes.ResourceOwnerSelf,
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err)
	require.Len(t, taggedAfter.ResourceShares, 1)
	assert.Empty(t, taggedAfter.ResourceShares[0].Tags)
}
