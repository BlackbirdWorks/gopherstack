package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestGetImageBlockPublicAccessState_FlatShape_RealClient covers
// gopherstack-6flj (final ec2 Get* remainder): GetImageBlockPublicAccessState,
// EnableImageBlockPublicAccess and DisableImageBlockPublicAccess all wrapped
// their state string one level too deep, as
// <imageBlockPublicAccessState><state>...</state></imageBlockPublicAccessState>.
// The real shape (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentGetImageBlockPublicAccessStateOutput) has
// <imageBlockPublicAccessState> hold the state text directly -- no nested
// <state> child. This is worse than silent-empty: smithy-go's NodeDecoder.Value
// (xml_decoder.go:106) hard-errors when it finds a child element instead of
// char data ("expected value for imageBlockPublicAccessState element, got
// StartElement"), so a real client's call failed outright rather than
// returning a zero value.
func TestGetImageBlockPublicAccessState_FlatShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	enableOut, err := client.EnableImageBlockPublicAccess(t.Context(), &ec2sdk.EnableImageBlockPublicAccessInput{
		ImageBlockPublicAccessState: types.ImageBlockPublicAccessEnabledStateBlockNewSharing,
	})
	require.NoError(t, err, "pre-fix this call hard-errored decoding the nested shape")
	assert.Equal(t,
		types.ImageBlockPublicAccessEnabledStateBlockNewSharing,
		enableOut.ImageBlockPublicAccessState,
	)

	getOut, err := client.GetImageBlockPublicAccessState(
		t.Context(), &ec2sdk.GetImageBlockPublicAccessStateInput{},
	)
	require.NoError(t, err, "pre-fix this call hard-errored decoding the nested shape")
	assert.Equal(t, "block-new-sharing", aws.ToString(getOut.ImageBlockPublicAccessState))

	disableOut, err := client.DisableImageBlockPublicAccess(
		t.Context(), &ec2sdk.DisableImageBlockPublicAccessInput{},
	)
	require.NoError(t, err, "pre-fix this call hard-errored decoding the nested shape")
	assert.Equal(t,
		types.ImageBlockPublicAccessDisabledStateUnblocked,
		disableOut.ImageBlockPublicAccessState,
	)
}

// TestGetRouteServerRoutingDatabase_AreRoutesPersisted_RealClient covers
// gopherstack-6flj (g8k9-flavor: backend tracks the data, response never
// emitted it): RouteServer.PersistRoutesState is tracked and settable via
// CreateRouteServer's PersistRoutes action, but
// GetRouteServerRoutingDatabaseOutput.AreRoutesPersisted
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentGetRouteServerRoutingDatabaseOutput's
// "areRoutesPersisted" case) was never emitted at all. A real client's
// AreRoutesPersisted was always the Go zero value (false) regardless of
// whether the route server actually had PersistRoutes enabled.
//
// This also covers an adjacent value bug found while wiring the fix: the
// request-side action enum (RouteServerPersistRoutesAction: "enable"/
// "disable"/"reset", types/enums.go:10663) and the response-side state enum
// (RouteServerPersistRoutesState: "enabled"/"disabled"/...,
// types/enums.go:10685) are different real enums for the same verb, but the
// handler stored the raw request action string as the state unnormalized --
// so DescribeRouteServers/GetRouteServerRoutingDatabase echoed back "enable",
// a value that doesn't exist in the real state enum, instead of "enabled".
func TestGetRouteServerRoutingDatabase_AreRoutesPersisted_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4200000000),
		PersistRoutes: types.RouteServerPersistRoutesActionEnable,
	})
	require.NoError(t, err)
	routeServerID := aws.ToString(created.RouteServer.RouteServerId)

	out, err := client.GetRouteServerRoutingDatabase(t.Context(), &ec2sdk.GetRouteServerRoutingDatabaseInput{
		RouteServerId: aws.String(routeServerID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.AreRoutesPersisted),
		"AreRoutesPersisted false - pre-fix it was never emitted despite PersistRoutes being enabled")

	disabled, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4200000001),
		PersistRoutes: types.RouteServerPersistRoutesActionDisable,
	})
	require.NoError(t, err)
	disabledID := aws.ToString(disabled.RouteServer.RouteServerId)

	disabledOut, err := client.GetRouteServerRoutingDatabase(t.Context(), &ec2sdk.GetRouteServerRoutingDatabaseInput{
		RouteServerId: aws.String(disabledID),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disabledOut.AreRoutesPersisted))
}

// TestGetManagedPrefixListAssociations_WrapperKey_RealClient covers
// gopherstack-6flj: the response wrapped under "associationSet", a key that
// doesn't exist anywhere in the real schema (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentGetManagedPrefixListAssociationsOutput has
// no case for it) -- the real wrapper is "prefixListAssociationSet". This
// backend doesn't track which resources reference a managed prefix list, so
// the set is always empty either way; a typed-client round-trip can't
// distinguish the two keys since both produce a nil slice. The fix is
// disclosed rather than round-trip tested for that reason -- see the ec2
// batch report for gopherstack-6flj.
func TestGetManagedPrefixListAssociations_WrapperKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	pl, err := client.CreateManagedPrefixList(t.Context(), &ec2sdk.CreateManagedPrefixListInput{
		PrefixListName: aws.String("wire-fixes-mpl"),
		AddressFamily:  aws.String("IPv4"),
		MaxEntries:     aws.Int32(5),
	})
	require.NoError(t, err)

	out, err := client.GetManagedPrefixListAssociations(t.Context(), &ec2sdk.GetManagedPrefixListAssociationsInput{
		PrefixListId: pl.PrefixList.PrefixListId,
	})
	require.NoError(t, err)
	assert.Empty(t, out.PrefixListAssociations, "no associations are tracked by this backend")
}
