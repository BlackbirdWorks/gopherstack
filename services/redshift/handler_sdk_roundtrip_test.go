package redshift_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// newTestRedshiftClient stands up the real aws-sdk-go-v2 Redshift client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK deserializer (rather than string-matching the raw
// XML body) is what proves a response is wire-compatible: unrecognized list
// wrapper/element names are skipped silently by the deserializer rather than
// erroring, so a plausible-looking response can still decode to an empty
// slice.
func newTestRedshiftClient(t *testing.T, h *redshift.Handler) *redshiftsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return redshiftsdk.NewFromConfig(cfg, func(o *redshiftsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

const rtTestRegion = "us-east-1"

// TestSDKRoundTrip_ListWrapperFixes covers six independent list-decoding
// bugs found by diffing every gopherstack redshift XML list tag against the
// pinned SDK's deserializer (redshift@v1.65.4): each handler wrapped list
// entries in the wrong element name, so a real client always saw an empty
// slice regardless of what the backend actually stored.
func TestSDKRoundTrip_ListWrapperFixes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client)
		name string
	}{
		{testDescribeCustomDomainAssociations, "describe custom domain associations"},
		{testDescribeSnapshotSchedulesAssociatedClusters, "describe snapshot schedules associated clusters"},
		{testDescribeAuthenticationProfiles, "describe authentication profiles"},
		{testDescribeDataShares, "describe data shares"},
		{testDescribeEndpointAuthorization, "describe endpoint authorization"},
		{testDescribeUsageLimits, "describe usage limits"},
		{testDescribeEventCategories, "describe event categories"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
			h := redshift.NewHandler(backend)
			client := newTestRedshiftClient(t, h)

			tc.run(t, backend, client)
		})
	}
}

// testDescribeCustomDomainAssociations: the handler wrapped entries in
// <CustomDomainAssociations><CustomDomainAssociation>, but the real
// deserializer (redshift@v1.65.4 deserializers.go:49708,23893) names the
// field Associations and wraps each entry in <Association>.
func testDescribeCustomDomainAssociations(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-cd-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = backend.CreateCustomDomainAssociation(
		"rt-cd-cluster", "db.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/rt",
	)
	require.NoError(t, err)

	out, err := client.DescribeCustomDomainAssociations(ctx, &redshiftsdk.DescribeCustomDomainAssociationsInput{
		CustomDomainName: aws.String("db.example.com"),
	})
	require.NoError(t, err)
	require.Len(t, out.Associations, 1)
	assert.Equal(
		t,
		"arn:aws:acm:us-east-1:000000000000:certificate/rt",
		aws.ToString(out.Associations[0].CustomDomainCertificateArn),
	)
}

// testDescribeSnapshotSchedulesAssociatedClusters: the handler wrapped
// AssociatedClusters entries in <member>, but the real deserializer
// (redshift@v1.65.4 deserializers.go:23753) wraps each entry in
// <ClusterAssociatedToSchedule>.
func testDescribeSnapshotSchedulesAssociatedClusters(
	t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client,
) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-sched-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = backend.CreateSnapshotSchedule("rt-sched", "roundtrip test", []string{"rate(12 hours)"}, nil)
	require.NoError(t, err)

	require.NoError(t, backend.ModifyClusterSnapshotSchedule("rt-sched-cluster", "rt-sched", false))

	out, err := client.DescribeSnapshotSchedules(ctx, &redshiftsdk.DescribeSnapshotSchedulesInput{
		ScheduleIdentifier: aws.String("rt-sched"),
	})
	require.NoError(t, err)
	require.Len(t, out.SnapshotSchedules, 1)
	require.Len(t, out.SnapshotSchedules[0].AssociatedClusters, 1)
	assert.Equal(t, "rt-sched-cluster", aws.ToString(out.SnapshotSchedules[0].AssociatedClusters[0].ClusterIdentifier))
}

// testDescribeAuthenticationProfiles: the handler wrapped entries in
// <AuthenticationProfile>, but the real deserializer (redshift@v1.65.4
// deserializers.go:24257) wraps each entry in <member>.
func testDescribeAuthenticationProfiles(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateAuthenticationProfile("rt-profile", `{"AllowedAllVPCs":true}`)
	require.NoError(t, err)

	out, err := client.DescribeAuthenticationProfiles(ctx, &redshiftsdk.DescribeAuthenticationProfilesInput{
		AuthenticationProfileName: aws.String("rt-profile"),
	})
	require.NoError(t, err)
	require.Len(t, out.AuthenticationProfiles, 1)
	assert.Equal(t, "rt-profile", aws.ToString(out.AuthenticationProfiles[0].AuthenticationProfileName))
}

// testDescribeDataShares: the handler wrapped entries in <DataShare>, but
// the real deserializer (redshift@v1.65.4 deserializers.go:29079) wraps
// each entry in <member>.
func testDescribeDataShares(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const dataShareArn = "arn:aws:redshift:us-east-1:000000000000:datashare:rt-namespace/rt-share"
	backend.AddDataShareInternal(&redshift.DataShare{
		DataShareArn:  dataShareArn,
		ProducerArn:   "arn:aws:redshift:us-east-1:000000000000:namespace:rt-namespace",
		DataShareType: "INTERNAL",
	})

	out, err := client.DescribeDataShares(ctx, &redshiftsdk.DescribeDataSharesInput{
		DataShareArn: aws.String(dataShareArn),
	})
	require.NoError(t, err)
	require.Len(t, out.DataShares, 1)
	assert.Equal(t, dataShareArn, aws.ToString(out.DataShares[0].DataShareArn))
}

// testDescribeEndpointAuthorization: the handler wrapped entries in
// <EndpointAuthorization>, but the real deserializer (redshift@v1.65.4
// deserializers.go:30628) wraps each entry in <member>.
func testDescribeEndpointAuthorization(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-epauth-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = backend.AuthorizeEndpointAccess("rt-epauth-cluster", "111122223333", nil)
	require.NoError(t, err)

	out, err := client.DescribeEndpointAuthorization(ctx, &redshiftsdk.DescribeEndpointAuthorizationInput{
		ClusterIdentifier: aws.String("rt-epauth-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.EndpointAuthorizationList, 1)
	assert.Equal(t, "111122223333", aws.ToString(out.EndpointAuthorizationList[0].Grantee))
}

// testDescribeUsageLimits: the handler wrapped entries in <UsageLimit>, but
// the real deserializer (redshift@v1.65.4 deserializers.go:45683) wraps
// each entry in <member>.
func testDescribeUsageLimits(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-ul-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = backend.CreateUsageLimit("rt-ul-cluster", "concurrency-scaling", "time", "log", 60, nil)
	require.NoError(t, err)

	out, err := client.DescribeUsageLimits(ctx, &redshiftsdk.DescribeUsageLimitsInput{
		ClusterIdentifier: aws.String("rt-ul-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.UsageLimits, 1)
	assert.Equal(t, "rt-ul-cluster", aws.ToString(out.UsageLimits[0].ClusterIdentifier))
}

// testDescribeEventCategories: the handler emitted a flat EventCategory
// string on each EventCategoriesMap entry, but the real deserializer
// (redshift@v1.65.4 deserializers.go:31075) has no such field -- category
// names live in a nested Events list of EventInfoMap, each carrying its own
// EventCategories. A real client always saw SourceType populated but Events
// permanently empty.
func testDescribeEventCategories(t *testing.T, _ *redshift.InMemoryBackend, client *redshiftsdk.Client) {
	t.Helper()
	ctx := t.Context()

	out, err := client.DescribeEventCategories(ctx, &redshiftsdk.DescribeEventCategoriesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.EventCategoriesMapList)

	var clusterEvents []string
	for _, m := range out.EventCategoriesMapList {
		if aws.ToString(m.SourceType) != "cluster" {
			continue
		}

		for _, ev := range m.Events {
			clusterEvents = append(clusterEvents, ev.EventCategories...)
		}
	}

	assert.Contains(t, clusterEvents, "maintenance")
}

// TestSDKRoundTrip_MutatingOpFixes covers gopherstack-7185: Create/Delete/Modify
// response shapes on classic Redshift, the class the List/Describe sweep never
// checked. Each case fails against the pre-fix code (verified by hand-reverting
// the corresponding source change and rerunning).
func TestSDKRoundTrip_MutatingOpFixes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client)
		name string
	}{
		{testCreateCustomDomainAssociationCertExpiryTime, "create custom domain association cert expiry time"},
		{testModifyCustomDomainAssociationCertExpiryTime, "modify custom domain association cert expiry time"},
		{testCreateSnapshotScheduleTags, "create snapshot schedule tags"},
		{testBatchDeleteClusterSnapshotsRealWireShape, "batch delete cluster snapshots real wire shape"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
			h := redshift.NewHandler(backend)
			client := newTestRedshiftClient(t, h)

			tc.run(t, backend, client)
		})
	}
}

// testCreateCustomDomainAssociationCertExpiryTime: CreateCustomDomainAssociationOutput
// carries CustomDomainCertExpiryTime (confirmed against
// aws-sdk-go-v2/service/redshift@v1.65.4/api_op_CreateCustomDomainAssociation.go),
// but the handler's response struct never had a field for it at all, so a real
// client always decoded an empty string.
func testCreateCustomDomainAssociationCertExpiryTime(
	t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client,
) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-cdexp-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	out, err := client.CreateCustomDomainAssociation(ctx, &redshiftsdk.CreateCustomDomainAssociationInput{
		ClusterIdentifier:          aws.String("rt-cdexp-cluster"),
		CustomDomainName:           aws.String("cdexp.example.com"),
		CustomDomainCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/rt-cdexp"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.CustomDomainCertExpiryTime))
}

// testModifyCustomDomainAssociationCertExpiryTime: same missing member on
// ModifyCustomDomainAssociationOutput.
func testModifyCustomDomainAssociationCertExpiryTime(
	t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client,
) {
	t.Helper()
	ctx := t.Context()

	_, err := backend.CreateCluster("rt-cdexp-mod-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = backend.CreateCustomDomainAssociation(
		"rt-cdexp-mod-cluster", "cdexp-mod.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/rt-old",
	)
	require.NoError(t, err)

	out, err := client.ModifyCustomDomainAssociation(ctx, &redshiftsdk.ModifyCustomDomainAssociationInput{
		ClusterIdentifier:          aws.String("rt-cdexp-mod-cluster"),
		CustomDomainName:           aws.String("cdexp-mod.example.com"),
		CustomDomainCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/rt-new"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.CustomDomainCertExpiryTime))
}

// testCreateSnapshotScheduleTags: CreateSnapshotScheduleOutput carries Tags
// (redshift@v1.65.4 deserializers.go:43027), and this backend already tracks
// SnapshotSchedule.Tags, but xmlSnapshotSchedule had no field for it, so a real
// client's tags on a newly created schedule always decoded to an empty slice.
func testCreateSnapshotScheduleTags(
	t *testing.T, _ *redshift.InMemoryBackend, client *redshiftsdk.Client,
) {
	t.Helper()
	ctx := t.Context()

	out, err := client.CreateSnapshotSchedule(ctx, &redshiftsdk.CreateSnapshotScheduleInput{
		ScheduleIdentifier:  aws.String("rt-sched-tags"),
		ScheduleDefinitions: []string{"rate(12 hours)"},
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
}

// testBatchDeleteClusterSnapshotsRealWireShape: BatchDeleteClusterSnapshotsInput.
// Identifiers is a list of DeleteClusterSnapshotMessage structs, serialized as
// "Identifiers.DeleteClusterSnapshotMessage.N.SnapshotIdentifier" (confirmed
// against aws-sdk-go-v2/service/redshift@v1.65.4/serializers.go). The handler
// previously read "Identifiers.DeleteClusterSnapshotMessage.N" and, failing
// that, "Identifiers.SnapshotIdentifier.N" -- neither is a key any real client
// ever sends, so every real BatchDeleteClusterSnapshots call silently deleted
// nothing while still returning 200 OK.
func testBatchDeleteClusterSnapshotsRealWireShape(
	t *testing.T, backend *redshift.InMemoryBackend, client *redshiftsdk.Client,
) {
	t.Helper()
	ctx := t.Context()

	backend.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "rt-batch-del-1", ClusterIdentifier: "c1", Status: "available"},
	)
	backend.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "rt-batch-del-2", ClusterIdentifier: "c1", Status: "available"},
	)

	out, err := client.BatchDeleteClusterSnapshots(ctx, &redshiftsdk.BatchDeleteClusterSnapshotsInput{
		Identifiers: []types.DeleteClusterSnapshotMessage{
			{SnapshotIdentifier: aws.String("rt-batch-del-1")},
			{SnapshotIdentifier: aws.String("rt-batch-del-2")},
		},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"rt-batch-del-1", "rt-batch-del-2"}, out.Resources)
	assert.Equal(t, 0, redshift.SnapshotCount(backend))
}
