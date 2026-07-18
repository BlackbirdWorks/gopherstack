package directoryservice_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestInMemoryBackend_SnapshotRestore_FullState is the Phase 3.3 full-state
// round-trip test: it exercises every store.Table the region-qualified-table
// conversion touched (directories, snapshots, dsRegions, schemaExtensions,
// conditionalForwarders, logSubscriptions, eventTopics, domainControllers,
// trusts, sharedDirectories, certificates, ldapsSettings, clientAuthSettings,
// radiusSettings, adAssessments, hybridADUpdates) plus every raw region-nested
// map left un-converted (aliases, ipRoutes, dirDataAccess, caEnrollment,
// dirSettings, updateInfoEntries), to guard against a silent persistence
// regression from the map -> pkgs/store conversion.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	original := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")

	dir, err := original.CreateDirectory(
		ctx, "corp.example.com", "CORP", "primary directory", "Password123!",
		directoryservice.DirectorySizeSmall, nil, nil,
	)
	require.NoError(t, err)
	dirID := dir.DirectoryID

	// aliases (raw map): change the alias so the reverse-index (alias ->
	// directoryID) actually differs from the directory's own auto-alias.
	require.NoError(t, original.CreateAlias(ctx, dirID, "corp-alias"))

	// snapshots
	_, err = original.CreateSnapshot(ctx, dirID, "manual-snap")
	require.NoError(t, err)

	// ipRoutes (raw map)
	require.NoError(t, original.AddIpRoutes(ctx, dirID, []directoryservice.IpRoute{
		{CidrIP: "10.0.0.0/24", Description: "office"},
	}))

	// dsRegions
	require.NoError(t, original.AddRegion(ctx, dirID, "us-west-2"))

	// schemaExtensions
	_, err = original.StartSchemaExtension(ctx, dirID, "add attr", "dn: cn=schema")
	require.NoError(t, err)

	// conditionalForwarders
	require.NoError(t, original.CreateConditionalForwarder(ctx, dirID, "remote.example.com", []string{"10.1.1.1"}))

	// logSubscriptions
	require.NoError(t, original.CreateLogSubscription(ctx, dirID, "/aws/directoryservice/corp"))

	// eventTopics
	require.NoError(t, original.RegisterEventTopic(ctx, dirID, "corp-topic"))

	// domainControllers
	require.NoError(t, original.UpdateNumberOfDomainControllers(ctx, dirID, 2))

	// trusts
	trustID, err := original.CreateTrust(ctx, dirID, "trusted.example.com", "TrustPW1!", "Two-Way", "Forest")
	require.NoError(t, err)

	// sharedDirectories
	_, err = original.ShareDirectory(ctx, dirID, "HANDSHAKE", "please accept", "222222222222")
	require.NoError(t, err)

	// certificates
	certID, err := original.RegisterCertificate(ctx, dirID, "cert-data", "ClientCertAuth")
	require.NoError(t, err)

	// ldapsSettings
	require.NoError(t, original.EnableLDAPS(ctx, dirID, "Client"))

	// clientAuthSettings
	require.NoError(t, original.EnableClientAuthentication(ctx, dirID, "SmartCard"))

	// radiusSettings (no public Describe; verified via export_test helper)
	require.NoError(t, original.EnableRadius(ctx, dirID, directoryservice.RadiusSettingsInput{
		AuthenticationProtocol: "PAP",
		DisplayLabel:           "corp-radius",
		SharedSecret:           "s3cr3t",
		RadiusServers:          []string{"10.2.2.2"},
		RadiusPort:             1812,
		RadiusRetries:          3,
		RadiusTimeout:          5,
	}))

	// dirDataAccess (raw map)
	require.NoError(t, original.EnableDirectoryDataAccess(ctx, dirID))

	// caEnrollment (raw map)
	require.NoError(t, original.EnableCAEnrollmentPolicy(ctx, dirID))

	// adAssessments
	assessmentID, err := original.StartADAssessment(ctx, dirID)
	require.NoError(t, err)

	// dirSettings (raw map)
	_, err = original.UpdateSettings(ctx, dirID, []directoryservice.DirectorySetting{
		{Name: "TLS_1_0", Value: "Disable"},
	})
	require.NoError(t, err)

	// updateInfoEntries (raw map)
	require.NoError(t, original.UpdateDirectorySetup(ctx, dirID, "OS", false))

	// hybridADUpdates
	hybridDir, hybridRequestID, err := original.CreateHybridAD(
		ctx, "hybrid.example.com", "HYBRID", "hybrid directory", "Password123!",
		directoryservice.DirectoryEditionEnterprise, nil,
	)
	require.NoError(t, err)

	// Snapshot the full backend and restore it into a fresh one.
	snap := original.BackendSnapshot()
	require.NotEmpty(t, snap)

	restored := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(ctx, snap))

	assertCoreStateRestored(t, restored, dirID, "corp-alias")
	assertForwardingStateRestored(t, restored, dirID)
	assertTrustStateRestored(t, restored, dirID, trustID, certID)
	assertSettingsStateRestored(t, restored, dirID, assessmentID)
	assertHybridStateRestored(t, restored, hybridDir.DirectoryID, hybridRequestID)
}

// assertCoreStateRestored verifies directories, snapshots, aliases and
// ipRoutes survived the round trip. Split out of the main test to keep it
// readable at this resource count.
func assertCoreStateRestored(t *testing.T, b *directoryservice.InMemoryBackend, dirID, alias string) {
	t.Helper()

	ctx := t.Context()

	dirs, _, err := b.DescribeDirectories(ctx, []string{dirID}, 0, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	assert.Equal(t, alias, dirs[0].Alias)

	// The reverse alias index (raw map) must also have survived: creating a
	// second directory and trying to steal the same alias must still fail.
	other, err := b.CreateDirectory(
		ctx, "other.example.com", "OTHER", "", "Password123!", directoryservice.DirectorySizeSmall, nil, nil,
	)
	require.NoError(t, err)
	err = b.CreateAlias(ctx, other.DirectoryID, alias)
	require.ErrorIs(t, err, directoryservice.ErrAliasAlreadyExists)

	snaps, _, err := b.DescribeSnapshots(ctx, dirID, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	assert.Equal(t, "manual-snap", snaps[0].Name)

	routes, _, err := b.ListIpRoutes(ctx, dirID, 0, "")
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "10.0.0.0/24", routes[0].CidrIP)
}

// assertForwardingStateRestored verifies dsRegions, schemaExtensions,
// conditionalForwarders, logSubscriptions and eventTopics survived the round
// trip.
func assertForwardingStateRestored(t *testing.T, b *directoryservice.InMemoryBackend, dirID string) {
	t.Helper()

	ctx := t.Context()

	regions, _, err := b.DescribeRegions(ctx, dirID, "", "")
	require.NoError(t, err)
	require.Len(t, regions, 1)
	assert.Equal(t, "us-west-2", regions[0].RegionName)

	extensions, _, err := b.ListSchemaExtensions(ctx, dirID, 0, "")
	require.NoError(t, err)
	require.Len(t, extensions, 1)
	assert.Equal(t, "add attr", extensions[0].Description)

	forwarders, err := b.DescribeConditionalForwarders(ctx, dirID, nil)
	require.NoError(t, err)
	require.Len(t, forwarders, 1)
	assert.Equal(t, "remote.example.com", forwarders[0].RemoteDomainName)

	subs, _, err := b.ListLogSubscriptions(ctx, dirID, 0, "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "/aws/directoryservice/corp", subs[0].LogGroupName)

	topics, err := b.DescribeEventTopics(ctx, dirID, nil)
	require.NoError(t, err)
	require.Len(t, topics, 1)
	assert.Equal(t, "corp-topic", topics[0].TopicName)

	controllers, _, err := b.DescribeDomainControllers(ctx, dirID, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, controllers, 2)
}

// assertTrustStateRestored verifies trusts, sharedDirectories, certificates
// and ldapsSettings survived the round trip.
func assertTrustStateRestored(t *testing.T, b *directoryservice.InMemoryBackend, dirID, trustID, certID string) {
	t.Helper()

	ctx := t.Context()

	trusts, _, err := b.DescribeTrusts(ctx, dirID, []string{trustID}, 0, "")
	require.NoError(t, err)
	require.Len(t, trusts, 1)
	assert.Equal(t, "trusted.example.com", trusts[0].RemoteDomainName)

	shared, _, err := b.DescribeSharedDirectories(ctx, dirID, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, shared, 1)
	assert.Equal(t, "222222222222", shared[0].SharedAccountID)

	cert, err := b.DescribeCertificate(ctx, dirID, certID)
	require.NoError(t, err)
	assert.Equal(t, "cert-data", cert.CertData)

	ldaps, _, err := b.DescribeLDAPSSettings(ctx, dirID, "", 0, "")
	require.NoError(t, err)
	require.Len(t, ldaps, 1)
	assert.Equal(t, "Client", ldaps[0].LDAPSType)
}

// assertSettingsStateRestored verifies clientAuthSettings, radiusSettings,
// dirDataAccess, caEnrollment, adAssessments, dirSettings and
// updateInfoEntries survived the round trip.
func assertSettingsStateRestored(t *testing.T, b *directoryservice.InMemoryBackend, dirID, assessmentID string) {
	t.Helper()

	ctx := t.Context()

	auths, _, err := b.DescribeClientAuthenticationSettings(ctx, dirID, "", 0, "")
	require.NoError(t, err)
	require.Len(t, auths, 1)
	assert.Equal(t, "SmartCard", auths[0].AuthType)

	protocol, ok := directoryservice.RadiusAuthProtocolForTest(b, dirID)
	require.True(t, ok)
	assert.Equal(t, "PAP", protocol)

	access, err := b.DescribeDirectoryDataAccess(ctx, dirID)
	require.NoError(t, err)
	assert.True(t, access.Enabled)

	policy, err := b.DescribeCAEnrollmentPolicy(ctx, dirID)
	require.NoError(t, err)
	assert.True(t, policy.Enabled)

	assessment, err := b.DescribeADAssessment(ctx, dirID, assessmentID)
	require.NoError(t, err)
	assert.Equal(t, "Operational", assessment.AssessType)

	settings, _, err := b.DescribeSettings(ctx, dirID, "", "")
	require.NoError(t, err)
	require.Len(t, settings, 1)
	assert.Equal(t, "TLS_1_0", settings[0].Name)

	updates, _, err := b.DescribeUpdateDirectory(ctx, dirID, "", "")
	require.NoError(t, err)
	require.Len(t, updates, 1)
	assert.Equal(t, "OS", updates[0].UpdateType)
}

// assertHybridStateRestored verifies the hybridADUpdates table survived the
// round trip.
func assertHybridStateRestored(t *testing.T, b *directoryservice.InMemoryBackend, hybridDirID, hybridRequestID string) {
	t.Helper()

	updates, err := b.DescribeHybridADUpdate(t.Context(), hybridDirID)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	assert.Equal(t, hybridRequestID, updates[0].RequestID)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDirectory(
		ctx, "seed.example.com", "SEED", "", "Password123!", directoryservice.DirectorySizeSmall, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, directoryservice.DirectoryCount(b))

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, directoryservice.DirectoryCount(b))
	assert.Equal(t, 0, directoryservice.SnapshotCount(b))
}

// TestInMemoryBackend_RestoreInvalidJSON verifies that malformed snapshot
// data surfaces as an error from Restore (via persistence.UnmarshalSnapshot)
// rather than being silently discarded or panicking.
func TestInMemoryBackend_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestBackendSnapshotSerializesAllState verifies that BackendSnapshot/Restore round-trips
// region state beyond directories/snapshots/aliases (trusts, log subscriptions, etc.).
func TestBackendSnapshotSerializesAllState(t *testing.T) {
	t.Parallel()

	b := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	h := directoryservice.NewHandler(b)

	// Create a directory and wait for Active.
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")
	require.True(t, directoryservice.WaitForDirectoryActive(b, dirID, 2*time.Second))

	// Add a log subscription (Appendix-A state not serialized previously).
	logRec := doRequest(t, h, "CreateLogSubscription", map[string]any{
		"DirectoryId":  dirID,
		"LogGroupName": "/aws/directoryservice/testgroup",
	})
	require.Equal(t, http.StatusOK, logRec.Code)

	// Snapshot and restore into a fresh backend.
	snap := b.BackendSnapshot()
	require.NotEmpty(t, snap)

	b2 := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	err := b2.Restore(context.Background(), snap)
	require.NoError(t, err)

	h2 := directoryservice.NewHandler(b2)

	// Log subscription must survive round-trip.
	listRec := doRequest(t, h2, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, listRec.Code)
	body := respBody(t, listRec)
	subs, _ := body["LogSubscriptions"].([]any)
	assert.Len(t, subs, 1, "log subscription must survive BackendSnapshot/Restore")
}
