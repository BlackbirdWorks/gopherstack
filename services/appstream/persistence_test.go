package appstream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (stacks, fleets, appBlocks, appBlockBuilders,
// applications, entitlements, directoryConfigs, images, imagePermissions,
// imageBuilders, exportTasks, users, sessions, themes) plus every raw map
// left un-converted (associations, tags, appBlockBuilderAssoc,
// appFleetAssoc, entitlementApps, softwareAssoc, userStackAssoc) and the
// scalar usageReport field, so a Snapshot from it exercises the entire
// persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) *appstream.InMemoryBackend {
	t.Helper()

	b := appstream.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateStack("stack1", "Stack One", "a stack", map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateFleet(
		"fleet1", "Fleet One", "a fleet", "stream.standard.medium", "ON_DEMAND", "image1", "",
		1, 0, 0, 0, nil, map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	require.NoError(t, b.AssociateFleet("fleet1", "stack1"))

	_, err = b.CreateAppBlock("appblock1", "an app block", map[string]string{"a": "b"})
	require.NoError(t, err)

	_, err = b.CreateAppBlockBuilder("builder1", "a builder", "WINDOWS", "stream.standard.medium", nil)
	require.NoError(t, err)

	require.NoError(t, b.AssociateAppBlockBuilderAppBlock("builder1", "appblock1"))

	_, err = b.CreateApplication(
		"app1", "App One", "an app", "C:\\app.exe", "", []string{"WINDOWS"}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, b.AssociateApplicationFleet("app1", "fleet1"))

	_, err = b.CreateEntitlement("ent1", "stack1", "an entitlement", "ALL", nil)
	require.NoError(t, err)

	require.NoError(t, b.AssociateApplicationToEntitlement("app1", "ent1", "stack1"))

	_, err = b.CreateDirectoryConfig("dir1", []string{"OU=test,DC=example,DC=com"})
	require.NoError(t, err)

	_, err = b.CreateImportedImage("image1", "an image", nil)
	require.NoError(t, err)

	require.NoError(t, b.UpdateImagePermissions("image1", "111111111111", true, false))

	_, err = b.CreateImageBuilder("imgbuilder1", "an image builder", "WINDOWS", "stream.standard.medium", nil)
	require.NoError(t, err)

	require.NoError(t, b.AssociateSoftwareToImageBuilder("imgbuilder1", []string{"sw1"}))

	_, err = b.CreateExportImageTask("image1", "bucket1", "prefix1/")
	require.NoError(t, err)

	_, err = b.CreateUser("user1", "user1@example.com", "First", "Last", "USERPOOL")
	require.NoError(t, err)

	_, err = b.BatchAssociateUserStack([]appstream.UserStackAssociation{
		{UserName: "user1", StackName: "stack1", AuthenticationType: "USERPOOL"},
	})
	require.NoError(t, err)

	_, err = b.CreateStreamingURL("stack1", "fleet1", "user1")
	require.NoError(t, err)

	_, err = b.CreateUsageReportSubscription("DAILY", "usage-bucket")
	require.NoError(t, err)

	_, err = b.CreateThemeForStack("stack1")
	require.NoError(t, err)

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table and every raw map left un-converted (see
// newPersistenceTestBackend) through Snapshot -> Restore into a fresh
// backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertRestoredCoreTables(t, fresh)
	assertRestoredAssociations(t, fresh)
	assertRestoredCountersAndScalar(t, fresh)
}

// assertRestoredCoreTables checks the store.Table-backed collections that
// don't participate in a many-to-many association (stacks, fleets,
// appBlocks, appBlockBuilders, applications, directoryConfigs, images,
// imageBuilders, users, themes).
func assertRestoredCoreTables(t *testing.T, fresh *appstream.InMemoryBackend) {
	t.Helper()

	stacks, err := fresh.DescribeStacks([]string{"stack1"})
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "test", stacks[0].Tags["env"])

	fleets, err := fresh.DescribeFleets([]string{"fleet1"})
	require.NoError(t, err)
	require.Len(t, fleets, 1)
	assert.Equal(t, "v", fleets[0].Tags["k"])

	appBlocks, err := fresh.DescribeAppBlocks([]string{"appblock1"})
	require.NoError(t, err)
	require.Len(t, appBlocks, 1)

	builders, err := fresh.DescribeAppBlockBuilders([]string{"builder1"})
	require.NoError(t, err)
	require.Len(t, builders, 1)

	apps, err := fresh.DescribeApplications([]string{"app1"})
	require.NoError(t, err)
	require.Len(t, apps, 1)

	dirConfigs, err := fresh.DescribeDirectoryConfigs([]string{"dir1"})
	require.NoError(t, err)
	require.Len(t, dirConfigs, 1)
	assert.Equal(t, []string{"OU=test,DC=example,DC=com"}, dirConfigs[0].OrganizationalUnitDistinguishedNames)

	images, err := fresh.DescribeImages([]string{"image1"})
	require.NoError(t, err)
	require.Len(t, images, 1)

	imgBuilders, err := fresh.DescribeImageBuilders([]string{"imgbuilder1"})
	require.NoError(t, err)
	require.Len(t, imgBuilders, 1)

	users, err := fresh.DescribeUsers("USERPOOL")
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "user1@example.com", users[0].Email)

	theme, err := fresh.DescribeThemeForStack("stack1")
	require.NoError(t, err)
	assert.Equal(t, "stack1", theme.StackName)

	ents, err := fresh.DescribeEntitlements("ent1", "stack1")
	require.NoError(t, err)
	require.Len(t, ents, 1)

	// imagePermissions (the table given a real ImageName identity field --
	// see storedImagePermissions in backend_image.go).
	perms, err := fresh.DescribeImagePermissions("image1")
	require.NoError(t, err)
	require.Len(t, perms, 1)
	assert.Equal(t, "111111111111", perms[0].SharedAccountID)
	assert.True(t, perms[0].ImagePermissions.AllowFleet)
}

// assertRestoredAssociations checks the seven raw (non-store.Table)
// many-to-many maps plus the tags map, all persisted directly (see
// persistence.go's backendSnapshot doc comment).
func assertRestoredAssociations(t *testing.T, fresh *appstream.InMemoryBackend) {
	t.Helper()

	// associations
	assocFleets, err := fresh.ListAssociatedFleets("stack1")
	require.NoError(t, err)
	assert.Equal(t, []string{"fleet1"}, assocFleets)

	// appBlockBuilderAssoc
	bbAssoc, err := fresh.DescribeAppBlockBuilderAppBlockAssociations("builder1", "")
	require.NoError(t, err)
	require.Len(t, bbAssoc, 1)

	// appFleetAssoc
	appFleetAssoc, err := fresh.DescribeApplicationFleetAssociations("app1", "")
	require.NoError(t, err)
	require.Len(t, appFleetAssoc, 1)

	// entitlementApps
	entApps, err := fresh.ListEntitledApplications("ent1", "stack1")
	require.NoError(t, err)
	require.Len(t, entApps, 1)
	assert.Equal(t, "app1", entApps[0].ApplicationIdentifier)

	// softwareAssoc
	swAssoc, err := fresh.DescribeSoftwareAssociations("imgbuilder1")
	require.NoError(t, err)
	require.Len(t, swAssoc, 1)
	assert.Equal(t, "sw1", swAssoc[0].Software)

	// userStackAssoc
	userStackAssoc, err := fresh.DescribeUserStackAssociations("stack1", "user1", "USERPOOL")
	require.NoError(t, err)
	require.Len(t, userStackAssoc, 1)

	// tags (keyed by ARN, exercised via the stack's ARN)
	stacks, err := fresh.DescribeStacks([]string{"stack1"})
	require.NoError(t, err)
	tags, err := fresh.ListTagsForResource(stacks[0].Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

// assertRestoredCountersAndScalar checks exportTasks/sessions (whose
// identities are derived from exportTaskSeq/sessionSeq counters that must
// themselves survive the round trip) and the scalar usageReport field.
func assertRestoredCountersAndScalar(t *testing.T, fresh *appstream.InMemoryBackend) {
	t.Helper()

	tasks, err := fresh.ListExportImageTasks([]string{"image1"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "export-task-00001", tasks[0].TaskID)

	sessions, err := fresh.DescribeSessions("stack1", "fleet1", "user1")
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	assert.Equal(t, "session-0000000001", sessions[0].ID)

	// exportTaskSeq/sessionSeq must have survived the round trip: the next
	// task/session minted after Restore must not collide with the restored
	// one.
	nextTask, err := fresh.CreateExportImageTask("image1", "bucket1", "prefix1/")
	require.NoError(t, err)
	assert.Equal(t, "export-task-00002", nextTask.TaskID)

	nextURL, err := fresh.CreateStreamingURL("stack1", "fleet1", "user1")
	require.NoError(t, err)
	assert.Contains(t, nextURL, "session-0000000002")

	reports, err := fresh.DescribeUsageReportSubscriptions()
	require.NoError(t, err)
	require.Len(t, reports, 1)
	assert.Equal(t, "usage-bucket", reports[0].S3BucketName)
	assert.Equal(t, "DAILY", reports[0].Schedule)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state (every store.Table
// plus every raw map) and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, appstream.StackCount(b))
	assert.Equal(t, 0, appstream.FleetCount(b))
	assert.Equal(t, 0, appstream.AppBlockCount(b))
	assert.Equal(t, 0, appstream.AppBlockBuilderCount(b))
	assert.Equal(t, 0, appstream.ApplicationCount(b))
	assert.Equal(t, 0, appstream.UserCount(b))
	assert.Equal(t, 0, appstream.ImageCount(b))
	assert.Equal(t, 0, appstream.ImageBuilderCount(b))

	stacks, err := b.DescribeStacks(nil)
	require.NoError(t, err)
	assert.Empty(t, stacks)

	reports, err := b.DescribeUsageReportSubscriptions()
	require.NoError(t, err)
	assert.Empty(t, reports, "usageReport scalar must be cleared on version mismatch")

	_, err = b.ListAssociatedFleets("stack1")
	require.ErrorIs(t, err, appstream.ErrNotFound, "associations raw map must be cleared on version mismatch")
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(newPersistenceTestBackend(t))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	stacks, err := h2.Backend.DescribeStacks([]string{"stack1"})
	require.NoError(t, err)
	require.Len(t, stacks, 1)
}
