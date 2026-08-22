package dms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// seedFullBackend populates one instance of every store.Table-backed
// resource on b (see store_setup.go), plus tags on a couple of them, so a
// Snapshot/Restore round trip exercises every registered table at least
// once. It returns the identifiers/ARNs the verify step checks for.
func seedFullBackend(t *testing.T, b *dms.InMemoryBackend) map[string]string {
	t.Helper()

	ctx := t.Context()
	ids := make(map[string]string)

	ri, err := b.CreateReplicationInstance(
		ctx,
		"ri-1",
		"dms.t3.micro",
		"",
		"",
		0,
		false,
		false,
		false,
		map[string]string{"env": "test"},
		dms.ReplicationInstanceSettings{},
	)
	require.NoError(t, err)
	ids["replicationInstanceArn"] = ri.ReplicationInstanceArn

	src, err := b.CreateEndpoint(
		ctx,
		"ep-src",
		"source",
		"mysql",
		"",
		"",
		"",
		"",
		0,
		nil,
		dms.EndpointConnectionSettings{},
	)
	require.NoError(t, err)
	ids["sourceEndpointArn"] = src.EndpointArn

	tgt, err := b.CreateEndpoint(
		ctx,
		"ep-tgt",
		"target",
		"mysql",
		"",
		"",
		"",
		"",
		0,
		nil,
		dms.EndpointConnectionSettings{},
	)
	require.NoError(t, err)
	ids["targetEndpointArn"] = tgt.EndpointArn

	rt, err := b.CreateReplicationTask(
		ctx,
		"task-1",
		src.EndpointArn,
		tgt.EndpointArn,
		ri.ReplicationInstanceArn,
		"full-load",
		"",
		"",
		nil,
	)
	require.NoError(t, err)
	ids["replicationTaskArn"] = rt.ReplicationTaskArn

	dm, err := b.CreateDataMigration(ctx, "dm-1", "", "full-load", "", "", 0, false, nil)
	require.NoError(t, err)
	ids["dataMigrationArn"] = dm.DataMigrationArn

	dp, err := b.CreateDataProvider(ctx, "dp-1", "mysql", "", nil)
	require.NoError(t, err)
	ids["dataProviderArn"] = dp.DataProviderArn

	_, err = b.CreateEventSubscription(
		ctx,
		"es-1",
		"arn:aws:sns:us-east-1:123456789012:topic",
		"",
		nil,
		nil,
		true,
		nil,
	)
	require.NoError(t, err)

	col, err := b.CreateFleetAdvisorCollector(ctx, "col-1", "", "", "")
	require.NoError(t, err)
	ids["collectorReferencedID"] = col.CollectorReferencedID

	ip, err := b.CreateInstanceProfile(ctx, "ip-1", "", "", "", "", "", false, nil)
	require.NoError(t, err)
	ids["instanceProfileArn"] = ip.InstanceProfileArn

	cert, err := b.ImportCertificate(ctx, "cert-1", "-----BEGIN CERTIFICATE-----")
	require.NoError(t, err)
	ids["certificateArn"] = cert.CertificateArn

	mp, err := b.CreateMigrationProject(ctx, "mp-1", "", ip.InstanceProfileName,
		[]dms.DataProviderDescriptorInput{{DataProviderIdentifier: dp.DataProviderName}},
		[]dms.DataProviderDescriptorInput{{DataProviderIdentifier: dp.DataProviderName}},
		nil)
	require.NoError(t, err)
	ids["migrationProjectArn"] = mp.MigrationProjectArn

	sg, err := b.CreateReplicationSubnetGroup(ctx, "sg-1", "", "vpc-1", nil)
	require.NoError(t, err)
	ids["subnetGroupArn"] = sg.ReplicationSubnetGroupArn

	maxCapacityUnits := int32(4)
	rc, err := b.CreateReplicationConfig(ctx, dms.CreateReplicationConfigParams{
		Identifier:        "rc-1",
		ReplicationType:   "full-load",
		SourceEndpointArn: src.EndpointArn,
		TargetEndpointArn: tgt.EndpointArn,
		TableMappings:     `{"rules":[]}`,
		ComputeConfig:     &dms.ComputeConfig{MaxCapacityUnits: &maxCapacityUnits},
	}, nil)
	require.NoError(t, err)
	ids["replicationConfigArn"] = rc.ReplicationConfigArn

	_, err = b.TestConnection(ctx, ri.ReplicationInstanceArn, src.EndpointArn)
	require.NoError(t, err)

	_, err = b.StartAssessmentRun(ctx, rt.ReplicationTaskArn, "", "", "run-1")
	require.NoError(t, err)

	reqID, err := b.StartMetadataModelRequest(ctx, mp.MigrationProjectName, "assessment", "", "")
	require.NoError(t, err)
	ids["metadataModelRequestID"] = reqID

	require.NoError(
		t,
		b.AddTagsToResource(
			ctx,
			ri.ReplicationInstanceArn,
			map[string]string{"owner": "phase-3.3"},
		),
	)

	return ids
}

// TestInMemoryBackend_SnapshotRestore is the full-state Snapshot->Restore
// round-trip test required by Phase 3.3 (see store_setup.go/persistence.go):
// every store.Table-backed resource, its secondary indexes, and its
// backend-owned Tags must survive a Snapshot()->Restore() cycle onto a fresh
// backend, and the request-scoped raw (never-persisted) state must not leak
// across the boundary.
func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	original := dms.NewInMemoryBackend("123456789012", "us-east-1")
	ids := seedFullBackend(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := dms.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	ctx := t.Context()

	ris, err := fresh.DescribeReplicationInstances(ctx, "")
	require.NoError(t, err)
	require.Len(t, ris, 1)
	assert.Equal(t, ids["replicationInstanceArn"], ris[0].ReplicationInstanceArn)
	// Tags is `json:"-"` -- backend-owned live state, deliberately excluded
	// from the snapshot both before and after Phase 3.3 (see the doc comment
	// on ReplicationInstance.Tags and reinitTagsLocked) -- so tag VALUES set
	// before the snapshot are expected to be lost. What must hold is that
	// Tags itself is re-initialised to a fresh, usable (non-nil) registry
	// rather than left nil, matching the pre-Phase-3.3 rebuildRI behavior.
	require.NotNil(t, ris[0].Tags, "Tags must be re-initialised after restore")
	_, ok := ris[0].Tags.Get("owner")
	assert.False(t, ok, "tag values are not expected to survive restore (Tags is json:\"-\")")

	// Lookup by ARN (byARN index) must also work post-restore.
	byARN, err := fresh.DescribeReplicationInstances(ctx, ids["replicationInstanceArn"])
	require.NoError(t, err)
	require.Len(t, byARN, 1)

	eps, err := fresh.DescribeEndpoints(ctx, "")
	require.NoError(t, err)
	assert.Len(t, eps, 2)

	tasks, err := fresh.DescribeReplicationTasks(ctx, "")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, ids["replicationTaskArn"], tasks[0].ReplicationTaskArn)

	dataMigrations, err := fresh.DescribeDataMigrations(ctx, "")
	require.NoError(t, err)
	require.Len(t, dataMigrations, 1)
	assert.Equal(t, ids["dataMigrationArn"], dataMigrations[0].DataMigrationArn)

	dps, err := fresh.DescribeDataProviders(ctx, "")
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.Equal(t, ids["dataProviderArn"], dps[0].DataProviderArn)

	subs, err := fresh.DescribeEventSubscriptions(ctx, "")
	require.NoError(t, err)
	assert.Len(t, subs, 1)

	cols, err := fresh.DescribeFleetAdvisorCollectors(ctx)
	require.NoError(t, err)
	require.Len(t, cols, 1)
	assert.Equal(t, ids["collectorReferencedID"], cols[0].CollectorReferencedID)

	// fleetAdvisorDatabases is seeded (2 per collector) by
	// CreateFleetAdvisorCollector and must survive restore too.
	dbs, err := fresh.DescribeFleetAdvisorDatabases(ctx)
	require.NoError(t, err)
	assert.Len(t, dbs, 2)

	ips, err := fresh.DescribeInstanceProfiles(ctx)
	require.NoError(t, err)
	require.Len(t, ips, 1)
	assert.Equal(t, ids["instanceProfileArn"], ips[0].InstanceProfileArn)

	certs, err := fresh.DescribeCertificates(ctx)
	require.NoError(t, err)
	require.Len(t, certs, 1)
	assert.Equal(t, ids["certificateArn"], certs[0].CertificateArn)

	mps, err := fresh.DescribeMigrationProjects(ctx)
	require.NoError(t, err)
	require.Len(t, mps, 1)
	assert.Equal(t, ids["migrationProjectArn"], mps[0].MigrationProjectArn)

	sgs, err := fresh.DescribeReplicationSubnetGroups(ctx)
	require.NoError(t, err)
	require.Len(t, sgs, 1)
	assert.Equal(t, ids["subnetGroupArn"], sgs[0].ReplicationSubnetGroupArn)

	rcs, err := fresh.DescribeReplicationConfigs(ctx)
	require.NoError(t, err)
	require.Len(t, rcs, 1)
	assert.Equal(t, ids["replicationConfigArn"], rcs[0].ReplicationConfigArn)
	assert.JSONEq(t, `{"rules":[]}`, rcs[0].TableMappings)
	require.NotNil(t, rcs[0].ComputeConfig)
	require.NotNil(t, rcs[0].ComputeConfig.MaxCapacityUnits)
	assert.Equal(t, int32(4), *rcs[0].ComputeConfig.MaxCapacityUnits)

	conns, err := fresh.DescribeConnections(ctx, "", "")
	require.NoError(t, err)
	assert.Len(t, conns, 1)

	runs, err := fresh.DescribeAssessmentRuns(ctx, "")
	require.NoError(t, err)
	assert.Len(t, runs, 1)

	reqs, err := fresh.ListMetadataModelRequests(ctx, ids["migrationProjectArn"], "assessment")
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	assert.Equal(t, ids["metadataModelRequestID"], reqs[0].RequestIdentifier)

	// events/recommendations are request-scoped raw state never wired into
	// backendSnapshot (see store_setup.go's registerAllTables doc comment) --
	// they must stay empty after a restore onto a fresh backend, matching
	// pre-Phase-3.3 behavior.
	events, err := fresh.DescribeEvents(ctx)
	require.NoError(t, err)
	assert.Empty(t, events)
}

// TestInMemoryBackend_SnapshotRestore_EmptyBackend exercises the round trip
// with nothing seeded, guarding against a nil-map/nil-slice panic in any
// registered table's Restore path.
func TestInMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := dms.NewInMemoryBackend("123456789012", "us-east-1")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := dms.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	ris, err := fresh.DescribeReplicationInstances(t.Context(), "")
	require.NoError(t, err)
	assert.Empty(t, ris)
}

// TestInMemoryBackend_Restore_IncompatibleVersion asserts that a snapshot
// with a version other than the current dmsSnapshotVersion (including the
// zero value produced by unmarshaling a pre-Phase-3.3, version-less
// snapshot) is discarded cleanly -- the backend resets to empty rather than
// partially decoding stale data -- and does not surface as an error.
func TestInMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	original := dms.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := original.CreateReplicationInstance(
		t.Context(),
		"ri-1",
		"dms.t3.micro",
		"",
		"",
		0,
		false,
		false,
		false,
		nil,
		dms.ReplicationInstanceSettings{},
	)
	require.NoError(t, err)

	// A pre-Phase-3.3 snapshot (or any payload with no "version" field)
	// decodes with Version == 0, which never matches dmsSnapshotVersion.
	legacyPayload := []byte(
		`{"replicationInstances":{},"accountID":"123456789012","region":"us-east-1"}`,
	)

	fresh := dms.NewInMemoryBackend("123456789012", "us-east-1")
	// Seed fresh with a resource first so we can prove the incompatible
	// restore actually resets it rather than leaving it untouched.
	_, err = fresh.CreateReplicationInstance(
		t.Context(),
		"stale",
		"dms.t3.micro",
		"",
		"",
		0,
		false,
		false,
		false,
		nil,
		dms.ReplicationInstanceSettings{},
	)
	require.NoError(t, err)

	require.NoError(t, fresh.Restore(t.Context(), legacyPayload))

	ris, err := fresh.DescribeReplicationInstances(t.Context(), "")
	require.NoError(t, err)
	assert.Empty(t, ris, "incompatible snapshot version must reset to empty, not partially decode")
}

// TestInMemoryBackend_Restore_MalformedData asserts that non-JSON snapshot
// data surfaces as an error rather than panicking or silently ignoring it.
func TestInMemoryBackend_Restore_MalformedData(t *testing.T) {
	t.Parallel()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate confirms Handler.Snapshot/Restore
// delegate to the backend unchanged.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := dms.NewHandler(dms.NewInMemoryBackend("123456789012", "us-east-1"))
	_, err := h.Backend.CreateReplicationInstance(
		t.Context(),
		"ri-1",
		"dms.t3.micro",
		"",
		"",
		0,
		false,
		false,
		false,
		nil,
		dms.ReplicationInstanceSettings{},
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := dms.NewHandler(dms.NewInMemoryBackend("123456789012", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	ris, err := h2.Backend.DescribeReplicationInstances(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, ris, 1)
	assert.Equal(t, "ri-1", ris[0].ReplicationInstanceIdentifier)
}
