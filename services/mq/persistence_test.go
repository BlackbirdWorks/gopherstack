package mq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateBroker(
		"seed-broker", "", mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, mq.BrokerCount(b))
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape, plain
// resource maps) decodes with Version == 0, which mismatches
// mqSnapshotVersion and is discarded the same way any other incompatible
// version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateBroker(
		"seed-broker", "", mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	// Pre-Phase-3.3 shape: plain resource maps, no "version"/"tables" keys.
	err = b.Restore(t.Context(), []byte(`{"brokers":{"b1":{"brokerId":"b1","brokerName":"old"}}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, mq.BrokerCount(b))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (brokers, configurations) plus the raw map left
// un-converted (tags), and verifies the shared-pointer invariant between
// b.tags[arn] and the owning resource's Tags field is re-established.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := mq.NewInMemoryBackend("111122223333", "us-west-2")

	br, err := original.CreateBrokerWithOptions(
		"broker-1", mq.DeploymentModeSingleInstance, mq.EngineTypeActiveMQ, "", "",
		true, true, []string{"sg-1"}, []string{"subnet-1"},
		[]*mq.User{{Username: "admin", Password: "supersecretpassword1", Console: true}},
		map[string]string{"env": "test"},
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, original.CreateUser(br.BrokerID, "second", "anothersecretpassword2", nil, false))

	cfg, err := original.CreateConfiguration(
		"config-1", "initial config", mq.EngineTypeActiveMQ, "", map[string]string{"team": "infra"},
	)
	require.NoError(t, err)

	_, err = original.UpdateConfiguration(cfg.ID, "second revision", "ZGF0YQ==")
	require.NoError(t, err)

	standaloneARN := "arn:aws:mq:us-west-2:111122223333:standalone-tag"
	require.NoError(t, original.CreateTags(standaloneARN, map[string]string{"k": "v"}))

	wantBrokers := mq.BrokerCount(original)
	wantConfigurations := mq.ConfigurationCount(original)
	wantTags := mq.TagCount(original)

	data := original.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restored := mq.NewInMemoryBackend("000000000000", "unset")
	require.NoError(t, restored.Restore(t.Context(), data))

	assert.Equal(t, wantBrokers, mq.BrokerCount(restored))
	assert.Equal(t, wantConfigurations, mq.ConfigurationCount(restored))
	assert.Equal(t, wantTags, mq.TagCount(restored))
	assert.Equal(t, "111122223333", restored.AccountID())
	assert.Equal(t, "us-west-2", restored.Region())

	restoredBroker, err := restored.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "broker-1", restoredBroker.BrokerName)
	assert.Equal(t, map[string]string{"env": "test"}, restoredBroker.Tags)
	// Broker.Users now carries a real json tag (see store_setup.go /
	// models.go) and round-trips through Snapshot/Restore: both users must
	// still be present, with their structural attributes intact. Password
	// deliberately keeps json:"-" (matching the LdapServerMetadata.
	// ServiceAccountPassword precedent of keeping secrets out of the
	// persisted blob), so a restored user's password is always blank.
	require.Len(t, restoredBroker.Users, 2)
	require.Contains(t, restoredBroker.Users, "admin")
	assert.True(t, restoredBroker.Users["admin"].Console)
	assert.Empty(t, restoredBroker.Users["admin"].Password)
	require.Contains(t, restoredBroker.Users, "second")

	restoredCfg, err := restored.DescribeConfiguration(cfg.ID)
	require.NoError(t, err)
	assert.Equal(t, "config-1", restoredCfg.Name)
	assert.Equal(t, map[string]string{"team": "infra"}, restoredCfg.Tags)
	// Configuration.Data/Revisions now carry real json tags too: the second
	// revision's content and both revisions' metadata must survive restore.
	require.Len(t, restoredCfg.Revisions, 2)
	assert.Equal(t, int32(2), restoredCfg.LatestRevision.Revision)
	assert.Equal(t, "ZGF0YQ==", restoredCfg.Data[2])

	// Shared-pointer invariant: b.tags[arn] and resource.Tags must reflect
	// the same content after restore, exactly as CreateTags/DeleteTags rely
	// on pre-restore.
	assert.Equal(t, restoredBroker.Tags, restored.ListTags(restoredBroker.BrokerArn))
	assert.Equal(t, restoredCfg.Tags, restored.ListTags(restoredCfg.Arn))
	assert.Equal(t, map[string]string{"k": "v"}, restored.ListTags(standaloneARN))
}

// TestPersistenceRoundTrip exercises a broker + configuration snapshot/restore
// round trip via the public API, including re-syncing tags after restore.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	br, err := b.CreateBroker(
		"persist-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeRabbitMQ, "3.13.2", "mq.m5.large",
		true, false, nil, nil, nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	cfg, err := b.CreateConfiguration(
		"persist-cfg",
		"desc",
		mq.EngineTypeActiveMQ,
		"5.18.3",
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, mq.BrokerCount(b2))
	assert.Equal(t, 1, mq.ConfigurationCount(b2))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "persist-broker", restored.BrokerName)
	assert.Equal(t, map[string]string{"env": "test"}, restored.Tags)

	rcfg, err := b2.DescribeConfiguration(cfg.ID)
	require.NoError(t, err)
	assert.Equal(t, "persist-cfg", rcfg.Name)

	// Tags should still be in sync after Restore.
	b2.CreateTags(br.BrokerArn, map[string]string{"new-key": "new-val"})

	restored2, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "new-val", restored2.Tags["new-key"])
}

// TestTagsSync_AfterRestore verifies that CreateTags via a broker's ARN
// remains reflected in DescribeBroker after a snapshot/restore cycle.
func TestTagsSync_AfterRestore(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBroker(
		"tag-sync-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil,
		map[string]string{"original": "yes"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// CreateTags via ARN should be reflected in DescribeBroker.
	b2.CreateTags(br.BrokerArn, map[string]string{"added": "after-restore"})

	got, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "yes", got.Tags["original"])
	assert.Equal(t, "after-restore", got.Tags["added"])
}
