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
	// Broker.Users carries json:"-" and was never part of the persisted
	// shape (a pre-existing behavior predating this refactor, preserved
	// as-is by the mechanical map->store.Table swap): it does not
	// round-trip through Snapshot/Restore.
	assert.Empty(t, restoredBroker.Users)

	restoredCfg, err := restored.DescribeConfiguration(cfg.ID)
	require.NoError(t, err)
	assert.Equal(t, "config-1", restoredCfg.Name)
	assert.Equal(t, map[string]string{"team": "infra"}, restoredCfg.Tags)

	// Shared-pointer invariant: b.tags[arn] and resource.Tags must reflect
	// the same content after restore, exactly as CreateTags/DeleteTags rely
	// on pre-restore.
	assert.Equal(t, restoredBroker.Tags, restored.ListTags(restoredBroker.BrokerArn))
	assert.Equal(t, restoredCfg.Tags, restored.ListTags(restoredCfg.Arn))
	assert.Equal(t, map[string]string{"k": "v"}, restored.ListTags(standaloneARN))
}
