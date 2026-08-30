package ce_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// tamperSnapshotVersion decodes a Snapshot's top-level "version" field, sets
// it to newVersion, and re-encodes -- used to simulate a snapshot produced by
// an incompatible (older or newer) build of the backend.
func tamperSnapshotVersion(t *testing.T, snap []byte, newVersion int) []byte {
	t.Helper()

	var raw map[string]any

	require.NoError(t, json.Unmarshal(snap, &raw))

	raw["version"] = newVersion

	out, err := json.Marshal(raw)
	require.NoError(t, err)

	return out
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that Restore discards
// (rather than partially decodes) a snapshot whose version does not match
// ceSnapshotVersion, resetting the backend to an empty state and returning a
// nil error -- an expected, recoverable condition (e.g. upgrading gopherstack
// across a snapshot-format change), not data corruption.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	original := ce.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := original.CreateAnomalyMonitor("VersionMon", "DIMENSIONAL", "SERVICE", nil, nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	tampered := tamperSnapshotVersion(t, snap, 999)

	// The target backend already holds state; a mismatched-version Restore
	// must wipe it, not merge with or ignore the incoming (discarded) data.
	target := ce.NewInMemoryBackend("000000000000", "us-east-1")

	_, err = target.CreateCostCategoryDefinition(
		"PreExisting", "CostCategoryExpression.v1", "", nil, nil, nil, "",
	)
	require.NoError(t, err)

	require.NoError(t, target.Restore(t.Context(), tampered))

	monitors, _, err := target.GetAnomalyMonitors(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, monitors, "mismatched-version snapshot data must not be adopted")

	cats, _ := target.ListCostCategoryDefinitions(0, "")
	assert.Empty(t, cats, "pre-existing state must be reset on version mismatch")
}

// TestInMemoryBackend_RestoreMissingVersion verifies that a pre-Phase-3.3
// snapshot with no "version" field at all (decodes as Version == 0) is
// treated the same as any other incompatible version: discarded, not
// partially decoded.
func TestInMemoryBackend_RestoreMissingVersion(t *testing.T) {
	t.Parallel()

	original := ce.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := original.CreateAnomalyMonitor("LegacyMon", "DIMENSIONAL", "SERVICE", nil, nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	var raw map[string]any

	require.NoError(t, json.Unmarshal(snap, &raw))
	delete(raw, "version")

	legacy, err := json.Marshal(raw)
	require.NoError(t, err)

	target := ce.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, target.Restore(t.Context(), legacy))

	monitors, _, err := target.GetAnomalyMonitors(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, monitors)
}
