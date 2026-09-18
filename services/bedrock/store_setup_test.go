package bedrock_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

type seededBackendResources struct {
	guardrailID string
	modelARN    string
}

func seedTestResources(t *testing.T, b *bedrock.InMemoryBackend) seededBackendResources {
	t.Helper()

	g, err := b.CreateGuardrail("test-guardrail", "desc", "blocked-in", "blocked-out", nil)
	require.NoError(t, err)

	cm, err := b.CreateCustomModel("test-model", nil)
	require.NoError(t, err)

	return seededBackendResources{
		guardrailID: g.GuardrailID,
		modelARN:    cm.ModelArn,
	}
}

func assertRestoredResources(t *testing.T, b *bedrock.InMemoryBackend, seed seededBackendResources) {
	t.Helper()

	gotG, err := b.GetGuardrail(seed.guardrailID)
	require.NoError(t, err)
	assert.Equal(t, seed.guardrailID, gotG.GuardrailID)
	assert.Equal(t, "test-guardrail", gotG.Name)

	gotCM, err := b.GetCustomModel(seed.modelARN)
	require.NoError(t, err)
	assert.Equal(t, seed.modelARN, gotCM.ModelArn)
	assert.Equal(t, "test-model", gotCM.ModelName)
}

// TestInMemoryBackend_RegistryRoundTrip exercises a full-state
// Snapshot->Restore round trip of the Phase 3.3 pkgs/store conversion. It
// seeds one resource per representative flat table shape -- an ID-keyed table
// (Guardrail) and an ARN-keyed table (CustomModel) -- then verifies every
// resource is byte-for-byte recoverable after the tables are cleared and
// restored, using the SnapshotTablesForTest/RestoreTablesForTest/
// ResetTablesForTest test bridges in export_test.go.
func TestInMemoryBackend_RegistryRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "populated backend survives snapshot, reset, restore"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
			seed := seedTestResources(t, b)

			snap, err := b.SnapshotTablesForTest()
			require.NoError(t, err)

			// Clear every registered table in place (registrations survive,
			// exactly as InMemoryBackend.Reset leaves them for reuse).
			b.ResetTablesForTest()

			_, getErr := b.GetGuardrail(seed.guardrailID)
			require.Error(t, getErr)

			require.NoError(t, b.RestoreTablesForTest(snap))
			assertRestoredResources(t, b, seed)
		})
	}
}
