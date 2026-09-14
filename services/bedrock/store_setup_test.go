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
	agentID     string
	flowID      string
	flowVersion string
}

func seedTestResources(t *testing.T, b *bedrock.InMemoryBackend) seededBackendResources {
	t.Helper()

	g, err := b.CreateGuardrail("test-guardrail", "desc", "blocked-in", "blocked-out", nil)
	require.NoError(t, err)

	cm, err := b.CreateCustomModel("test-model", nil)
	require.NoError(t, err)

	ag, err := b.CreateAgent("test-agent", "anthropic.claude-v2", "be helpful", "role-arn", nil)
	require.NoError(t, err)

	fl, err := b.CreateFlow("test-flow", "desc", "arn:aws:iam::000000000000:role/flow-role", nil)
	require.NoError(t, err)

	fv, err := b.CreateFlowVersion(fl.FlowID)
	require.NoError(t, err)

	return seededBackendResources{
		guardrailID: g.GuardrailID,
		modelARN:    cm.ModelArn,
		agentID:     ag.AgentID,
		flowID:      fl.FlowID,
		flowVersion: fv.Version,
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

	gotAgent, err := b.GetAgent(seed.agentID)
	require.NoError(t, err)
	assert.Equal(t, seed.agentID, gotAgent.AgentID)
	assert.Equal(t, "test-agent", gotAgent.AgentName)

	gotFlow, err := b.GetFlow(seed.flowID)
	require.NoError(t, err)
	assert.Equal(t, seed.flowID, gotFlow.FlowID)
	assert.Equal(t, "test-flow", gotFlow.Name)

	gotFV, err := b.GetFlowVersion(seed.flowID, seed.flowVersion)
	require.NoError(t, err)
	assert.Equal(t, seed.flowID, gotFV.FlowID)
	assert.Equal(t, seed.flowVersion, gotFV.Version)
}

// TestInMemoryBackend_RegistryRoundTrip exercises a full-state
// Snapshot->Restore round trip of the Phase 3.3 pkgs/store conversion.
// bedrock has no persistence.go of its own (it was never wired into
// pkgs/persistence's Manager), so there is no production Snapshot/Restore to
// call through; instead this drives the same store.Registry machinery a real
// persistence layer would via the SnapshotTablesForTest/RestoreTablesForTest/
// ResetTablesForTest test bridges in export_test.go. It seeds one resource
// per representative table shape -- a simple flat table (Guardrail), an
// ARN-keyed table (CustomModel), an agent-domain table (Agent), and the
// per-parent lazily-registered nested shape (Flow + FlowVersion) -- then
// verifies every resource is byte-for-byte recoverable after the tables are
// cleared and restored.
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
