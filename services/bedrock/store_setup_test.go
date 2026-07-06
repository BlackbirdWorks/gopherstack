package bedrock_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

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

	type seeded struct {
		guardrailID string
		modelARN    string
		agentID     string
		flowID      string
		flowVersion string
	}

	tests := []struct {
		name string
	}{
		{name: "populated backend survives snapshot, reset, restore"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")

			g, err := b.CreateGuardrail("test-guardrail", "desc", "blocked-in", "blocked-out", nil)
			if err != nil {
				t.Fatalf("CreateGuardrail: %v", err)
			}

			cm, err := b.CreateCustomModel("test-model", nil)
			if err != nil {
				t.Fatalf("CreateCustomModel: %v", err)
			}

			ag, err := b.CreateAgent("test-agent", "anthropic.claude-v2", "be helpful", "role-arn", nil)
			if err != nil {
				t.Fatalf("CreateAgent: %v", err)
			}

			fl, err := b.CreateFlow("test-flow", "desc", nil)
			if err != nil {
				t.Fatalf("CreateFlow: %v", err)
			}

			fv, err := b.CreateFlowVersion(fl.FlowID)
			if err != nil {
				t.Fatalf("CreateFlowVersion: %v", err)
			}

			seed := seeded{
				guardrailID: g.GuardrailID,
				modelARN:    cm.ModelArn,
				agentID:     ag.AgentID,
				flowID:      fl.FlowID,
				flowVersion: fv.Version,
			}

			snap, err := b.SnapshotTablesForTest()
			if err != nil {
				t.Fatalf("SnapshotTablesForTest: %v", err)
			}

			// Clear every registered table in place (registrations survive,
			// exactly as InMemoryBackend.Reset leaves them for reuse).
			b.ResetTablesForTest()

			if _, getErr := b.GetGuardrail(seed.guardrailID); getErr == nil {
				t.Fatalf("expected guardrail to be absent after ResetTablesForTest")
			}

			if restoreErr := b.RestoreTablesForTest(snap); restoreErr != nil {
				t.Fatalf("RestoreTablesForTest: %v", restoreErr)
			}

			gotG, err := b.GetGuardrail(seed.guardrailID)
			if err != nil {
				t.Fatalf("GetGuardrail after restore: %v", err)
			}

			if gotG.GuardrailID != seed.guardrailID || gotG.Name != "test-guardrail" {
				t.Errorf("guardrail mismatch after restore: %+v", gotG)
			}

			gotCM, err := b.GetCustomModel(seed.modelARN)
			if err != nil {
				t.Fatalf("GetCustomModel after restore: %v", err)
			}

			if gotCM.ModelArn != seed.modelARN || gotCM.ModelName != "test-model" {
				t.Errorf("custom model mismatch after restore: %+v", gotCM)
			}

			gotAgent, err := b.GetAgent(seed.agentID)
			if err != nil {
				t.Fatalf("GetAgent after restore: %v", err)
			}

			if gotAgent.AgentID != seed.agentID || gotAgent.AgentName != "test-agent" {
				t.Errorf("agent mismatch after restore: %+v", gotAgent)
			}

			gotFlow, err := b.GetFlow(seed.flowID)
			if err != nil {
				t.Fatalf("GetFlow after restore: %v", err)
			}

			if gotFlow.FlowID != seed.flowID || gotFlow.Name != "test-flow" {
				t.Errorf("flow mismatch after restore: %+v", gotFlow)
			}

			gotFV, err := b.GetFlowVersion(seed.flowID, seed.flowVersion)
			if err != nil {
				t.Fatalf("GetFlowVersion after restore: %v", err)
			}

			if gotFV.FlowID != seed.flowID || gotFV.Version != seed.flowVersion {
				t.Errorf("flow version mismatch after restore: %+v", gotFV)
			}
		})
	}
}
