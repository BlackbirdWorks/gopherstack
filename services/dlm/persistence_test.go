package dlm_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// TestPersistence_FullStateRoundTrip exercises a Snapshot -> Restore cycle
// covering every piece of persisted backend state: the policies store.Table
// (every field: description, execution role ARN, state, tags, policy
// details, timestamps) and the raw counter field used to generate the next
// PolicyID.
func TestPersistence_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "single policy", count: 1},
		{name: "multiple policies", count: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("111122223333", "eu-west-1")

			type created struct {
				tags          map[string]string
				policyDetails map[string]any
				id            string
				arn           string
				description   string
			}

			var want []created

			for i := range tc.count {
				desc := "policy-" + string(rune('a'+i))
				details := map[string]any{
					"PolicyType": "EBS_SNAPSHOT_MANAGEMENT",
					"idx":        float64(i),
				}
				tags := map[string]string{"owner": "team-x", "idx": desc}

				p, err := b1.CreateLifecyclePolicy(
					desc, "arn:aws:iam::111122223333:role/dlm-role", "ENABLED", tags, details,
				)
				require.NoError(t, err)

				want = append(want, created{
					id: p.PolicyID, arn: p.PolicyArn, description: desc,
					tags: tags, policyDetails: details,
				})
			}

			// Additional tag mutation via TagResource/UntagResource must also
			// survive the round trip.
			require.NoError(t, b1.TagResource(want[0].arn, map[string]string{"extra": "yes"}))
			require.NoError(t, b1.UntagResource(want[0].arn, []string{"owner"}))

			snap := b1.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			b2 := dlm.NewInMemoryBackend("111122223333", "eu-west-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			for i, w := range want {
				got, err := b2.GetLifecyclePolicy(w.id)
				require.NoError(t, err)

				assert.Equal(t, w.description, got.Description)
				assert.Equal(t, w.arn, got.PolicyArn)
				assert.Equal(t, "ENABLED", got.State)
				assert.Equal(t, "arn:aws:iam::111122223333:role/dlm-role", got.ExecutionRoleARN)
				assert.Equal(t, w.policyDetails, got.PolicyDetails)
				assert.False(t, got.DateCreated.IsZero())
				assert.False(t, got.DateModified.IsZero())

				if i == 0 {
					assert.Equal(t, "yes", got.Tags["extra"])
					assert.Equal(t, w.description, got.Tags["idx"])
					_, hasOwner := got.Tags["owner"]
					assert.False(t, hasOwner, "untagged key must not survive restore")
				} else {
					assert.Equal(t, w.tags, got.Tags)
				}
			}

			// The counter must survive so a freshly restored backend never
			// reissues a PolicyID that already exists.
			existing := make(map[string]struct{}, len(want))
			for _, w := range want {
				existing[w.id] = struct{}{}
			}

			next, err := b2.CreateLifecyclePolicy(
				"next",
				"arn:aws:iam::111122223333:role/r",
				"ENABLED",
				nil,
				nil,
			)
			require.NoError(t, err)

			_, collided := existing[next.PolicyID]
			assert.False(t, collided, "new policyID %s collides with a restored ID", next.PolicyID)

			// GetLifecyclePolicies must return exactly the restored set (plus
			// the newly created one), confirming no phantom/duplicate entries.
			all, err := b2.GetLifecyclePolicies(dlm.PolicyFilter{})
			require.NoError(t, err)
			assert.Len(t, all, tc.count+1)
		})
	}
}

// TestPersistence_DefaultPolicyEchoSurvivesRestore verifies the derived
// top-level DefaultPolicy bool (see policyDetailsIsDefaultPolicy in
// models.go) re-derives correctly after a Snapshot -> Restore cycle, proving
// it stays in sync with the persisted PolicyDetails.PolicyLanguage it is
// computed from rather than drifting as separately-stored state would.
func TestPersistence_DefaultPolicyEchoSurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policyDetails map[string]any
		name          string
		want          bool
	}{
		{
			name:          "simplified policy language survives as DefaultPolicy true",
			policyDetails: map[string]any{"PolicyLanguage": "SIMPLIFIED", "ResourceType": "VOLUME"},
			want:          true,
		},
		{
			name:          "standard policy language survives as DefaultPolicy false",
			policyDetails: map[string]any{"PolicyLanguage": "STANDARD"},
			want:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b1.CreateLifecyclePolicy(
				"d", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, tc.policyDetails,
			)
			require.NoError(t, err)

			snap := b1.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			got, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.DefaultPolicy)
		})
	}
}

// TestPersistence_HandlerDelegatesToBackend verifies the dead-wiring fix:
// Handler.Snapshot/Restore must delegate to the backend so the persistence
// manager (which type-asserts service.Registerable, i.e. *Handler, against a
// Snapshot/Restore interface -- see cli.go's setupPersistence) actually picks
// up dlm for snapshot-based state save/restore.
func TestPersistence_HandlerDelegatesToBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler snapshot/restore round-trips backend state"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			h1 := dlm.NewHandler(backend1)

			p, err := backend1.CreateLifecyclePolicy(
				"handler-delegation", "arn:aws:iam::000000000000:role/r", "ENABLED",
				map[string]string{"k": "v"}, nil,
			)
			require.NoError(t, err)

			snap := h1.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			backend2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			h2 := dlm.NewHandler(backend2)
			require.NoError(t, h2.Restore(context.Background(), snap))

			got, err := backend2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "handler-delegation", got.Description)
			assert.Equal(t, "v", got.Tags["k"])
		})
	}
}

// TestPersistence_VersionMismatch verifies every non-happy-path branch of
// Restore's version guard: an absent/legacy version (decodes as 0), an
// explicit but incompatible version, and malformed JSON.
func TestPersistence_VersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		payload     []byte
		wantErr     bool
		wantEmptied bool
	}{
		{
			name: "legacy pre-3.3 snapshot shape decodes version as 0 and is discarded",
			payload: []byte(
				`{"policies":{"policy-0000000000000001":{"policyId":"policy-0000000000000001"}},"counter":7}`,
			),
			wantErr:     false,
			wantEmptied: true,
		},
		{
			name:        "explicit incompatible version is discarded",
			payload:     []byte(`{"version":99,"tables":{"policies":"[]"},"counter":3}`),
			wantErr:     false,
			wantEmptied: true,
		},
		{
			name:    "malformed JSON returns an error via persistence.UnmarshalSnapshot",
			payload: []byte(`not json`),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			// Seed with a policy so we can observe that a discarded/erroring
			// restore does not silently leave stale pre-restore state mixed
			// with new state; in the discard case it must reset to empty.
			_, err := b.CreateLifecyclePolicy(
				"seed",
				"arn:aws:iam::000000000000:role/r",
				"ENABLED",
				nil,
				nil,
			)
			require.NoError(t, err)

			restoreErr := b.Restore(context.Background(), tc.payload)

			if tc.wantErr {
				require.Error(t, restoreErr)

				return
			}

			require.NoError(t, restoreErr)

			if tc.wantEmptied {
				policies, listErr := b.GetLifecyclePolicies(dlm.PolicyFilter{})
				require.NoError(t, listErr)
				assert.Empty(t, policies)

				// Counter must also reset to 0 on a discarded snapshot, so
				// the next PolicyID does not depend on the rejected payload.
				next, createErr := b.CreateLifecyclePolicy(
					"post-reset",
					"arn:aws:iam::000000000000:role/r",
					"ENABLED",
					nil,
					nil,
				)
				require.NoError(t, createErr)
				assert.Equal(t, "policy-0000000000000001", next.PolicyID)
			}
		})
	}
}

// TestPersistence_CurrentVersionRoundTripsThroughJSON verifies the on-disk
// shape produced by Snapshot decodes with the current version and a Tables
// entry for the policies table, guarding against accidental field-name
// changes to backendSnapshot's JSON tags.
func TestPersistence_CurrentVersionRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()

	b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateLifecyclePolicy("v", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(context.Background())

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &raw))

	_, hasVersion := raw["version"]
	assert.True(t, hasVersion, "snapshot must carry a top-level version field")

	_, hasTables := raw["tables"]
	assert.True(t, hasTables, "snapshot must carry a top-level tables field")

	_, hasCounter := raw["counter"]
	assert.True(t, hasCounter, "snapshot must carry a top-level counter field")

	var tables map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw["tables"], &tables))

	_, hasPoliciesTable := tables["policies"]
	assert.True(
		t,
		hasPoliciesTable,
		"tables must carry the policies table under its registered name",
	)
}

// TestPersistence_CounterSerialization_NoCollision verifies the PolicyID
// counter survives a Snapshot -> Restore cycle so a restored backend never
// reissues a PolicyID that already exists.
func TestPersistence_CounterSerialization_NoCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "counter_survives_restore"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			// Create 2 policies.
			p1, err := b1.CreateLifecyclePolicy("p1", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)
			p2, err := b1.CreateLifecyclePolicy("p2", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			existingIDs := map[string]struct{}{
				p1.PolicyID: {},
				p2.PolicyID: {},
			}

			// Snapshot and restore into fresh backend.
			snap := b1.Snapshot(context.Background())
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			// Create a new policy in restored backend — must not collide.
			p3, err := b2.CreateLifecyclePolicy("p3", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			_, collision := existingIDs[p3.PolicyID]
			assert.False(t, collision, "new policyID %s collides with existing IDs", p3.PolicyID)

			// New ID must be strictly greater (lexicographically) than existing ones.
			assert.Greater(t, p3.PolicyID, p2.PolicyID)
		})
	}
}
