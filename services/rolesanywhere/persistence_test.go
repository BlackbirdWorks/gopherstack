// This file is deliberately in package rolesanywhere (not _test): it needs
// access to the unexported Subject.region field to seed round-trip test
// data, since Subject has no public Create operation.
package rolesanywhere //nolint:testpackage // needs unexported Subject.region access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersistence_SnapshotRestoreRoundTrip proves that a full Snapshot →
// Restore cycle preserves every converted store.Table (trustAnchors,
// profiles, crls, subjects) and every raw-left persisted map (tags,
// attributeMappings, notificationSettings) -- see store_setup.go's
// registerAllTables doc for which is which.
func TestPersistence_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *InMemoryBackend)
		verify func(t *testing.T, b *InMemoryBackend)
		name   string
	}{
		{
			name: "trust_anchors_with_tags_and_notification_settings",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()
				src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

				ta, err := b.CreateTrustAnchor(ctx, "anchor-1", src, []TagEntry{{Key: "env", Value: "prod"}})
				require.NoError(t, err)
				require.NoError(t, b.TagResource(ctx, ta.TrustAnchorArn, []TagEntry{{Key: "team", Value: "platform"}}))

				threshold := int32(45)
				_, err = b.PutNotificationSettings(ctx, ta.TrustAnchorID, []NotificationSetting{
					{Event: "CERTIFICATE_EXPIRY", Threshold: &threshold, Enabled: true},
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				list, _, err := b.ListTrustAnchors(ctx, "", 0)
				require.NoError(t, err)
				require.Len(t, list, 1)

				ta := list[0]
				assert.Equal(t, "anchor-1", ta.Name)
				assert.True(t, ta.Enabled)
				require.Len(t, ta.Tags, 1)
				assert.Equal(t, "prod", ta.Tags[0].Value)

				// TagResource writes to a separate resourceARN-keyed map
				// (b.tags) from the creation-time ta.Tags field -- they are
				// never merged (see ListTagsForResource in backend.go) -- so
				// ListTagsForResource only ever reflects TagResource calls.
				tags, err := b.ListTagsForResource(ctx, ta.TrustAnchorArn)
				require.NoError(t, err)
				require.Len(t, tags, 1)
				assert.Equal(t, "team", tags[0].Key)
				assert.Equal(t, "platform", tags[0].Value)

				settings := b.GetNotificationSettings(ctx, ta.TrustAnchorID)
				require.Len(t, settings, 1)
				assert.Equal(t, "CERTIFICATE_EXPIRY", settings[0].Event)
				require.NotNil(t, settings[0].Threshold)
				assert.Equal(t, int32(45), *settings[0].Threshold)
			},
		},
		{
			name: "profiles_with_attribute_mappings",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				p, err := b.CreateProfile(
					ctx, "profile-1", []string{"arn:aws:iam::000000000000:role/Example"}, nil, nil, nil, "", false,
				)
				require.NoError(t, err)

				_, err = b.PutAttributeMapping(ctx, p.ProfileID, "x509Subject", []MappingRule{{Specifier: "CN"}})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				list, _, err := b.ListProfiles(ctx, "", 0)
				require.NoError(t, err)
				require.Len(t, list, 1)
				assert.Equal(t, "profile-1", list[0].Name)

				mappings := b.GetAttributeMappings(ctx, list[0].ProfileID)
				require.Len(t, mappings, 1)
				assert.Equal(t, "x509Subject", mappings[0].CertificateField)
				require.Len(t, mappings[0].MappingRules, 1)
				assert.Equal(t, "CN", mappings[0].MappingRules[0].Specifier)
			},
		},
		{
			name: "crls_with_tags",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()
				anchorArn := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/abc"

				_, err := b.ImportCrl(
					ctx, "crl-1", []byte("crldata"), anchorArn, true, []TagEntry{{Key: "owner", Value: "sec"}},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				list, _, err := b.ListCrls(ctx, "", 0)
				require.NoError(t, err)
				require.Len(t, list, 1)
				assert.Equal(t, "crl-1", list[0].Name)
				assert.Equal(t, []byte("crldata"), list[0].CrlData)

				tags, err := b.ListTagsForResource(ctx, list[0].CrlArn)
				require.NoError(t, err)
				require.Len(t, tags, 1)
				assert.Equal(t, "sec", tags[0].Value)
			},
		},
		{
			// Subject has no public Create operation -- AWS only ever
			// creates one as a side effect of a successful certificate-based
			// authentication, which this emulator does not implement. Seed
			// one directly through the unexported table to prove the
			// "dirty" subjects DTO (see persistence.go's regionalDTO) round
			// trips correctly.
			name: "subjects_seeded_directly",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				b.subjects.Put(&Subject{
					SubjectID:   "subj-1",
					SubjectArn:  "arn:aws:rolesanywhere:us-east-1:000000000000:subject/subj-1",
					X509Subject: "CN=test",
					Enabled:     true,
					region:      "us-east-1",
				})
			},
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				list, _, err := b.ListSubjects(ctx, "", 0)
				require.NoError(t, err)
				require.Len(t, list, 1)
				assert.Equal(t, "subj-1", list[0].SubjectID)

				got, err := b.GetSubject(ctx, "subj-1")
				require.NoError(t, err)
				assert.Equal(t, "CN=test", got.X509Subject)
				assert.True(t, got.Enabled)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(t *testing.T, _ *InMemoryBackend) { t.Helper() },
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				anchors, _, err := b.ListTrustAnchors(context.Background(), "", 0)
				require.NoError(t, err)
				assert.Empty(t, anchors)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(t, original)

			snap := original.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			fresh := NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(context.Background(), snap))

			tt.verify(t, fresh)
		})
	}
}

// TestPersistence_RestoreVersionMismatch verifies that a snapshot whose
// "version" field does not match rolesanywhereSnapshotVersion is discarded
// wholesale (registry.ResetAll plus each dirty table's own Reset, plus a
// reset of every raw-left persisted map) rather than partially decoded --
// see the rolesanywhereSnapshotVersion doc comment in persistence.go. It
// also proves the discard clears any state already present in the restore
// target, not just leaves the mismatched snapshot's state out.
func TestPersistence_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(snap map[string]any)
		name   string
	}{
		{
			name: "absent_version",
			// Mirrors a pre-Phase-3.3 snapshot, which carried no version
			// field (nor a "tables" envelope) at all and so decodes as
			// Version == 0.
			mutate: func(snap map[string]any) { delete(snap, "version") },
		},
		{
			name:   "future_version",
			mutate: func(snap map[string]any) { snap["version"] = 999 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

			donor := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := donor.CreateTrustAnchor(ctx, "donor-anchor", src, nil)
			require.NoError(t, err)

			var snapMap map[string]any
			require.NoError(t, json.Unmarshal(donor.Snapshot(ctx), &snapMap))
			tt.mutate(snapMap)

			mutatedSnap, err := json.Marshal(snapMap)
			require.NoError(t, err)

			// The restore target starts with its own, different state so we
			// can prove the mismatch discards it rather than leaving it
			// untouched or merging in the donor's trust anchor.
			target := NewInMemoryBackend("000000000000", "us-east-1")
			_, err = target.CreateTrustAnchor(ctx, "target-anchor", src, nil)
			require.NoError(t, err)

			require.NoError(t, target.Restore(ctx, mutatedSnap))

			list, _, err := target.ListTrustAnchors(ctx, "", 0)
			require.NoError(t, err)
			assert.Empty(t, list, "version-mismatched restore must discard all state, not merge or keep it")
		})
	}
}

// TestPersistence_RestoreInvalidJSON verifies that malformed JSON is
// rejected with an error rather than silently discarded (unlike a
// version-mismatched but otherwise well-formed snapshot; see
// TestPersistence_RestoreVersionMismatch).
func TestPersistence_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(context.Background(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestore_Delegates proves the dead-wiring fix: prior to
// Phase 3.3, InMemoryBackend fully implemented Snapshot/Restore but Handler
// had no methods of its own, so the service registry's persistence wiring
// (setupPersistence in cli.go only registers a service.Registerable that
// also satisfies an inline Snapshot/Restore interface) never persisted
// RolesAnywhere state at all. Handler.Snapshot/Restore (persistence.go) now
// delegate to the backend, closing that gap.
func TestHandler_SnapshotRestore_Delegates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(backend)

	_, err := backend.CreateTrustAnchor(ctx, "handler-anchor", src, nil)
	require.NoError(t, err)

	snap := h.Snapshot(ctx)
	require.NotEmpty(t, snap)

	freshBackend := NewInMemoryBackend("000000000000", "us-east-1")
	freshHandler := NewHandler(freshBackend)
	require.NoError(t, freshHandler.Restore(ctx, snap))

	list, _, err := freshBackend.ListTrustAnchors(ctx, "", 0)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "handler-anchor", list[0].Name)
}

// TestHandler_ImplementsPersistable is a compile-time-flavored guard against
// a regression back to the pre-Phase-3.3 dead-wiring gap: the service
// registry's persistence setup only auto-registers a service.Registerable
// that also satisfies this shape (see setupPersistence in cli.go).
func TestHandler_ImplementsPersistable(t *testing.T) {
	t.Parallel()

	type persistable interface {
		Snapshot(ctx context.Context) []byte
		Restore(ctx context.Context, data []byte) error
	}

	var _ persistable = (*Handler)(nil)
}
