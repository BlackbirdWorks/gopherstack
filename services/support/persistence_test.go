package support_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *support.InMemoryBackend) string
		verify func(t *testing.T, b *support.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *support.InMemoryBackend) string {
				c, err := b.CreateCase("test subject", "amazon-dynamodb", "general-guidance", "low", "test body")
				if err != nil {
					return ""
				}

				return c.CaseID
			},
			verify: func(t *testing.T, b *support.InMemoryBackend, id string) {
				t.Helper()

				cases := b.DescribeCases([]string{id}, true)
				require.Len(t, cases, 1)
				assert.Equal(t, id, cases[0].CaseID)
				assert.Equal(t, "test subject", cases[0].Subject)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *support.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *support.InMemoryBackend, _ string) {
				t.Helper()

				cases := b.DescribeCases(nil, false)
				assert.Empty(t, cases)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := support.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := support.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly (registry
// ResetAll, not a partial decode) rather than partially applied: the backend
// resets to empty state and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	_, err := b.CreateCase("seed subject", "amazon-s3", "general-guidance", "low", "seed body")
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.DescribeCases(nil, true))
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches supportSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied. This covers the
// pre-Phase-3.3 snapshot shape, which had no "tables"/"version" wrapper.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	_, err := b.CreateCase("seed subject", "amazon-s3", "general-guidance", "low", "seed body")
	require.NoError(t, err)

	// A shape entirely lacking "version"/"tables" keys, mimicking the
	// pre-Phase-3.3 flat-map snapshot format.
	err = b.Restore(t.Context(), []byte(`{"cases":{"case-1":{"caseID":"case-1"}}}`))
	require.NoError(t, err)

	assert.Empty(t, b.DescribeCases(nil, true))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource the Phase 3.3
// conversion touched (cases, attachments, checkRefreshStatuses, checkResults,
// and the "dirty" attachmentSets table with its hidden ID field) plus the
// plain communications map left un-converted because case communications are
// order-sensitive.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := support.NewInMemoryBackend()

	// checkResults has no public writer in the Support API (unlike
	// checkRefreshStatuses, RefreshTrustedAdvisorCheck never populates a
	// check *result*, only its refresh status): this backend has only ever
	// populated it via Restore, both before and after the Phase 3.3
	// conversion. Seed it directly through Restore so the round trip below
	// also covers it.
	seed := `{"version":1,"tables":{"checkResults":[{` +
		`"checkId":"Pfx0RwqBli","status":"warning","timestamp":"2024-01-01T00:00:00Z",` +
		`"flaggedResources":[],` +
		`"resourcesSummary":{"resourcesFlagged":1,"resourcesIgnored":0,"resourcesProcessed":10,"resourcesSuppressed":0},` +
		`"categorySpecificSummary":null}]},"communications":{}}`
	require.NoError(t, original.Restore(t.Context(), []byte(seed)))

	// cases table + communications plain map, seeded together.
	c, err := original.CreateCaseWithOptions(support.CreateCaseOptions{
		Subject: "s", ServiceCode: "amazon-s3", CategoryCode: "general-guidance",
		SeverityCode: "low", CommunicationBody: "initial body",
	})
	require.NoError(t, err)

	// communications plain map: a second, later communication -- order must
	// survive the round trip since case communications are chronological.
	require.NoError(t, original.AddCommunicationWithOptions(support.AddCommunicationOptions{
		CaseID: c.CaseID, CommunicationBody: "follow up",
	}))

	// attachments table + attachmentSets "dirty" table (hidden ID field):
	// stage an attachment set with one attachment.
	setID, _, err := original.AddAttachmentsToSetWithAttachments("", []support.Attachment{
		{FileName: "file.txt", Data: []byte("hello")},
	})
	require.NoError(t, err)

	// checkRefreshStatuses table.
	_, err = original.RefreshTrustedAdvisorCheck("Pfx0RwqBli")
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := support.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// cases table.
	cases := fresh.DescribeCases([]string{c.CaseID}, true)
	require.Len(t, cases, 1)
	assert.Equal(t, "s", cases[0].Subject)

	// communications plain map: both communications survive, in order.
	comms, err := fresh.DescribeCommunications(c.CaseID)
	require.NoError(t, err)
	require.Len(t, comms, 2)
	assert.Equal(t, "initial body", comms[0].Body)
	assert.Equal(t, "follow up", comms[1].Body)

	// attachmentSets (dirty table, hidden ID) + attachments table: the
	// staged set must still be usable by its original ID after restore, and
	// consuming it must resolve the attachment via the restored attachments
	// table.
	c2, err := fresh.CreateCaseWithOptions(support.CreateCaseOptions{
		Subject: "s2", ServiceCode: "amazon-s3", CategoryCode: "general-guidance",
		SeverityCode: "low", CommunicationBody: "uses restored set", AttachmentSetID: setID,
	})
	require.NoError(t, err)
	comms2, err := fresh.DescribeCommunications(c2.CaseID)
	require.NoError(t, err)
	require.Len(t, comms2, 1)
	require.Len(t, comms2[0].AttachmentSet, 1)
	assert.Equal(t, "file.txt", comms2[0].AttachmentSet[0].FileName)

	// checkRefreshStatuses table.
	statuses := fresh.DescribeTrustedAdvisorCheckRefreshStatuses([]string{"Pfx0RwqBli"})
	require.Len(t, statuses, 1)
	assert.NotEqual(t, "none", statuses[0].Status)

	// checkResults table (no public writer -- seeded via Restore above).
	result := fresh.DescribeTrustedAdvisorCheckResult("Pfx0RwqBli", "en")
	assert.Equal(t, "warning", result.Status)
	assert.Empty(t, result.FlaggedResources)
	assert.Equal(t, int64(1), result.ResourcesSummary.ResourcesFlagged)
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which the persistence layer actually calls -- correctly
// delegate to the backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := support.NewHandler(support.NewInMemoryBackend())

	_, err := h.Backend.CreateCase("delegate subject", "amazon-s3", "general-guidance", "low", "body")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := support.NewHandler(support.NewInMemoryBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))

	cases := h2.Backend.DescribeCases(nil, true)
	require.Len(t, cases, 1)
	assert.Equal(t, "delegate subject", cases[0].Subject)
}
