package directoryservice_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// isBase64Int returns true if s is a base64-encoded decimal integer.
func isBase64Int(s string) bool {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return false
	}

	_, err = strconv.Atoi(string(b))

	return err == nil
}

// TestPaginationTokensAreOpaque verifies that nextToken values are base64-encoded
// integer offsets, not raw item IDs.
func TestPaginationTokensAreOpaque(t *testing.T) {
	t.Parallel()

	t.Run("DescribeDirectories token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		for i := range 3 {
			mustCreateSimpleAD(t, h, fmt.Sprintf("d%d.example.com", i))
		}

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"Limit": 2})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)

		// Decoded offset must equal page size.
		b, _ := base64.StdEncoding.DecodeString(tok)
		offset, _ := strconv.Atoi(string(b))
		assert.Equal(t, 2, offset)
	})

	t.Run("DescribeSnapshots token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		for range 3 {
			doRequest(t, h, "CreateSnapshot", map[string]any{
				"DirectoryId": dirID,
				"Name":        "snap",
			})
		}

		rec := doRequest(t, h, "DescribeSnapshots", map[string]any{
			"DirectoryId": dirID,
			"Limit":       2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)
	})

	t.Run("ListTagsForResource token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		tags := make([]map[string]any, 3)
		for i := range 3 {
			tags[i] = map[string]any{"Key": fmt.Sprintf("k%d", i), "Value": "v"}
		}

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       tags,
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID,
			"Limit":      2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)
	})
}

// TestDirectoryCreationLifecycle verifies the Requested → Creating → Active stage transitions.
func TestDirectoryCreationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(h *directoryservice.Handler) string
		name   string
	}{
		{
			name:   "SimpleAD",
			create: func(h *directoryservice.Handler) string { return mustCreateSimpleAD(t, h, "corp.example.com") },
		},
		{
			name:   "MicrosoftAD",
			create: func(h *directoryservice.Handler) string { return mustCreateMicrosoftAD(t, h, "corp.example.com") },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			backend := h.Backend.(*directoryservice.InMemoryBackend)
			dirID := tc.create(h)

			// Immediately after creation the stage must not yet be Active.
			initial := directoryservice.DirectoryStageForTest(backend, dirID)
			assert.Equal(t, "Requested", initial, "directory should start as Requested")

			// Eventually reaches Active.
			ok := directoryservice.WaitForDirectoryActive(backend, dirID, 2*time.Second)
			assert.True(t, ok, "directory should reach Active within 2s")

			final := directoryservice.DirectoryStageForTest(backend, dirID)
			assert.Equal(t, "Active", final)
		})
	}
}

// TestRestoreFromSnapshotTransitionsBackToActive verifies that after RestoreFromSnapshot
// the directory automatically transitions back to Active.
func TestRestoreFromSnapshotTransitionsBackToActive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	backend := h.Backend.(*directoryservice.InMemoryBackend)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")
	require.True(t, directoryservice.WaitForDirectoryActive(backend, dirID, 2*time.Second))

	snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, snapRec.Code)
	snapID := respBody(t, snapRec)["SnapshotId"].(string)

	doRequest(t, h, "RestoreFromSnapshot", map[string]any{"SnapshotId": snapID})

	// Immediately after the call the directory must be Restoring.
	assert.Equal(t, "Restoring", directoryservice.DirectoryStageForTest(backend, dirID))

	// Within 2s it must return to Active.
	ok := directoryservice.WaitForDirectoryActive(backend, dirID, 2*time.Second)
	assert.True(t, ok, "directory should return to Active after restore")
}

// TestBackendSnapshotSerializesAllState verifies that BackendSnapshot/Restore round-trips
// region state beyond directories/snapshots/aliases (trusts, log subscriptions, etc.).
func TestBackendSnapshotSerializesAllState(t *testing.T) {
	t.Parallel()

	b := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	h := directoryservice.NewHandler(b)

	// Create a directory and wait for Active.
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")
	require.True(t, directoryservice.WaitForDirectoryActive(b, dirID, 2*time.Second))

	// Add a log subscription (Appendix-A state not serialized previously).
	logRec := doRequest(t, h, "CreateLogSubscription", map[string]any{
		"DirectoryId":  dirID,
		"LogGroupName": "/aws/directoryservice/testgroup",
	})
	require.Equal(t, http.StatusOK, logRec.Code)

	// Snapshot and restore into a fresh backend.
	snap := b.BackendSnapshot()
	require.NotEmpty(t, snap)

	b2 := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	err := b2.Restore(context.Background(), snap)
	require.NoError(t, err)

	h2 := directoryservice.NewHandler(b2)

	// Log subscription must survive round-trip.
	listRec := doRequest(t, h2, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, listRec.Code)
	body := respBody(t, listRec)
	subs, _ := body["LogSubscriptions"].([]any)
	assert.Len(t, subs, 1, "log subscription must survive BackendSnapshot/Restore")
}
