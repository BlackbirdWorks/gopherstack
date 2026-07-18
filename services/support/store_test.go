package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

// TestSupport_StorageBackendInterface verifies var _ assertion compiles.
func TestSupport_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ support.StorageBackend = (*support.InMemoryBackend)(nil)
}

// TestSupport_Reset verifies handler and backend Reset.
func TestSupport_Reset(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)

	rec := doSupportRequest(t, h, "CreateCase", map[string]any{"subject": "Test Reset", "communicationBody": "Initial"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, support.CaseCount(b))

	h.Reset()
	assert.Equal(t, 0, support.CaseCount(b))
}

// TestSupport_ExportCounts verifies export helpers work correctly.
func TestSupport_ExportCounts(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	assert.Equal(t, 0, support.CaseCount(b))
	assert.Equal(t, 0, support.AttachmentCount(b))
	assert.Equal(t, 0, support.CheckRefreshStatusCount(b))
	assert.Equal(t, 0, support.CommunicationCount(b))
}
