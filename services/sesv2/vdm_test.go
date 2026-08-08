package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestConfigSetVdmOptions validates config set exists.
func TestConfigSetVdmOptions(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)

	_, err := backend.CreateConfigurationSet("vdm-cs", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/vdm-cs/vdm-options",
		map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestConfigSetVdmOptionsNotFound returns 404 for missing config set.
func TestConfigSetVdmOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/vdm-options",
		map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestBackendPutConfigSetVdmOptionsNotFound errors for missing set.
func TestBackendPutConfigSetVdmOptionsNotFound(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	err := backend.PutConfigurationSetVdmOptions("no-such", nil, nil)
	require.Error(t, err)
}
