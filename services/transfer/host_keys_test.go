package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostKeyFingerprintComputedOnImport(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	hk, err := b.ImportHostKey(s.ServerID, testHostKeyEd25519, "fp test", nil)
	require.NoError(t, err)
	assert.Contains(t, hk.Fingerprint, "SHA256:", "Fingerprint must be SHA256:")
}
