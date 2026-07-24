package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestInMemoryBackend_ListCertificateAuthorities_ResourceOwner covers
// ListCertificateAuthoritiesInput.ResourceOwner: SELF (and the empty default)
// lists this account's CAs, OTHER_ACCOUNTS always returns an empty page (no
// cross-account CA sharing is modeled), and any other value is rejected.
// Previously accepted-but-ignored entirely (PARITY.md gap).
func TestInMemoryBackend_ListCertificateAuthorities_ResourceOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		resourceOwner string
		wantErr       bool
		wantCount     int
	}{
		{name: "empty defaults to SELF", resourceOwner: "", wantCount: 1},
		{name: "explicit SELF", resourceOwner: "SELF", wantCount: 1},
		{name: "OTHER_ACCOUNTS returns empty", resourceOwner: "OTHER_ACCOUNTS", wantCount: 0},
		{name: "unsupported value is rejected", resourceOwner: "BOGUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateCertificateAuthority(context.Background(), "ROOT", rootCACfg("Owned CA"))
			require.NoError(t, err)

			p, err := b.ListCertificateAuthorities(context.Background(), "", 0, tt.resourceOwner)
			if tt.wantErr {
				require.ErrorIs(t, err, acmpca.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}
