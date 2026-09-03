package acmpca_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_ListCertificateAuthorities_MaxResultsCappedAt100
// verifies api_op_ListCertificateAuthorities.go's documented ceiling: "Although
// the maximum value is 1000, the action only returns a maximum of 100 items."
// A caller-requested MaxResults above 100 (even up to the 1000 max) must still
// page at 100, and an omitted MaxResults must default to 100, not the whole
// account's CA inventory.
func TestInMemoryBackend_ListCertificateAuthorities_MaxResultsCappedAt100(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ctx := context.Background()

	const totalCAs = 105
	for i := range totalCAs {
		_, err := b.CreateCertificateAuthority(ctx, "ROOT", rootCACfg(fmt.Sprintf("Test CA %d", i)))
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
	}{
		{name: "omitted defaults to 100", maxResults: 0, wantLen: 100},
		{name: "requested above 100 still caps at 100", maxResults: 500, wantLen: 100},
		{name: "requested at documented max still caps at 100", maxResults: 1000, wantLen: 100},
		{name: "requested below 100 honored", maxResults: 10, wantLen: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := b.ListCertificateAuthorities(ctx, "", tt.maxResults, "")
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantLen)
		})
	}
}
