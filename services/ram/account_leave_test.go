package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestHandleAccountLeftOrganization(t *testing.T) {
	t.Parallel()

	const leaver = "111122223333"

	tests := []struct {
		retain     *bool
		name       string
		leaver     string
		wantStatus string
	}{
		{name: "unset_disassociates", retain: nil, leaver: leaver, wantStatus: "DISASSOCIATED"},
		{name: "false_disassociates", retain: new(false), leaver: leaver, wantStatus: "DISASSOCIATED"},
		{name: "true_retains", retain: new(true), leaver: leaver, wantStatus: "ASSOCIATED"},
		{name: "other_account_untouched", retain: nil, leaver: "444455556666", wantStatus: "ASSOCIATED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(account, "us-east-1")
			rs, err := b.CreateResourceShare("s", true, nil, []string{leaver}, nil)
			require.NoError(t, err)
			require.NoError(t, b.ConfigureResourceShare(rs.ARN, tt.retain, nil))

			b.HandleAccountLeftOrganization(tt.leaver)

			assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
			require.Len(t, assocs, 1)
			assert.Equal(t, tt.wantStatus, assocs[0].Status)
		})
	}
}
