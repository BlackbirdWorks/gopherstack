package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
)

func TestWireRAMOrganizations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wire       bool
	}{
		{name: "wired", wire: true, wantStatus: "DISASSOCIATED"},
		{name: "unwired", wire: false, wantStatus: "ASSOCIATED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			orgBk := organizationsbackend.NewInMemoryBackend("000000000000", "us-east-1")
			_, _, err := orgBk.CreateOrganization("ALL")
			require.NoError(t, err)

			ramBk := rambackend.NewInMemoryBackend("000000000000", "us-east-1")

			if tt.wire {
				wireRAMOrganizations(rambackend.NewHandler(ramBk), organizationsbackend.NewHandler(orgBk))
			}

			status, err := orgBk.CreateAccount("leaver", "leaver@example.com", "", "", nil)
			require.NoError(t, err)

			rs, err := ramBk.CreateResourceShare("s", true, nil, []string{status.AccountID}, nil)
			require.NoError(t, err)

			require.NoError(t, orgBk.RemoveAccountFromOrganization(status.AccountID))

			assocs := ramBk.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
			require.Len(t, assocs, 1)
			assert.Equal(t, tt.wantStatus, assocs[0].Status)
		})
	}
}
