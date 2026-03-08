package sts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestNewInMemoryBackendWithConfig_GetCallerIdentityUsesInjectedAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantARN   string
	}{
		{
			name:      "default account",
			accountID: sts.MockAccountID,
			wantARN:   "arn:aws:iam::000000000000:root",
		},
		{
			name:      "custom account",
			accountID: "123456789012",
			wantARN:   "arn:aws:iam::123456789012:root",
		},
		{
			name:      "another custom account",
			accountID: "999999999999",
			wantARN:   "arn:aws:iam::999999999999:root",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackendWithConfig(tc.accountID)
			resp, err := b.GetCallerIdentity("")
			require.NoError(t, err)
			assert.Equal(t, tc.accountID, resp.GetCallerIdentityResult.Account)
			assert.Equal(t, tc.wantARN, resp.GetCallerIdentityResult.Arn)
		})
	}
}

func TestMockAccountID_IsZeroesNotAmazonAccount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "000000000000", sts.MockAccountID,
		"MockAccountID should be all-zeros to match other services")
}
