package account_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

// TestBackend_GetAccountInformation verifies GetAccountInformation reports
// the account ID, tracks PutAccountName, always reports ACTIVE (this
// service has no operation that changes account lifecycle state -- that is
// an AWS Organizations concept), and reports a stable (non-regenerating)
// creation date across repeated calls.
func TestBackend_GetAccountInformation(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("111122223333", "us-west-2")

	info, err := b.GetAccountInformation()
	require.NoError(t, err)
	assert.Equal(t, "111122223333", info.AccountID)
	assert.Equal(t, account.StateActive, info.AccountState)
	assert.NotEmpty(t, info.AccountCreatedDate)

	require.NoError(t, b.PutAccountName("New Name"))

	info2, err := b.GetAccountInformation()
	require.NoError(t, err)
	assert.Equal(t, "New Name", info2.AccountName)
	assert.Equal(t, info.AccountCreatedDate, info2.AccountCreatedDate, "creation date must not change")
}

// TestBackend_GetAccountInformation_CreatedDateSurvivesReset verifies Reset
// does not fabricate a new creation date -- Reset wipes created resources,
// but the backing account itself was never destroyed, matching how
// accountID/region are also preserved across Reset.
func TestBackend_GetAccountInformation_CreatedDateSurvivesReset(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	before, err := b.GetAccountInformation()
	require.NoError(t, err)

	b.Reset()

	after, err := b.GetAccountInformation()
	require.NoError(t, err)
	assert.Equal(t, before.AccountCreatedDate, after.AccountCreatedDate)
}
