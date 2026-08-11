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

// TestBackend_GetPrimaryEmailUpdateStatus exercises the never-started,
// pending, and accepted states.
func TestBackend_GetPrimaryEmailUpdateStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus account.PrimaryEmailUpdateStatus
		accept     bool
		wantErr    bool
	}{
		{name: "never_started", wantErr: true},
		{name: "pending", wantStatus: account.PrimaryEmailUpdateStatusPending},
		{name: "accepted", accept: true, wantStatus: account.PrimaryEmailUpdateStatusAccepted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := account.NewInMemoryBackend("000000000000", "us-east-1")

			var otp string
			if tt.name != "never_started" {
				var err error
				otp, err = b.StartPrimaryEmailUpdate("new@example.com")
				require.NoError(t, err)
			}

			if tt.accept {
				require.NoError(t, b.AcceptPrimaryEmailUpdate(otp, "new@example.com"))
			}

			status, updatedAt, err := b.GetPrimaryEmailUpdateStatus()
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
			assert.False(t, updatedAt.IsZero())
		})
	}
}

// TestBackend_StartPrimaryEmailUpdate_ConflictsOnCurrentEmail verifies
// StartPrimaryEmailUpdate rejects a "new" email that's already in use --
// the only email this single-account backend can know is in use is its own
// current primaryEmail (types.ConflictException's documented trigger).
func TestBackend_StartPrimaryEmailUpdate_ConflictsOnCurrentEmail(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.StartPrimaryEmailUpdate(b.GetPrimaryEmail())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConflictException")

	status, _, statusErr := b.GetPrimaryEmailUpdateStatus()
	require.Error(t, statusErr, "a rejected request must not start a pending update")
	assert.Empty(t, status)
}

// TestBackend_GetGovCloudAccountInformation_NeverLinked verifies this
// standalone (non-organization-member) backend never has a linked GovCloud
// account -- there is no operation anywhere in this service that could link
// one.
func TestBackend_GetGovCloudAccountInformation_NeverLinked(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	govCloudID, state, err := b.GetGovCloudAccountInformation()
	require.Error(t, err)
	assert.Empty(t, govCloudID)
	assert.Empty(t, state)
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
