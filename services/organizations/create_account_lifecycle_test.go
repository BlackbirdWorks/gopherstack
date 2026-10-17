package organizations_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestCreateAccount_AsyncLifecycle(t *testing.T) {
	t.Parallel()

	const delay = 5 * time.Second

	tests := []struct {
		name      string
		wantState string
		wantFail  string
		govCloud  bool
		duplicate bool
	}{
		{name: "account_succeeds_after_delay", wantState: "SUCCEEDED"},
		{name: "govcloud_succeeds_after_delay", govCloud: true, wantState: "SUCCEEDED"},
		{
			name: "duplicate_email_fails_after_delay", duplicate: true,
			wantState: "FAILED", wantFail: "EMAIL_ALREADY_EXISTS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend()
				b.SetLifecycleDelay(delay)

				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)

				create := b.CreateAccount
				if tt.govCloud {
					create = b.CreateGovCloudAccount
				}

				if tt.duplicate {
					first, cerr := b.CreateAccount("first", "dup@example.com", "", "", nil)
					require.NoError(t, cerr)
					time.Sleep(delay)

					got, derr := b.DescribeCreateAccountStatus(first.ID)
					require.NoError(t, derr)
					require.Equal(t, "SUCCEEDED", got.State)
				}

				email := "a@example.com"
				if tt.duplicate {
					email = "dup@example.com"
				}

				status, err := create("acct", email, "", "", nil)
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", status.State)
				assert.Empty(t, status.AccountID)
				assert.Empty(t, status.GovCloudAccountID)

				got, err := b.DescribeCreateAccountStatus(status.ID)
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", got.State)

				inProgress, err := b.ListCreateAccountStatus([]string{"IN_PROGRESS"})
				require.NoError(t, err)
				assert.Len(t, inProgress, 1)

				accts, err := b.ListAccounts()
				require.NoError(t, err)
				wantBefore := 1
				if tt.duplicate {
					wantBefore = 2
				}
				assert.Len(t, accts, wantBefore)

				time.Sleep(delay)

				got, err = b.DescribeCreateAccountStatus(status.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.wantState, got.State)
				assert.Equal(t, tt.wantFail, got.FailureReason)

				inProgress, err = b.ListCreateAccountStatus([]string{"IN_PROGRESS"})
				require.NoError(t, err)
				assert.Empty(t, inProgress)

				accts, err = b.ListAccounts()
				require.NoError(t, err)

				if tt.wantState == "SUCCEEDED" {
					assert.NotEmpty(t, got.AccountID)
					assert.Len(t, accts, wantBefore+1)
					assert.Equal(t, tt.govCloud, got.GovCloudAccountID != "")
				} else {
					assert.Empty(t, got.AccountID)
					assert.Len(t, accts, wantBefore)
				}
			})
		})
	}
}

func TestDescribeCreateAccountStatus_Unknown(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, _, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	_, err = b.DescribeCreateAccountStatus("car-000000000999")
	require.ErrorIs(t, err, organizations.ErrCreateAccountStatusNotFound)
}
