package organizations_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type provisionCall struct{ accountID, roleName, trust string }

type recordingProvisioner struct {
	calls []provisionCall
	mu    sync.Mutex
}

func (r *recordingProvisioner) ProvisionAccessRole(accountID, roleName, trust string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, provisionCall{accountID, roleName, trust})

	return nil
}

func (r *recordingProvisioner) snapshot() []provisionCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]provisionCall(nil), r.calls...)
}

func TestCreateAccount_ProvisionsAccessRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleName string
		wantRole string
		wired    bool
		govCloud bool
		wantCall bool
	}{
		{name: "default role", wired: true, wantRole: "OrganizationAccountAccessRole", wantCall: true},
		{name: "custom role", wired: true, roleName: "Custom", wantRole: "Custom", wantCall: true},
		{name: "govcloud", wired: true, govCloud: true, wantRole: "OrganizationAccountAccessRole", wantCall: true},
		{name: "unwired", wired: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			rec := &recordingProvisioner{}

			if tt.wired {
				b.SetAccountProvisioner(rec)
			}

			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			create := b.CreateAccount
			if tt.govCloud {
				create = b.CreateGovCloudAccount
			}

			status, err := create("dev", "dev@example.com", tt.roleName, "", nil)
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", status.State)

			calls := rec.snapshot()
			if !tt.wantCall {
				assert.Empty(t, calls)

				return
			}

			require.Len(t, calls, 1)
			assert.Equal(t, status.AccountID, calls[0].accountID)
			assert.Equal(t, tt.wantRole, calls[0].roleName)
			assert.Contains(t, calls[0].trust, "arn:aws:iam::"+b.AccountID()+":root")
			assert.Contains(t, calls[0].trust, "sts:AssumeRole")
		})
	}
}

func TestCreateAccount_ProvisionsOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	const delay = 5 * time.Second

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend()
		rec := &recordingProvisioner{}
		b.SetAccountProvisioner(rec)
		b.SetLifecycleDelay(delay)

		_, _, err := b.CreateOrganization("ALL")
		require.NoError(t, err)

		first, err := b.CreateAccount("a", "dup@example.com", "", "", nil)
		require.NoError(t, err)
		assert.Empty(t, rec.snapshot())

		time.Sleep(delay)

		got, err := b.DescribeCreateAccountStatus(first.ID)
		require.NoError(t, err)
		require.Equal(t, "SUCCEEDED", got.State)
		require.Len(t, rec.snapshot(), 1)

		dup, err := b.CreateAccount("b", "dup@example.com", "", "", nil)
		require.NoError(t, err)

		time.Sleep(delay)

		got, err = b.DescribeCreateAccountStatus(dup.ID)
		require.NoError(t, err)
		require.Equal(t, "FAILED", got.State)
		assert.Len(t, rec.snapshot(), 1)
	})
}
