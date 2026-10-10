package account_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func TestBackend_PrimaryEmailUpdate_CompletesAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want account.PrimaryEmailUpdateStatus
		wait time.Duration
	}{
		{name: "accepted", wait: 0, want: account.PrimaryEmailUpdateStatusAccepted},
		{name: "completed", wait: time.Second, want: account.PrimaryEmailUpdateStatusCompleted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := account.NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Close()

				otp, err := b.StartPrimaryEmailUpdate("new@example.com")
				require.NoError(t, err)
				require.NoError(t, b.AcceptPrimaryEmailUpdate(otp, "new@example.com"))

				time.Sleep(tt.wait)

				status, _, err := b.GetPrimaryEmailUpdateStatus()
				require.NoError(t, err)
				assert.Equal(t, tt.want, status)
			})
		})
	}
}

func TestBackend_RegionTransition_RestoreSettles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		toggle func(*account.InMemoryBackend, string) error
		want   account.RegionOptStatus
	}{
		{
			name:   "disabling",
			region: "af-south-1",
			toggle: (*account.InMemoryBackend).DisableRegion,
			want:   account.RegionOptStatusDisabled,
		},
		{name: "enabling", region: "af-south-1", toggle: func(b *account.InMemoryBackend, r string) error {
			if err := b.DisableRegion(r); err != nil {
				return err
			}

			time.Sleep(time.Second)

			return b.EnableRegion(r)
		}, want: account.RegionOptStatusEnabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				src := account.NewInMemoryBackend("000000000000", "us-east-1")
				defer src.Close()

				require.NoError(t, tt.toggle(src, tt.region))

				snap := src.Snapshot(t.Context())

				dst := account.NewInMemoryBackend("000000000000", "us-east-1")
				defer dst.Close()

				require.NoError(t, dst.Restore(t.Context(), snap))

				got, err := dst.GetRegionOptStatus(tt.region)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		})
	}
}
