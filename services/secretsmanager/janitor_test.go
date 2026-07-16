package secretsmanager_test

// janitor_test.go consolidates the background deletion-sweep tests that were
// previously part of an older test file. Ported verbatim (assertions unchanged); the
// janitor implementation itself (janitor.go) is untouched.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Janitor — recovery window honoured on purge (bug fix: was hardcoded to 30d)
// ---------------------------------------------------------------------------

// TestJanitor_RecoveryWindowRespected verifies that secrets deleted with a
// custom RecoveryWindowInDays are purged by the janitor at the correct time.
// It uses SetNowForTest to pin the backend clock so DeletedDate / ScheduledDeletionDate
// are set relative to a known point; the janitor's sweep uses real time.Now() which
// is always in the future of any pinned past time.
func TestJanitor_RecoveryWindowRespected(t *testing.T) {
	t.Parallel()

	// Anchor: 8 days ago. ScheduledDeletionDate = anchor + 7 days = 1 day ago → purge.
	pastTime := time.Now().Add(-8 * 24 * time.Hour)

	t.Run("7day_window_expired_after_8_days_purged", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		b.SetNowForTest(func() time.Time { return pastTime })

		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "window-7d",
			SecretString: "secret",
		})
		require.NoError(t, err)

		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{
			SecretID:             "window-7d",
			RecoveryWindowInDays: ptr64(7),
		})
		require.NoError(t, err)

		// ScheduledDeletionDate = pastTime + 7 days = 1 day ago — janitor must purge.
		j := secretsmanager.NewJanitor(b, 0)
		j.SweepOnce(context.Background())
		assert.Equal(t, 0, secretsmanager.SecretCount(b),
			"secret with 7-day window deleted 8 days ago must be purged")
	})

	t.Run("30day_window_not_expired_after_8_days_preserved", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		b.SetNowForTest(func() time.Time { return pastTime })

		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "window-30d",
			SecretString: "secret",
		})
		require.NoError(t, err)

		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{
			SecretID:             "window-30d",
			RecoveryWindowInDays: ptr64(30),
		})
		require.NoError(t, err)

		// ScheduledDeletionDate = pastTime + 30 days = 22 days from now — must NOT purge.
		j := secretsmanager.NewJanitor(b, 0)
		j.SweepOnce(context.Background())
		assert.Equal(t, 1, secretsmanager.SecretCount(b),
			"secret with 30-day window deleted 8 days ago must NOT be purged")
	})
}

func TestJanitor_ForceDeletePurgesImmediately(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "force-delete-test",
		SecretString: "value",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{
		SecretID:                   "force-delete-test",
		ForceDeleteWithoutRecovery: true,
	})
	require.NoError(t, err)
	// ForceDelete removes immediately — no janitor sweep needed.
	assert.Equal(t, 0, secretsmanager.SecretCount(b), "force-deleted secret must be removed immediately")
}

func TestJanitor_ActiveSecretNotPurged(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "active-secret",
		SecretString: "value",
	})
	require.NoError(t, err)

	j := secretsmanager.NewJanitor(b, 0)
	j.SweepOnce(context.Background())
	assert.Equal(t, 1, secretsmanager.SecretCount(b), "active secret must not be purged by janitor")
}

func TestJanitor_DeletionDateReturnedMatchesRecoveryWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		recoveryDays int64
	}{
		{"7_days", 7},
		{"14_days", 14},
		{"30_days", 30},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			epoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			b := secretsmanager.NewInMemoryBackend()
			b.SetNowForTest(func() time.Time { return epoch })

			_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name:         "del-date-" + tc.name,
				SecretString: "v",
			})
			require.NoError(t, err)

			out, err := b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{
				SecretID:             "del-date-" + tc.name,
				RecoveryWindowInDays: ptr64(tc.recoveryDays),
			})
			require.NoError(t, err)

			expected := epoch.Add(time.Duration(tc.recoveryDays) * 24 * time.Hour)
			assert.InDelta(t, float64(expected.Unix()), out.DeletionDate, 1.0,
				"DeletionDate must be epoch + %d days", tc.recoveryDays)
		})
	}
}
