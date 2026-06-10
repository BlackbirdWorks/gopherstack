package sts_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestAssumeRole_EvictsExpiredSessionsWithoutJanitor verifies that creating a
// new session opportunistically sweeps expired sessions once the store grows
// past the eviction threshold, so b.sessions stays bounded even when the
// background janitor is disabled.
func TestAssumeRole_EvictsExpiredSessionsWithoutJanitor(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()

	// Seed more expired sessions than the eviction threshold so the next insert
	// triggers the inline sweep.
	expired := sts.SessionEvictThreshold + 16
	past := time.Now().UTC().Add(-time.Hour)

	for i := range expired {
		akid := fmt.Sprintf("ASIAEXPIRED%06d", i)
		b.AddSessionInternal(&sts.SessionInfo{
			AccessKeyID: akid,
			Expiration:  past,
		})
	}

	require.Equal(t, expired, b.SessionCount(), "all seeded expired sessions present before insert")

	// A single real AssumeRole insert should evict every expired session and add
	// exactly one live session.
	resp, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Role1",
		RoleSessionName: "live",
		DurationSeconds: 900,
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)

	require.Equal(t, 1, b.SessionCount(),
		"expired sessions must be evicted on insert, leaving only the live one")
}

// TestAssumeRole_BelowThresholdKeepsExpired confirms the sweep is a no-op below
// the threshold (steady-state inserts stay O(1) and do not eagerly scan).
func TestAssumeRole_BelowThresholdKeepsExpired(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	past := time.Now().UTC().Add(-time.Hour)

	b.AddSessionInternal(&sts.SessionInfo{AccessKeyID: "ASIAEXPIRED0", Expiration: past})

	_, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Role1",
		RoleSessionName: "live",
		DurationSeconds: 900,
	})
	require.NoError(t, err)

	// Below threshold: the expired session is not eagerly swept, so both remain.
	require.Equal(t, 2, b.SessionCount())
}
