package ce_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestInMemoryBackend_AnomalyMonitorNotFound verifies that every anomaly-monitor backend
// method returns ce.ErrUnknownMonitor (real AWS CE's UnknownMonitorException) -- not the
// generic ce.ErrNotFound -- when given a MonitorArn that doesn't exist.
func TestInMemoryBackend_AnomalyMonitorNotFound(t *testing.T) {
	t.Parallel()

	const missingARN = "arn:aws:ce::000000000000:anomalymonitor/does-not-exist"

	tests := []struct {
		run  func(b *ce.InMemoryBackend) error
		name string
	}{
		{
			name: "DeleteAnomalyMonitor",
			run: func(b *ce.InMemoryBackend) error {
				return b.DeleteAnomalyMonitor(missingARN)
			},
		},
		{
			name: "UpdateAnomalyMonitor",
			run: func(b *ce.InMemoryBackend) error {
				_, err := b.UpdateAnomalyMonitor(missingARN, "NewName")

				return err
			},
		},
		{
			name: "GetAnomalyMonitors_by_arn_list",
			run: func(b *ce.InMemoryBackend) error {
				_, _, err := b.GetAnomalyMonitors([]string{missingARN}, 0, "")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")

			err := tt.run(b)
			require.Error(t, err)
			require.ErrorIs(t, err, ce.ErrUnknownMonitor, "want ErrUnknownMonitor, got %v", err)
			assert.NotErrorIs(t, err, ce.ErrNotFound, "must not also match the generic ErrNotFound sentinel")
		})
	}
}

// TestInMemoryBackend_AnomalySubscriptionNotFound verifies that every anomaly-subscription
// backend method returns ce.ErrUnknownSubscription (real AWS CE's UnknownSubscriptionException)
// -- not the generic ce.ErrNotFound -- when given a SubscriptionArn that doesn't exist.
func TestInMemoryBackend_AnomalySubscriptionNotFound(t *testing.T) {
	t.Parallel()

	const missingARN = "arn:aws:ce::000000000000:anomalysubscription/does-not-exist"

	tests := []struct {
		run  func(b *ce.InMemoryBackend) error
		name string
	}{
		{
			name: "DeleteAnomalySubscription",
			run: func(b *ce.InMemoryBackend) error {
				return b.DeleteAnomalySubscription(missingARN)
			},
		},
		{
			name: "UpdateAnomalySubscription",
			run: func(b *ce.InMemoryBackend) error {
				_, err := b.UpdateAnomalySubscription(missingARN, "DAILY", "", nil, nil, 0, nil)

				return err
			},
		},
		{
			name: "GetAnomalySubscriptions_by_arn_list",
			run: func(b *ce.InMemoryBackend) error {
				_, _, err := b.GetAnomalySubscriptions([]string{missingARN}, "", 0, "")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")

			err := tt.run(b)
			require.Error(t, err)
			require.ErrorIs(t, err, ce.ErrUnknownSubscription, "want ErrUnknownSubscription, got %v", err)
			assert.NotErrorIs(t, err, ce.ErrNotFound, "must not also match the generic ErrNotFound sentinel")
		})
	}
}

// TestInMemoryBackend_CreateAnomalySubscription_UnknownMonitor verifies that
// CreateAnomalySubscription rejects a MonitorArnList entry that doesn't reference an
// existing monitor, instead of silently persisting a subscription pointed at a
// nonexistent monitor (real AWS CE returns UnknownMonitorException here).
func TestInMemoryBackend_CreateAnomalySubscription_UnknownMonitor(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	sub, err := b.CreateAnomalySubscription(
		"BadSub", "DAILY",
		[]string{"arn:aws:ce::000000000000:anomalymonitor/does-not-exist"},
		nil, 0, nil, nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ce.ErrUnknownMonitor)
	assert.Nil(t, sub)

	// The rejected create must not have persisted any subscription.
	subs, _, err := b.GetAnomalySubscriptions(nil, "", 0, "")
	require.NoError(t, err)
	assert.Empty(t, subs)
}

// TestInMemoryBackend_UpdateAnomalySubscription_UnknownMonitor verifies that updating a
// subscription's MonitorArnList to reference a nonexistent monitor is rejected, and that
// the rejected update does not mutate the existing subscription (atomicity).
func TestInMemoryBackend_UpdateAnomalySubscription_UnknownMonitor(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	mon, err := b.CreateAnomalyMonitor("RealMonitor", "DIMENSIONAL", "SERVICE", nil, nil)
	require.NoError(t, err)

	sub, err := b.CreateAnomalySubscription(
		"RealSub", "DAILY", []string{mon.MonitorARN}, nil, 0, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.UpdateAnomalySubscription(
		sub.SubscriptionARN, "", "",
		[]string{"arn:aws:ce::000000000000:anomalymonitor/does-not-exist"},
		nil, 0, nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ce.ErrUnknownMonitor)

	// Original MonitorArnList must be untouched.
	subs, _, err := b.GetAnomalySubscriptions([]string{sub.SubscriptionARN}, "", 0, "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, []string{mon.MonitorARN}, subs[0].MonitorARNList)
}

// TestInMemoryBackend_GetAnomalySubscriptions_MonitorArnFilterIgnoresUnknown verifies that
// the MonitorArn filter parameter (distinct from SubscriptionArnList) does not require the
// referenced monitor to exist -- real AWS CE documents no UnknownMonitorException for
// GetAnomalySubscriptions, it simply returns zero matches.
func TestInMemoryBackend_GetAnomalySubscriptions_MonitorArnFilterIgnoresUnknown(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	subs, _, err := b.GetAnomalySubscriptions(
		nil, "arn:aws:ce::000000000000:anomalymonitor/does-not-exist", 0, "",
	)
	require.NoError(t, err)
	assert.Empty(t, subs)
}
