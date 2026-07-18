package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_SQLHa_EnableDescribeSettlesToActive(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	regs, err := b.EnableInstanceSQLHaStandbyDetections([]string{instanceID}, "arn:aws:secretsmanager:sql-creds")
	require.NoError(t, err)
	require.Len(t, regs, 1)
	assert.Equal(t, "processing", regs[0].HaStatus)

	described := b.DescribeInstanceSQLHaStates([]string{instanceID})
	require.Len(t, described, 1)
	assert.Equal(t, "active", described[0].HaStatus)
}

func TestBackend_SQLHa_EnableUnknownInstanceFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.EnableInstanceSQLHaStandbyDetections([]string{"i-doesnotexist"}, "")
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_SQLHa_EnableRequiresInstanceIDs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.EnableInstanceSQLHaStandbyDetections(nil, "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_SQLHa_DisableRemovesRegistration(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	_, err = b.EnableInstanceSQLHaStandbyDetections([]string{instanceID}, "")
	require.NoError(t, err)

	disabled, err := b.DisableInstanceSQLHaStandbyDetections([]string{instanceID})
	require.NoError(t, err)
	require.Len(t, disabled, 1)

	assert.Empty(t, b.DescribeInstanceSQLHaStates([]string{instanceID}))
}

func TestBackend_SQLHa_HistoryStatesTracksTransitions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	_, err = b.EnableInstanceSQLHaStandbyDetections([]string{instanceID}, "")
	require.NoError(t, err)
	b.DescribeInstanceSQLHaStates([]string{instanceID}) // settles processing -> active, appends history

	_, err = b.DisableInstanceSQLHaStandbyDetections([]string{instanceID})
	require.NoError(t, err)

	history := b.DescribeInstanceSQLHaHistoryStates([]string{instanceID}, time.Time{}, time.Time{})
	require.GreaterOrEqual(t, len(history), 3) // enable(processing) -> settle(active) -> disable

	future := time.Now().Add(24 * time.Hour)
	assert.Empty(t, b.DescribeInstanceSQLHaHistoryStates([]string{instanceID}, future, time.Time{}))
}
