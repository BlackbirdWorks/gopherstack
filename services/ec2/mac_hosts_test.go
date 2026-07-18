package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_DescribeMacHosts_DerivedFromDedicatedHosts(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.AllocateHosts("us-east-1a", "mac2.metal", 1)
	require.NoError(t, err)

	nonMac, err := b.AllocateHosts("us-east-1a", "c5.metal", 1)
	require.NoError(t, err)
	require.Len(t, nonMac, 1)

	hosts := b.DescribeMacHosts(nil)
	require.Len(t, hosts, 1)
	assert.Contains(t, hosts[0].HostID, "h-")
	assert.Contains(t, hosts[0].MacOSLatestSupportedVersions, "15.3.1")
}

func TestBackend_DescribeMacHosts_FilterByID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts1, err := b.AllocateHosts("us-east-1a", "mac1.metal", 1)
	require.NoError(t, err)

	_, err = b.AllocateHosts("us-east-1a", "mac2.metal", 1)
	require.NoError(t, err)

	filtered := b.DescribeMacHosts([]string{hosts1[0].HostID})
	require.Len(t, filtered, 1)
	assert.Equal(t, hosts1[0].HostID, filtered[0].HostID)
	assert.Contains(t, filtered[0].MacOSLatestSupportedVersions, "12.7.6")
}

func newMacInstance(t *testing.T, b *ec2.InMemoryBackend) string {
	t.Helper()

	instances, err := b.RunInstances("ami-test", "mac2.metal", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	return instances[0].ID
}

func TestBackend_CreateMacSIPModificationTask_RoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newMacInstance(t, b)

	task, err := b.CreateMacSystemIntegrityProtectionModificationTask(
		instanceID, "enabled", &ec2.MacSIPConfig{NvramProtections: "disabled"}, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, task.MacModificationTaskID, "macmodtask-")
	assert.Equal(t, "sip-modification", task.TaskType)
	assert.Equal(t, "pending", task.TaskState)
	require.NotNil(t, task.SIPConfig)
	assert.Equal(t, "enabled", task.SIPConfig.Status)
	assert.Equal(t, "disabled", task.SIPConfig.NvramProtections)

	tasks := b.DescribeMacModificationTasks([]string{task.MacModificationTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "successful", tasks[0].TaskState)
}

func TestBackend_CreateDelegateMacVolumeOwnershipTask_RoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newMacInstance(t, b)

	task, err := b.CreateDelegateMacVolumeOwnershipTask(instanceID, `{"internalDiskPassword":""}`, nil)
	require.NoError(t, err)
	assert.Equal(t, "volume-ownership-delegation", task.TaskType)
	assert.Equal(t, instanceID, task.InstanceID)

	tasks := b.DescribeMacModificationTasks(nil)
	require.Len(t, tasks, 1)
	assert.Equal(t, "successful", tasks[0].TaskState)
}

func TestBackend_MacModificationTasks_RejectNonMacInstance(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	_, err = b.CreateMacSystemIntegrityProtectionModificationTask(instanceID, "enabled", nil, nil)
	require.ErrorIs(t, err, ec2.ErrMacInstanceRequired)

	_, err = b.CreateDelegateMacVolumeOwnershipTask(instanceID, "creds", nil)
	require.ErrorIs(t, err, ec2.ErrMacInstanceRequired)
}

func TestBackend_MacModificationTasks_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateMacSystemIntegrityProtectionModificationTask("i-doesnotexist", "enabled", nil, nil)
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)

	_, err = b.CreateDelegateMacVolumeOwnershipTask("i-doesnotexist", "creds", nil)
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_CreateMacSIPModificationTask_RequiresStatus(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newMacInstance(t, b)

	_, err := b.CreateMacSystemIntegrityProtectionModificationTask(instanceID, "", nil, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}
