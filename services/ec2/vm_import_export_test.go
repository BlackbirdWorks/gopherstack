package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func newBundleTestInstance(t *testing.T, b *ec2.InMemoryBackend) string {
	t.Helper()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	return instances[0].ID
}

func TestBackend_BundleTask_LifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newBundleTestInstance(t, b)

	task, err := b.BundleInstance(instanceID, "my-bucket", "windows/")
	require.NoError(t, err)
	assert.Contains(t, task.BundleID, "bundle-")
	assert.Equal(t, "pending", task.State)

	tasks := b.DescribeBundleTasks([]string{task.BundleID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "complete", tasks[0].State)
	assert.Equal(t, "100%", tasks[0].Progress)
}

func TestBackend_BundleTask_CancelSettlesToCancelled(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newBundleTestInstance(t, b)

	task, err := b.BundleInstance(instanceID, "my-bucket", "")
	require.NoError(t, err)

	cancelled, err := b.CancelBundleTask(task.BundleID)
	require.NoError(t, err)
	assert.Equal(t, "cancelling", cancelled.State)

	tasks := b.DescribeBundleTasks([]string{task.BundleID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "cancelled", tasks[0].State)

	_, err = b.CancelBundleTask(task.BundleID)
	require.ErrorIs(t, err, ec2.ErrTaskNotCancellable)
}

func TestBackend_BundleTask_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CancelBundleTask("bundle-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrBundleTaskNotFound)

	assert.Empty(t, b.DescribeBundleTasks([]string{"bundle-doesnotexist"}))
}

func TestBackend_BundleInstance_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.BundleInstance("i-doesnotexist", "my-bucket", "")
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_ConversionTask_ImportInstanceRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	task, err := b.ImportInstance("test import", "Linux", "", "VMDK", 1024, 20)
	require.NoError(t, err)
	assert.Contains(t, task.ConversionTaskID, "import-i-")
	assert.Equal(t, "active", task.State)

	tasks := b.DescribeConversionTasks([]string{task.ConversionTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "completed", tasks[0].State)
}

func TestBackend_ConversionTask_ImportInstanceRequiresPlatform(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.ImportInstance("desc", "", "", "VMDK", 1024, 20)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_ConversionTask_ImportVolumeRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	task, err := b.ImportVolume("test import", "us-east-1a", "VMDK", 2048, 40)
	require.NoError(t, err)
	assert.Contains(t, task.ConversionTaskID, "import-vol-")

	cancelled, err := b.CancelConversionTask(task.ConversionTaskID)
	require.NoError(t, err)
	assert.Equal(t, "cancelling", cancelled.State)

	tasks := b.DescribeConversionTasks([]string{task.ConversionTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "cancelled", tasks[0].State)
}

func TestBackend_ConversionTask_ImportVolumeRequiresAZAndSize(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.ImportVolume("desc", "", "VMDK", 1024, 20)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.ImportVolume("desc", "us-east-1a", "VMDK", 1024, 0)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_ConversionTask_CancelNotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CancelConversionTask("import-i-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrConversionTaskNotFound)
}

func TestBackend_ExportTask_LifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newBundleTestInstance(t, b)

	task, err := b.CreateInstanceExportTask(instanceID, "export test", "citrix", "VMDK", "OVA", "my-bucket", "exports/")
	require.NoError(t, err)
	assert.Contains(t, task.ExportTaskID, "export-i-")
	assert.Equal(t, "active", task.State)
	assert.Contains(t, task.S3Key, task.ExportTaskID)

	tasks := b.DescribeExportTasks([]string{task.ExportTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "completed", tasks[0].State)
}

func TestBackend_ExportTask_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateInstanceExportTask("i-doesnotexist", "", "citrix", "", "", "", "")
	require.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

func TestBackend_ExportTask_CancelAndNotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	instanceID := newBundleTestInstance(t, b)

	task, err := b.CreateInstanceExportTask(instanceID, "", "citrix", "", "", "", "")
	require.NoError(t, err)

	require.NoError(t, b.CancelExportTask(task.ExportTaskID))

	tasks := b.DescribeExportTasks([]string{task.ExportTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "cancelled", tasks[0].State)

	err = b.CancelExportTask("export-i-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrExportTaskNotFound)
}

func TestBackend_ExportImageTask_DescribeSettlesToCompleted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	task, err := b.ExportImage("ami-test", "", "", "", "", "")
	require.NoError(t, err)
	assert.Contains(t, task.ExportImageTaskID, "export-ami-")
	assert.Equal(t, "active", task.Status)
	assert.NotEmpty(t, task.S3Bucket)
	assert.Equal(t, "vmimport", task.RoleName)

	tasks := b.DescribeExportImageTasks([]string{task.ExportImageTaskID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "completed", tasks[0].Status)
}

func TestBackend_CancelImportTask_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, _, err := b.CancelImportTask("import-ami-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrImportTaskNotFound)
}

func TestBackend_CancelImportTask_AlreadyCompletedFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	imgTask, err := b.ImportImage("test", "x86_64", "Linux/UNIX")
	require.NoError(t, err)
	require.Equal(t, "completed", imgTask.Status)

	_, _, err = b.CancelImportTask(imgTask.ImportTaskID)
	require.ErrorIs(t, err, ec2.ErrTaskNotCancellable)

	snapTask, err := b.ImportSnapshot("test")
	require.NoError(t, err)

	_, _, err = b.CancelImportTask(snapTask.ImportTaskID)
	require.ErrorIs(t, err, ec2.ErrTaskNotCancellable)
}

func TestBackend_CancelImportTask_RequiresID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, _, err := b.CancelImportTask("")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}
