package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_AllowedImagesSettings_DefaultsToDisabled(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "disabled", b.GetAllowedImagesSettings().State)
}

func TestBackend_AllowedImagesSettings_EnableReplaceDisableRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	state, err := b.EnableAllowedImagesSettings("enabled")
	require.NoError(t, err)
	assert.Equal(t, "enabled", state)

	settings := b.GetAllowedImagesSettings()
	assert.Equal(t, "enabled", settings.State)
	assert.Equal(t, "account", settings.ManagedBy)

	ok := b.ReplaceImageCriteriaInAllowedImagesSettings([]ec2.ImageCriterion{
		{ImageProviders: []string{"amazon"}, ImageNames: []string{"amzn2-*"}},
	})
	assert.True(t, ok)

	settings = b.GetAllowedImagesSettings()
	require.Len(t, settings.ImageCriteria, 1)
	assert.Equal(t, []string{"amazon"}, settings.ImageCriteria[0].ImageProviders)

	disabledState := b.DisableAllowedImagesSettings()
	assert.Equal(t, "disabled", disabledState)
	assert.Equal(t, "disabled", b.GetAllowedImagesSettings().State)
}

func TestBackend_AllowedImagesSettings_EnableInvalidStateFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.EnableAllowedImagesSettings("bogus")
	require.Error(t, err)
}

func TestBackend_StoreRestoreImageTask_CreateDescribeRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	img, err := b.RegisterImage("my-ami", "test ami", "")
	require.NoError(t, err)

	task, err := b.CreateStoreImageTask(img.ImageID, "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, img.ImageID+".bin", task.S3ObjectKey)
	assert.Equal(t, "completed", task.StoreTaskState)
	assert.Equal(t, int32(100), task.ProgressPercentage)

	tasks := b.DescribeStoreImageTasks([]string{img.ImageID})
	require.Len(t, tasks, 1)
	assert.Equal(t, "my-bucket", tasks[0].Bucket)

	restored, err := b.CreateRestoreImageTask("my-bucket", task.S3ObjectKey, "restored-name")
	require.NoError(t, err)
	assert.Contains(t, restored.ImageID, "ami-")
	assert.Equal(t, "restored-name", restored.Name)
	assert.NotEqual(t, img.ImageID, restored.ImageID)
}

func TestBackend_CreateStoreImageTask_UnknownImageFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateStoreImageTask("ami-doesnotexist", "my-bucket")
	require.Error(t, err)
}

func TestBackend_DescribeStoreImageTasks_UnknownImageReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Empty(t, b.DescribeStoreImageTasks([]string{"ami-doesnotexist"}))
}

func TestBackend_CreateRestoreImageTask_UnknownObjectFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateRestoreImageTask("no-such-bucket", "no-such-key", "")
	require.Error(t, err)
}

func TestBackend_ImageUsageReport_CreateDescribeDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	img, err := b.RegisterImage("my-ami", "", "")
	require.NoError(t, err)

	_, err = b.RunInstances(img.ImageID, "t2.micro", "", 2)
	require.NoError(t, err)

	report, err := b.CreateImageUsageReport(img.ImageID, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, report.ReportID, "imgusgrpt-")

	entries := b.DescribeImageUsageReportEntries([]string{report.ReportID}, nil)
	require.Len(t, entries, 1)
	assert.Equal(t, "ec2:Instance", entries[0].ResourceType)
	assert.Equal(t, int64(2), entries[0].UsageCount)

	require.NoError(t, b.DeleteImageUsageReport(report.ReportID))
	assert.Empty(t, b.DescribeImageUsageReportEntries([]string{report.ReportID}, nil))
}

func TestBackend_CreateImageUsageReport_UnknownImageFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateImageUsageReport("ami-doesnotexist", nil, nil)
	require.Error(t, err)
}

func TestBackend_DeleteImageUsageReport_UnknownFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.DeleteImageUsageReport("imgusgrpt-doesnotexist")
	require.Error(t, err)
}

func TestBackend_ConfirmProductInstance_NoMatchingCodeReturnsFalse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-12345", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	ok, err := b.ConfirmProductInstance(instances[0].ID, "prod-abc123")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestBackend_ConfirmProductInstance_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.ConfirmProductInstance("i-doesnotexist", "prod-abc123")
	require.Error(t, err)
}

func TestBackend_ConfirmProductInstance_MissingProductCodeFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-12345", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	_, err = b.ConfirmProductInstance(instances[0].ID, "")
	require.Error(t, err)
}
