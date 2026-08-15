package ec2_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlerLaunchTemplateVersions covers handleCreateLaunchTemplateVersion,
// handleDeleteLaunchTemplateVersions, handleGetLaunchTemplateData,
// handleDescribeLaunchTemplateVersions.
func TestHandlerLaunchTemplateVersions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a launch template.
	lt, err := b.CreateLaunchTemplate("test-lt", "ami-12345", "t3.micro", nil)
	require.NoError(t, err)
	ltID := lt.ID

	// Create a new version.
	rec := postForm(t, h, "Action=CreateLaunchTemplateVersion&Version=2016-11-15"+
		"&LaunchTemplateId="+ltID+
		"&LaunchTemplateData.ImageId=ami-99999"+
		"&LaunchTemplateData.InstanceType=t3.small")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateLaunchTemplateVersionResponse")

	// Describe versions.
	rec = postForm(
		t,
		h,
		"Action=DescribeLaunchTemplateVersions&Version=2016-11-15&LaunchTemplateId="+ltID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeLaunchTemplateVersionsResponse")

	// Get launch template data from an instance.
	insts, err := b.RunInstances("ami-12345", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := insts[0].ID

	rec = postForm(t, h, "Action=GetLaunchTemplateData&Version=2016-11-15&InstanceId="+instanceID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetLaunchTemplateDataResponse")

	// Delete a version.
	rec = postForm(t, h, "Action=DeleteLaunchTemplateVersions&Version=2016-11-15"+
		"&LaunchTemplateId="+ltID+
		"&LaunchTemplateVersion.1=2")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteLaunchTemplateVersionsResponse")
}

// TestHandlerDeleteSnapshot covers handleDeleteSnapshot.
