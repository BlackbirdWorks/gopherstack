package ec2_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteLaunchTemplate verifies launch template deletion.
func TestDeleteLaunchTemplate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	lt, err := b.CreateLaunchTemplate("my-template", "ami-0c55b159cbfafe1f0", "t3.micro", nil)
	require.NoError(t, err)

	_, err = b.DeleteLaunchTemplate(lt.ID)
	require.NoError(t, err)

	versions, err := b.DescribeLaunchTemplateVersions(lt.ID)
	require.Error(t, err)
	assert.Nil(t, versions)
}

// TestDescribeLaunchTemplateVersions verifies version listing.

// TestDescribeLaunchTemplateVersions verifies version listing.
func TestDescribeLaunchTemplateVersions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	lt, err := b.CreateLaunchTemplate("versioned", "ami-0c55b159cbfafe1f0", "t3.small", nil)
	require.NoError(t, err)

	versions, err := b.DescribeLaunchTemplateVersions(lt.ID)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, lt.ID, versions[0].ID)
	assert.Equal(t, int64(1), versions[0].DefaultVersionNumber)
}

// TestDeleteVpcEndpoints verifies VPC endpoint deletion.

// TestHTTP_DeleteLaunchTemplate verifies the HTTP handler for DeleteLaunchTemplate.
func TestHTTP_DeleteLaunchTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create one first
	rec := postForm(t, h, "Action=CreateLaunchTemplate&Version=2016-11-15"+
		"&LaunchTemplateName=test&LaunchTemplateData.ImageId=ami-0c55b159cbfafe1f0")
	require.Equal(t, http.StatusOK, rec.Code)

	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	lts := b.DescribeLaunchTemplates(nil)
	require.Len(t, lts, 1)

	rec = postForm(
		t,
		h,
		"Action=DeleteLaunchTemplate&Version=2016-11-15&LaunchTemplateId="+lts[0].ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
