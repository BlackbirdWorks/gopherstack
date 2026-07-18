package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestInMemoryBackend_DomainAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	subs := []amplify.SubDomainSetting{
		{Prefix: "www", BranchName: "main"},
	}

	// Create
	da, err := b.CreateDomainAssociation(app.AppID, "example.com", subs, true)
	require.NoError(t, err)
	assert.Equal(t, "example.com", da.DomainName)
	assert.Equal(t, app.AppID, da.AppID)
	assert.Len(t, da.SubDomains, 1)
	assert.NotEmpty(t, da.ARN)

	// Duplicate create
	_, err = b.CreateDomainAssociation(app.AppID, "example.com", subs, false)
	require.Error(t, err)

	// Create for nonexistent app
	_, err = b.CreateDomainAssociation("nonexistent", "example.com", subs, false)
	require.Error(t, err)

	// Get
	got, err := b.GetDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, "example.com", got.DomainName)

	// Get nonexistent
	_, err = b.GetDomainAssociation(app.AppID, "nothere.com")
	require.Error(t, err)

	// List
	list, _, err := b.ListDomainAssociations(app.AppID, "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// List for nonexistent app
	_, _, err = b.ListDomainAssociations("nonexistent", "", 0)
	require.Error(t, err)

	// Update
	newSubs := []amplify.SubDomainSetting{
		{Prefix: "api", BranchName: "main"},
	}
	updated, err := b.UpdateDomainAssociation(app.AppID, "example.com", newSubs, false)
	require.NoError(t, err)
	assert.Len(t, updated.SubDomains, 1)
	assert.Equal(t, "api", updated.SubDomains[0].SubDomainSetting.Prefix)

	// Update nonexistent
	_, err = b.UpdateDomainAssociation(app.AppID, "nothere.com", newSubs, false)
	require.Error(t, err)

	// Delete
	deleted, err := b.DeleteDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, "example.com", deleted.DomainName)

	// Delete again
	_, err = b.DeleteDomainAssociation(app.AppID, "example.com")
	require.Error(t, err)
}
