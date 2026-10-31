package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcprmbackend "github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

func TestGCPResourceManagerSettingsReachTheProvider(t *testing.T) {
	t.Parallel()

	cli := &CLI{GCPResourceManager: gcprmbackend.Settings{
		OrganizationID: "42", Domain: "corp.example", SeedProjects: "wired-one,wired-two",
		DeleteRetention: time.Hour, Port: 8999,
	}}

	assert.Equal(t, cli.GCPResourceManager, cli.GetGCPResourceManagerSettings())

	reg, err := (&gcprmbackend.Provider{}).Init(&service.AppContext{Config: cli})
	require.NoError(t, err)

	h, ok := reg.(*gcprmbackend.Handler)
	require.True(t, ok)
	assert.Equal(t, 8999, h.Port)
	assert.Equal(t, []string{"wired-one", "wired-two"}, h.Backend.ProjectIDs())
	assert.Equal(t, "organizations/42", h.Backend.OrganizationName())
}

func TestGCPResourceManagerProviderRejectsNilContext(t *testing.T) {
	t.Parallel()

	_, err := (&gcprmbackend.Provider{}).Init(nil)
	require.ErrorIs(t, err, gcprmbackend.ErrNilAppContext)
}
