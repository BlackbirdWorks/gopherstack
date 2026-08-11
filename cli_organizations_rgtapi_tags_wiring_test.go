package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// TestInitializeServices_OrganizationsTagsWiring drives the actual composition
// root (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireTaggingOrganizations directly, so that deleting the wiring call
// from wireResourceGroupsTaggingSweep6 -- not just breaking the helper
// function itself -- is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_OrganizationsTagsWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	orgH, ok := byName["Organizations"].(*organizationsbackend.Handler)
	require.True(t, ok, "Organizations handler must be registered")

	orgBk, ok := orgH.Backend.(*organizationsbackend.InMemoryBackend)
	require.True(t, ok, "Organizations backend must be an InMemoryBackend")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	_, root, err := orgBk.CreateOrganization("ALL")
	require.NoError(t, err)
	require.NotEmpty(t, root.ID)

	ou, err := orgBk.CreateOrganizationalUnit(root.ID, "wiring-test-ou",
		[]organizationsbackend.Tag{{Key: "Team", Value: "emu"}},
	)
	require.NoError(t, err)
	require.NotEmpty(t, ou.ARN)

	ctx := t.Context()

	out, err := rgtH.Backend.GetResources(ctx, &rgtapibackend.GetResourcesInput{
		TagFilters: []rgtapibackend.TagFilter{
			{Key: "Team", Values: []string{"emu"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out)

	gotARNs := make([]string, 0, len(out.ResourceTagMappingList))
	for _, m := range out.ResourceTagMappingList {
		gotARNs = append(gotARNs, m.ResourceARN)
	}

	assert.Contains(t, gotARNs, ou.ARN,
		"GetResources must see the tagged Organizations OU through the actual cli.go "+
			"composition root, not just the wiring helper called directly")
}
