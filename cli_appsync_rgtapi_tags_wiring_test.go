package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestInitializeServices_AppSyncTagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than invoking
// wireTaggingAppSync directly, so that deleting the wiring call from
// wireResourceGroupsTaggingSweep6 -- not just breaking the helper function itself --
// is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_AppSyncTagsWiring(t *testing.T) {
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

	apH, ok := byName["AppSync"].(*appsyncbackend.Handler)
	require.True(t, ok, "AppSync handler must be registered")

	apBk, ok := apH.Backend.(*appsyncbackend.InMemoryBackend)
	require.True(t, ok, "AppSync backend must be an InMemoryBackend")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	api, err := apBk.CreateGraphqlAPI(
		"wiring-test-api", "", false, "", "", nil,
		map[string]string{"Team": "emu"}, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, api.ARN)

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

	assert.Contains(t, gotARNs, api.ARN,
		"GetResources must see the tagged AppSync API through the actual cli.go "+
			"composition root, not just the wiring helper called directly")
}
