package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestInitializeServices_AMPTagsWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	ampH, ok := byName["AMP"].(*ampbackend.Handler)
	require.True(t, ok)

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	ws, err := ampH.Backend.CreateWorkspace(t.Context(), ampbackend.CreateWorkspaceInput{
		Alias: "w", Tags: map[string]string{"Team": "emu"},
	})
	require.NoError(t, err)

	filter := &rgtapibackend.GetResourcesInput{
		TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{"emu"}}},
	}

	out, err := rgtH.Backend.GetResources(t.Context(), filter)
	require.NoError(t, err)

	arns := make([]string, 0, len(out.ResourceTagMappingList))
	for _, m := range out.ResourceTagMappingList {
		arns = append(arns, m.ResourceARN)
	}

	assert.Contains(t, arns, ws.Arn)

	tagged, err := rgtH.Backend.TagResources(t.Context(), &rgtapibackend.TagResourcesInput{
		ResourceARNList: []string{ws.Arn}, Tags: map[string]string{"Env": "dev"},
	})
	require.NoError(t, err)
	assert.Empty(t, tagged.FailedResourcesMap)

	got, err := ampH.Backend.ListTagsForResource(t.Context(), ws.Arn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"Team": "emu", "Env": "dev"}, got)

	untagged, err := rgtH.Backend.UntagResources(t.Context(), &rgtapibackend.UntagResourcesInput{
		ResourceARNList: []string{ws.Arn}, TagKeys: []string{"Env"},
	})
	require.NoError(t, err)
	assert.Empty(t, untagged.FailedResourcesMap)

	got, err = ampH.Backend.ListTagsForResource(t.Context(), ws.Arn)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"Team": "emu"}, got)
}
