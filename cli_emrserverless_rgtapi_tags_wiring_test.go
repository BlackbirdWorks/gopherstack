package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	emrserverlessbackend "github.com/blackbirdworks/gopherstack/services/emrserverless"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestInitializeServices_EmrServerlessTagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than invoking
// wireTaggingEmrServerless directly, so that deleting the wiring call from
// wireResourceGroupsTaggingSweep6 -- not just breaking the helper function itself --
// is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_EmrServerlessTagsWiring(t *testing.T) {
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

	emrH, ok := byName["EmrServerless"].(*emrserverlessbackend.Handler)
	require.True(t, ok, "EmrServerless handler must be registered")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	app, err := emrH.Backend.CreateApplication(
		"wiring-test-app", "SPARK", "emr-7.1.0", "",
		map[string]string{"Team": "emu"},
	)
	require.NoError(t, err)
	require.NotEmpty(t, app.Arn)

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

	assert.Contains(t, gotARNs, app.Arn,
		"GetResources must see the tagged EMR Serverless application through the actual "+
			"cli.go composition root, not just the wiring helper called directly")
}
