package main

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TestInitializeServices_APIGatewayTagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than invoking
// wireTaggingAPIGateway directly, so that deleting the wiring call from
// wireResourceGroupsTaggingSweep6 -- not just breaking the helper function
// itself -- is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_APIGatewayTagsWiring(t *testing.T) {
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

	apigwH, ok := byName["APIGateway"].(*apigwbackend.Handler)
	require.True(t, ok, "APIGateway handler must be registered")

	apigwBk, ok := apigwH.Backend.(*apigwbackend.InMemoryBackend)
	require.True(t, ok, "APIGateway backend must be an InMemoryBackend")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	api, err := apigwBk.CreateRestAPI(apigwbackend.CreateRestAPIInput{
		Name: "wiring-test-api",
		Tags: tags.FromMap("", map[string]string{"Team": "emu"}),
	})
	require.NoError(t, err)
	require.NotEmpty(t, api.ID)

	wantARN := fmt.Sprintf("arn:aws:apigateway:us-east-1::/restapis/%s", api.ID)

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

	assert.Contains(t, gotARNs, wantARN,
		"GetResources must see the tagged API Gateway REST API through the actual cli.go "+
			"composition root, not just the wiring helper called directly")
}
