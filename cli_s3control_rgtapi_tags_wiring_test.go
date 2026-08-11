package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestInitializeServices_S3ControlTagsWiring drives the actual composition
// root (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireTaggingS3Control directly, so that deleting the wiring call
// from wireResourceGroupsTaggingSweep5 -- not just breaking the helper
// function itself -- is what this test is sensitive to (gopherstack-8kco).
func TestInitializeServices_S3ControlTagsWiring(t *testing.T) {
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

	s3cH, ok := byName["S3Control"].(*s3controlbackend.Handler)
	require.True(t, ok, "S3Control handler must be registered")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	ap := s3cH.Backend.CreateAccessPoint(cli.AccountID, "wiring-test-ap", "wiring-test-bucket")
	require.NotEmpty(t, ap.AccessPointArn)

	s3cH.Backend.TagResource(ap.AccessPointArn, map[string]string{"Team": "emu"})

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

	assert.Contains(t, gotARNs, ap.AccessPointArn,
		"GetResources must see the tagged S3 Control access point through the actual "+
			"cli.go composition root, not just the wiring helper called directly")
}
