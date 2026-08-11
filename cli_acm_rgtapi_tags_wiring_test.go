package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// TestInitializeServices_ACMTagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireTaggingACM directly, so that deleting the wiring call from
// wireResourceGroupsTaggingSweep6 -- not just breaking the helper function
// itself -- is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_ACMTagsWiring(t *testing.T) {
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

	acmH, ok := byName["ACM"].(*acmbackend.Handler)
	require.True(t, ok, "ACM handler must be registered")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	ctx := t.Context()

	cert, err := acmH.Backend.RequestCertificate(
		ctx, "wiring-test.example.com", "", "", "", "", "", "", nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, cert.ARN)

	err = acmH.TagResource(ctx, cert.ARN, map[string]string{"Team": "emu"})
	require.NoError(t, err)

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

	assert.Contains(t, gotARNs, cert.ARN,
		"GetResources must see the tagged ACM certificate through the actual cli.go "+
			"composition root, not just the wiring helper called directly")
}
