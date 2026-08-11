package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// TestInitializeServices_SSOAdminTagsWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireTaggingSSOAdmin directly, so that deleting the wiring call
// from wireResourceGroupsTaggingSweep6 -- not just breaking the helper
// function itself -- is what this test is sensitive to (gopherstack-pdqm).
func TestInitializeServices_SSOAdminTagsWiring(t *testing.T) {
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

	ssoH, ok := byName["SsoAdmin"].(*ssoadminbackend.Handler)
	require.True(t, ok, "SsoAdmin handler must be registered")

	ssoBk, ok := ssoH.Backend.(*ssoadminbackend.InMemoryBackend)
	require.True(t, ok, "SsoAdmin backend must be an InMemoryBackend")

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	instances := ssoBk.ListInstances()
	require.NotEmpty(t, instances, "a default SSO instance must be pre-seeded")
	instanceArn := instances[0].InstanceArn

	ps, err := ssoBk.CreatePermissionSet(
		instanceArn, "wiring-test-permission-set", "", "", "",
		map[string]string{"Team": "emu"},
	)
	require.NoError(t, err)
	require.NotEmpty(t, ps.PermissionSetArn)

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

	assert.Contains(t, gotARNs, ps.PermissionSetArn,
		"GetResources must see the tagged SSO Admin permission set through the actual "+
			"cli.go composition root, not just the wiring helper called directly")
}
