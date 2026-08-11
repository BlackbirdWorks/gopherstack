package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestInitializeServices_ResourceGroupsTaggingPolicyWiring drives the actual
// composition root (initializeServices, the function cli.go's Run() calls)
// rather than invoking wireResourceGroupsTaggingPolicy directly, so that
// deleting the wiring call from wireResourceGroupsTagging -- not just
// breaking the helper function itself -- is what this test is sensitive to.
// This is the shape iot/appsync/sagemaker's undetected routing gaps had:
// correct code nothing reaches (gopherstack-2mwl).
func TestInitializeServices_ResourceGroupsTaggingPolicyWiring(t *testing.T) {
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

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

	orgH, ok := byName["Organizations"].(*organizationsbackend.Handler)
	require.True(t, ok, "Organizations handler must be registered")

	org, _, err := orgH.Backend.CreateOrganization("ALL")
	require.NoError(t, err)

	policy, err := orgH.Backend.CreatePolicy(
		"require-costcenter", "", `{"tags":{"CostCenter":{"report_required_tag_for":{"@@assign":["ec2:instance"]}}}}`,
		"TAG_POLICY", nil,
	)
	require.NoError(t, err)

	require.NoError(t, orgH.Backend.AttachPolicy(policy.PolicySummary.ID, org.MasterAccountID))

	out := rgtH.Backend.ListRequiredTags(t.Context(), &rgtapibackend.ListRequiredTagsInput{})
	require.NotNil(t, out)
	require.Len(t, out.RequiredTags, 1, "ListRequiredTags must derive rows from Organizations' "+
		"effective TAG_POLICY through the actual cli.go composition root, not just the wiring "+
		"helper called directly")
	require.NotNil(t, out.RequiredTags[0].ResourceType)
	assert.Equal(t, "ec2:instance", *out.RequiredTags[0].ResourceType)
	assert.Equal(t, []string{"CostCenter"}, out.RequiredTags[0].ReportingTagKeys)
}
