package app

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

type rgtWiringCase struct {
	listTags     func(ctx context.Context) (map[string]string, error)
	name         string
	arn          string
	resourceType string
}

func newRGTWiringEnv(t *testing.T) (map[string]service.Registerable, *rgtapibackend.Handler) {
	t.Helper()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	return byName, rgtH
}

// runRGTWiringCases checks GetResources (tag and type filters), TagResources and UntagResources for
// resources created with {"Team":"emu"}.
func runRGTWiringCases(t *testing.T, rgtH *rgtapibackend.Handler, cases []rgtWiringCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			filter := &rgtapibackend.GetResourcesInput{
				TagFilters:          []rgtapibackend.TagFilter{{Key: "Team", Values: []string{"emu"}}},
				ResourceTypeFilters: []string{tc.resourceType},
			}

			out, err := rgtH.Backend.GetResources(ctx, filter)
			require.NoError(t, err)

			found := false

			for _, m := range out.ResourceTagMappingList {
				found = found || m.ResourceARN == tc.arn
			}

			require.True(t, found, "%s missing from GetResources", tc.arn)

			tagged, err := rgtH.Backend.TagResources(ctx, &rgtapibackend.TagResourcesInput{
				ResourceARNList: []string{tc.arn}, Tags: map[string]string{"Env": "dev"},
			})
			require.NoError(t, err)
			assert.Empty(t, tagged.FailedResourcesMap)

			got, err := tc.listTags(ctx)
			require.NoError(t, err)
			assert.Equal(t, "dev", got["Env"])
			assert.Equal(t, "emu", got["Team"])

			untagged, err := rgtH.Backend.UntagResources(ctx, &rgtapibackend.UntagResourcesInput{
				ResourceARNList: []string{tc.arn}, TagKeys: []string{"Env"},
			})
			require.NoError(t, err)
			assert.Empty(t, untagged.FailedResourcesMap)

			got, err = tc.listTags(ctx)
			require.NoError(t, err)
			assert.NotContains(t, got, "Env")
			assert.Equal(t, "emu", got["Team"])
		})
	}
}
