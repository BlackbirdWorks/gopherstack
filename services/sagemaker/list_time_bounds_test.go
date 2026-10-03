package sagemaker_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const listBoundsRole = "arn:aws:iam::000000000000:role/access"

// TestListCreationTimeBounds pins whether each List op's creation-time bounds
// include a resource created at exactly the bound, per the SDK input docs.
func TestListCreationTimeBounds(t *testing.T) {
	t.Parallel()

	type listFn func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int

	tests := []struct {
		create     func(t *testing.T, c *sagemakersdk.Client)
		list       listFn
		name       string
		wantAtLow  int
		wantAtHigh int
	}{
		{
			name:       "human_task_uis_after_inclusive",
			wantAtLow:  1,
			wantAtHigh: 0,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateHumanTaskUi(t.Context(), &sagemakersdk.CreateHumanTaskUiInput{
					HumanTaskUiName: aws.String("ui"),
					UiTemplate:      &smtypes.UiTemplate{Content: aws.String("<html></html>")},
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListHumanTaskUis(t.Context(), &sagemakersdk.ListHumanTaskUisInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.HumanTaskUiSummaries)
			},
		},
		{
			name:       "flow_definitions_after_inclusive",
			wantAtLow:  1,
			wantAtHigh: 0,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateFlowDefinition(t.Context(), &sagemakersdk.CreateFlowDefinitionInput{
					FlowDefinitionName: aws.String("flow"),
					OutputConfig:       &smtypes.FlowDefinitionOutputConfig{S3OutputPath: aws.String("s3://b/o")},
					RoleArn:            aws.String(listBoundsRole),
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListFlowDefinitions(t.Context(), &sagemakersdk.ListFlowDefinitionsInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.FlowDefinitionSummaries)
			},
		},
		{
			name:       "endpoints_after_inclusive",
			wantAtLow:  1,
			wantAtHigh: 0,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
					EndpointConfigName: aws.String("cfg"),
					ProductionVariants: []smtypes.ProductionVariant{{
						VariantName:          aws.String("v1"),
						ModelName:            aws.String("m"),
						InitialInstanceCount: aws.Int32(1),
						InstanceType:         smtypes.ProductionVariantInstanceTypeMlM5Large,
					}},
				})
				require.NoError(t, err)

				_, err = c.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
					EndpointName: aws.String("ep"), EndpointConfigName: aws.String("cfg"),
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListEndpoints(t.Context(), &sagemakersdk.ListEndpointsInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.Endpoints)
			},
		},
		{
			name:       "images_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
					ImageName: aws.String("img"), RoleArn: aws.String(listBoundsRole),
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListImages(t.Context(), &sagemakersdk.ListImagesInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.Images)
			},
		},
		{
			name:       "contexts_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateContext(t.Context(), &sagemakersdk.CreateContextInput{
					ContextName: aws.String("ctx"),
					ContextType: aws.String("Endpoint"),
					Source:      &smtypes.ContextSource{SourceUri: aws.String("s3://b/k")},
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListContexts(t.Context(), &sagemakersdk.ListContextsInput{
					CreatedAfter: after, CreatedBefore: before,
				})
				require.NoError(t, err)

				return len(out.ContextSummaries)
			},
		},
		{
			name:       "actions_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
					ActionName: aws.String("act"),
					ActionType: aws.String("ModelDeployment"),
					Source:     &smtypes.ActionSource{SourceUri: aws.String("s3://b/k")},
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListActions(t.Context(), &sagemakersdk.ListActionsInput{
					CreatedAfter: after, CreatedBefore: before,
				})
				require.NoError(t, err)

				return len(out.ActionSummaries)
			},
		},
		{
			name:       "artifacts_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
					ArtifactType: aws.String("Model"),
					Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://b/k")},
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListArtifacts(t.Context(), &sagemakersdk.ListArtifactsInput{
					CreatedAfter: after, CreatedBefore: before,
				})
				require.NoError(t, err)

				return len(out.ArtifactSummaries)
			},
		},
		{
			name:       "app_image_configs_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateAppImageConfig(t.Context(), &sagemakersdk.CreateAppImageConfigInput{
					AppImageConfigName: aws.String("aic"),
					KernelGatewayImageConfig: &smtypes.KernelGatewayImageConfig{
						KernelSpecs: []smtypes.KernelSpec{{Name: aws.String("python3")}},
					},
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListAppImageConfigs(t.Context(), &sagemakersdk.ListAppImageConfigsInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.AppImageConfigs)
			},
		},
		{
			name:       "studio_lifecycle_configs_on_or_after_on_or_before",
			wantAtLow:  1,
			wantAtHigh: 1,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateStudioLifecycleConfig(t.Context(), &sagemakersdk.CreateStudioLifecycleConfigInput{
					StudioLifecycleConfigName:    aws.String("slc"),
					StudioLifecycleConfigContent: aws.String("IyEvYmluL2Jhc2g="),
					StudioLifecycleConfigAppType: smtypes.StudioLifecycleConfigAppTypeJupyterServer,
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListStudioLifecycleConfigs(t.Context(), &sagemakersdk.ListStudioLifecycleConfigsInput{
					CreationTimeAfter: after, CreationTimeBefore: before,
				})
				require.NoError(t, err)

				return len(out.StudioLifecycleConfigs)
			},
		},
		{
			name:       "experiments_after_before_exclusive",
			wantAtLow:  0,
			wantAtHigh: 0,
			create: func(t *testing.T, c *sagemakersdk.Client) {
				t.Helper()

				_, err := c.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{
					ExperimentName: aws.String("exp"),
				})
				require.NoError(t, err)
			},
			list: func(t *testing.T, c *sagemakersdk.Client, after, before *time.Time) int {
				t.Helper()

				out, err := c.ListExperiments(t.Context(), &sagemakersdk.ListExperimentsInput{
					CreatedAfter: after, CreatedBefore: before,
				})
				require.NoError(t, err)

				return len(out.ExperimentSummaries)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newTestHandler(t)
				defer h.Shutdown(t.Context())

				c := newTestSageMakerClient(t, h)
				tc.create(t, c)

				bound := time.Now()

				assert.Equal(t, tc.wantAtLow, tc.list(t, c, &bound, nil), "after == creation time")
				assert.Equal(t, tc.wantAtHigh, tc.list(t, c, nil, &bound), "before == creation time")

				past, future := bound.Add(-time.Hour), bound.Add(time.Hour)
				assert.Equal(t, 1, tc.list(t, c, &past, &future), "window around creation time")
				assert.Equal(t, 0, tc.list(t, c, &future, nil), "after a later time")
				assert.Equal(t, 0, tc.list(t, c, nil, &past), "before an earlier time")
			})
		})
	}
}

// TestListAIWorkloadConfigsDefaultOrder pins ListAIWorkloadConfigs' documented
// defaults: SortBy CreationTime, SortOrder Descending.
func TestListAIWorkloadConfigsDefaultOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sortBy    smtypes.ListAIWorkloadConfigsSortBy
		sortOrder smtypes.SortOrder
		want      []string
	}{
		{name: "defaults", want: []string{"a-new", "b-old"}},
		{name: "ascending", sortOrder: smtypes.SortOrderAscending, want: []string{"b-old", "a-new"}},
		{
			name:   "by_name_default_order",
			sortBy: smtypes.ListAIWorkloadConfigsSortByName,
			want:   []string{"b-old", "a-new"},
		},
		{
			name:      "by_name_ascending",
			sortBy:    smtypes.ListAIWorkloadConfigsSortByName,
			sortOrder: smtypes.SortOrderAscending,
			want:      []string{"a-new", "b-old"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			{
				c := newRealClient(t)

				for _, name := range []string{"b-old", "a-new"} {
					_, err := c.CreateAIWorkloadConfig(t.Context(), &sagemakersdk.CreateAIWorkloadConfigInput{
						AIWorkloadConfigName: aws.String(name),
						AIWorkloadConfigs: &smtypes.AIWorkloadConfigs{
							WorkloadSpec: &smtypes.WorkloadSpecMemberInline{Value: "{}"},
						},
					})
					require.NoError(t, err)
				}

				out, err := c.ListAIWorkloadConfigs(t.Context(), &sagemakersdk.ListAIWorkloadConfigsInput{
					SortBy: tc.sortBy, SortOrder: tc.sortOrder,
				})
				require.NoError(t, err)

				got := make([]string, 0, len(out.AIWorkloadConfigs))
				for _, s := range out.AIWorkloadConfigs {
					got = append(got, aws.ToString(s.AIWorkloadConfigName))
				}

				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// TestListAppsRejectsUserProfileWithSpace pins ListAppsInput's documented
// exclusion between UserProfileNameEquals and SpaceNameEquals.
func TestListAppsRejectsUserProfileWithSpace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		profile *string
		space   *string
		name    string
		wantErr bool
	}{
		{name: "both_set", profile: aws.String("p"), space: aws.String("s"), wantErr: true},
		{name: "profile_only", profile: aws.String("p")},
		{name: "space_only", space: aws.String("s")},
		{name: "neither"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			_, err := c.ListApps(t.Context(), &sagemakersdk.ListAppsInput{
				UserProfileNameEquals: tc.profile, SpaceNameEquals: tc.space,
			})

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}

			require.NoError(t, err)
		})
	}
}
