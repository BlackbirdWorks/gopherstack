package codebuild_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

type enumCall func(ctx context.Context, c *codebuildsdk.Client, v string) error

func enumStrings[T ~string](vs []T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}

	return out
}

func sortOrderCall(call func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error) enumCall {
	return func(ctx context.Context, c *codebuildsdk.Client, v string) error {
		return call(ctx, c, cbtypes.SortOrderType(v))
	}
}

func TestSDK_EnumInputValidation(t *testing.T) {
	t.Parallel()

	ro := &cbtypes.ReportExportConfig{ExportConfigType: cbtypes.ReportExportConfigTypeNoExport}
	sortValues := enumStrings(cbtypes.SortOrderType("").Values())
	computeValues := enumStrings(cbtypes.ComputeType("").Values())
	envValues := enumStrings(cbtypes.EnvironmentType("").Values())
	overflowValues := enumStrings(cbtypes.FleetOverflowBehavior("").Values())
	pullValues := enumStrings(cbtypes.ImagePullCredentialsType("").Values())
	sourceValues := enumStrings(cbtypes.SourceType("").Values())
	webhookValues := enumStrings(cbtypes.WebhookBuildType("").Values())
	sharedSortValues := enumStrings(cbtypes.SharedResourceSortByType("").Values())

	tests := []struct {
		call   enumCall
		name   string
		field  string
		values []string
	}{
		{name: "create fleet compute", field: "computeType", values: computeValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.CreateFleet(ctx, &codebuildsdk.CreateFleetInput{
					Name: aws.String("f"), BaseCapacity: aws.Int32(1), ComputeType: cbtypes.ComputeType(v),
					EnvironmentType: cbtypes.EnvironmentTypeLinuxContainer,
				})

				return err
			}},
		{name: "create fleet environment", field: "environmentType", values: envValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.CreateFleet(ctx, &codebuildsdk.CreateFleetInput{
					Name: aws.String("f"), BaseCapacity: aws.Int32(1), EnvironmentType: cbtypes.EnvironmentType(v),
					ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
				})

				return err
			}},
		{name: "create fleet overflow", field: "overflowBehavior", values: overflowValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.CreateFleet(ctx, &codebuildsdk.CreateFleetInput{
					Name:             aws.String("f"),
					BaseCapacity:     aws.Int32(1),
					OverflowBehavior: cbtypes.FleetOverflowBehavior(v),
					ComputeType:      cbtypes.ComputeTypeBuildGeneral1Small,
					EnvironmentType:  cbtypes.EnvironmentTypeLinuxContainer,
				})

				return err
			}},
		{name: "update fleet compute", field: "computeType", values: computeValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.UpdateFleet(ctx, &codebuildsdk.UpdateFleetInput{
					Arn: aws.String(
						"arn:aws:codebuild:us-east-1:000000000000:fleet/x",
					), ComputeType: cbtypes.ComputeType(v),
				})

				return err
			}},
		{name: "update fleet environment", field: "environmentType", values: envValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.UpdateFleet(ctx, &codebuildsdk.UpdateFleetInput{
					Arn: aws.String(
						"arn:aws:codebuild:us-east-1:000000000000:fleet/x",
					), EnvironmentType: cbtypes.EnvironmentType(v),
				})

				return err
			}},
		{name: "update fleet overflow", field: "overflowBehavior", values: overflowValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.UpdateFleet(ctx, &codebuildsdk.UpdateFleetInput{
					Arn: aws.String(
						"arn:aws:codebuild:us-east-1:000000000000:fleet/x",
					), OverflowBehavior: cbtypes.FleetOverflowBehavior(v),
				})

				return err
			}},
		{name: "create report group type", field: "type", values: enumStrings(cbtypes.ReportType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.CreateReportGroup(ctx, &codebuildsdk.CreateReportGroupInput{
					Name: aws.String("rg"), Type: cbtypes.ReportType(v), ExportConfig: ro,
				})

				return err
			}},
		{name: "create webhook build type", field: "buildType", values: webhookValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.CreateWebhook(ctx, &codebuildsdk.CreateWebhookInput{
					ProjectName: aws.String("p"), BuildType: cbtypes.WebhookBuildType(v),
				})

				return err
			}},
		{name: "update webhook build type", field: "buildType", values: webhookValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.UpdateWebhook(ctx, &codebuildsdk.UpdateWebhookInput{
					ProjectName: aws.String("p"), BuildType: cbtypes.WebhookBuildType(v),
				})

				return err
			}},
		{name: "describe code coverages sort by", field: "sortBy",
			values: enumStrings(cbtypes.ReportCodeCoverageSortByType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.DescribeCodeCoverages(ctx, &codebuildsdk.DescribeCodeCoveragesInput{
					ReportArn: aws.String("arn:aws:codebuild:us-east-1:000000000000:report/x"),
					SortBy:    cbtypes.ReportCodeCoverageSortByType(v),
				})

				return err
			}},
		{name: "describe code coverages sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.DescribeCodeCoverages(ctx, &codebuildsdk.DescribeCodeCoveragesInput{
					ReportArn: aws.String("arn:aws:codebuild:us-east-1:000000000000:report/x"), SortOrder: v,
				})

				return err
			})},
		{name: "import source credentials auth type", field: "authType",
			values: enumStrings(cbtypes.AuthType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.ImportSourceCredentials(ctx, &codebuildsdk.ImportSourceCredentialsInput{
					Token: aws.String("t"), AuthType: cbtypes.AuthType(v), ServerType: cbtypes.ServerTypeGithub,
				})

				return err
			}},
		{name: "import source credentials server type", field: "serverType",
			values: enumStrings(cbtypes.ServerType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.ImportSourceCredentials(ctx, &codebuildsdk.ImportSourceCredentialsInput{
					Token: aws.String(
						"t",
					), AuthType: cbtypes.AuthTypePersonalAccessToken, ServerType: cbtypes.ServerType(v),
				})

				return err
			}},
		{name: "retry build batch type", field: "retryType",
			values: enumStrings(cbtypes.RetryBuildBatchType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.RetryBuildBatch(ctx, &codebuildsdk.RetryBuildBatchInput{
					Id: aws.String("p:1"), RetryType: cbtypes.RetryBuildBatchType(v),
				})

				return err
			}},
		{name: "start build batch compute", field: "computeTypeOverride", values: computeValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuildBatch(ctx, &codebuildsdk.StartBuildBatchInput{
					ProjectName: aws.String("p"), ComputeTypeOverride: cbtypes.ComputeType(v),
				})

				return err
			}},
		{name: "start build batch environment", field: "environmentTypeOverride", values: envValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuildBatch(ctx, &codebuildsdk.StartBuildBatchInput{
					ProjectName: aws.String("p"), EnvironmentTypeOverride: cbtypes.EnvironmentType(v),
				})

				return err
			}},
		{name: "start build batch pull credentials", field: "imagePullCredentialsTypeOverride", values: pullValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuildBatch(ctx, &codebuildsdk.StartBuildBatchInput{
					ProjectName: aws.String("p"), ImagePullCredentialsTypeOverride: cbtypes.ImagePullCredentialsType(v),
				})

				return err
			}},
		{name: "start build batch source", field: "sourceTypeOverride", values: sourceValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuildBatch(ctx, &codebuildsdk.StartBuildBatchInput{
					ProjectName: aws.String("p"), SourceTypeOverride: cbtypes.SourceType(v),
				})

				return err
			}},
		{name: "start build compute", field: "computeTypeOverride", values: computeValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuild(ctx, &codebuildsdk.StartBuildInput{
					ProjectName: aws.String("p"), ComputeTypeOverride: cbtypes.ComputeType(v),
				})

				return err
			}},
		{name: "start build environment", field: "environmentTypeOverride", values: envValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuild(ctx, &codebuildsdk.StartBuildInput{
					ProjectName: aws.String("p"), EnvironmentTypeOverride: cbtypes.EnvironmentType(v),
				})

				return err
			}},
		{name: "start build host kernel", field: "hostKernelOverride",
			values: enumStrings(cbtypes.HostKernel("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuild(ctx, &codebuildsdk.StartBuildInput{
					ProjectName: aws.String("p"), HostKernelOverride: cbtypes.HostKernel(v),
				})

				return err
			}},
		{name: "start build pull credentials", field: "imagePullCredentialsTypeOverride", values: pullValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuild(ctx, &codebuildsdk.StartBuildInput{
					ProjectName: aws.String("p"), ImagePullCredentialsTypeOverride: cbtypes.ImagePullCredentialsType(v),
				})

				return err
			}},
		{name: "start build source", field: "sourceTypeOverride", values: sourceValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.StartBuild(ctx, &codebuildsdk.StartBuildInput{
					ProjectName: aws.String("p"), SourceTypeOverride: cbtypes.SourceType(v),
				})

				return err
			}},
		{name: "update project visibility", field: "projectVisibility",
			values: enumStrings(cbtypes.ProjectVisibilityType("").Values()),
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.UpdateProjectVisibility(ctx, &codebuildsdk.UpdateProjectVisibilityInput{
					ProjectArn: aws.String(
						"arn:aws:codebuild:us-east-1:000000000000:project/p",
					), ProjectVisibility: cbtypes.ProjectVisibilityType(v),
				})

				return err
			}},
		{name: "list shared projects sort by", field: "sortBy", values: sharedSortValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.ListSharedProjects(ctx, &codebuildsdk.ListSharedProjectsInput{
					SortBy: cbtypes.SharedResourceSortByType(v),
				})

				return err
			}},
		{name: "list shared report groups sort by", field: "sortBy", values: sharedSortValues,
			call: func(ctx context.Context, c *codebuildsdk.Client, v string) error {
				_, err := c.ListSharedReportGroups(ctx, &codebuildsdk.ListSharedReportGroupsInput{
					SortBy: cbtypes.SharedResourceSortByType(v),
				})

				return err
			}},
		{name: "list shared projects sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListSharedProjects(ctx, &codebuildsdk.ListSharedProjectsInput{SortOrder: v})

				return err
			})},
		{name: "list shared report groups sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListSharedReportGroups(ctx, &codebuildsdk.ListSharedReportGroupsInput{SortOrder: v})

				return err
			})},
		{name: "list build batches sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListBuildBatches(ctx, &codebuildsdk.ListBuildBatchesInput{SortOrder: v})

				return err
			})},
		{name: "list build batches for project sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListBuildBatchesForProject(ctx, &codebuildsdk.ListBuildBatchesForProjectInput{
					ProjectName: aws.String("p"), SortOrder: v,
				})

				return err
			})},
		{name: "list builds sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListBuilds(ctx, &codebuildsdk.ListBuildsInput{SortOrder: v})

				return err
			})},
		{name: "list builds for project sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListBuildsForProject(ctx, &codebuildsdk.ListBuildsForProjectInput{
					ProjectName: aws.String("p"), SortOrder: v,
				})

				return err
			})},
		{name: "list command executions sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListCommandExecutionsForSandbox(ctx, &codebuildsdk.ListCommandExecutionsForSandboxInput{
					SandboxId: aws.String("s"), SortOrder: v,
				})

				return err
			})},
		{name: "list fleets sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListFleets(ctx, &codebuildsdk.ListFleetsInput{SortOrder: v})

				return err
			})},
		{name: "list projects sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListProjects(ctx, &codebuildsdk.ListProjectsInput{SortOrder: v})

				return err
			})},
		{name: "list report groups sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListReportGroups(ctx, &codebuildsdk.ListReportGroupsInput{SortOrder: v})

				return err
			})},
		{name: "list reports sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListReports(ctx, &codebuildsdk.ListReportsInput{SortOrder: v})

				return err
			})},
		{name: "list reports for report group sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListReportsForReportGroup(ctx, &codebuildsdk.ListReportsForReportGroupInput{
					ReportGroupArn: aws.String("arn:aws:codebuild:us-east-1:000000000000:report-group/x"), SortOrder: v,
				})

				return err
			})},
		{name: "list sandboxes sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListSandboxes(ctx, &codebuildsdk.ListSandboxesInput{SortOrder: v})

				return err
			})},
		{name: "list sandboxes for project sort order", field: "sortOrder", values: sortValues,
			call: sortOrderCall(func(ctx context.Context, c *codebuildsdk.Client, v cbtypes.SortOrderType) error {
				_, err := c.ListSandboxesForProject(ctx, &codebuildsdk.ListSandboxesForProjectInput{
					ProjectName: aws.String("p"), SortOrder: v,
				})

				return err
			})},
	}

	client := newTestCodeBuildClient(t, codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1")))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, tt.values)

			t.Run("invalid", func(t *testing.T) {
				t.Parallel()

				err := tt.call(t.Context(), client, "NOT_A_REAL_VALUE")

				var invalid *cbtypes.InvalidInputException

				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, invalid.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if invalid, ok := errors.AsType[*cbtypes.InvalidInputException](err); ok {
						assert.NotContains(t, invalid.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
