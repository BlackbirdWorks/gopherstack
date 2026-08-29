package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/require"
)

// TestListSourceServerActions_ActionIDsFilterHonoured proves
// ListSourceServerActions applies Filters.ActionIDs
// (SourceServerActionsRequestFilters.ActionIDs), which the handler parsed
// but never passed to the backend before the fix.
func TestListSourceServerActions_ActionIDsFilterHonoured(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := seedSourceServerViaImport(t, h, client, "actions-server")
	serverID := aws.ToString(seeded.SourceServerID)

	for _, actionID := range []string{"action-1", "action-2"} {
		_, err := client.PutSourceServerAction(ctx, &mgnsdk.PutSourceServerActionInput{
			SourceServerID:     aws.String(serverID),
			ActionID:           aws.String(actionID),
			ActionName:         aws.String("name-" + actionID),
			DocumentIdentifier: aws.String("AWS-RunShellScript"),
			Order:              aws.Int32(1),
		})
		require.NoError(t, err)
	}

	out, err := client.ListSourceServerActions(ctx, &mgnsdk.ListSourceServerActionsInput{
		SourceServerID: aws.String(serverID),
		Filters:        &types.SourceServerActionsRequestFilters{ActionIDs: []string{"action-1"}},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "Filters.ActionIDs must exclude action-2")
	require.Equal(t, "action-1", aws.ToString(out.Items[0].ActionID))
}

// TestListTemplateActions_ActionIDsFilterHonoured is the same proof for
// ListTemplateActions (TemplateActionsRequestFilters.ActionIDs).
func TestListTemplateActions_ActionIDsFilterHonoured(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	tmplOut, err := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{})
	require.NoError(t, err)

	templateID := aws.ToString(tmplOut.LaunchConfigurationTemplateID)

	for _, actionID := range []string{"action-1", "action-2"} {
		_, putErr := client.PutTemplateAction(ctx, &mgnsdk.PutTemplateActionInput{
			LaunchConfigurationTemplateID: aws.String(templateID),
			ActionID:                      aws.String(actionID),
			ActionName:                    aws.String("name-" + actionID),
			DocumentIdentifier:            aws.String("AWS-RunShellScript"),
			Order:                         aws.Int32(1),
		})
		require.NoError(t, putErr)
	}

	out, err := client.ListTemplateActions(ctx, &mgnsdk.ListTemplateActionsInput{
		LaunchConfigurationTemplateID: aws.String(templateID),
		Filters:                       &types.TemplateActionsRequestFilters{ActionIDs: []string{"action-2"}},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "Filters.ActionIDs must exclude action-1")
	require.Equal(t, "action-2", aws.ToString(out.Items[0].ActionID))
}

// nmMinimalDefinition creates a NetworkMigrationDefinition satisfying every
// required member (Name, TargetNetwork.Topology, TargetS3Configuration).
func nmMinimalDefinition(t *testing.T, client *mgnsdk.Client, name string) string {
	t.Helper()

	out, err := client.CreateNetworkMigrationDefinition(t.Context(), &mgnsdk.CreateNetworkMigrationDefinitionInput{
		Name:          aws.String(name),
		TargetNetwork: &types.TargetNetwork{Topology: types.TargetNetworkTopologyIsolatedVpc},
		TargetS3Configuration: &types.TargetS3Configuration{
			S3Bucket:      aws.String("nm-bucket"),
			S3BucketOwner: aws.String("000000000000"),
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.NetworkMigrationDefinitionID)
}

// TestListNetworkMigrationAnalyses_JobIDsFilterHonoured proves
// ListNetworkMigrationAnalyses applies Filters.JobIDs
// (ListNetworkMigrationAnalysesFilters.JobIDs), which the handler's shared
// listNMScopedRequest wire struct did not even carry a field for before the
// fix -- silently dropped regardless of what a real client sent.
func TestListNetworkMigrationAnalyses_JobIDsFilterHonoured(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	definitionID := nmMinimalDefinition(t, client, "nm-def-analyses")
	executionID := "exec-1"

	const analysisJobCount = 2

	jobIDs := make([]string, 0, analysisJobCount)

	for range analysisJobCount {
		out, err := client.StartNetworkMigrationAnalysis(ctx, &mgnsdk.StartNetworkMigrationAnalysisInput{
			NetworkMigrationDefinitionID: aws.String(definitionID),
			NetworkMigrationExecutionID:  aws.String(executionID),
		})
		require.NoError(t, err)
		jobIDs = append(jobIDs, aws.ToString(out.JobID))
	}

	require.NotEqual(t, jobIDs[0], jobIDs[1], "each StartNetworkMigrationAnalysis call must mint a distinct job")

	out, err := client.ListNetworkMigrationAnalyses(ctx, &mgnsdk.ListNetworkMigrationAnalysesInput{
		NetworkMigrationDefinitionID: aws.String(definitionID),
		NetworkMigrationExecutionID:  aws.String(executionID),
		Filters:                      &types.ListNetworkMigrationAnalysesFilters{JobIDs: []string{jobIDs[0]}},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "Filters.JobIDs must exclude the second job")
	require.Equal(t, jobIDs[0], aws.ToString(out.Items[0].JobID))
}
