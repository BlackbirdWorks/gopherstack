package codebuild_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// createSlice34Project creates a minimal LINUX_CONTAINER project (optionally
// with a batch-capable buildspec) and returns its name and ARN.
func createSlice34Project(
	t *testing.T,
	client *codebuildsdk.Client,
	name string,
	buildspec string,
) (string, string) {
	t.Helper()

	source := &cbtypes.ProjectSource{Type: cbtypes.SourceTypeNoSource}
	if buildspec != "" {
		source.Buildspec = aws.String(buildspec)
	}

	out, err := client.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
		Name:        aws.String(name),
		Source:      source,
		ServiceRole: aws.String("arn:aws:iam::000000000000:role/slice34-codebuild"),
		Artifacts:   &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeNoArtifacts},
		Environment: &cbtypes.ProjectEnvironment{
			Type:        cbtypes.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.Project.Name), aws.ToString(out.Project.Arn)
}

// TestFleetLifecycle_RealClient drives CreateFleet, BatchGetFleets, ListFleets,
// UpdateFleet and DeleteFleet through a real client.
func TestFleetLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	created, err := client.CreateFleet(t.Context(), &codebuildsdk.CreateFleetInput{
		Name:            aws.String("slice34-fleet"),
		BaseCapacity:    aws.Int32(1),
		ComputeType:     cbtypes.ComputeTypeBuildGeneral1Small,
		EnvironmentType: cbtypes.EnvironmentTypeLinuxContainer,
	})
	require.NoError(t, err)
	fleetArn := aws.ToString(created.Fleet.Arn)
	require.NotEmpty(t, fleetArn)

	got, getErr := client.BatchGetFleets(t.Context(), &codebuildsdk.BatchGetFleetsInput{
		Names: []string{fleetArn},
	})
	require.NoError(t, getErr)
	require.Len(t, got.Fleets, 1)
	assert.Equal(t, "slice34-fleet", aws.ToString(got.Fleets[0].Name))

	listed, listErr := client.ListFleets(t.Context(), &codebuildsdk.ListFleetsInput{})
	require.NoError(t, listErr)
	assert.Contains(t, listed.Fleets, fleetArn)

	updated, updateErr := client.UpdateFleet(t.Context(), &codebuildsdk.UpdateFleetInput{
		Arn:          aws.String(fleetArn),
		BaseCapacity: aws.Int32(2),
	})
	require.NoError(t, updateErr)
	assert.Equal(t, int32(2), aws.ToInt32(updated.Fleet.BaseCapacity))

	_, delErr := client.DeleteFleet(t.Context(), &codebuildsdk.DeleteFleetInput{Arn: aws.String(fleetArn)})
	require.NoError(t, delErr)

	after, afterErr := client.BatchGetFleets(t.Context(), &codebuildsdk.BatchGetFleetsInput{Names: []string{fleetArn}})
	require.NoError(t, afterErr)
	assert.Empty(t, after.Fleets)
	assert.Contains(t, after.FleetsNotFound, fleetArn)
}

// TestReportGroupAndReportLifecycle_RealClient drives DeleteReport,
// DescribeCodeCoverages, DescribeTestCases, GetReportGroupTrend,
// ListReportGroups, ListReports, ListReportsForReportGroup,
// ListSharedReportGroups and UpdateReportGroup through a real client.
func TestReportGroupAndReportLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	rg, err := client.CreateReportGroup(t.Context(), &codebuildsdk.CreateReportGroupInput{
		Name: aws.String("slice34-report-group"),
		Type: cbtypes.ReportTypeTest,
		ExportConfig: &cbtypes.ReportExportConfig{
			ExportConfigType: cbtypes.ReportExportConfigTypeNoExport,
		},
	})
	require.NoError(t, err)
	rgArn := aws.ToString(rg.ReportGroup.Arn)
	require.NotEmpty(t, rgArn)

	backend.AddReportInternal(&codebuild.Report{
		Arn:            rgArn + ":slice34report",
		ReportGroupArn: rgArn,
		Type:           string(cbtypes.ReportTypeTest),
		Status:         "SUCCEEDED",
	})
	reportArn := rgArn + ":slice34report"

	t.Run("ListReportGroups", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListReportGroups(t.Context(), &codebuildsdk.ListReportGroupsInput{})
		require.NoError(t, listErr)
		assert.Contains(t, listed.ReportGroups, rgArn)
	})

	t.Run("ListSharedReportGroups", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListSharedReportGroups(t.Context(), &codebuildsdk.ListSharedReportGroupsInput{})
		require.NoError(t, listErr)
		assert.NotNil(t, listed.ReportGroups)
	})

	t.Run("ListReports", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListReports(t.Context(), &codebuildsdk.ListReportsInput{})
		require.NoError(t, listErr)
		assert.Contains(t, listed.Reports, reportArn)
	})

	t.Run("ListReportsForReportGroup", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListReportsForReportGroup(t.Context(), &codebuildsdk.ListReportsForReportGroupInput{
			ReportGroupArn: aws.String(rgArn),
		})
		require.NoError(t, listErr)
		assert.Contains(t, listed.Reports, reportArn)
	})

	t.Run("DescribeCodeCoverages", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeCodeCoverages(t.Context(), &codebuildsdk.DescribeCodeCoveragesInput{
			ReportArn: aws.String(reportArn),
		})
		require.NoError(t, descErr)
		assert.NotNil(t, out.CodeCoverages)
	})

	t.Run("DescribeTestCases", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeTestCases(t.Context(), &codebuildsdk.DescribeTestCasesInput{
			ReportArn: aws.String(reportArn),
		})
		require.NoError(t, descErr)
		assert.NotNil(t, out.TestCases)
	})

	t.Run("GetReportGroupTrend", func(t *testing.T) {
		t.Parallel()

		out, trendErr := client.GetReportGroupTrend(t.Context(), &codebuildsdk.GetReportGroupTrendInput{
			ReportGroupArn: aws.String(rgArn),
			TrendField:     cbtypes.ReportGroupTrendFieldTypePassRate,
		})
		require.NoError(t, trendErr)
		assert.NotNil(t, out.Stats)
	})

	t.Run("UpdateReportGroup", func(t *testing.T) {
		t.Parallel()

		updated, updateErr := client.UpdateReportGroup(t.Context(), &codebuildsdk.UpdateReportGroupInput{
			Arn: aws.String(rgArn),
			ExportConfig: &cbtypes.ReportExportConfig{
				ExportConfigType: cbtypes.ReportExportConfigTypeNoExport,
			},
		})
		require.NoError(t, updateErr)
		assert.Equal(t, rgArn, aws.ToString(updated.ReportGroup.Arn))
	})

	t.Run("DeleteReport", func(t *testing.T) {
		t.Parallel()

		delBackend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
		delClient := newTestCodeBuildClient(t, codebuild.NewHandler(delBackend))

		delRG, delRGErr := delClient.CreateReportGroup(t.Context(), &codebuildsdk.CreateReportGroupInput{
			Name: aws.String("slice34-delrg"),
			Type: cbtypes.ReportTypeTest,
			ExportConfig: &cbtypes.ReportExportConfig{
				ExportConfigType: cbtypes.ReportExportConfigTypeNoExport,
			},
		})
		require.NoError(t, delRGErr)
		delRGArn := aws.ToString(delRG.ReportGroup.Arn)

		delBackend.AddReportInternal(&codebuild.Report{
			Arn:            delRGArn + ":todelete",
			ReportGroupArn: delRGArn,
			Type:           string(cbtypes.ReportTypeTest),
			Status:         "SUCCEEDED",
		})

		_, delErr := delClient.DeleteReport(t.Context(), &codebuildsdk.DeleteReportInput{
			Arn: aws.String(delRGArn + ":todelete"),
		})
		require.NoError(t, delErr)

		got, batchErr := delClient.BatchGetReports(t.Context(), &codebuildsdk.BatchGetReportsInput{
			ReportArns: []string{delRGArn + ":todelete"},
		})
		require.NoError(t, batchErr)
		assert.Empty(t, got.Reports)
	})
}

// TestBuildBatchAndBuildLifecycle_RealClient drives DeleteBuildBatch,
// ListBuildBatches, ListBuildBatchesForProject, RetryBuildBatch, ListBuilds,
// StopBuild and BatchDeleteBuilds through a real client.
func TestBuildBatchAndBuildLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("build batch lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

		projectName, _ := createSlice34Project(
			t, client, "slice34-batch-proj",
			"version: 0.2\nbatch:\n  build-list:\n    - identifier: only\n",
		)

		started, startErr := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
			ProjectName: aws.String(projectName),
		})
		require.NoError(t, startErr)
		batchID := aws.ToString(started.BuildBatch.Id)
		require.NotEmpty(t, batchID)

		listed, listErr := client.ListBuildBatchesForProject(
			t.Context(),
			&codebuildsdk.ListBuildBatchesForProjectInput{ProjectName: aws.String(projectName)},
		)
		require.NoError(t, listErr)
		assert.Contains(t, listed.Ids, batchID)

		allListed, allListErr := client.ListBuildBatches(t.Context(), &codebuildsdk.ListBuildBatchesInput{})
		require.NoError(t, allListErr)
		assert.Contains(t, allListed.Ids, batchID)

		retried, retryErr := client.RetryBuildBatch(t.Context(), &codebuildsdk.RetryBuildBatchInput{
			Id: aws.String(batchID),
		})
		require.NoError(t, retryErr)
		require.NotNil(t, retried.BuildBatch)

		_, delErr := client.DeleteBuildBatch(t.Context(), &codebuildsdk.DeleteBuildBatchInput{Id: aws.String(batchID)})
		require.NoError(t, delErr)

		after, afterErr := client.BatchGetBuildBatches(
			t.Context(),
			&codebuildsdk.BatchGetBuildBatchesInput{Ids: []string{batchID}},
		)
		require.NoError(t, afterErr)
		assert.Empty(t, after.BuildBatches)
	})

	t.Run("build lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

		projectName, _ := createSlice34Project(t, client, "slice34-build-proj", "")

		started, startErr := client.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{
			ProjectName: aws.String(projectName),
		})
		require.NoError(t, startErr)
		buildID := aws.ToString(started.Build.Id)
		require.NotEmpty(t, buildID)

		stopped, stopErr := client.StopBuild(t.Context(), &codebuildsdk.StopBuildInput{Id: aws.String(buildID)})
		require.NoError(t, stopErr)
		assert.Equal(t, cbtypes.StatusTypeStopped, stopped.Build.BuildStatus)

		listed, listErr := client.ListBuilds(t.Context(), &codebuildsdk.ListBuildsInput{})
		require.NoError(t, listErr)
		assert.Contains(t, listed.Ids, buildID)

		deleted, delErr := client.BatchDeleteBuilds(t.Context(), &codebuildsdk.BatchDeleteBuildsInput{
			Ids: []string{buildID},
		})
		require.NoError(t, delErr)
		assert.Contains(t, deleted.BuildsDeleted, buildID)
	})
}

// TestSandboxAndCommandExecutionLifecycle_RealClient drives BatchGetCommandExecutions,
// ListCommandExecutionsForSandbox, ListSandboxes, ListSandboxesForProject,
// StartSandboxConnection and StopSandbox through a real client.
func TestSandboxAndCommandExecutionLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	projectName, _ := createSlice34Project(t, client, "slice34-sandbox-proj", "")

	sb, sbErr := client.StartSandbox(t.Context(), &codebuildsdk.StartSandboxInput{
		ProjectName: aws.String(projectName),
	})
	require.NoError(t, sbErr)
	sandboxID := aws.ToString(sb.Sandbox.Id)
	require.NotEmpty(t, sandboxID)

	t.Run("ListSandboxes", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListSandboxes(t.Context(), &codebuildsdk.ListSandboxesInput{})
		require.NoError(t, listErr)
		assert.Contains(t, listed.Ids, sandboxID)
	})

	t.Run("ListSandboxesForProject", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.ListSandboxesForProject(
			t.Context(),
			&codebuildsdk.ListSandboxesForProjectInput{ProjectName: aws.String(projectName)},
		)
		require.NoError(t, listErr)
		assert.Contains(t, listed.Ids, sandboxID)
	})

	// TestStartSandboxConnection_RealClient_SSMSessionShape proves the fix for a
	// real wire bug: StartSandboxConnectionOutput.SsmSession (the real member,
	// codebuild@v1.72.4 api_op_StartSandboxConnection.go:38-41) was never set --
	// the handler emitted a fabricated {"endpoint": "..."} field instead, so a
	// real client's out.SsmSession was always nil regardless of what gopherstack
	// sent.
	t.Run("StartSandboxConnection", func(t *testing.T) {
		t.Parallel()

		conn, connErr := client.StartSandboxConnection(t.Context(), &codebuildsdk.StartSandboxConnectionInput{
			SandboxId: aws.String(sandboxID),
		})
		require.NoError(t, connErr)
		require.NotNil(t, conn.SsmSession)
		assert.NotEmpty(t, aws.ToString(conn.SsmSession.SessionId))
		assert.NotEmpty(t, aws.ToString(conn.SsmSession.StreamUrl))
		assert.NotEmpty(t, aws.ToString(conn.SsmSession.TokenValue))
	})

	t.Run("BatchGetCommandExecutions + ListCommandExecutionsForSandbox", func(t *testing.T) {
		t.Parallel()

		ceBackend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
		ceClient := newTestCodeBuildClient(t, codebuild.NewHandler(ceBackend))

		ceProjectName, _ := createSlice34Project(t, ceClient, "slice34-ce-proj", "")

		ceSB, ceSBErr := ceClient.StartSandbox(t.Context(), &codebuildsdk.StartSandboxInput{
			ProjectName: aws.String(ceProjectName),
		})
		require.NoError(t, ceSBErr)
		ceSandboxID := aws.ToString(ceSB.Sandbox.Id)

		started, startErr := ceClient.StartCommandExecution(t.Context(), &codebuildsdk.StartCommandExecutionInput{
			SandboxId: aws.String(ceSandboxID),
			Command:   aws.String("echo hi"),
			Type:      cbtypes.CommandTypeShell,
		})
		require.NoError(t, startErr)
		ceID := aws.ToString(started.CommandExecution.Id)
		require.NotEmpty(t, ceID)

		got, getErr := ceClient.BatchGetCommandExecutions(t.Context(), &codebuildsdk.BatchGetCommandExecutionsInput{
			SandboxId:           aws.String(ceSandboxID),
			CommandExecutionIds: []string{ceID},
		})
		require.NoError(t, getErr)
		require.Len(t, got.CommandExecutions, 1)
		assert.Equal(t, ceID, aws.ToString(got.CommandExecutions[0].Id))

		listed, listErr := ceClient.ListCommandExecutionsForSandbox(
			t.Context(),
			&codebuildsdk.ListCommandExecutionsForSandboxInput{SandboxId: aws.String(ceSandboxID)},
		)
		require.NoError(t, listErr)
		require.Len(t, listed.CommandExecutions, 1)
		assert.Equal(t, ceID, aws.ToString(listed.CommandExecutions[0].Id))
	})

	t.Run("StopSandbox", func(t *testing.T) {
		t.Parallel()

		stopBackend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
		stopClient := newTestCodeBuildClient(t, codebuild.NewHandler(stopBackend))

		stopProjectName, _ := createSlice34Project(t, stopClient, "slice34-stop-proj", "")

		stopSB, stopSBErr := stopClient.StartSandbox(t.Context(), &codebuildsdk.StartSandboxInput{
			ProjectName: aws.String(stopProjectName),
		})
		require.NoError(t, stopSBErr)

		stopped, stopErr := stopClient.StopSandbox(t.Context(), &codebuildsdk.StopSandboxInput{
			Id: stopSB.Sandbox.Id,
		})
		require.NoError(t, stopErr)
		assert.Equal(t, "STOPPED", aws.ToString(stopped.Sandbox.Status))
	})
}

// TestSourceCredentialsLifecycle_RealClient drives ImportSourceCredentials and
// ListSourceCredentials through a real client.
func TestSourceCredentialsLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	imported, err := client.ImportSourceCredentials(t.Context(), &codebuildsdk.ImportSourceCredentialsInput{
		Token:      aws.String("slice34-token"),
		AuthType:   cbtypes.AuthTypePersonalAccessToken,
		ServerType: cbtypes.ServerTypeGithub,
	})
	require.NoError(t, err)
	credArn := aws.ToString(imported.Arn)
	require.NotEmpty(t, credArn)

	listed, listErr := client.ListSourceCredentials(t.Context(), &codebuildsdk.ListSourceCredentialsInput{})
	require.NoError(t, listErr)

	var found bool
	for _, info := range listed.SourceCredentialsInfos {
		if aws.ToString(info.Arn) == credArn {
			found = true
			assert.Equal(t, cbtypes.ServerTypeGithub, info.ServerType)
			assert.Equal(t, cbtypes.AuthTypePersonalAccessToken, info.AuthType)
		}
	}
	assert.True(t, found, "imported credential must appear in ListSourceCredentials")
}

// TestWebhookLifecycle_RealClient drives CreateWebhook, UpdateWebhook and
// DeleteWebhook through a real client.
func TestWebhookLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	projectName, _ := createSlice34Project(t, client, "slice34-webhook-proj", "")

	created, err := client.CreateWebhook(t.Context(), &codebuildsdk.CreateWebhookInput{
		ProjectName:  aws.String(projectName),
		BranchFilter: aws.String("main"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Webhook)
	assert.Equal(t, "main", aws.ToString(created.Webhook.BranchFilter))

	updated, updateErr := client.UpdateWebhook(t.Context(), &codebuildsdk.UpdateWebhookInput{
		ProjectName:  aws.String(projectName),
		BranchFilter: aws.String("develop"),
	})
	require.NoError(t, updateErr)
	assert.Equal(t, "develop", aws.ToString(updated.Webhook.BranchFilter))

	_, delErr := client.DeleteWebhook(
		t.Context(),
		&codebuildsdk.DeleteWebhookInput{ProjectName: aws.String(projectName)},
	)
	require.NoError(t, delErr)
}

// TestResourcePolicyLifecycle_RealClient drives PutResourcePolicy,
// GetResourcePolicy and DeleteResourcePolicy through a real client.
func TestResourcePolicyLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	projectName, projectArn := createSlice34Project(t, client, "slice34-policy-proj", "")
	_ = projectName

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"codebuild:BatchGetProjects","Resource":"*"}]}`

	put, putErr := client.PutResourcePolicy(t.Context(), &codebuildsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(projectArn),
		Policy:      aws.String(policy),
	})
	require.NoError(t, putErr)
	assert.Equal(t, projectArn, aws.ToString(put.ResourceArn))

	got, getErr := client.GetResourcePolicy(t.Context(), &codebuildsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(projectArn),
	})
	require.NoError(t, getErr)
	assert.Equal(t, policy, aws.ToString(got.Policy))

	_, delErr := client.DeleteResourcePolicy(t.Context(), &codebuildsdk.DeleteResourcePolicyInput{
		ResourceArn: aws.String(projectArn),
	})
	require.NoError(t, delErr)

	_, getAfterErr := client.GetResourcePolicy(t.Context(), &codebuildsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(projectArn),
	})
	require.Error(t, getAfterErr, "policy must be gone after delete")
}

// TestCuratedEnvironmentImages_RealClient drives ListCuratedEnvironmentImages
// through a real client.
func TestCuratedEnvironmentImages_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	out, err := client.ListCuratedEnvironmentImages(t.Context(), &codebuildsdk.ListCuratedEnvironmentImagesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Platforms)
	assert.NotEmpty(t, out.Platforms[0].Languages)
}

// TestProjectVisibilityAndSharedProjects_RealClient drives UpdateProjectVisibility
// and ListSharedProjects through a real client.
func TestProjectVisibilityAndSharedProjects_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	_, projectArn := createSlice34Project(t, client, "slice34-visibility-proj", "")

	updated, err := client.UpdateProjectVisibility(t.Context(), &codebuildsdk.UpdateProjectVisibilityInput{
		ProjectArn:        aws.String(projectArn),
		ProjectVisibility: cbtypes.ProjectVisibilityTypePublicRead,
	})
	require.NoError(t, err)
	assert.Equal(t, cbtypes.ProjectVisibilityTypePublicRead, updated.ProjectVisibility)

	listed, listErr := client.ListSharedProjects(t.Context(), &codebuildsdk.ListSharedProjectsInput{})
	require.NoError(t, listErr)
	assert.NotNil(t, listed.Projects)
}

// TestInvalidateProjectCache_RealClient drives InvalidateProjectCache through a
// real client.
func TestInvalidateProjectCache_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	projectName, _ := createSlice34Project(t, client, "slice34-cache-proj", "")

	_, err := client.InvalidateProjectCache(t.Context(), &codebuildsdk.InvalidateProjectCacheInput{
		ProjectName: aws.String(projectName),
	})
	require.NoError(t, err)
}
