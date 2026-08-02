package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mgn"
)

// TestRoundTrip_UninitializedAccountGate confirms every legacy op is gated
// behind InitializeService (PARITY.md: 69 of 95 ops return
// UninitializedAccountException until it is called once), and that
// InitializeService itself never requires it.
func TestRoundTrip_UninitializedAccountGate(t *testing.T) {
	t.Parallel()

	backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := mgn.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.Error(t, err)

	var uninit *types.UninitializedAccountException
	require.ErrorAs(t, err, &uninit)

	_, err = client.InitializeService(ctx, &mgnsdk.InitializeServiceInput{})
	require.NoError(t, err)

	_, err = client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.NoError(t, err)
}

// TestRoundTrip_SourceServerLifecycle drives the entire replication/test/
// cutover flow: SeedSourceServer (this package's non-SDK convenience --
// see sourceservers.go) seeds a server, its data replication progresses to
// READY_FOR_TEST, StartTest completes a Job moving it to
// READY_FOR_CUTOVER, StartCutover moves it to CUTTING_OVER, and
// FinalizeCutover completes it to CUTOVER.
func TestRoundTrip_SourceServerLifecycle(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{})
	id := seeded.SourceServerID

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{
			Filters: &types.DescribeSourceServersRequestFilters{SourceServerIDs: []string{id}},
		})
		if describeErr != nil || len(out.Items) != 1 {
			return false
		}

		return out.Items[0].LifeCycle != nil && out.Items[0].LifeCycle.State == types.LifeCycleStateReadyForTest
	}, defaultAsyncWait, defaultAsyncPoll, "source server never reached READY_FOR_TEST")

	testOut, err := client.StartTest(ctx, &mgnsdk.StartTestInput{SourceServerIDs: []string{id}})
	require.NoError(t, err)
	require.NotNil(t, testOut.Job)
	require.Equal(t, types.JobTypeLaunch, testOut.Job.Type)

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{
			Filters: &types.DescribeSourceServersRequestFilters{SourceServerIDs: []string{id}},
		})

		return describeErr == nil && len(out.Items) == 1 && out.Items[0].LifeCycle != nil &&
			out.Items[0].LifeCycle.State == types.LifeCycleStateReadyForCutover
	}, defaultAsyncWait, defaultAsyncPoll, "source server never reached READY_FOR_CUTOVER")

	cutoverOut, err := client.StartCutover(ctx, &mgnsdk.StartCutoverInput{SourceServerIDs: []string{id}})
	require.NoError(t, err)
	require.NotNil(t, cutoverOut.Job)

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{
			Filters: &types.DescribeSourceServersRequestFilters{SourceServerIDs: []string{id}},
		})

		return describeErr == nil && len(out.Items) == 1 && out.Items[0].LifeCycle != nil &&
			out.Items[0].LifeCycle.State == types.LifeCycleStateCuttingOver
	}, defaultAsyncWait, defaultAsyncPoll, "source server never reached CUTTING_OVER")

	finalized, err := client.FinalizeCutover(ctx, &mgnsdk.FinalizeCutoverInput{SourceServerID: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, types.LifeCycleStateCutover, finalized.LifeCycle.State)

	_, err = client.DisconnectFromService(ctx, &mgnsdk.DisconnectFromServiceInput{SourceServerID: aws.String(id)})
	require.NoError(t, err)

	// A never-seeded ID cannot 404 on StartTest (no ResourceNotFoundException
	// in its error set, PARITY.md) -- it folds into ValidationException.
	_, err = client.StartTest(ctx, &mgnsdk.StartTestInput{SourceServerIDs: []string{"s-doesnotexist"}})
	require.Error(t, err)

	var valErr *types.ValidationException
	require.ErrorAs(t, err, &valErr)
}

// TestRoundTrip_ApplicationsAndWaves drives CreateApplication/CreateWave,
// their association hierarchy, and the recomputed rollup status.
func TestRoundTrip_ApplicationsAndWaves(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &mgnsdk.CreateApplicationInput{Name: aws.String("my-app")})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationID)

	waveOut, err := client.CreateWave(ctx, &mgnsdk.CreateWaveInput{Name: aws.String("wave-1")})
	require.NoError(t, err)
	waveID := aws.ToString(waveOut.WaveID)

	_, err = client.AssociateApplications(ctx, &mgnsdk.AssociateApplicationsInput{
		WaveID: aws.String(waveID), ApplicationIDs: []string{appID},
	})
	require.NoError(t, err)

	seeded := h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{ApplicationID: appID})

	_, err = client.AssociateSourceServers(ctx, &mgnsdk.AssociateSourceServersInput{
		ApplicationID: aws.String(appID), SourceServerIDs: []string{seeded.SourceServerID},
	})
	require.NoError(t, err)

	described, err := client.ListApplications(ctx, &mgnsdk.ListApplicationsInput{
		Filters: &types.ListApplicationsRequestFilters{ApplicationIDs: []string{appID}},
	})
	require.NoError(t, err)
	require.Len(t, described.Items, 1)
	require.EqualValues(t, 1, described.Items[0].ApplicationAggregatedStatus.TotalSourceServers)

	listedWaves, err := client.ListWaves(ctx, &mgnsdk.ListWavesInput{
		Filters: &types.ListWavesRequestFilters{WaveIDs: []string{waveID}},
	})
	require.NoError(t, err)
	require.Len(t, listedWaves.Items, 1)
	require.EqualValues(t, 1, listedWaves.Items[0].WaveAggregatedStatus.TotalApplications)

	// DeleteApplication is rejected while it still has an associated
	// SourceServer.
	_, err = client.DeleteApplication(ctx, &mgnsdk.DeleteApplicationInput{ApplicationID: aws.String(appID)})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)

	_, err = client.DisassociateSourceServers(ctx, &mgnsdk.DisassociateSourceServersInput{
		ApplicationID: aws.String(appID), SourceServerIDs: []string{seeded.SourceServerID},
	})
	require.NoError(t, err)

	_, err = client.DisassociateApplications(ctx, &mgnsdk.DisassociateApplicationsInput{
		WaveID: aws.String(waveID), ApplicationIDs: []string{appID},
	})
	require.NoError(t, err)

	_, err = client.DeleteApplication(ctx, &mgnsdk.DeleteApplicationInput{ApplicationID: aws.String(appID)})
	require.NoError(t, err)

	_, err = client.DeleteWave(ctx, &mgnsdk.DeleteWaveInput{WaveID: aws.String(waveID)})
	require.NoError(t, err)
}

// TestRoundTrip_ConfigTemplates drives CreateLaunchConfigurationTemplate
// (nothing required) and CreateReplicationConfigurationTemplate (11 fields
// required, PARITY.md's documented asymmetry).
func TestRoundTrip_ConfigTemplates(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	lct, err := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(lct.LaunchConfigurationTemplateID))

	updatedLct, err := client.UpdateLaunchConfigurationTemplate(ctx, &mgnsdk.UpdateLaunchConfigurationTemplateInput{
		LaunchConfigurationTemplateID: lct.LaunchConfigurationTemplateID,
		BootMode:                      types.BootModeUefi,
	})
	require.NoError(t, err)
	require.Equal(t, types.BootModeUefi, updatedLct.BootMode)

	_, err = client.DeleteLaunchConfigurationTemplate(ctx, &mgnsdk.DeleteLaunchConfigurationTemplateInput{
		LaunchConfigurationTemplateID: lct.LaunchConfigurationTemplateID,
	})
	require.NoError(t, err)

	rct, err := client.CreateReplicationConfigurationTemplate(ctx, &mgnsdk.CreateReplicationConfigurationTemplateInput{
		AssociateDefaultSecurityGroup:       aws.Bool(true),
		BandwidthThrottling:                 100,
		CreatePublicIP:                      aws.Bool(false),
		DataPlaneRouting:                    types.ReplicationConfigurationDataPlaneRoutingPrivateIp,
		DefaultLargeStagingDiskType:         types.ReplicationConfigurationDefaultLargeStagingDiskTypeGp3,
		EbsEncryption:                       types.ReplicationConfigurationEbsEncryptionDefault,
		ReplicationServerInstanceType:       aws.String("t3.small"),
		ReplicationServersSecurityGroupsIDs: []string{"sg-1"},
		StagingAreaSubnetId:                 aws.String("subnet-1"),
		StagingAreaTags:                     map[string]string{"k": "v"},
		UseDedicatedReplicationServer:       aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(rct.ReplicationConfigurationTemplateID))

	_, err = client.DeleteReplicationConfigurationTemplate(ctx, &mgnsdk.DeleteReplicationConfigurationTemplateInput{
		ReplicationConfigurationTemplateID: rct.ReplicationConfigurationTemplateID,
	})
	require.NoError(t, err)
}

// TestRoundTrip_PerServerConfiguration drives Get/UpdateLaunchConfiguration
// and Get/UpdateReplicationConfiguration -- the two flattened,
// no-backing-named-type shapes (PARITY.md wire-trap #2).
func TestRoundTrip_PerServerConfiguration(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{})
	id := seeded.SourceServerID

	lc, err := client.GetLaunchConfiguration(ctx, &mgnsdk.GetLaunchConfigurationInput{SourceServerID: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, id, aws.ToString(lc.SourceServerID))

	updatedLC, err := client.UpdateLaunchConfiguration(ctx, &mgnsdk.UpdateLaunchConfigurationInput{
		SourceServerID: aws.String(id), Name: aws.String("new-name"),
	})
	require.NoError(t, err)
	require.Equal(t, "new-name", aws.ToString(updatedLC.Name))

	rc, err := client.GetReplicationConfiguration(
		ctx,
		&mgnsdk.GetReplicationConfigurationInput{SourceServerID: aws.String(id)},
	)
	require.NoError(t, err)
	require.Equal(t, id, aws.ToString(rc.SourceServerID))

	updatedRC, err := client.UpdateReplicationConfiguration(ctx, &mgnsdk.UpdateReplicationConfigurationInput{
		SourceServerID: aws.String(id), Name: aws.String("new-rc-name"),
	})
	require.NoError(t, err)
	require.Equal(t, "new-rc-name", aws.ToString(updatedRC.Name))
}

// TestRoundTrip_Connectors drives Create/Update/List/DeleteConnector.
func TestRoundTrip_Connectors(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, &mgnsdk.CreateConnectorInput{
		Name: aws.String("my-connector"), SsmInstanceID: aws.String("mi-1234567890abcdef0"),
	})
	require.NoError(t, err)
	id := aws.ToString(created.ConnectorID)

	updated, err := client.UpdateConnector(ctx, &mgnsdk.UpdateConnectorInput{
		ConnectorID: aws.String(id), Name: aws.String("renamed"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", aws.ToString(updated.Name))

	listed, err := client.ListConnectors(ctx, &mgnsdk.ListConnectorsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)

	_, err = client.DeleteConnector(ctx, &mgnsdk.DeleteConnectorInput{ConnectorID: aws.String(id)})
	require.NoError(t, err)
}

// TestRoundTrip_VcenterClients drives SeedVcenterClient (this package's
// non-SDK convenience -- see vcenterclients.go) plus
// DescribeVcenterClients/DeleteVcenterClient.
func TestRoundTrip_VcenterClients(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := h.Backend.SeedVcenterClient(mgn.SeedVcenterClientOptions{Hostname: "vcenter.example.com"})

	described, err := client.DescribeVcenterClients(ctx, &mgnsdk.DescribeVcenterClientsInput{})
	require.NoError(t, err)
	require.Len(t, described.Items, 1)
	require.Equal(t, "vcenter.example.com", aws.ToString(described.Items[0].Hostname))

	_, err = client.DeleteVcenterClient(ctx, &mgnsdk.DeleteVcenterClientInput{
		VcenterClientID: aws.String(seeded.VcenterClientID),
	})
	require.NoError(t, err)

	describedAfter, err := client.DescribeVcenterClients(ctx, &mgnsdk.DescribeVcenterClientsInput{})
	require.NoError(t, err)
	require.Empty(t, describedAfter.Items)
}

// TestRoundTrip_ExportImport drives StartExport/ListExports/ListExportErrors
// and StartImport/ListImports/ListImportErrors, confirming StartExport's
// counts are real (never fabricated) and StartImport's are always zero
// (this emulator never reads real S3 content -- see exportimport.go).
func TestRoundTrip_ExportImport(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{})

	exported, err := client.StartExport(ctx, &mgnsdk.StartExportInput{
		S3Bucket: aws.String("my-bucket"), S3Key: aws.String("export.json"),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, exported.ExportTask.Summary.ServersCount)

	listedExports, err := client.ListExports(ctx, &mgnsdk.ListExportsInput{})
	require.NoError(t, err)
	require.Len(t, listedExports.Items, 1)

	exportErrs, err := client.ListExportErrors(ctx, &mgnsdk.ListExportErrorsInput{
		ExportID: exported.ExportTask.ExportID,
	})
	require.NoError(t, err)
	require.Empty(t, exportErrs.Items)

	imported, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
		S3BucketSource: &types.S3BucketSource{S3Bucket: aws.String("my-bucket"), S3Key: aws.String("import.json")},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, imported.ImportTask.Summary.Servers.CreatedCount)

	listedImports, err := client.ListImports(ctx, &mgnsdk.ListImportsInput{})
	require.NoError(t, err)
	require.Len(t, listedImports.Items, 1)

	importErrs, err := client.ListImportErrors(ctx, &mgnsdk.ListImportErrorsInput{
		ImportID: imported.ImportTask.ImportID,
	})
	require.NoError(t, err)
	require.Empty(t, importErrs.Items)
}

// TestRoundTrip_PostLaunchActions drives Put/List/RemoveSourceServerAction
// and Put/List/RemoveTemplateAction.
func TestRoundTrip_PostLaunchActions(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{})

	putAction, err := client.PutSourceServerAction(ctx, &mgnsdk.PutSourceServerActionInput{
		ActionID: aws.String("action-1"), ActionName: aws.String("my-action"),
		DocumentIdentifier: aws.String("AWS-RunShellScript"), Order: aws.Int32(1),
		SourceServerID: aws.String(seeded.SourceServerID),
	})
	require.NoError(t, err)
	require.Equal(t, "my-action", aws.ToString(putAction.ActionName))

	listed, err := client.ListSourceServerActions(ctx, &mgnsdk.ListSourceServerActionsInput{
		SourceServerID: aws.String(seeded.SourceServerID),
	})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)

	_, err = client.RemoveSourceServerAction(ctx, &mgnsdk.RemoveSourceServerActionInput{
		ActionID: aws.String("action-1"), SourceServerID: aws.String(seeded.SourceServerID),
	})
	require.NoError(t, err)

	tmpl, err := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{})
	require.NoError(t, err)

	putTemplateAction, err := client.PutTemplateAction(ctx, &mgnsdk.PutTemplateActionInput{
		ActionID: aws.String("t-action-1"), ActionName: aws.String("template-action"),
		DocumentIdentifier: aws.String("AWS-RunShellScript"), Order: aws.Int32(1),
		LaunchConfigurationTemplateID: tmpl.LaunchConfigurationTemplateID,
	})
	require.NoError(t, err)
	require.Equal(t, "template-action", aws.ToString(putTemplateAction.ActionName))

	listedTemplateActions, err := client.ListTemplateActions(ctx, &mgnsdk.ListTemplateActionsInput{
		LaunchConfigurationTemplateID: tmpl.LaunchConfigurationTemplateID,
	})
	require.NoError(t, err)
	require.Len(t, listedTemplateActions.Items, 1)

	_, err = client.RemoveTemplateAction(ctx, &mgnsdk.RemoveTemplateActionInput{
		ActionID:                      aws.String("t-action-1"),
		LaunchConfigurationTemplateID: tmpl.LaunchConfigurationTemplateID,
	})
	require.NoError(t, err)
}

// TestRoundTrip_Tagging drives Tag/Untag/ListTagsForResource against a
// SourceServer ARN -- one of the 12 taggable resource kinds sharing the
// /tags/{resourceArn} path.
func TestRoundTrip_Tagging(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := h.Backend.SeedSourceServer(mgn.SeedSourceServerOptions{})

	_, err := client.TagResource(ctx, &mgnsdk.TagResourceInput{
		ResourceArn: aws.String(seeded.Arn), Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(
		ctx,
		&mgnsdk.ListTagsForResourceInput{ResourceArn: aws.String(seeded.Arn)},
	)
	require.NoError(t, err)
	require.Equal(t, "prod", listed.Tags["env"])

	_, err = client.UntagResource(ctx, &mgnsdk.UntagResourceInput{
		ResourceArn: aws.String(seeded.Arn), TagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listedAfter, err := client.ListTagsForResource(
		ctx,
		&mgnsdk.ListTagsForResourceInput{ResourceArn: aws.String(seeded.Arn)},
	)
	require.NoError(t, err)
	require.Empty(t, listedAfter.Tags)
}

// TestRoundTrip_NetworkMigrationDefinitions drives the family-M/N surface:
// Create/Get/Update/Delete/ListNetworkMigrationDefinitions, the mapper
// segment family's honest-empty behavior, and the NetworkMigrationExecutionID
// auto-vivification convention (see networkmigrationjobs.go).
func TestRoundTrip_NetworkMigrationDefinitions(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateNetworkMigrationDefinition(ctx, &mgnsdk.CreateNetworkMigrationDefinitionInput{
		Name:          aws.String("my-nm-def"),
		TargetNetwork: &types.TargetNetwork{Topology: types.TargetNetworkTopologyIsolatedVpc},
		TargetS3Configuration: &types.TargetS3Configuration{
			S3Bucket: aws.String("bucket"), S3BucketOwner: aws.String(rtTestAccountID),
		},
	})
	require.NoError(t, err)
	defID := aws.ToString(created.NetworkMigrationDefinitionID)

	described, err := client.GetNetworkMigrationDefinition(ctx, &mgnsdk.GetNetworkMigrationDefinitionInput{
		NetworkMigrationDefinitionID: aws.String(defID),
	})
	require.NoError(t, err)
	require.Equal(t, "my-nm-def", aws.ToString(described.Name))

	updated, err := client.UpdateNetworkMigrationDefinition(ctx, &mgnsdk.UpdateNetworkMigrationDefinitionInput{
		NetworkMigrationDefinitionID: aws.String(defID), Name: aws.String("renamed-def"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed-def", aws.ToString(updated.Name))

	listed, err := client.ListNetworkMigrationDefinitions(ctx, &mgnsdk.ListNetworkMigrationDefinitionsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)

	// StartNetworkMigrationMapping auto-vivifies a NetworkMigrationExecution
	// for an ExecutionID never seen before -- this package's documented
	// resolution to the "no op creates a NetworkMigrationExecutionID" gap.
	execID := "exec-1"

	mappingOut, err := client.StartNetworkMigrationMapping(ctx, &mgnsdk.StartNetworkMigrationMappingInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(mappingOut.JobID))

	executions, err := client.ListNetworkMigrationExecutions(ctx, &mgnsdk.ListNetworkMigrationExecutionsInput{
		NetworkMigrationDefinitionID: aws.String(defID),
	})
	require.NoError(t, err)
	require.Len(t, executions.Items, 1)
	require.Equal(t, execID, aws.ToString(executions.Items[0].NetworkMigrationExecutionID))
	require.Equal(t, types.ExecutionStageActivityMapping, executions.Items[0].Activity)

	require.Eventually(t, func() bool {
		out, listErr := client.ListNetworkMigrationMappings(ctx, &mgnsdk.ListNetworkMigrationMappingsInput{
			NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
		})

		return listErr == nil && len(out.Items) == 1 && out.Items[0].Status == types.NetworkMigrationJobStatusSucceeded
	}, defaultAsyncWait, defaultAsyncPoll, "mapping job never reached SUCCEEDED")

	// Mapper segments are never populated (no real network-analysis engine
	// exists) -- an honest, documented empty list, and
	// GetNetworkMigrationMapperSegmentConstruct always 404s.
	segments, err := client.ListNetworkMigrationMapperSegments(ctx, &mgnsdk.ListNetworkMigrationMapperSegmentsInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.Empty(t, segments.Items)

	_, err = client.GetNetworkMigrationMapperSegmentConstruct(
		ctx,
		&mgnsdk.GetNetworkMigrationMapperSegmentConstructInput{
			NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
			SegmentID: aws.String("seg-1"), ConstructID: aws.String("construct-1"),
		},
	)
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)

	// DeleteNetworkMigrationDefinition is rejected while it still has
	// executions.
	_, err = client.DeleteNetworkMigrationDefinition(ctx, &mgnsdk.DeleteNetworkMigrationDefinitionInput{
		NetworkMigrationDefinitionID: aws.String(defID),
	})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)
}

// TestRoundTrip_NetworkMigrationAnalysisAndDeployment drives
// StartNetworkMigrationAnalysis/CodeGeneration/Deployment and their List*
// job-details ops, confirming the analysis/codegen/deployment CONTENT lists
// (Results/Segments/DeployedStacks) always stay empty even after the
// parent job SUCCEEDS -- see networkmigrationjobs.go's doc comment on why
// that content is never fabricated.
func TestRoundTrip_NetworkMigrationAnalysisAndDeployment(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateNetworkMigrationDefinition(ctx, &mgnsdk.CreateNetworkMigrationDefinitionInput{
		Name:          aws.String("analysis-def"),
		TargetNetwork: &types.TargetNetwork{Topology: types.TargetNetworkTopologyHubAndSpoke},
		TargetS3Configuration: &types.TargetS3Configuration{
			S3Bucket: aws.String("bucket"), S3BucketOwner: aws.String(rtTestAccountID),
		},
	})
	require.NoError(t, err)
	defID := aws.ToString(created.NetworkMigrationDefinitionID)
	execID := "exec-analysis"

	analysisOut, err := client.StartNetworkMigrationAnalysis(ctx, &mgnsdk.StartNetworkMigrationAnalysisInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(analysisOut.JobID))

	require.Eventually(t, func() bool {
		out, listErr := client.ListNetworkMigrationAnalyses(ctx, &mgnsdk.ListNetworkMigrationAnalysesInput{
			NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
		})

		return listErr == nil && len(out.Items) == 1 && out.Items[0].Status == types.NetworkMigrationJobStatusSucceeded
	}, defaultAsyncWait, defaultAsyncPoll, "analysis job never reached SUCCEEDED")

	results, err := client.ListNetworkMigrationAnalysisResults(ctx, &mgnsdk.ListNetworkMigrationAnalysisResultsInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.Empty(t, results.Items)

	codeGenOut, err := client.StartNetworkMigrationCodeGeneration(ctx, &mgnsdk.StartNetworkMigrationCodeGenerationInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(codeGenOut.JobID))

	segments, err := client.ListNetworkMigrationCodeGenerationSegments(
		ctx, &mgnsdk.ListNetworkMigrationCodeGenerationSegmentsInput{
			NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
		},
	)
	require.NoError(t, err)
	require.Empty(t, segments.Items)

	deployOut, err := client.StartNetworkMigrationDeployment(ctx, &mgnsdk.StartNetworkMigrationDeploymentInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(deployOut.JobID))

	stacks, err := client.ListNetworkMigrationDeployedStacks(ctx, &mgnsdk.ListNetworkMigrationDeployedStacksInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
	})
	require.NoError(t, err)
	require.Empty(t, stacks.Items)

	executions, err := client.ListNetworkMigrationExecutions(ctx, &mgnsdk.ListNetworkMigrationExecutionsInput{
		NetworkMigrationDefinitionID: aws.String(defID),
	})
	require.NoError(t, err)
	require.Len(t, executions.Items, 1)
	// The same ExecutionID was reused for Analysis -> CodeGeneration ->
	// Deployment, so its Activity/Stage reflect the LAST Start* call --
	// this package's documented convention (networkmigrationjobs.go).
	require.Equal(t, types.ExecutionStageActivityDeploy, executions.Items[0].Activity)
}

// TestRoundTrip_ManagedAccounts drives ListManagedAccounts, confirming it
// only ever returns the calling account itself (no fabricated
// cross-account data -- PARITY.md).
func TestRoundTrip_ManagedAccounts(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	out, err := client.ListManagedAccounts(ctx, &mgnsdk.ListManagedAccountsInput{})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.Equal(t, rtTestAccountID, aws.ToString(out.Items[0].AccountId))
}
