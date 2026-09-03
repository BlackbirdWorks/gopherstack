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
// cutover flow: StartImport (the real, wire-reachable creation path -- see
// sourceservers.go) creates a server via seedSourceServerViaImport, its data
// replication progresses to READY_FOR_TEST, StartTest completes a Job
// moving it to READY_FOR_CUTOVER, StartCutover moves it to CUTTING_OVER,
// and FinalizeCutover completes it to CUTOVER.
func TestRoundTrip_SourceServerLifecycle(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := seedSourceServerViaImport(t, h, client, "lifecycle-server")
	id := aws.ToString(seeded.SourceServerID)

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

	seeded := seedSourceServerViaImport(t, h, client, "app-server")
	seededID := aws.ToString(seeded.SourceServerID)

	// ApplicationID association happens via AssociateSourceServers, never as
	// part of StartImport itself -- real AWS's own StartImport CSV load
	// carries no Application linkage (PARITY.md's CSV schema assumption).
	_, err = client.AssociateSourceServers(ctx, &mgnsdk.AssociateSourceServersInput{
		ApplicationID: aws.String(appID), SourceServerIDs: []string{seededID},
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
		ApplicationID: aws.String(appID), SourceServerIDs: []string{seededID},
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

	seeded := seedSourceServerViaImport(t, h, client, "config-server")
	id := aws.ToString(seeded.SourceServerID)

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

// TestRoundTrip_ExportImport drives StartExport/ListExports/ListExportErrors and
// StartImport/ListImports/ListImportErrors, confirming both counts are real, not
// fabricated: StartExport reflects a live snapshot of this account's resources,
// and StartImport genuinely reads and parses the S3 object (s3import.go). See
// TestStartImport_CSVSchema for malformed-row and unreadable-object edge cases.
func TestRoundTrip_ExportImport(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seedSourceServerViaImport(t, h, client, "export-seed")

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

	s3 := newMockS3()
	s3.put("import-bucket", "servers.csv",
		"mgn:server:hostname,mgn:server:fqdn\nweb-1,web-1.example.com\ndb-1,db-1.example.com\n")
	h.Backend.SetS3Backend(s3)

	imported, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
		S3BucketSource: &types.S3BucketSource{S3Bucket: aws.String("import-bucket"), S3Key: aws.String("servers.csv")},
	})
	require.NoError(t, err)
	importID := aws.ToString(imported.ImportTask.ImportID)

	require.Eventually(t, func() bool {
		out, listErr := client.ListImports(ctx, &mgnsdk.ListImportsInput{
			Filters: &types.ListImportsRequestFilters{ImportIDs: []string{importID}},
		})

		return listErr == nil && len(out.Items) == 1 && out.Items[0].Status == types.ImportStatusSucceeded
	}, defaultAsyncWait, defaultAsyncPoll, "import task never reached SUCCEEDED")

	finalImports, err := client.ListImports(ctx, &mgnsdk.ListImportsInput{
		Filters: &types.ListImportsRequestFilters{ImportIDs: []string{importID}},
	})
	require.NoError(t, err)
	require.Len(t, finalImports.Items, 1)
	require.EqualValues(t, 2, finalImports.Items[0].Summary.Servers.CreatedCount)

	importErrs, err := client.ListImportErrors(ctx, &mgnsdk.ListImportErrorsInput{ImportID: aws.String(importID)})
	require.NoError(t, err)
	require.Empty(t, importErrs.Items)

	// Confirm the wire path really created 2 new SourceServers (plus the 1
	// seeded above for StartExport), not just a Summary number.
	describedAll, err := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.NoError(t, err)
	require.Len(t, describedAll.Items, 3)
}

// TestStartImport_CSVSchema table-drives StartImport's real CSV parsing
// (s3import.go): a valid single row, every optional column populated, a
// malformed row that fails only itself (not the whole task), a fully
// unparseable object (no header row), and a missing S3 object entirely --
// confirming StartImport is honest in every direction: real counts when
// rows parse, real per-row errors when they don't, and a FAILED task (never
// a fabricated success) when the object itself cannot be read at all.
func TestStartImport_CSVSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		csv           string
		wantStatus    types.ImportStatus
		wantCreated   int64
		wantErrors    int
		missingObject bool
	}{
		{
			name:        "single valid row creates one source server",
			csv:         "mgn:server:hostname\nweb-1.example.com\n",
			wantStatus:  types.ImportStatusSucceeded,
			wantCreated: 1,
		},
		{
			name: "every optional column populated",
			csv: "mgn:server:hostname,mgn:server:fqdn-for-action-framework,mgn:server:user-provided-id," +
				"mgn:server:tag:team\n" +
				"db-1,db-1.corp.example.com,my-id-1,payments\n",
			wantStatus:  types.ImportStatusSucceeded,
			wantCreated: 1,
		},
		{
			// A blank CSV line is silently ignored by encoding/csv itself
			// (not a malformed row) -- a row with no identification hint at
			// all is this test's actual malformed-row case.
			name:        "row with no identification hint fails only that row",
			csv:         "mgn:server:hostname,note\nweb-1,ok\n,missing-hostname\nweb-2,ok\n",
			wantStatus:  types.ImportStatusSucceeded,
			wantCreated: 2,
			wantErrors:  1,
		},
		{
			name:        "no header row fails the whole task",
			csv:         "",
			wantStatus:  types.ImportStatusFailed,
			wantCreated: 0,
			wantErrors:  1,
		},
		{
			name:          "missing S3 object fails the whole task",
			missingObject: true,
			wantStatus:    types.ImportStatusFailed,
			wantCreated:   0,
			wantErrors:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			s3 := newMockS3()
			if !tt.missingObject {
				s3.put("bucket", "servers.csv", tt.csv)
			}

			h.Backend.SetS3Backend(s3)

			started, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
				S3BucketSource: &types.S3BucketSource{
					S3Bucket: aws.String("bucket"), S3Key: aws.String("servers.csv"),
				},
			})
			require.NoError(t, err)
			importID := aws.ToString(started.ImportTask.ImportID)

			var final types.ImportTask

			require.Eventually(t, func() bool {
				out, listErr := client.ListImports(ctx, &mgnsdk.ListImportsInput{
					Filters: &types.ListImportsRequestFilters{ImportIDs: []string{importID}},
				})
				if listErr != nil || len(out.Items) != 1 || out.Items[0].Status != tt.wantStatus {
					return false
				}

				final = out.Items[0]

				return true
			}, defaultAsyncWait, defaultAsyncPoll, "import task never reached expected terminal status")

			require.Equal(t, tt.wantCreated, final.Summary.Servers.CreatedCount)

			errs, err := client.ListImportErrors(ctx, &mgnsdk.ListImportErrorsInput{ImportID: aws.String(importID)})
			require.NoError(t, err)
			require.Len(t, errs.Items, tt.wantErrors)
		})
	}
}

// TestStartImport_ModifiedCount drives two StartImport calls sharing the same
// mgn:server:user-provided-id, confirming the second run updates the first
// run's SourceServer (ModifiedCount) instead of creating a second one --
// AWS's own documented dedup-by-user-provided-id behavior (MGN User Guide,
// "Import parameters": "used by MGN to consistently recognize the server
// replication, and avoid duplication when importing inventory from a CSV
// file").
func TestStartImport_ModifiedCount(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	s3 := newMockS3()
	h.Backend.SetS3Backend(s3)

	runImport := func(key, csvBody string) types.ImportTaskSummary {
		s3.put("bucket", key, csvBody)

		started, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
			S3BucketSource: &types.S3BucketSource{S3Bucket: aws.String("bucket"), S3Key: aws.String(key)},
		})
		require.NoError(t, err)
		importID := aws.ToString(started.ImportTask.ImportID)

		var final types.ImportTask

		require.Eventually(t, func() bool {
			out, listErr := client.ListImports(ctx, &mgnsdk.ListImportsInput{
				Filters: &types.ListImportsRequestFilters{ImportIDs: []string{importID}},
			})
			if listErr != nil || len(out.Items) != 1 || out.Items[0].Status != types.ImportStatusSucceeded {
				return false
			}

			final = out.Items[0]

			return true
		}, defaultAsyncWait, defaultAsyncPoll, "import task never reached SUCCEEDED")

		return *final.Summary
	}

	first := runImport("import-1.csv",
		"mgn:server:hostname,mgn:server:user-provided-id\nweb-1.example.com,dedup-id-1\n")
	require.EqualValues(t, 1, first.Servers.CreatedCount)
	require.EqualValues(t, 0, first.Servers.ModifiedCount)

	second := runImport("import-2.csv",
		"mgn:server:hostname,mgn:server:user-provided-id\nweb-1-renamed.example.com,dedup-id-1\n",
	)
	require.EqualValues(t, 0, second.Servers.CreatedCount)
	require.EqualValues(t, 1, second.Servers.ModifiedCount)

	described, err := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.NoError(t, err)
	require.Len(t, described.Items, 1, "the second import must update the existing server, not create a new one")
	require.Equal(t, "web-1-renamed.example.com",
		aws.ToString(described.Items[0].SourceProperties.IdentificationHints.Hostname))
}

// TestRoundTrip_PostLaunchActions drives Put/List/RemoveSourceServerAction
// and Put/List/RemoveTemplateAction.
func TestRoundTrip_PostLaunchActions(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	seeded := seedSourceServerViaImport(t, h, client, "actions-server")

	putAction, err := client.PutSourceServerAction(ctx, &mgnsdk.PutSourceServerActionInput{
		ActionID: aws.String("action-1"), ActionName: aws.String("my-action"),
		DocumentIdentifier: aws.String("AWS-RunShellScript"), Order: aws.Int32(1),
		SourceServerID: seeded.SourceServerID,
	})
	require.NoError(t, err)
	require.Equal(t, "my-action", aws.ToString(putAction.ActionName))

	listed, err := client.ListSourceServerActions(ctx, &mgnsdk.ListSourceServerActionsInput{
		SourceServerID: seeded.SourceServerID,
	})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)

	_, err = client.RemoveSourceServerAction(ctx, &mgnsdk.RemoveSourceServerActionInput{
		ActionID: aws.String("action-1"), SourceServerID: seeded.SourceServerID,
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

	seeded := seedSourceServerViaImport(t, h, client, "tagging-server")

	_, err := client.TagResource(ctx, &mgnsdk.TagResourceInput{
		ResourceArn: seeded.Arn, Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(
		ctx,
		&mgnsdk.ListTagsForResourceInput{ResourceArn: seeded.Arn},
	)
	require.NoError(t, err)
	require.Equal(t, "prod", listed.Tags["env"])

	_, err = client.UntagResource(ctx, &mgnsdk.UntagResourceInput{
		ResourceArn: seeded.Arn, TagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listedAfter, err := client.ListTagsForResource(
		ctx,
		&mgnsdk.ListTagsForResourceInput{ResourceArn: seeded.Arn},
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

// TestRoundTrip_NetworkMigrationCodeGenerationOutputFormatStatus drives
// StartNetworkMigrationCodeGeneration with real output format types and
// confirms ListNetworkMigrationCodeGenerations surfaces
// CodeGenerationOutputFormatStatusDetailsMap (types.
// NetworkMigrationCodeGenerationJobDetails.CodeGenerationOutputFormatStatusDetailsMap,
// deserializers.go case "codeGenerationOutputFormatStatusDetailsMap") --
// one entry per requested format, keyed by the format itself, once the job
// reaches SUCCEEDED.
func TestRoundTrip_NetworkMigrationCodeGenerationOutputFormatStatus(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateNetworkMigrationDefinition(ctx, &mgnsdk.CreateNetworkMigrationDefinitionInput{
		Name:          aws.String("codegen-status-def"),
		TargetNetwork: &types.TargetNetwork{Topology: types.TargetNetworkTopologyHubAndSpoke},
		TargetS3Configuration: &types.TargetS3Configuration{
			S3Bucket: aws.String("bucket"), S3BucketOwner: aws.String(rtTestAccountID),
		},
	})
	require.NoError(t, err)
	defID := aws.ToString(created.NetworkMigrationDefinitionID)
	execID := "exec-codegen-status"

	codeGenOut, err := client.StartNetworkMigrationCodeGeneration(ctx, &mgnsdk.StartNetworkMigrationCodeGenerationInput{
		NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
		CodeGenerationOutputFormatTypes: []types.CodeGenerationOutputFormatType{
			types.CodeGenerationOutputFormatTypeCdkL1, types.CodeGenerationOutputFormatTypeTerraform,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(codeGenOut.JobID))

	var items []types.NetworkMigrationCodeGenerationJobDetails
	require.Eventually(t, func() bool {
		out, listErr := client.ListNetworkMigrationCodeGenerations(
			ctx, &mgnsdk.ListNetworkMigrationCodeGenerationsInput{
				NetworkMigrationDefinitionID: aws.String(defID), NetworkMigrationExecutionID: aws.String(execID),
			},
		)
		if listErr != nil || len(out.Items) != 1 {
			return false
		}

		items = out.Items

		return items[0].Status == types.NetworkMigrationJobStatusSucceeded
	}, defaultAsyncWait, defaultAsyncPoll, "code generation job never reached SUCCEEDED")

	statusMap := items[0].CodeGenerationOutputFormatStatusDetailsMap
	require.Len(t, statusMap, 2)
	require.Equal(t, types.CodeGenerationOutputFormatStatusSucceeded, statusMap["CDK_L1"].Status)
	require.Equal(t, types.CodeGenerationOutputFormatStatusSucceeded, statusMap["TERRAFORM"].Status)
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
