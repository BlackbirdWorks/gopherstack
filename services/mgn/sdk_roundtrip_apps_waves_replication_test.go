package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_AppsWavesReplication drives mgn's remaining typed-client-
// blind ops through the real aws-sdk-go-v2 client (gopherstack-n3zi).
func TestRealClient_AppsWavesReplication(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "application archive unarchive update",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				created, err := client.CreateApplication(
					ctx,
					&mgnsdk.CreateApplicationInput{Name: aws.String("s27-app")},
				)
				require.NoError(t, err)
				appID := created.ApplicationID

				updOut, err := client.UpdateApplication(ctx, &mgnsdk.UpdateApplicationInput{
					ApplicationID: appID, Name: aws.String("s27-app-renamed"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s27-app-renamed", aws.ToString(updOut.Name))

				archOut, err := client.ArchiveApplication(
					ctx,
					&mgnsdk.ArchiveApplicationInput{ApplicationID: appID},
				)
				require.NoError(t, err)
				assert.True(t, aws.ToBool(archOut.IsArchived))

				unarchOut, err := client.UnarchiveApplication(
					ctx,
					&mgnsdk.UnarchiveApplicationInput{ApplicationID: appID},
				)
				require.NoError(t, err)
				assert.False(t, aws.ToBool(unarchOut.IsArchived))
			},
		},
		{
			name: "wave archive unarchive update",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				created, err := client.CreateWave(
					ctx,
					&mgnsdk.CreateWaveInput{Name: aws.String("s27-wave")},
				)
				require.NoError(t, err)
				waveID := created.WaveID

				updOut, err := client.UpdateWave(ctx, &mgnsdk.UpdateWaveInput{
					WaveID: waveID, Name: aws.String("s27-wave-renamed"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s27-wave-renamed", aws.ToString(updOut.Name))

				archOut, err := client.ArchiveWave(ctx, &mgnsdk.ArchiveWaveInput{WaveID: waveID})
				require.NoError(t, err)
				assert.True(t, aws.ToBool(archOut.IsArchived))

				unarchOut, err := client.UnarchiveWave(ctx, &mgnsdk.UnarchiveWaveInput{WaveID: waveID})
				require.NoError(t, err)
				assert.False(t, aws.ToBool(unarchOut.IsArchived))
			},
		},
		{
			name: "source server replication lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				seeded := seedSourceServerViaImport(t, h, client, "s27-replication-server")
				id := aws.ToString(seeded.SourceServerID)

				startOut, err := client.StartReplication(
					ctx,
					&mgnsdk.StartReplicationInput{SourceServerID: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, id, aws.ToString(startOut.SourceServerID))
				require.NotNil(t, startOut.DataReplicationInfo)
				assert.Equal(
					t,
					types.DataReplicationStateInitiating,
					startOut.DataReplicationInfo.DataReplicationState,
				)

				require.Eventually(t, func() bool {
					out, describeErr := client.DescribeSourceServers(
						ctx,
						&mgnsdk.DescribeSourceServersInput{
							Filters: &types.DescribeSourceServersRequestFilters{
								SourceServerIDs: []string{id},
							},
						},
					)

					return describeErr == nil && len(out.Items) == 1 &&
						out.Items[0].DataReplicationInfo != nil &&
						out.Items[0].DataReplicationInfo.DataReplicationState == types.DataReplicationStateContinuous
				}, defaultAsyncWait, defaultAsyncPoll, "replication never reached CONTINUOUS")

				pauseOut, err := client.PauseReplication(
					ctx,
					&mgnsdk.PauseReplicationInput{SourceServerID: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					types.DataReplicationStatePaused,
					pauseOut.DataReplicationInfo.DataReplicationState,
				)

				resumeOut, err := client.ResumeReplication(
					ctx,
					&mgnsdk.ResumeReplicationInput{SourceServerID: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					types.DataReplicationStateContinuous,
					resumeOut.DataReplicationInfo.DataReplicationState,
				)

				retryOut, err := client.RetryDataReplication(
					ctx,
					&mgnsdk.RetryDataReplicationInput{SourceServerID: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					types.DataReplicationStateInitiating,
					retryOut.DataReplicationInfo.DataReplicationState,
				)

				stopOut, err := client.StopReplication(
					ctx,
					&mgnsdk.StopReplicationInput{SourceServerID: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					types.DataReplicationStateStopped,
					stopOut.DataReplicationInfo.DataReplicationState,
				)
			},
		},
		{
			name: "job delete",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				seeded := seedSourceServerViaImport(t, h, client, "s27-job-server")
				id := aws.ToString(seeded.SourceServerID)

				require.Eventually(t, func() bool {
					out, describeErr := client.DescribeSourceServers(
						ctx,
						&mgnsdk.DescribeSourceServersInput{
							Filters: &types.DescribeSourceServersRequestFilters{
								SourceServerIDs: []string{id},
							},
						},
					)

					return describeErr == nil && len(out.Items) == 1 && out.Items[0].LifeCycle != nil &&
						out.Items[0].LifeCycle.State == types.LifeCycleStateReadyForTest
				}, defaultAsyncWait, defaultAsyncPoll, "source server never reached READY_FOR_TEST")

				testOut, err := client.StartTest(ctx, &mgnsdk.StartTestInput{SourceServerIDs: []string{id}})
				require.NoError(t, err)
				require.NotNil(t, testOut.Job)
				jobID := testOut.Job.JobID

				require.Eventually(t, func() bool {
					out, describeErr := client.DescribeJobs(ctx, &mgnsdk.DescribeJobsInput{
						Filters: &types.DescribeJobsRequestFilters{JobIDs: []string{aws.ToString(jobID)}},
					})

					return describeErr == nil && len(out.Items) == 1 &&
						out.Items[0].Status == types.JobStatusCompleted
				}, defaultAsyncWait, defaultAsyncPoll, "job never reached COMPLETED")

				_, err = client.DeleteJob(ctx, &mgnsdk.DeleteJobInput{JobID: jobID})
				require.NoError(t, err)

				out, err := client.DescribeJobs(ctx, &mgnsdk.DescribeJobsInput{
					Filters: &types.DescribeJobsRequestFilters{JobIDs: []string{aws.ToString(jobID)}},
				})
				require.NoError(t, err)
				assert.Empty(t, out.Items)
			},
		},
		{
			name: "import file enrichment start and list",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				source := &types.EnrichmentSourceS3Configuration{
					S3Bucket: aws.String("source-bucket"), S3BucketOwner: aws.String(rtTestAccountID),
					S3Key: aws.String("source.csv"),
				}
				target := &types.EnrichmentTargetS3Configuration{
					S3Bucket: aws.String("target-bucket"), S3BucketOwner: aws.String(rtTestAccountID),
					S3Key: aws.String("target.csv"),
				}

				startOut, err := client.StartImportFileEnrichment(ctx, &mgnsdk.StartImportFileEnrichmentInput{
					S3BucketSource: source, S3BucketTarget: target,
				})
				require.NoError(t, err)
				jobID := aws.ToString(startOut.JobID)
				require.NotEmpty(t, jobID)

				listOut, err := client.ListImportFileEnrichments(
					ctx,
					&mgnsdk.ListImportFileEnrichmentsInput{},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Items, 1)
				assert.Equal(t, jobID, aws.ToString(listOut.Items[0].JobID))
				require.NotNil(t, listOut.Items[0].S3BucketTarget)
				assert.Equal(t, "target-bucket", aws.ToString(listOut.Items[0].S3BucketTarget.S3Bucket))
			},
		},
		{
			name: "network migration mapper segment constructs and mapping update",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				created, err := client.CreateNetworkMigrationDefinition(
					ctx,
					&mgnsdk.CreateNetworkMigrationDefinitionInput{
						Name: aws.String("s27-nm-def"),
						TargetNetwork: &types.TargetNetwork{
							Topology: types.TargetNetworkTopologyIsolatedVpc,
						},
						TargetS3Configuration: &types.TargetS3Configuration{
							S3Bucket: aws.String("bucket"), S3BucketOwner: aws.String(rtTestAccountID),
						},
					},
				)
				require.NoError(t, err)
				defID := aws.ToString(created.NetworkMigrationDefinitionID)
				execID := "s27-exec"

				mappingOut, err := client.StartNetworkMigrationMapping(
					ctx,
					&mgnsdk.StartNetworkMigrationMappingInput{
						NetworkMigrationDefinitionID: aws.String(
							defID,
						), NetworkMigrationExecutionID: aws.String(execID),
					},
				)
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(mappingOut.JobID))

				require.Eventually(t, func() bool {
					out, listErr := client.ListNetworkMigrationMappings(
						ctx,
						&mgnsdk.ListNetworkMigrationMappingsInput{
							NetworkMigrationDefinitionID: aws.String(
								defID,
							), NetworkMigrationExecutionID: aws.String(execID),
						},
					)

					return listErr == nil && len(out.Items) == 1 &&
						out.Items[0].Status == types.NetworkMigrationJobStatusSucceeded
				}, defaultAsyncWait, defaultAsyncPoll, "mapping job never reached SUCCEEDED")

				// Mapper segments/constructs are never populated -- no real
				// network-analysis engine exists (networkmigration.go doc comment) --
				// so these are honest, documented empty-list/NotFound results, not a
				// gap this pass fixes.
				constructsOut, err := client.ListNetworkMigrationMapperSegmentConstructs(
					ctx,
					&mgnsdk.ListNetworkMigrationMapperSegmentConstructsInput{
						NetworkMigrationDefinitionID: aws.String(
							defID,
						), NetworkMigrationExecutionID: aws.String(execID),
						SegmentID: aws.String("seg-1"),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, constructsOut.Items)

				_, err = client.UpdateNetworkMigrationMapperSegment(
					ctx,
					&mgnsdk.UpdateNetworkMigrationMapperSegmentInput{
						NetworkMigrationDefinitionID: aws.String(
							defID,
						), NetworkMigrationExecutionID: aws.String(execID),
						SegmentID: aws.String("seg-1"), ScopeTags: map[string]string{"env": "prod"},
					},
				)
				require.Error(t, err)

				var notFound *types.ResourceNotFoundException
				require.ErrorAs(t, err, &notFound)

				mapUpdOut, err := client.StartNetworkMigrationMappingUpdate(
					ctx,
					&mgnsdk.StartNetworkMigrationMappingUpdateInput{
						NetworkMigrationDefinitionID: aws.String(
							defID,
						), NetworkMigrationExecutionID: aws.String(execID),
					},
				)
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(mapUpdOut.JobID))

				require.Eventually(t, func() bool {
					out, listErr := client.ListNetworkMigrationMappingUpdates(
						ctx,
						&mgnsdk.ListNetworkMigrationMappingUpdatesInput{
							NetworkMigrationDefinitionID: aws.String(
								defID,
							), NetworkMigrationExecutionID: aws.String(execID),
						},
					)

					return listErr == nil && len(out.Items) == 1 &&
						out.Items[0].Status == types.NetworkMigrationJobStatusSucceeded
				}, defaultAsyncWait, defaultAsyncPoll, "mapping update job never reached SUCCEEDED")
			},
		},
		{
			name: "network migration deployments listing",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				created, err := client.CreateNetworkMigrationDefinition(
					ctx,
					&mgnsdk.CreateNetworkMigrationDefinitionInput{
						Name: aws.String("s27-nm-deploy-def"),
						TargetNetwork: &types.TargetNetwork{
							Topology: types.TargetNetworkTopologyHubAndSpoke,
						},
						TargetS3Configuration: &types.TargetS3Configuration{
							S3Bucket: aws.String("bucket"), S3BucketOwner: aws.String(rtTestAccountID),
						},
					},
				)
				require.NoError(t, err)
				defID := aws.ToString(created.NetworkMigrationDefinitionID)
				execID := "s27-deploy-exec"

				deployOut, err := client.StartNetworkMigrationDeployment(
					ctx,
					&mgnsdk.StartNetworkMigrationDeploymentInput{
						NetworkMigrationDefinitionID: aws.String(
							defID,
						), NetworkMigrationExecutionID: aws.String(execID),
					},
				)
				require.NoError(t, err)
				jobID := aws.ToString(deployOut.JobID)
				require.NotEmpty(t, jobID)

				require.Eventually(t, func() bool {
					out, listErr := client.ListNetworkMigrationDeployments(
						ctx,
						&mgnsdk.ListNetworkMigrationDeploymentsInput{
							NetworkMigrationDefinitionID: aws.String(
								defID,
							), NetworkMigrationExecutionID: aws.String(execID),
						},
					)

					return listErr == nil && len(out.Items) == 1 &&
						aws.ToString(out.Items[0].JobID) == jobID &&
						out.Items[0].Status == types.NetworkMigrationJobStatusSucceeded
				}, defaultAsyncWait, defaultAsyncPoll, "deployment job never reached SUCCEEDED")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
