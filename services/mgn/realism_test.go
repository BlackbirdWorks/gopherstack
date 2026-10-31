package mgn_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	mgntypes "github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransitionDelay_Export(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		delay     time.Duration
		wantStays bool
	}{
		{name: "long delay stays started", delay: time.Hour, wantStays: true},
		{name: "short delay settles", delay: 20 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h, client := newInMemoryHandlerAndClient(t)
				h.Backend.SetTransitionDelay(tt.delay)
				ctx := t.Context()

				_, err := client.InitializeService(ctx, &mgnsdk.InitializeServiceInput{})
				require.NoError(t, err)

				started, err := client.StartExport(ctx, &mgnsdk.StartExportInput{
					S3Bucket: aws.String("my-bucket"), S3Key: aws.String("export.json"),
				})
				require.NoError(t, err)

				status := func() mgntypes.ExportStatus {
					out, lErr := client.ListExports(ctx, &mgnsdk.ListExportsInput{
						Filters: &mgntypes.ListExportsRequestFilters{
							ExportIDs: []string{aws.ToString(started.ExportTask.ExportID)},
						},
					})
					require.NoError(t, lErr)
					require.Len(t, out.Items, 1)

					return out.Items[0].Status
				}

				if tt.wantStays {
					assert.NotEqual(t, mgntypes.ExportStatusSucceeded, status())

					return
				}

				time.Sleep(10 * tt.delay)
				synctest.Wait()

				assert.Equal(t, mgntypes.ExportStatusSucceeded, status())
			})
		})
	}
}

func TestInputValidation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.InitializeService(ctx, &mgnsdk.InitializeServiceInput{})
	require.NoError(t, err)

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "list applications bad token",
			call: func() error {
				_, cErr := client.ListApplications(ctx, &mgnsdk.ListApplicationsInput{NextToken: aws.String("junk")})

				return cErr
			},
		},
		{
			name: "describe source servers bad token",
			call: func() error {
				_, cErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{
					NextToken: aws.String("junk"),
				})

				return cErr
			},
		},
		{
			name: "replication template bad routing",
			call: func() error {
				_, cErr := client.CreateReplicationConfigurationTemplate(
					ctx,
					replicationTemplateInput(func(in *replTemplateInput) {
						in.DataPlaneRouting = mgntypes.ReplicationConfigurationDataPlaneRouting("BOGUS")
					}),
				)

				return cErr
			},
		},
		{
			name: "replication template bad disk type",
			call: func() error {
				_, cErr := client.CreateReplicationConfigurationTemplate(
					ctx,
					replicationTemplateInput(func(in *replTemplateInput) {
						in.DefaultLargeStagingDiskType = mgntypes.ReplicationConfigurationDefaultLargeStagingDiskType(
							"IO9",
						)
					}),
				)

				return cErr
			},
		},
		{
			name: "launch template bad boot mode",
			call: func() error {
				_, cErr := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{
					BootMode: mgntypes.BootMode("BIOS9"),
				})

				return cErr
			},
		},
		{
			name: "launch template bad disposition",
			call: func() error {
				_, cErr := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{
					LaunchDisposition: mgntypes.LaunchDisposition("PAUSED"),
				})

				return cErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(), &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}

type replTemplateInput = mgnsdk.CreateReplicationConfigurationTemplateInput

func replicationTemplateInput(mutate func(*replTemplateInput)) *replTemplateInput {
	in := &replTemplateInput{
		AssociateDefaultSecurityGroup:       aws.Bool(true),
		BandwidthThrottling:                 0,
		CreatePublicIP:                      aws.Bool(false),
		DataPlaneRouting:                    mgntypes.ReplicationConfigurationDataPlaneRoutingPrivateIp,
		DefaultLargeStagingDiskType:         mgntypes.ReplicationConfigurationDefaultLargeStagingDiskTypeGp2,
		EbsEncryption:                       mgntypes.ReplicationConfigurationEbsEncryptionDefault,
		ReplicationServerInstanceType:       aws.String("t3.small"),
		ReplicationServersSecurityGroupsIDs: []string{"sg-1"},
		StagingAreaSubnetId:                 aws.String("subnet-1"),
		StagingAreaTags:                     map[string]string{"a": "b"},
		UseDedicatedReplicationServer:       aws.Bool(false),
	}
	mutate(in)

	return in
}
