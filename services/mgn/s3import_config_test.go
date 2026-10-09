package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartImport_LaunchAndReplicationColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *mgnsdk.GetLaunchConfigurationOutput, *mgnsdk.GetReplicationConfigurationOutput)
		name       string
		header     string
		row        string
		wantErrors int
	}{
		{
			name: "applies_values",
			header: "mgn:server:hostname,mgn:launch:boot-mode,mgn:launch:copy-private-ip," +
				"mgn:launch:operating-system-licensing,mgn:launch:start-instance,mgn:launch:transfer-server-tags," +
				"mgn:launch:map-tagging,mgn:launch:map-tag-value," +
				"mgn:replication:bandwidth-throttling,mgn:replication:data-replication-routing," +
				"mgn:replication:security-group-id:1,mgn:replication:security-group-id:0," +
				"mgn:replication:staging-area-tag:Team,mgn:replication:storage-type," +
				"mgn:replication:fsx-ontap:storage-virtual-machine-id,mgn:replication:fsx-ontap:credentials-secret-arn," +
				"mgn:replication:use-fips-endpoint",
			row: "h1.example.com,UEFI,true,BYOL,false,true,true,mpe-1,50,PUBLIC_IP,sg-b,sg-a,Net," +
				"FSX_ONTAP,svm-1,arn:aws:secretsmanager:us-east-1:123456789012:secret:s,true",
			check: func(t *testing.T, l *mgnsdk.GetLaunchConfigurationOutput, r *mgnsdk.GetReplicationConfigurationOutput) {
				t.Helper()

				assert.Equal(t, types.BootModeUefi, l.BootMode)
				assert.True(t, aws.ToBool(l.CopyPrivateIp))
				assert.True(t, aws.ToBool(l.CopyTags))
				assert.True(t, aws.ToBool(l.EnableMapAutoTagging))
				assert.Equal(t, "mpe-1", aws.ToString(l.MapAutoTaggingMpeID))
				assert.Equal(t, types.LaunchDispositionStopped, l.LaunchDisposition)
				require.NotNil(t, l.Licensing)
				assert.True(t, aws.ToBool(l.Licensing.OsByol))
				assert.EqualValues(t, 50, r.BandwidthThrottling)
				assert.Equal(t, types.ReplicationConfigurationDataPlaneRoutingPublicIp, r.DataPlaneRouting)
				assert.Equal(t, []string{"sg-a", "sg-b"}, r.ReplicationServersSecurityGroupsIDs)
				assert.Equal(t, map[string]string{"Team": "Net"}, r.StagingAreaTags)
				assert.True(t, aws.ToBool(r.UseFipsEndpoint))
				require.NotNil(t, r.StorageConfiguration)
				assert.Equal(t, types.StorageTypeFsxOntap, r.StorageConfiguration.StorageType)
			},
		},
		{
			name:       "bad_boot_mode",
			header:     "mgn:server:hostname,mgn:launch:boot-mode",
			row:        "h2.example.com,BIOS",
			wantErrors: 1,
		},
		{
			name:       "fsx_missing_secret",
			header:     "mgn:server:hostname,mgn:replication:storage-type,mgn:replication:fsx-ontap:storage-virtual-machine-id",
			row:        "h3.example.com,FSX_ONTAP,svm-1",
			wantErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)
			s3 := newMockS3()
			h.Backend.SetS3Backend(s3)

			final := runImportAndWait(t, client, "cfg-bucket", tt.header+"\n"+tt.row+"\n", s3)
			require.Equal(t, types.ImportStatusSucceeded, final.Status)

			errs, err := client.ListImportErrors(t.Context(), &mgnsdk.ListImportErrorsInput{ImportID: final.ImportID})
			require.NoError(t, err)
			require.Len(t, errs.Items, tt.wantErrors)

			if tt.check == nil {
				return
			}

			servers, err := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})
			require.NoError(t, err)
			require.Len(t, servers.Items, 1)

			id := servers.Items[0].SourceServerID
			launch, err := client.GetLaunchConfiguration(
				t.Context(),
				&mgnsdk.GetLaunchConfigurationInput{SourceServerID: id},
			)
			require.NoError(t, err)
			repl, err := client.GetReplicationConfiguration(
				t.Context(), &mgnsdk.GetReplicationConfigurationInput{SourceServerID: id},
			)
			require.NoError(t, err)

			tt.check(t, launch, repl)
		})
	}
}
