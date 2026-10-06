package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

type droppedEnv struct {
	backend *redshift.InMemoryBackend
	client  *redshiftsdk.Client
}

func newDroppedEnv(t *testing.T) droppedEnv {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)

	return droppedEnv{backend: backend, client: client}
}

func (e droppedEnv) createCluster(
	t *testing.T,
	id string,
	mutate func(*redshiftsdk.CreateClusterInput),
) *types.Cluster {
	t.Helper()

	in := &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:  aws.String(id),
		NodeType:           aws.String("ra3.xlplus"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Passw0rdOK"),
	}
	if mutate != nil {
		mutate(in)
	}

	out, err := e.client.CreateCluster(t.Context(), in)
	require.NoError(t, err)

	return out.Cluster
}

func TestDroppedMembers_ClusterSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e droppedEnv)
		name string
	}{
		{
			name: "create stores and echoes track, ip type, relocation and elastic ip",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				c := e.createCluster(t, "settings-c1", func(in *redshiftsdk.CreateClusterInput) {
					in.MaintenanceTrackName = aws.String("trailing")
					in.IpAddressType = aws.String("dualstack")
					in.AvailabilityZoneRelocation = aws.Bool(true)
					in.ElasticIp = aws.String("203.0.113.10")
				})
				assert.Equal(t, "trailing", aws.ToString(c.MaintenanceTrackName))
				assert.Equal(t, "dualstack", aws.ToString(c.IpAddressType))
				assert.Equal(t, "enabled", aws.ToString(c.AvailabilityZoneRelocationStatus))
				require.NotNil(t, c.ElasticIpStatus)
				assert.Equal(t, "203.0.113.10", aws.ToString(c.ElasticIpStatus.ElasticIp))

				got, err := e.client.DescribeClusters(t.Context(), &redshiftsdk.DescribeClustersInput{
					ClusterIdentifier: aws.String("settings-c1"),
				})
				require.NoError(t, err)
				assert.Equal(t, "trailing", aws.ToString(got.Clusters[0].MaintenanceTrackName))
			},
		},
		{
			name: "defaults are current track and ipv4",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				c := e.createCluster(t, "settings-c2", nil)
				assert.Equal(t, "current", aws.ToString(c.MaintenanceTrackName))
				assert.Equal(t, "ipv4", aws.ToString(c.IpAddressType))
				assert.Equal(t, "disabled", aws.ToString(c.AvailabilityZoneRelocationStatus))
				assert.Nil(t, c.ElasticIpStatus)
			},
		},
		{
			name: "unknown track and bad elastic ip are rejected with the sdk codes",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				_, err := e.client.CreateCluster(t.Context(), &redshiftsdk.CreateClusterInput{
					ClusterIdentifier: aws.String("settings-bad-track"), NodeType: aws.String("ra3.xlplus"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rdOK"),
					MaintenanceTrackName: aws.String("nope"),
				})
				var track *types.InvalidClusterTrackFault
				require.ErrorAs(t, err, &track)

				_, err = e.client.CreateCluster(t.Context(), &redshiftsdk.CreateClusterInput{
					ClusterIdentifier: aws.String("settings-bad-eip"), NodeType: aws.String("ra3.xlplus"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rdOK"),
					ElasticIp: aws.String("not-an-ip"),
				})
				var eip *types.InvalidElasticIpFault
				require.ErrorAs(t, err, &eip)
			},
		},
		{
			name: "manage master password mints a secret and conflicts with an explicit password",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				c := e.createCluster(t, "settings-managed", func(in *redshiftsdk.CreateClusterInput) {
					in.MasterUserPassword = nil
					in.ManageMasterPassword = aws.Bool(true)
					in.MasterPasswordSecretKmsKeyId = aws.String("key-1")
				})
				assert.Contains(t, aws.ToString(c.MasterPasswordSecretArn), ":secretsmanager:")
				assert.Equal(t, "key-1", aws.ToString(c.MasterPasswordSecretKmsKeyId))

				_, err := e.client.CreateCluster(t.Context(), &redshiftsdk.CreateClusterInput{
					ClusterIdentifier: aws.String("settings-conflict"), NodeType: aws.String("ra3.xlplus"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rdOK"),
					ManageMasterPassword: aws.Bool(true),
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterCombination")
			},
		},
		{
			name: "hsm identifiers must exist and are echoed in HsmStatus",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				_, err := e.client.CreateCluster(t.Context(), &redshiftsdk.CreateClusterInput{
					ClusterIdentifier: aws.String("settings-nohsm"), NodeType: aws.String("ra3.xlplus"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rdOK"),
					HsmClientCertificateIdentifier: aws.String("missing"),
				})
				var nf *types.HsmClientCertificateNotFoundFault
				require.ErrorAs(t, err, &nf)

				_, err = e.backend.CreateHsmClientCertificate("hsm-cert", nil)
				require.NoError(t, err)
				_, err = e.backend.CreateHsmConfiguration("hsm-cfg", "d", "10.0.0.1", "p1", nil)
				require.NoError(t, err)

				c := e.createCluster(t, "settings-hsm", func(in *redshiftsdk.CreateClusterInput) {
					in.HsmClientCertificateIdentifier = aws.String("hsm-cert")
					in.HsmConfigurationIdentifier = aws.String("hsm-cfg")
				})
				require.NotNil(t, c.HsmStatus)
				assert.Equal(t, "hsm-cert", aws.ToString(c.HsmStatus.HsmClientCertificateIdentifier))
				assert.Equal(t, "hsm-cfg", aws.ToString(c.HsmStatus.HsmConfigurationIdentifier))
				assert.Equal(t, "active", aws.ToString(c.HsmStatus.Status))

				plain := e.createCluster(t, "settings-modify-hsm", nil)
				_, err = e.client.ModifyCluster(t.Context(), &redshiftsdk.ModifyClusterInput{
					ClusterIdentifier: plain.ClusterIdentifier, HsmConfigurationIdentifier: aws.String("nope"),
				})
				var cfgNF *types.HsmConfigurationNotFoundFault
				require.ErrorAs(t, err, &cfgNF)
			},
		},
		{
			name: "modify applies ip type and relocation, track stays pending, secret toggles",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "settings-modify", nil)

				out, err := e.client.ModifyCluster(t.Context(), &redshiftsdk.ModifyClusterInput{
					ClusterIdentifier:          aws.String("settings-modify"),
					IpAddressType:              aws.String("dualstack"),
					AvailabilityZoneRelocation: aws.Bool(true),
					ElasticIp:                  aws.String("198.51.100.7"),
					MaintenanceTrackName:       aws.String("trailing"),
					ManageMasterPassword:       aws.Bool(true),
				})
				require.NoError(t, err)

				c := out.Cluster
				assert.Equal(t, "dualstack", aws.ToString(c.IpAddressType))
				assert.Equal(t, "enabled", aws.ToString(c.AvailabilityZoneRelocationStatus))
				assert.Equal(t, "198.51.100.7", aws.ToString(c.ElasticIpStatus.ElasticIp))
				assert.Equal(t, "current", aws.ToString(c.MaintenanceTrackName))
				require.NotNil(t, c.PendingModifiedValues)
				assert.Equal(t, "trailing", aws.ToString(c.PendingModifiedValues.MaintenanceTrackName))
				assert.NotEmpty(t, aws.ToString(c.MasterPasswordSecretArn))

				_, err = e.client.ModifyCluster(t.Context(), &redshiftsdk.ModifyClusterInput{
					ClusterIdentifier:    aws.String("settings-modify"),
					ManageMasterPassword: aws.Bool(false),
				})
				require.Error(t, err)

				off, err := e.client.ModifyCluster(t.Context(), &redshiftsdk.ModifyClusterInput{
					ClusterIdentifier:    aws.String("settings-modify"),
					ManageMasterPassword: aws.Bool(false),
					MasterUserPassword:   aws.String("Passw0rdOK"),
				})
				require.NoError(t, err)
				assert.Empty(t, aws.ToString(off.Cluster.MasterPasswordSecretArn))
			},
		},
		{
			name: "restore carries the settings and resolves SnapshotArn",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "settings-src", nil)
				snap, err := e.client.CreateClusterSnapshot(t.Context(), &redshiftsdk.CreateClusterSnapshotInput{
					ClusterIdentifier: aws.String("settings-src"), SnapshotIdentifier: aws.String("settings-snap"),
				})
				require.NoError(t, err)

				restored, err := e.client.RestoreFromClusterSnapshot(
					t.Context(),
					&redshiftsdk.RestoreFromClusterSnapshotInput{
						ClusterIdentifier:    aws.String("settings-restored"),
						SnapshotArn:          snap.Snapshot.SnapshotArn,
						MaintenanceTrackName: aws.String("trailing"),
						IpAddressType:        aws.String("dualstack"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "trailing", aws.ToString(restored.Cluster.MaintenanceTrackName))
				assert.Equal(t, "dualstack", aws.ToString(restored.Cluster.IpAddressType))

				_, err = e.client.RestoreFromClusterSnapshot(t.Context(), &redshiftsdk.RestoreFromClusterSnapshotInput{
					ClusterIdentifier:  aws.String("settings-both"),
					SnapshotArn:        snap.Snapshot.SnapshotArn,
					SnapshotIdentifier: aws.String("settings-snap"),
				})
				require.Error(t, err)

				_, err = e.client.RestoreFromClusterSnapshot(t.Context(), &redshiftsdk.RestoreFromClusterSnapshotInput{
					ClusterIdentifier: aws.String("settings-badarn"),
					SnapshotArn:       aws.String("arn:aws:redshift:us-east-1:000000000000:snapshot:x/nope"),
				})
				var nf *types.ClusterSnapshotNotFoundFault
				require.ErrorAs(t, err, &nf)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newDroppedEnv(t))
		})
	}
}

func TestDroppedMembers_ClusterMaintenance(t *testing.T) {
	t.Parallel()

	start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		run  func(t *testing.T, e droppedEnv)
		name string
	}{
		{
			name: "duration window is stored with its identifier and removed by DeferMaintenance=false",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "maint-c1", nil)

				out, err := e.client.ModifyClusterMaintenance(t.Context(), &redshiftsdk.ModifyClusterMaintenanceInput{
					ClusterIdentifier:          aws.String("maint-c1"),
					DeferMaintenance:           aws.Bool(true),
					DeferMaintenanceDuration:   aws.Int32(3),
					DeferMaintenanceIdentifier: aws.String("dm-a"),
					DeferMaintenanceStartTime:  aws.Time(start),
				})
				require.NoError(t, err)
				require.Len(t, out.Cluster.DeferredMaintenanceWindows, 1)

				w := out.Cluster.DeferredMaintenanceWindows[0]
				assert.Equal(t, "dm-a", aws.ToString(w.DeferMaintenanceIdentifier))
				assert.True(t, start.Equal(aws.ToTime(w.DeferMaintenanceStartTime)))
				assert.True(t, start.Add(72*time.Hour).Equal(aws.ToTime(w.DeferMaintenanceEndTime)))

				cleared, err := e.client.ModifyClusterMaintenance(
					t.Context(),
					&redshiftsdk.ModifyClusterMaintenanceInput{
						ClusterIdentifier:          aws.String("maint-c1"),
						DeferMaintenance:           aws.Bool(false),
						DeferMaintenanceIdentifier: aws.String("dm-a"),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, cleared.Cluster.DeferredMaintenanceWindows)
			},
		},
		{
			name: "end time window and validation errors",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "maint-c2", nil)

				out, err := e.client.ModifyClusterMaintenance(t.Context(), &redshiftsdk.ModifyClusterMaintenanceInput{
					ClusterIdentifier:         aws.String("maint-c2"),
					DeferMaintenance:          aws.Bool(true),
					DeferMaintenanceStartTime: aws.Time(start),
					DeferMaintenanceEndTime:   aws.Time(start.Add(48 * time.Hour)),
				})
				require.NoError(t, err)
				require.Len(t, out.Cluster.DeferredMaintenanceWindows, 1)
				assert.NotEmpty(t, aws.ToString(out.Cluster.DeferredMaintenanceWindows[0].DeferMaintenanceIdentifier))

				for _, in := range []*redshiftsdk.ModifyClusterMaintenanceInput{
					{
						ClusterIdentifier: aws.String("maint-c2"), DeferMaintenance: aws.Bool(true),
						DeferMaintenanceDuration: aws.Int32(61),
					},
					{
						ClusterIdentifier: aws.String("maint-c2"), DeferMaintenance: aws.Bool(true),
						DeferMaintenanceDuration: aws.Int32(1), DeferMaintenanceEndTime: aws.Time(start),
					},
					{ClusterIdentifier: aws.String("maint-c2"), DeferMaintenance: aws.Bool(true)},
				} {
					_, err = e.client.ModifyClusterMaintenance(t.Context(), in)
					require.Error(t, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newDroppedEnv(t))
		})
	}
}

func TestDroppedMembers_LoggingDataSharesAndIdc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e droppedEnv)
		name string
	}{
		{
			name: "enable logging stores destination type and exports",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "log-c1", nil)

				out, err := e.client.EnableLogging(t.Context(), &redshiftsdk.EnableLoggingInput{
					ClusterIdentifier:  aws.String("log-c1"),
					LogDestinationType: types.LogDestinationTypeCloudwatch,
					LogExports:         []string{"connectionlog", "userlog"},
				})
				require.NoError(t, err)
				assert.Equal(t, types.LogDestinationTypeCloudwatch, out.LogDestinationType)
				assert.Equal(t, []string{"connectionlog", "userlog"}, out.LogExports)

				got, err := e.client.DescribeLoggingStatus(t.Context(), &redshiftsdk.DescribeLoggingStatusInput{
					ClusterIdentifier: aws.String("log-c1"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.LogDestinationTypeCloudwatch, got.LogDestinationType)
				assert.Equal(t, []string{"connectionlog", "userlog"}, got.LogExports)

				_, err = e.client.EnableLogging(t.Context(), &redshiftsdk.EnableLoggingInput{
					ClusterIdentifier: aws.String("log-c1"), LogDestinationType: types.LogDestinationTypeCloudwatch,
					LogExports: []string{"bogus"},
				})
				require.Error(t, err)

				_, err = e.client.EnableLogging(t.Context(), &redshiftsdk.EnableLoggingInput{
					ClusterIdentifier: aws.String("log-c1"),
				})
				require.Error(t, err, "s3 destination still needs a bucket")
			},
		},
		{
			name: "data share write flags land on the right association",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				arn := "arn:aws:redshift:us-east-1:000000000000:datashare:ns-1/share-w"
				e.backend.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: arn, ProducerArn: "arn:aws:redshift:us-east-1:000000000000:namespace:ns-1",
					DataShareType: "INTERNAL",
				})

				authorized, err := e.client.AuthorizeDataShare(t.Context(), &redshiftsdk.AuthorizeDataShareInput{
					DataShareArn: aws.String(
						arn,
					), ConsumerIdentifier: aws.String("222222222222"), AllowWrites: aws.Bool(true),
				})
				require.NoError(t, err)
				require.Len(t, authorized.DataShareAssociations, 1)
				assert.True(t, aws.ToBool(authorized.DataShareAssociations[0].ProducerAllowedWrites))

				associated, err := e.client.AssociateDataShareConsumer(
					t.Context(),
					&redshiftsdk.AssociateDataShareConsumerInput{
						DataShareArn: aws.String(
							arn,
						), ConsumerArn: aws.String("arn:aws:redshift:us-east-1:222222222222:namespace:ns-2"),
						AllowWrites: aws.Bool(true),
					},
				)
				require.NoError(t, err)
				require.Len(t, associated.DataShareAssociations, 2)
				assert.True(t, aws.ToBool(associated.DataShareAssociations[1].ConsumerAcceptedWrites))
				assert.False(t, aws.ToBool(associated.DataShareAssociations[1].ProducerAllowedWrites))
			},
		},
		{
			name: "idc application stores namespace, token issuers and sso tag keys",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				created, err := e.client.CreateRedshiftIdcApplication(
					t.Context(),
					&redshiftsdk.CreateRedshiftIdcApplicationInput{
						RedshiftIdcApplicationName: aws.String("idc-app"),
						IdcInstanceArn:             aws.String("arn:aws:sso:::instance/ssoins-1"),
						IdcDisplayName:             aws.String("Idc"),
						IamRoleArn:                 aws.String("arn:aws:iam::000000000000:role/r"),
						IdentityNamespace:          aws.String("AWSIDC"),
						SsoTagKeys:                 []string{"team"},
						AuthorizedTokenIssuerList: []types.AuthorizedTokenIssuer{{
							TrustedTokenIssuerArn:   aws.String("arn:aws:sso::1:trustedTokenIssuer/ssoins-1/tti-1"),
							AuthorizedAudiencesList: []string{"aud-1", "aud-2"},
						}},
					},
				)
				require.NoError(t, err)

				app := created.RedshiftIdcApplication
				assert.Equal(t, "AWSIDC", aws.ToString(app.IdentityNamespace))
				assert.Equal(t, []string{"team"}, app.SsoTagKeys)
				require.Len(t, app.AuthorizedTokenIssuerList, 1)
				assert.Equal(t, []string{"aud-1", "aud-2"}, app.AuthorizedTokenIssuerList[0].AuthorizedAudiencesList)

				modified, err := e.client.ModifyRedshiftIdcApplication(
					t.Context(),
					&redshiftsdk.ModifyRedshiftIdcApplicationInput{
						RedshiftIdcApplicationArn: app.RedshiftIdcApplicationArn,
						IdentityNamespace:         aws.String("OTHER"),
						AuthorizedTokenIssuerList: []types.AuthorizedTokenIssuer{{
							TrustedTokenIssuerArn: aws.String("arn:aws:sso::1:trustedTokenIssuer/ssoins-1/tti-2"),
						}},
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "OTHER", aws.ToString(modified.RedshiftIdcApplication.IdentityNamespace))
				require.Len(t, modified.RedshiftIdcApplication.AuthorizedTokenIssuerList, 1)
				assert.Contains(
					t,
					aws.ToString(modified.RedshiftIdcApplication.AuthorizedTokenIssuerList[0].TrustedTokenIssuerArn),
					"tti-2",
				)

				described, err := e.client.DescribeRedshiftIdcApplications(
					t.Context(),
					&redshiftsdk.DescribeRedshiftIdcApplicationsInput{},
				)
				require.NoError(t, err)
				require.Len(t, described.RedshiftIdcApplications, 1)
				assert.Equal(t, "OTHER", aws.ToString(described.RedshiftIdcApplications[0].IdentityNamespace))
			},
		},
		{
			name: "snapshot copy manual retention is kept apart from automated",
			run: func(t *testing.T, e droppedEnv) {
				t.Helper()

				e.createCluster(t, "copy-c1", nil)
				_, err := e.client.EnableSnapshotCopy(t.Context(), &redshiftsdk.EnableSnapshotCopyInput{
					ClusterIdentifier: aws.String("copy-c1"), DestinationRegion: aws.String("us-west-2"),
				})
				require.NoError(t, err)

				out, err := e.client.ModifySnapshotCopyRetentionPeriod(
					t.Context(),
					&redshiftsdk.ModifySnapshotCopyRetentionPeriodInput{
						ClusterIdentifier: aws.String(
							"copy-c1",
						), RetentionPeriod: aws.Int32(-1), Manual: aws.Bool(true),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, out.Cluster.ClusterSnapshotCopyStatus)
				assert.EqualValues(
					t,
					-1,
					aws.ToInt32(out.Cluster.ClusterSnapshotCopyStatus.ManualSnapshotRetentionPeriod),
				)
				assert.EqualValues(t, 7, aws.ToInt64(out.Cluster.ClusterSnapshotCopyStatus.RetentionPeriod))

				_, err = e.client.ModifySnapshotCopyRetentionPeriod(
					t.Context(),
					&redshiftsdk.ModifySnapshotCopyRetentionPeriodInput{
						ClusterIdentifier: aws.String("copy-c1"), RetentionPeriod: aws.Int32(36),
					},
				)
				require.Error(t, err, "automated copies are limited to 35 days")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newDroppedEnv(t))
		})
	}
}
