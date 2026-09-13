package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// newRealClient stands up a fresh backend/handler/client
// triple for gopherstack-n3zi.
func newRealClient(t *testing.T) *directoryservicesdk.Client {
	t.Helper()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("000000000000", "us-east-1"))

	return newTestDirectoryServiceClient(t, h)
}

// createTestDirectory creates a minimal SimpleAD directory via the real
// client and returns its ID -- the shared fixture every subtest below builds
// on, since this backend's auxiliary ops don't require the directory to
// reach the Active stage (see the package's directoryLifecycleDelay note).
func createTestDirectory(t *testing.T, client *directoryservicesdk.Client) string {
	t.Helper()

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("slice16.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)
	require.NotEmpty(t, dirID)

	return dirID
}

// TestRealClient_DirectoryLifecycleAndConfiguration drives every op the census
// still listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage).
func TestRealClient_DirectoryLifecycleAndConfiguration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "directory_alias_computer_reset_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			aliased, err := client.CreateAlias(t.Context(), &directoryservicesdk.CreateAliasInput{
				DirectoryId: aws.String(dirID),
				Alias:       aws.String("slice16-alias"),
			})
			require.NoError(t, err)
			assert.Equal(t, "slice16-alias", aws.ToString(aliased.Alias))
			assert.Equal(t, dirID, aws.ToString(aliased.DirectoryId))

			computer, err := client.CreateComputer(t.Context(), &directoryservicesdk.CreateComputerInput{
				DirectoryId:  aws.String(dirID),
				ComputerName: aws.String("WORKSTATION1"),
				Password:     aws.String("Comp1234!"),
			})
			require.NoError(t, err)
			require.NotNil(t, computer.Computer)
			assert.Equal(t, "WORKSTATION1", aws.ToString(computer.Computer.ComputerName))

			_, err = client.ResetUserPassword(t.Context(), &directoryservicesdk.ResetUserPasswordInput{
				DirectoryId: aws.String(dirID),
				UserName:    aws.String("jdoe"),
				NewPassword: aws.String("NewPass1234!"),
			})
			require.NoError(t, err)

			limits, err := client.GetDirectoryLimits(t.Context(), &directoryservicesdk.GetDirectoryLimitsInput{})
			require.NoError(t, err)
			require.NotNil(t, limits.DirectoryLimits)
			require.NotNil(t, limits.DirectoryLimits.CloudOnlyDirectoriesCurrentCount)
			assert.GreaterOrEqual(t, *limits.DirectoryLimits.CloudOnlyDirectoriesCurrentCount, int32(1))
		}},
		{name: "connect_directory", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)

			connected, err := client.ConnectDirectory(t.Context(), &directoryservicesdk.ConnectDirectoryInput{
				Name:     aws.String("connected.example.com"),
				Password: aws.String("Connect1234!"),
				Size:     types.DirectorySizeSmall,
				ConnectSettings: &types.DirectoryConnectSettings{
					CustomerUserName: aws.String("admin"),
					VpcId:            aws.String("vpc-123"),
					SubnetIds:        []string{"subnet-1", "subnet-2"},
				},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(connected.DirectoryId))
		}},
		{name: "tags_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.AddTagsToResource(t.Context(), &directoryservicesdk.AddTagsToResourceInput{
				ResourceId: aws.String(dirID),
				Tags:       []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			})
			require.NoError(t, err)

			listed, err := client.ListTagsForResource(t.Context(), &directoryservicesdk.ListTagsForResourceInput{
				ResourceId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.Tags, 1)
			assert.Equal(t, "env", aws.ToString(listed.Tags[0].Key))

			_, err = client.RemoveTagsFromResource(t.Context(), &directoryservicesdk.RemoveTagsFromResourceInput{
				ResourceId: aws.String(dirID),
				TagKeys:    []string{"env"},
			})
			require.NoError(t, err)

			listed, err = client.ListTagsForResource(t.Context(), &directoryservicesdk.ListTagsForResourceInput{
				ResourceId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, listed.Tags)
		}},
		{name: "conditional_forwarder_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.CreateConditionalForwarder(
				t.Context(),
				&directoryservicesdk.CreateConditionalForwarderInput{
					DirectoryId:      aws.String(dirID),
					RemoteDomainName: aws.String("remote.example.com"),
					DnsIpAddrs:       []string{"10.0.0.1"},
				},
			)
			require.NoError(t, err)

			_, err = client.UpdateConditionalForwarder(
				t.Context(),
				&directoryservicesdk.UpdateConditionalForwarderInput{
					DirectoryId:      aws.String(dirID),
					RemoteDomainName: aws.String("remote.example.com"),
					DnsIpAddrs:       []string{"10.0.0.2"},
				},
			)
			require.NoError(t, err)

			described, err := client.DescribeConditionalForwarders(
				t.Context(),
				&directoryservicesdk.DescribeConditionalForwardersInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			require.Len(t, described.ConditionalForwarders, 1)
			assert.Equal(t, []string{"10.0.0.2"}, described.ConditionalForwarders[0].DnsIpAddrs)

			_, err = client.DeleteConditionalForwarder(
				t.Context(),
				&directoryservicesdk.DeleteConditionalForwarderInput{
					DirectoryId:      aws.String(dirID),
					RemoteDomainName: aws.String("remote.example.com"),
				},
			)
			require.NoError(t, err)

			described, err = client.DescribeConditionalForwarders(
				t.Context(),
				&directoryservicesdk.DescribeConditionalForwardersInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			assert.Empty(t, described.ConditionalForwarders)
		}},
		{name: "certificate_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			registered, err := client.RegisterCertificate(t.Context(), &directoryservicesdk.RegisterCertificateInput{
				DirectoryId:     aws.String(dirID),
				CertificateData: aws.String(testCertPEM),
				Type:            types.CertificateTypeClientCertAuth,
			})
			require.NoError(t, err)
			certID := aws.ToString(registered.CertificateId)
			require.NotEmpty(t, certID)

			_, err = client.DeregisterCertificate(t.Context(), &directoryservicesdk.DeregisterCertificateInput{
				DirectoryId:   aws.String(dirID),
				CertificateId: aws.String(certID),
			})
			require.NoError(t, err)

			_, err = client.DescribeCertificate(t.Context(), &directoryservicesdk.DescribeCertificateInput{
				DirectoryId:   aws.String(dirID),
				CertificateId: aws.String(certID),
			})
			require.Error(t, err)
		}},
		{name: "event_topic_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.RegisterEventTopic(t.Context(), &directoryservicesdk.RegisterEventTopicInput{
				DirectoryId: aws.String(dirID),
				TopicName:   aws.String("ds-events"),
			})
			require.NoError(t, err)

			described, err := client.DescribeEventTopics(t.Context(), &directoryservicesdk.DescribeEventTopicsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.EventTopics, 1)

			_, err = client.DeregisterEventTopic(t.Context(), &directoryservicesdk.DeregisterEventTopicInput{
				DirectoryId: aws.String(dirID),
				TopicName:   aws.String("ds-events"),
			})
			require.NoError(t, err)

			described, err = client.DescribeEventTopics(t.Context(), &directoryservicesdk.DescribeEventTopicsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, described.EventTopics)
		}},
		{name: "log_subscription_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.CreateLogSubscription(t.Context(), &directoryservicesdk.CreateLogSubscriptionInput{
				DirectoryId:  aws.String(dirID),
				LogGroupName: aws.String("/aws/directoryservice/slice16"),
			})
			require.NoError(t, err)

			listed, err := client.ListLogSubscriptions(t.Context(), &directoryservicesdk.ListLogSubscriptionsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.LogSubscriptions, 1)
			assert.Equal(t, "/aws/directoryservice/slice16", aws.ToString(listed.LogSubscriptions[0].LogGroupName))

			_, err = client.DeleteLogSubscription(t.Context(), &directoryservicesdk.DeleteLogSubscriptionInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)

			listed, err = client.ListLogSubscriptions(t.Context(), &directoryservicesdk.ListLogSubscriptionsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, listed.LogSubscriptions)
		}},
		{name: "ldaps_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.EnableLDAPS(t.Context(), &directoryservicesdk.EnableLDAPSInput{
				DirectoryId: aws.String(dirID),
				Type:        types.LDAPSTypeClient,
			})
			require.NoError(t, err)

			described, err := client.DescribeLDAPSSettings(t.Context(), &directoryservicesdk.DescribeLDAPSSettingsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.LDAPSSettingsInfo, 1)
			assert.Equal(t, types.LDAPSStatusEnabled, described.LDAPSSettingsInfo[0].LDAPSStatus)

			_, err = client.DisableLDAPS(t.Context(), &directoryservicesdk.DisableLDAPSInput{
				DirectoryId: aws.String(dirID),
				Type:        types.LDAPSTypeClient,
			})
			require.NoError(t, err)

			described, err = client.DescribeLDAPSSettings(t.Context(), &directoryservicesdk.DescribeLDAPSSettingsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.LDAPSSettingsInfo, 1)
			assert.Equal(t, types.LDAPSStatusDisabled, described.LDAPSSettingsInfo[0].LDAPSStatus)
		}},
		{name: "radius_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.EnableRadius(t.Context(), &directoryservicesdk.EnableRadiusInput{
				DirectoryId: aws.String(dirID),
				RadiusSettings: &types.RadiusSettings{
					AuthenticationProtocol: types.RadiusAuthenticationProtocolPap,
					RadiusServers:          []string{"10.0.1.1"},
					SharedSecret:           aws.String("shared-secret-1"),
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateRadius(t.Context(), &directoryservicesdk.UpdateRadiusInput{
				DirectoryId: aws.String(dirID),
				RadiusSettings: &types.RadiusSettings{
					AuthenticationProtocol: types.RadiusAuthenticationProtocolChap,
					RadiusServers:          []string{"10.0.1.2"},
					SharedSecret:           aws.String("shared-secret-2"),
				},
			})
			require.NoError(t, err)

			_, err = client.DisableRadius(t.Context(), &directoryservicesdk.DisableRadiusInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
		}},
		{name: "sso_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.EnableSso(t.Context(), &directoryservicesdk.EnableSsoInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)

			_, err = client.DisableSso(t.Context(), &directoryservicesdk.DisableSsoInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
		}},
		{name: "client_auth_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.EnableClientAuthentication(
				t.Context(),
				&directoryservicesdk.EnableClientAuthenticationInput{
					DirectoryId: aws.String(dirID),
					Type:        types.ClientAuthenticationTypeSmartCard,
				},
			)
			require.NoError(t, err)

			_, err = client.DisableClientAuthentication(
				t.Context(),
				&directoryservicesdk.DisableClientAuthenticationInput{
					DirectoryId: aws.String(dirID),
					Type:        types.ClientAuthenticationTypeSmartCard,
				},
			)
			require.NoError(t, err)

			described, err := client.DescribeClientAuthenticationSettings(
				t.Context(),
				&directoryservicesdk.DescribeClientAuthenticationSettingsInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			require.Len(t, described.ClientAuthenticationSettingsInfo, 1)
			assert.Equal(
				t,
				types.ClientAuthenticationStatusDisabled,
				described.ClientAuthenticationSettingsInfo[0].Status,
			)
		}},
		{name: "directory_data_access_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.EnableDirectoryDataAccess(
				t.Context(),
				&directoryservicesdk.EnableDirectoryDataAccessInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)

			described, err := client.DescribeDirectoryDataAccess(
				t.Context(),
				&directoryservicesdk.DescribeDirectoryDataAccessInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.DataAccessStatusEnabled, described.DataAccessStatus)

			_, err = client.DisableDirectoryDataAccess(
				t.Context(),
				&directoryservicesdk.DisableDirectoryDataAccessInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)

			described, err = client.DescribeDirectoryDataAccess(
				t.Context(),
				&directoryservicesdk.DescribeDirectoryDataAccessInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.DataAccessStatusDisabled, described.DataAccessStatus)
		}},
		{name: "shared_directory_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			rejectShare, err := client.ShareDirectory(t.Context(), &directoryservicesdk.ShareDirectoryInput{
				DirectoryId: aws.String(dirID),
				ShareMethod: types.ShareMethodHandshake,
				ShareTarget: &types.ShareTarget{Id: aws.String("111111111111"), Type: types.TargetTypeAccount},
			})
			require.NoError(t, err)
			rejectShareID := aws.ToString(rejectShare.SharedDirectoryId)
			require.NotEmpty(t, rejectShareID)

			unshareTargetShare, err := client.ShareDirectory(t.Context(), &directoryservicesdk.ShareDirectoryInput{
				DirectoryId: aws.String(dirID),
				ShareMethod: types.ShareMethodHandshake,
				ShareTarget: &types.ShareTarget{Id: aws.String("222222222222"), Type: types.TargetTypeAccount},
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(unshareTargetShare.SharedDirectoryId))

			described, err := client.DescribeSharedDirectories(
				t.Context(),
				&directoryservicesdk.DescribeSharedDirectoriesInput{OwnerDirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			require.Len(t, described.SharedDirectories, 2)

			rejected, err := client.RejectSharedDirectory(
				t.Context(),
				&directoryservicesdk.RejectSharedDirectoryInput{SharedDirectoryId: aws.String(rejectShareID)},
			)
			require.NoError(t, err)
			assert.Equal(t, rejectShareID, aws.ToString(rejected.SharedDirectoryId))

			_, err = client.UnshareDirectory(t.Context(), &directoryservicesdk.UnshareDirectoryInput{
				DirectoryId: aws.String(dirID),
				UnshareTarget: &types.UnshareTarget{
					Id:   aws.String("222222222222"),
					Type: types.TargetTypeAccount,
				},
			})
			require.NoError(t, err)
		}},
		{name: "region_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.AddRegion(t.Context(), &directoryservicesdk.AddRegionInput{
				DirectoryId: aws.String(dirID),
				RegionName:  aws.String("us-west-2"),
				VPCSettings: &types.DirectoryVpcSettings{
					VpcId:     aws.String("vpc-456"),
					SubnetIds: []string{"subnet-3", "subnet-4"},
				},
			})
			require.NoError(t, err)

			described, err := client.DescribeRegions(t.Context(), &directoryservicesdk.DescribeRegionsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.RegionsDescription, 1)
			assert.Equal(t, "us-west-2", aws.ToString(described.RegionsDescription[0].RegionName))

			_, err = client.RemoveRegion(t.Context(), &directoryservicesdk.RemoveRegionInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)

			described, err = client.DescribeRegions(t.Context(), &directoryservicesdk.DescribeRegionsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, described.RegionsDescription)
		}},
		{name: "domain_controller_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.UpdateNumberOfDomainControllers(
				t.Context(),
				&directoryservicesdk.UpdateNumberOfDomainControllersInput{
					DirectoryId:   aws.String(dirID),
					DesiredNumber: aws.Int32(2),
				},
			)
			require.NoError(t, err)

			described, err := client.DescribeDomainControllers(
				t.Context(),
				&directoryservicesdk.DescribeDomainControllersInput{DirectoryId: aws.String(dirID)},
			)
			require.NoError(t, err)
			require.Len(t, described.DomainControllers, 2)
			assert.Equal(t, dirID, aws.ToString(described.DomainControllers[0].DirectoryId))
		}},
		{name: "snapshot_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			limits, err := client.GetSnapshotLimits(t.Context(), &directoryservicesdk.GetSnapshotLimitsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.NotNil(t, limits.SnapshotLimits)
			assert.Equal(t, int32(0), aws.ToInt32(limits.SnapshotLimits.ManualSnapshotsCurrentCount))

			created, err := client.CreateSnapshot(t.Context(), &directoryservicesdk.CreateSnapshotInput{
				DirectoryId: aws.String(dirID),
				Name:        aws.String("snap-1"),
			})
			require.NoError(t, err)
			snapID := aws.ToString(created.SnapshotId)
			require.NotEmpty(t, snapID)

			described, err := client.DescribeSnapshots(t.Context(), &directoryservicesdk.DescribeSnapshotsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.Snapshots, 1)
			assert.Equal(t, "snap-1", aws.ToString(described.Snapshots[0].Name))

			_, err = client.RestoreFromSnapshot(t.Context(), &directoryservicesdk.RestoreFromSnapshotInput{
				SnapshotId: aws.String(snapID),
			})
			require.NoError(t, err)

			_, err = client.DeleteSnapshot(t.Context(), &directoryservicesdk.DeleteSnapshotInput{
				SnapshotId: aws.String(snapID),
			})
			require.NoError(t, err)

			described, err = client.DescribeSnapshots(t.Context(), &directoryservicesdk.DescribeSnapshotsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, described.Snapshots)
		}},
		{name: "schema_extension_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			started, err := client.StartSchemaExtension(t.Context(), &directoryservicesdk.StartSchemaExtensionInput{
				DirectoryId:                         aws.String(dirID),
				Description:                         aws.String("add custom attribute"),
				LdifContent:                         aws.String("dn: cn=schema\nchangetype: modify\n"),
				CreateSnapshotBeforeSchemaExtension: true,
			})
			require.NoError(t, err)
			extID := aws.ToString(started.SchemaExtensionId)
			require.NotEmpty(t, extID)

			listed, err := client.ListSchemaExtensions(t.Context(), &directoryservicesdk.ListSchemaExtensionsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.SchemaExtensionsInfo, 1)
			assert.Equal(t, extID, aws.ToString(listed.SchemaExtensionsInfo[0].SchemaExtensionId))

			_, err = client.CancelSchemaExtension(t.Context(), &directoryservicesdk.CancelSchemaExtensionInput{
				DirectoryId:       aws.String(dirID),
				SchemaExtensionId: aws.String(extID),
			})
			require.NoError(t, err)
		}},
		{name: "trust_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			created, err := client.CreateTrust(t.Context(), &directoryservicesdk.CreateTrustInput{
				DirectoryId:      aws.String(dirID),
				RemoteDomainName: aws.String("trust.example.com"),
				TrustDirection:   types.TrustDirectionTwoWay,
				TrustPassword:    aws.String("TrustPass1234!"),
			})
			require.NoError(t, err)
			trustID := aws.ToString(created.TrustId)
			require.NotEmpty(t, trustID)

			described, err := client.DescribeTrusts(t.Context(), &directoryservicesdk.DescribeTrustsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, described.Trusts, 1)
			assert.Equal(t, "trust.example.com", aws.ToString(described.Trusts[0].RemoteDomainName))

			updated, err := client.UpdateTrust(t.Context(), &directoryservicesdk.UpdateTrustInput{
				TrustId:       aws.String(trustID),
				SelectiveAuth: types.SelectiveAuthEnabled,
			})
			require.NoError(t, err)
			assert.Equal(t, trustID, aws.ToString(updated.TrustId))

			verified, err := client.VerifyTrust(t.Context(), &directoryservicesdk.VerifyTrustInput{
				TrustId: aws.String(trustID),
			})
			require.NoError(t, err)
			assert.Equal(t, trustID, aws.ToString(verified.TrustId))

			_, err = client.DeleteTrust(t.Context(), &directoryservicesdk.DeleteTrustInput{
				TrustId: aws.String(trustID),
			})
			require.NoError(t, err)

			described, err = client.DescribeTrusts(t.Context(), &directoryservicesdk.DescribeTrustsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, described.Trusts)
		}},
		{name: "hybrid_ad_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			assessment, err := client.StartADAssessment(t.Context(), &directoryservicesdk.StartADAssessmentInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assessmentID := aws.ToString(assessment.AssessmentId)
			require.NotEmpty(t, assessmentID)

			hybrid, err := client.CreateHybridAD(t.Context(), &directoryservicesdk.CreateHybridADInput{
				AssessmentId: aws.String(assessmentID),
				SecretArn:    aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:hybrid-secret"),
			})
			require.NoError(t, err)
			hybridDirID := aws.ToString(hybrid.DirectoryId)
			require.NotEmpty(t, hybridDirID)

			updated, err := client.UpdateHybridAD(t.Context(), &directoryservicesdk.UpdateHybridADInput{
				DirectoryId: aws.String(hybridDirID),
				HybridAdministratorAccountUpdate: &types.HybridAdministratorAccountUpdate{
					SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:hybrid-secret-2"),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, hybridDirID, aws.ToString(updated.DirectoryId))

			described, err := client.DescribeHybridADUpdate(
				t.Context(),
				&directoryservicesdk.DescribeHybridADUpdateInput{DirectoryId: aws.String(hybridDirID)},
			)
			require.NoError(t, err)
			require.NotNil(t, described.UpdateActivities)
			require.Len(t, described.UpdateActivities.HybridAdministratorAccount, 1)
		}},
		{name: "ip_routes_family", run: func(t *testing.T) {
			t.Helper()

			client := newRealClient(t)
			dirID := createTestDirectory(t, client)

			_, err := client.AddIpRoutes(t.Context(), &directoryservicesdk.AddIpRoutesInput{
				DirectoryId: aws.String(dirID),
				IpRoutes: []types.IpRoute{
					{CidrIp: aws.String("10.1.0.0/24"), Description: aws.String("branch office")},
				},
			})
			require.NoError(t, err)

			listed, err := client.ListIpRoutes(t.Context(), &directoryservicesdk.ListIpRoutesInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.IpRoutesInfo, 1)
			assert.Equal(t, "10.1.0.0/24", aws.ToString(listed.IpRoutesInfo[0].CidrIp))
			_, err = client.RemoveIpRoutes(t.Context(), &directoryservicesdk.RemoveIpRoutesInput{
				DirectoryId: aws.String(dirID),
				CidrIps:     []string{"10.1.0.0/24"},
			})
			require.NoError(t, err)

			listed, err = client.ListIpRoutes(t.Context(), &directoryservicesdk.ListIpRoutesInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			assert.Empty(t, listed.IpRoutesInfo)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
