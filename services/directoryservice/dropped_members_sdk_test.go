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

func newSDKDirectory(t *testing.T) (*directoryservicesdk.Client, string) {
	t.Helper()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)

	return client, aws.ToString(created.DirectoryId)
}

func TestTrust_ConditionalForwarderMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deleteForwarder   bool
		wantForwarderGone bool
	}{
		{name: "forwarder kept by default", deleteForwarder: false, wantForwarderGone: false},
		{name: "forwarder deleted on request", deleteForwarder: true, wantForwarderGone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, dirID := newSDKDirectory(t)

			created, err := client.CreateTrust(t.Context(), &directoryservicesdk.CreateTrustInput{
				DirectoryId:                 aws.String(dirID),
				RemoteDomainName:            aws.String("remote.example.com"),
				TrustPassword:               aws.String("TrustPW1!"),
				TrustDirection:              types.TrustDirectionTwoWay,
				ConditionalForwarderIpAddrs: []string{"10.1.1.1", "10.1.1.2"},
			})
			require.NoError(t, err)

			fwd, err := client.DescribeConditionalForwarders(
				t.Context(),
				&directoryservicesdk.DescribeConditionalForwardersInput{
					DirectoryId: aws.String(dirID),
				},
			)
			require.NoError(t, err)
			require.Len(t, fwd.ConditionalForwarders, 1)
			assert.Equal(t, []string{"10.1.1.1", "10.1.1.2"}, fwd.ConditionalForwarders[0].DnsIpAddrs)

			_, err = client.DeleteTrust(t.Context(), &directoryservicesdk.DeleteTrustInput{
				TrustId:                              created.TrustId,
				DeleteAssociatedConditionalForwarder: tt.deleteForwarder,
			})
			require.NoError(t, err)

			fwd, err = client.DescribeConditionalForwarders(
				t.Context(),
				&directoryservicesdk.DescribeConditionalForwardersInput{
					DirectoryId: aws.String(dirID),
				},
			)
			require.NoError(t, err)

			if tt.wantForwarderGone {
				assert.Empty(t, fwd.ConditionalForwarders)

				return
			}

			assert.Len(t, fwd.ConditionalForwarders, 1)
		})
	}
}

func TestCreateComputer_EchoesAttributes(t *testing.T) {
	t.Parallel()

	client, dirID := newSDKDirectory(t)

	out, err := client.CreateComputer(t.Context(), &directoryservicesdk.CreateComputerInput{
		DirectoryId:  aws.String(dirID),
		ComputerName: aws.String("WS01"),
		Password:     aws.String("Admin1234!"),
		ComputerAttributes: []types.Attribute{
			{Name: aws.String("description"), Value: aws.String("front desk")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Computer)
	require.Len(t, out.Computer.ComputerAttributes, 1)
	assert.Equal(t, "description", aws.ToString(out.Computer.ComputerAttributes[0].Name))
	assert.Equal(t, "front desk", aws.ToString(out.Computer.ComputerAttributes[0].Value))
}

func TestUpdateDirectorySetup_AppliesSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    directoryservicesdk.UpdateDirectorySetupInput
		name     string
		wantOS   types.OSVersion
		wantSize types.DirectorySize
		wantNet  types.NetworkType
		wantCode string
		wantIPv6 bool
	}{
		{
			name: "os",
			input: directoryservicesdk.UpdateDirectorySetupInput{
				UpdateType:       types.UpdateTypeOs,
				OSUpdateSettings: &types.OSUpdateSettings{OSVersion: types.OSVersionVersion2019},
			},
			wantOS: types.OSVersionVersion2019, wantSize: types.DirectorySizeSmall, wantNet: types.NetworkTypeIpv4Only,
		},
		{
			name: "size",
			input: directoryservicesdk.UpdateDirectorySetupInput{
				UpdateType: types.UpdateTypeSize,
				DirectorySizeUpdateSettings: &types.DirectorySizeUpdateSettings{
					DirectorySize: types.DirectorySizeLarge,
				},
			},
			wantSize: types.DirectorySizeLarge, wantNet: types.NetworkTypeIpv4Only,
		},
		{
			name: "network",
			input: directoryservicesdk.UpdateDirectorySetupInput{
				UpdateType:            types.UpdateTypeNetwork,
				NetworkUpdateSettings: &types.NetworkUpdateSettings{NetworkType: types.NetworkTypeDualStack},
			},
			wantSize: types.DirectorySizeSmall, wantNet: types.NetworkTypeDualStack, wantIPv6: true,
		},
		{
			name: "invalid os version",
			input: directoryservicesdk.UpdateDirectorySetupInput{
				UpdateType:       types.UpdateTypeOs,
				OSUpdateSettings: &types.OSUpdateSettings{OSVersion: "SERVER_1999"},
			},
			wantCode: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, dirID := newSDKDirectory(t)
			tt.input.DirectoryId = aws.String(dirID)

			_, err := client.UpdateDirectorySetup(t.Context(), &tt.input)

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
				DirectoryIds: []string{dirID},
			})
			require.NoError(t, err)
			require.Len(t, desc.DirectoryDescriptions, 1)

			d := desc.DirectoryDescriptions[0]
			assert.Equal(t, tt.wantOS, d.OsVersion)
			assert.Equal(t, tt.wantSize, d.Size)
			assert.Equal(t, tt.wantNet, d.NetworkType)
			assert.Equal(t, tt.wantIPv6, len(d.DnsIpv6Addrs) > 0)

			updates, err := client.DescribeUpdateDirectory(
				t.Context(),
				&directoryservicesdk.DescribeUpdateDirectoryInput{
					DirectoryId: aws.String(dirID),
					UpdateType:  tt.input.UpdateType,
				},
			)
			require.NoError(t, err)
			require.Len(t, updates.UpdateActivities, 1)

			if tt.wantOS != "" {
				require.NotNil(t, updates.UpdateActivities[0].NewValue)
				require.NotNil(t, updates.UpdateActivities[0].NewValue.OSUpdateSettings)
				assert.Equal(t, tt.wantOS, updates.UpdateActivities[0].NewValue.OSUpdateSettings.OSVersion)
			}
		})
	}
}

func TestDescribeUpdateDirectory_RegionNameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		regionName string
		wantCount  int
	}{
		{name: "matching region", regionName: "us-east-1", wantCount: 1},
		{name: "other region", regionName: "eu-west-1", wantCount: 0},
		{name: "no filter", regionName: "", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, dirID := newSDKDirectory(t)

			_, err := client.UpdateDirectorySetup(t.Context(), &directoryservicesdk.UpdateDirectorySetupInput{
				DirectoryId: aws.String(dirID),
				UpdateType:  types.UpdateTypeOs,
			})
			require.NoError(t, err)

			in := &directoryservicesdk.DescribeUpdateDirectoryInput{
				DirectoryId: aws.String(dirID),
				UpdateType:  types.UpdateTypeOs,
			}
			if tt.regionName != "" {
				in.RegionName = aws.String(tt.regionName)
			}

			out, err := client.DescribeUpdateDirectory(t.Context(), in)
			require.NoError(t, err)
			assert.Len(t, out.UpdateActivities, tt.wantCount)
		})
	}
}
