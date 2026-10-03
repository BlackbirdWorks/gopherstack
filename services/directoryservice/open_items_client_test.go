package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_OpenItemsBurnDown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *directoryservicesdk.Client, dirID string)
		name string
	}{
		{name: "radius_ipv6_servers", run: func(t *testing.T, client *directoryservicesdk.Client, dirID string) {
			t.Helper()

			settings := func(v6 ...string) *types.RadiusSettings {
				return &types.RadiusSettings{
					AuthenticationProtocol: types.RadiusAuthenticationProtocolPap,
					RadiusServers:          []string{"10.0.1.1"},
					RadiusServersIpv6:      v6,
					SharedSecret:           aws.String("secret"),
				}
			}

			_, err := client.EnableRadius(t.Context(), &directoryservicesdk.EnableRadiusInput{
				DirectoryId: aws.String(dirID), RadiusSettings: settings("2001:db8::1"),
			})
			require.NoError(t, err)

			out, err := client.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
				DirectoryIds: []string{dirID},
			})
			require.NoError(t, err)
			require.Len(t, out.DirectoryDescriptions, 1)
			assert.Equal(t, []string{"2001:db8::1"}, out.DirectoryDescriptions[0].RadiusSettings.RadiusServersIpv6)

			_, err = client.UpdateRadius(t.Context(), &directoryservicesdk.UpdateRadiusInput{
				DirectoryId: aws.String(dirID), RadiusSettings: settings("2001:db8::2", "2001:db8::3"),
			})
			require.NoError(t, err)

			out, err = client.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
				DirectoryIds: []string{dirID},
			})
			require.NoError(t, err)
			assert.Equal(t,
				[]string{"2001:db8::2", "2001:db8::3"},
				out.DirectoryDescriptions[0].RadiusSettings.RadiusServersIpv6)
		}},
		{name: "ip_routes_ipv6", run: func(t *testing.T, client *directoryservicesdk.Client, dirID string) {
			t.Helper()

			_, err := client.AddIpRoutes(t.Context(), &directoryservicesdk.AddIpRoutesInput{
				DirectoryId: aws.String(dirID),
				IpRoutes: []types.IpRoute{
					{CidrIp: aws.String("10.1.0.0/24")},
					{CidrIpv6: aws.String("2001:db8::/32"), Description: aws.String("v6")},
					{CidrIpv6: aws.String("2001:db8::/32")},
				},
			})
			require.NoError(t, err)

			listed, err := client.ListIpRoutes(t.Context(), &directoryservicesdk.ListIpRoutesInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.IpRoutesInfo, 2)

			var v6 []string
			for _, r := range listed.IpRoutesInfo {
				if r.CidrIpv6 != nil {
					v6 = append(v6, *r.CidrIpv6)
					assert.Equal(t, "v6", aws.ToString(r.Description))
				}
			}
			assert.Equal(t, []string{"2001:db8::/32"}, v6)

			_, err = client.RemoveIpRoutes(t.Context(), &directoryservicesdk.RemoveIpRoutesInput{
				DirectoryId: aws.String(dirID),
				CidrIpv6s:   []string{"2001:db8::/32"},
			})
			require.NoError(t, err)

			listed, err = client.ListIpRoutes(t.Context(), &directoryservicesdk.ListIpRoutesInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, listed.IpRoutesInfo, 1)
			assert.Equal(t, "10.1.0.0/24", aws.ToString(listed.IpRoutesInfo[0].CidrIp))
			assert.Nil(t, listed.IpRoutesInfo[0].CidrIpv6)
		}},
		{
			name: "consumer_side_shared_directory",
			run: func(t *testing.T, client *directoryservicesdk.Client, dirID string) {
				t.Helper()

				shared, err := client.ShareDirectory(t.Context(), &directoryservicesdk.ShareDirectoryInput{
					DirectoryId: aws.String(dirID),
					ShareMethod: types.ShareMethodHandshake,
					ShareNotes:  aws.String("hello"),
					ShareTarget: &types.ShareTarget{Id: aws.String("111122223333"), Type: types.TargetTypeAccount},
				})
				require.NoError(t, err)
				sharedID := aws.ToString(shared.SharedDirectoryId)

				_, err = client.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
					DirectoryIds: []string{sharedID},
				})
				require.Error(t, err)

				_, err = client.AcceptSharedDirectory(t.Context(), &directoryservicesdk.AcceptSharedDirectoryInput{
					SharedDirectoryId: aws.String(sharedID),
				})
				require.NoError(t, err)

				out, err := client.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
					DirectoryIds: []string{sharedID},
				})
				require.NoError(t, err)
				require.Len(t, out.DirectoryDescriptions, 1)

				d := out.DirectoryDescriptions[0]
				assert.Equal(t, sharedID, aws.ToString(d.DirectoryId))
				assert.Equal(t, types.DirectoryTypeSharedMicrosoftAd, d.Type)
				assert.Equal(t, types.ShareStatusShared, d.ShareStatus)
				assert.Equal(t, types.ShareMethodHandshake, d.ShareMethod)
				assert.Equal(t, "hello", aws.ToString(d.ShareNotes))
				require.NotNil(t, d.OwnerDirectoryDescription)
				assert.Equal(t, dirID, aws.ToString(d.OwnerDirectoryDescription.DirectoryId))
				assert.Equal(t, "000000000000", aws.ToString(d.OwnerDirectoryDescription.AccountId))
			},
		},
		{
			name: "share_target_type_validated",
			run: func(t *testing.T, client *directoryservicesdk.Client, dirID string) {
				t.Helper()

				_, err := client.ShareDirectory(t.Context(), &directoryservicesdk.ShareDirectoryInput{
					DirectoryId: aws.String(dirID),
					ShareMethod: types.ShareMethodHandshake,
					ShareTarget: &types.ShareTarget{
						Id:   aws.String("111122223333"),
						Type: types.TargetType("ORGANIZATION"),
					},
				})
				var ipe *types.InvalidParameterException
				require.ErrorAs(t, err, &ipe)
			},
		},
		{name: "settings_last_requested", run: func(t *testing.T, client *directoryservicesdk.Client, dirID string) {
			t.Helper()

			_, err := client.UpdateSettings(t.Context(), &directoryservicesdk.UpdateSettingsInput{
				DirectoryId: aws.String(dirID),
				Settings:    []types.Setting{{Name: aws.String("TLS_1_0"), Value: aws.String("Disable")}},
			})
			require.NoError(t, err)

			out, err := client.DescribeSettings(t.Context(), &directoryservicesdk.DescribeSettingsInput{
				DirectoryId: aws.String(dirID),
			})
			require.NoError(t, err)
			require.Len(t, out.SettingEntries, 1)
			require.NotNil(t, out.SettingEntries[0].LastRequestedDateTime)
			assert.False(t, out.SettingEntries[0].LastRequestedDateTime.IsZero())
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			tc.run(t, client, createTestDirectory(t, client))
		})
	}
}
