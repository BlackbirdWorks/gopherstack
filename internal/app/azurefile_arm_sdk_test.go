package app

import (
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	pkgservice "github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

const (
	afSub     = "00000000-0000-0000-0000-000000000000"
	afRG      = "files-rg"
	afAccount = "filesacct"
)

type afStack struct {
	fileServices *armstorage.FileServicesClient
	shares       *service.Client
	h            *azurefile.Handler
}

func newAFStack(t *testing.T) *afStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&pkgservice.AppContext{})
	require.NoError(t, err)

	fileH := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	wireAzureFile(map[string]pkgservice.Registerable{"AzureARM": armSvc, "AzureFile": fileH})

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	armEcho := echo.New()
	armEcho.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(armEcho)
	t.Cleanup(armSrv.Close)

	fileEcho := echo.New()
	fileEcho.Any("/*", fileH.Handler())

	fileSrv := httptest.NewServer(fileEcho)
	t.Cleanup(fileSrv.Close)

	factory, err := armstorage.NewClientFactory(afSub, fnSDKCredential{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: armSrv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: armSrv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	poller, err := factory.NewAccountsClient().
		BeginCreate(t.Context(), afRG, afAccount, armstorage.AccountCreateParameters{
			Kind: new(armstorage.KindStorageV2), Location: new("local"),
			SKU: &armstorage.SKU{Name: new(armstorage.SKUNameStandardLRS)},
		}, nil)
	require.NoError(t, err)
	_, err = poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	cred, err := service.NewSharedKeyCredential(azureauth.DefaultAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err)

	shares, err := service.NewClientWithSharedKeyCredential(fileSrv.URL+"/"+afAccount+"/", cred, nil)
	require.NoError(t, err)

	return &afStack{fileServices: factory.NewFileServicesClient(), shares: shares, h: fileH}
}

func (s *afStack) setRetention(t *testing.T, enabled bool, days int32) error {
	t.Helper()

	_, err := s.fileServices.SetServiceProperties(t.Context(), afRG, afAccount, armstorage.FileServiceProperties{
		FileServiceProperties: &armstorage.FileServicePropertiesProperties{
			ShareDeleteRetentionPolicy: &armstorage.DeleteRetentionPolicy{Enabled: &enabled, Days: &days},
		},
	}, nil)

	return err
}

func TestAzureFileARMShareSoftDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, s *afStack)
		name  string
	}{
		{name: "defaults off", check: func(t *testing.T, s *afStack) {
			t.Helper()

			got, err := s.fileServices.GetServiceProperties(t.Context(), afRG, afAccount, nil)
			require.NoError(t, err)
			assert.Equal(t, "default", *got.Name)
			assert.Equal(t, "Microsoft.Storage/storageAccounts/fileServices", *got.Type)
			assert.False(t, *got.FileServiceProperties.FileServiceProperties.ShareDeleteRetentionPolicy.Enabled)
		}},
		{name: "set get list", check: func(t *testing.T, s *afStack) {
			t.Helper()
			require.NoError(t, s.setRetention(t, true, 9))

			got, err := s.fileServices.GetServiceProperties(t.Context(), afRG, afAccount, nil)
			require.NoError(t, err)
			assert.True(t, *got.FileServiceProperties.FileServiceProperties.ShareDeleteRetentionPolicy.Enabled)
			assert.Equal(t, int32(9), *got.FileServiceProperties.FileServiceProperties.ShareDeleteRetentionPolicy.Days)

			list, err := s.fileServices.List(t.Context(), afRG, afAccount, nil)
			require.NoError(t, err)
			require.Len(t, list.Value, 1)
			assert.Equal(t, int32(9), *list.Value[0].FileServiceProperties.ShareDeleteRetentionPolicy.Days)
		}},
		{name: "delete and restore", check: func(t *testing.T, s *afStack) {
			t.Helper()
			require.NoError(t, s.setRetention(t, true, 5))

			sc := s.shares.NewShareClient("softshare")
			_, err := sc.Create(t.Context(), nil)
			require.NoError(t, err)
			_, err = sc.Delete(t.Context(), nil)
			require.NoError(t, err)

			version := ""
			pager := s.shares.NewListSharesPager(
				&service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true}},
			)
			for pager.More() {
				page, pageErr := pager.NextPage(t.Context())
				require.NoError(t, pageErr)

				for _, sh := range page.Shares {
					if sh.Deleted != nil && *sh.Deleted {
						version = *sh.Version
						assert.Equal(t, int32(5), *sh.Properties.RemainingRetentionDays)
					}
				}
			}

			require.NotEmpty(t, version)

			_, err = s.shares.RestoreShare(t.Context(), "softshare", version, nil)
			require.NoError(t, err)
			_, err = sc.GetProperties(t.Context(), nil)
			require.NoError(t, err)
		}},
		{name: "disabled means hard delete", check: func(t *testing.T, s *afStack) {
			t.Helper()
			require.NoError(t, s.setRetention(t, true, 5))
			require.NoError(t, s.setRetention(t, false, 0))

			sc := s.shares.NewShareClient("hardshare")
			_, err := sc.Create(t.Context(), nil)
			require.NoError(t, err)
			_, err = sc.Delete(t.Context(), nil)
			require.NoError(t, err)

			_, err = s.shares.RestoreShare(t.Context(), "hardshare", "v", nil)
			require.Error(t, err)
			assert.True(t, fileerror.HasCode(err, fileerror.ShareNotFound))
		}},
		{name: "invalid days rejected", check: func(t *testing.T, s *afStack) {
			t.Helper()
			require.Error(t, s.setRetention(t, true, 0))
			require.Error(t, s.setRetention(t, true, 366))
		}},
		{name: "unknown account", check: func(t *testing.T, s *afStack) {
			t.Helper()

			_, err := s.fileServices.GetServiceProperties(t.Context(), afRG, "noacct", nil)
			require.Error(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newAFStack(t))
		})
	}
}
