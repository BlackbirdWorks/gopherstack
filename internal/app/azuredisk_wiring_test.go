package app

import (
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgservice "github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func diskFooter(virtual int64) []byte {
	f := make([]byte, 512)
	copy(f, "conectix")
	binary.BigEndian.PutUint64(f[48:], uint64(virtual))
	binary.BigEndian.PutUint32(f[60:], 2)

	var sum uint32

	for i, b := range f {
		if i < 64 || i >= 68 {
			sum += uint32(b)
		}
	}

	binary.BigEndian.PutUint32(f[64:], ^sum)

	return f
}

func TestAzureDiskImportReadsBlobService(t *testing.T) {
	t.Parallel()

	const (
		sub     = "00000000-0000-0000-0000-000000000000"
		rg      = "disk-rg"
		virtual = 1 << 20
	)

	armSvc, err := (&azurearm.Provider{}).Init(&pkgservice.AppContext{})
	require.NoError(t, err)

	blobH := azureblob.NewHandler(azureblob.NewInMemoryBackend())
	wireAzureDisks(map[string]pkgservice.Registerable{"AzureARM": armSvc, "AzureBlob": blobH})

	_, err = blobH.Backend.CreateContainerWith("vhds", azureblob.ContainerOptions{})
	require.NoError(t, err)

	_, err = blobH.Backend.PutBlobWith("vhds", "os.vhd", nil, azureblob.BlobAttrs{
		BlobType: azureblob.BlobTypePage, PageLength: virtual + 512,
	})
	require.NoError(t, err)
	_, err = blobH.Backend.PutPage("vhds", "os.vhd", virtual, virtual+511, diskFooter(virtual), false)
	require.NoError(t, err)
	_, err = blobH.Backend.PutBlobWith("vhds", "plain.vhd", []byte("not a vhd"), azureblob.BlobAttrs{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	cf, err := armcompute.NewClientFactory(sub, fnSDKCredential{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	rgReq, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		srv.URL+"/subscriptions/"+sub+"/resourcegroups/"+rg, strings.NewReader(`{"location":"westus"}`))
	require.NoError(t, err)
	rgReq.Header.Set("Content-Type", "application/json")

	rgResp, err := srv.Client().Do(rgReq)
	require.NoError(t, err)
	require.NoError(t, rgResp.Body.Close())

	tests := []struct {
		name    string
		blob    string
		wantErr bool
	}{
		{name: "uploaded vhd", blob: "os.vhd"},
		{name: "block blob", blob: "plain.vhd", wantErr: true},
		{name: "missing blob", blob: "gone.vhd", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pl, perr := cf.NewDisksClient().BeginCreateOrUpdate(t.Context(), rg, "d-"+tt.blob, armcompute.Disk{
				Location: new("westus"),
				Properties: &armcompute.DiskProperties{CreationData: &armcompute.CreationData{
					CreateOption: new(armcompute.DiskCreateOptionImport),
					SourceURI:    new("https://acct.blob.core.windows.net/vhds/" + tt.blob),
				}},
			}, nil)
			if perr == nil {
				var got armcompute.DisksClientCreateOrUpdateResponse

				got, perr = pl.PollUntilDone(t.Context(), pollOpts())
				if !tt.wantErr {
					require.NoError(t, perr)
					assert.Equal(t, int32(1), *got.Properties.DiskSizeGB)

					return
				}
			}

			require.Error(t, perr)
		})
	}
}
