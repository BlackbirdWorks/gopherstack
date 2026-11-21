package app

import (
	"cmp"
	"encoding/binary"
	"math"
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
	binary.BigEndian.PutUint32(f[8:], 2)
	binary.BigEndian.PutUint32(f[12:], 0x00010000)
	binary.BigEndian.PutUint64(f[16:], math.MaxUint64)
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

	blobH.EnforcePublicAccess = true

	for container, access := range map[string]string{"vhds": azureblob.AccessBlob, "private": ""} {
		_, err = blobH.Backend.CreateContainerWith(container, azureblob.ContainerOptions{PublicAccess: access})
		require.NoError(t, err)

		_, err = blobH.Backend.PutBlobWith(container, "os.vhd", nil, azureblob.BlobAttrs{
			BlobType: azureblob.BlobTypePage, PageLength: virtual + 512,
		})
		require.NoError(t, err)
		_, err = blobH.Backend.PutPage(container, "os.vhd", virtual, virtual+511, diskFooter(virtual), false)
		require.NoError(t, err)
	}

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

	acctReq, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		srv.URL+"/subscriptions/"+sub+"/resourceGroups/"+rg+
			"/providers/Microsoft.Storage/storageAccounts/acct?api-version=2023-01-01",
		strings.NewReader(`{"location":"westus","kind":"StorageV2","sku":{"name":"Standard_LRS"}}`))
	require.NoError(t, err)
	acctReq.Header.Set("Content-Type", "application/json")

	acctResp, err := srv.Client().Do(acctReq)
	require.NoError(t, err)
	require.NoError(t, acctResp.Body.Close())
	require.Less(t, acctResp.StatusCode, 300)

	tests := []struct {
		name      string
		container string
		blob      string
		query     string
		wantErr   bool
	}{
		{name: "uploaded vhd", blob: "os.vhd"},
		{name: "private container without sas", container: "private", blob: "os.vhd", wantErr: true},
		{name: "block blob", blob: "plain.vhd", wantErr: true},
		{name: "missing blob", blob: "gone.vhd", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			diskName := "d-" + strings.ReplaceAll(tt.name, " ", "-")
			src := "https://acct.blob.core.windows.net/" + cmp.Or(tt.container, "vhds") + "/" + tt.blob + tt.query

			pl, perr := cf.NewDisksClient().BeginCreateOrUpdate(t.Context(), rg, diskName, armcompute.Disk{
				Location: new("westus"),
				Properties: &armcompute.DiskProperties{CreationData: &armcompute.CreationData{
					CreateOption: new(armcompute.DiskCreateOptionImport),
					SourceURI:    new(src),
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
