package azurefile_test

import (
	"net"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

func TestSDK_ShareProvisioningOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sc := e.svc.NewShareClient("provisioned")
	_, err := sc.Create(ctx(t), &share.CreateOptions{
		PaidBurstingEnabled: new(
			true,
		),
		PaidBurstingMaxIops:                  new(int64(10000)),
		PaidBurstingMaxBandwidthMibps:        new(int64(500)),
		ShareProvisionedIops:                 new(int64(3000)),
		ShareProvisionedBandwidthMibps:       new(int64(125)),
		EnableSnapshotVirtualDirectoryAccess: new(true),
		EnableSMBDirectoryLease:              new(false),
	})
	require.NoError(t, err)

	props, err := sc.GetProperties(ctx(t), nil)
	require.NoError(t, err)
	assert.True(t, *props.PaidBurstingEnabled)
	assert.Equal(t, int64(10000), *props.PaidBurstingMaxIops)
	assert.Equal(t, int64(500), *props.PaidBurstingMaxBandwidthMibps)
	assert.Equal(t, int32(3000), *props.ProvisionedIops)
	assert.Equal(t, int32(125), *props.ProvisionedBandwidthMiBps)
	assert.True(t, *props.EnableSnapshotVirtualDirectoryAccess)
	assert.False(t, *props.EnableSMBDirectoryLease)

	_, err = sc.SetProperties(ctx(t), &share.SetPropertiesOptions{ShareProvisionedIops: new(int64(4000))})
	require.NoError(t, err)

	props, err = sc.GetProperties(ctx(t), nil)
	require.NoError(t, err)
	assert.Equal(t, int32(4000), *props.ProvisionedIops)
	assert.True(t, *props.PaidBurstingEnabled, "unset options are left alone")

	pager := e.svc.NewListSharesPager(nil)
	page, err := pager.NextPage(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, int32(4000), *page.Shares[0].Properties.ProvisionedIops)
}

func TestSDK_SMBTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, fc *file.Client)
		name  string
	}{
		{name: "explicit times round trip", check: func(t *testing.T, fc *file.Client) {
			t.Helper()

			created := time.Date(2019, 1, 2, 3, 4, 5, 600000000, time.UTC)
			written := time.Date(2020, 2, 3, 4, 5, 6, 0, time.UTC)

			_, err := fc.SetHTTPHeaders(ctx(t), &file.SetHTTPHeadersOptions{SMBProperties: &file.SMBProperties{
				CreationTime: &created, LastWriteTime: &written,
			}})
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.True(t, created.Equal(*props.FileCreationTime))
			assert.True(t, written.Equal(*props.FileLastWriteTime))
		}},
		{name: "omitted properties are preserved", check: func(t *testing.T, fc *file.Client) {
			t.Helper()

			before, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)

			_, err = fc.SetHTTPHeaders(
				ctx(t),
				&file.SetHTTPHeadersOptions{HTTPHeaders: &file.HTTPHeaders{ContentType: new("a/b")}},
			)
			require.NoError(t, err)

			after, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, *before.FileCreationTime, *after.FileCreationTime)
			assert.Equal(t, *before.FilePermissionKey, *after.FilePermissionKey)
			assert.Equal(t, *before.FileAttributes, *after.FileAttributes)
		}},
		{name: "upload range advances last write time", check: func(t *testing.T, fc *file.Client) {
			t.Helper()

			before, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)

			upload(t, fc, 0, "x")

			after, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.False(t, after.FileLastWriteTime.Before(*before.FileLastWriteTime))
			assert.NotEqual(t, *before.ETag, *after.ETag)
		}},
		{name: "preserve keeps last write", check: func(t *testing.T, fc *file.Client) {
			t.Helper()

			pinned := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
			_, err := fc.SetHTTPHeaders(
				ctx(t),
				&file.SetHTTPHeadersOptions{SMBProperties: &file.SMBProperties{LastWriteTime: &pinned}},
			)
			require.NoError(t, err)

			_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("y"), &file.UploadRangeOptions{
				LastWrittenMode: new(file.LastWrittenModePreserve),
			})
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.True(t, pinned.Equal(*props.FileLastWriteTime))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			tt.check(t, newFile(t, sc, "times.txt", 4))
		})
	}
}

func TestSDK_ThroughListener(t *testing.T) {
	t.Parallel()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	h := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	h.Port = port
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	cred, err := service.NewSharedKeyCredential(azureauth.DefaultAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err)

	svc, err := service.NewClientWithSharedKeyCredential(
		"http://127.0.0.1:"+itoa(port)+"/"+azureauth.DefaultAccountName+"/", cred, nil)
	require.NoError(t, err)

	sc := svc.NewShareClient("listener")
	_, err = sc.Create(ctx(t), nil)
	require.NoError(t, err)

	fc := sc.NewRootDirectoryClient().NewFileClient("f")
	_, err = fc.Create(ctx(t), 2, nil)
	require.NoError(t, err)
	upload(t, fc, 0, "ok")
	assert.Equal(t, "ok", downloadAll(t, fc))

	_, err = sc.NewRootDirectoryClient().NewFileClient("missing").Delete(ctx(t), nil)
	requireCode(t, err, fileerror.ResourceNotFound)
}
