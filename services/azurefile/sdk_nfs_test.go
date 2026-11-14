package azurefile_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNFSShare(t *testing.T) *share.Client {
	t.Helper()

	e := newEnv(t)
	sc := e.svc.NewShareClient("nfsshare")
	_, err := sc.Create(ctx(t), &share.CreateOptions{EnabledProtocols: new("NFS")})
	require.NoError(t, err)

	return sc
}

func TestSDK_NFSProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "file defaults", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("f")
			created, err := fc.Create(ctx(t), 4, &file.CreateOptions{NFSProperties: &file.NFSProperties{}})
			require.NoError(t, err)
			assert.Equal(t, "0664", *created.FileMode)
			assert.Equal(t, "0", *created.Owner)
			assert.Equal(t, "0", *created.Group)
			assert.Equal(t, "Regular", string(*created.NFSFileType))

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "0664", *props.FileMode)
			assert.EqualValues(t, 1, *props.LinkCount)
			assert.Nil(t, props.FileAttributes)
		}},
		{name: "mode owner group set and symbolic mode", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("f")
			_, err := fc.Create(ctx(t), 4, &file.CreateOptions{
				NFSProperties: &file.NFSProperties{FileMode: new("0640"), Owner: new("1000"), Group: new("1001")},
			})
			require.NoError(t, err)

			props, err := fc.SetHTTPHeaders(ctx(t), &file.SetHTTPHeadersOptions{
				NFSProperties: &file.NFSProperties{FileMode: new("rwxr-xr--")},
			})
			require.NoError(t, err)
			assert.Equal(t, "0754", *props.FileMode)
			assert.Equal(t, "1000", *props.Owner)
			assert.Equal(t, "1001", *props.Group)
		}},
		{name: "invalid mode owner", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("f")
			_, err := fc.Create(
				ctx(t),
				4,
				&file.CreateOptions{NFSProperties: &file.NFSProperties{FileMode: new("0999")}},
			)
			requireCode(t, err, fileerror.InvalidHeaderValue)

			_, err = fc.Create(ctx(t), 4, &file.CreateOptions{NFSProperties: &file.NFSProperties{Owner: new("root")}})
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
		{name: "directory defaults", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("d")
			created, err := dc.Create(ctx(t), &directory.CreateOptions{FileNFSProperties: &file.NFSProperties{}})
			require.NoError(t, err)
			assert.Equal(t, "0755", *created.FileMode)
			assert.Equal(t, "Directory", string(*created.NFSFileType))

			root, err := sc.NewRootDirectoryClient().GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "0755", *root.FileMode)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newNFSShare(t))
		})
	}
}

func TestSDK_NFSLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "hard link shares content", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src", 4)
			link := sc.NewRootDirectoryClient().NewFileClient("link")

			created, err := link.CreateHardLink(ctx(t), "src", nil)
			require.NoError(t, err)
			assert.EqualValues(t, 2, *created.LinkCount)

			upload(t, src, 0, "data")
			assert.Equal(t, "data", downloadAll(t, link))

			props, err := link.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.EqualValues(t, 2, *props.LinkCount)

			_, err = src.Delete(ctx(t), nil)
			require.NoError(t, err)

			props, err = link.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.EqualValues(t, 1, *props.LinkCount)
			assert.Equal(t, "data", downloadAll(t, link))
		}},
		{name: "hard link errors", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			newFile(t, sc, "src", 1)
			newFile(t, sc, "taken", 1)

			_, err := sc.NewRootDirectoryClient().NewFileClient("taken").CreateHardLink(ctx(t), "src", nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)

			_, err = sc.NewRootDirectoryClient().NewFileClient("l").CreateHardLink(ctx(t), "missing", nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "symbolic link round trip", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			link := sc.NewRootDirectoryClient().NewFileClient("sym")
			created, err := link.CreateSymbolicLink(ctx(t), "../target/file", &file.CreateSymbolicLinkOptions{
				FileNFSProperties: &file.NFSProperties{Owner: new("5"), Group: new("6")},
			})
			require.NoError(t, err)
			assert.Equal(t, "SymLink", string(*created.NFSFileType))
			assert.Equal(t, "5", *created.Owner)

			got, err := link.GetSymbolicLink(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "../target/file", *got.LinkText)

			_, err = link.CreateSymbolicLink(ctx(t), "x", nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)

			_, err = newFile(t, sc, "plain", 1).GetSymbolicLink(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceTypeMismatch)
		}},
		{name: "hard link to symlink refused", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewRootDirectoryClient().NewFileClient("sym").CreateSymbolicLink(ctx(t), "x", nil)
			require.NoError(t, err)

			_, err = sc.NewRootDirectoryClient().NewFileClient("l").CreateHardLink(ctx(t), "sym", nil)
			requireCode(t, err, fileerror.Code("XNfs4FileTypeUnsupportedException"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newNFSShare(t))
		})
	}
}

func TestSDK_NFSLinksNeedNFSShare(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	newFile(t, sc, "src", 1)

	_, err := sc.NewRootDirectoryClient().NewFileClient("l").CreateHardLink(ctx(t), "src", nil)
	requireCode(t, err, fileerror.InvalidURI)

	_, err = sc.NewRootDirectoryClient().NewFileClient("s").CreateSymbolicLink(ctx(t), "src", nil)
	requireCode(t, err, fileerror.InvalidURI)
}
