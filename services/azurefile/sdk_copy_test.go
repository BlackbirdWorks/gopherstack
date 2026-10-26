package azurefile_test

import (
	"errors"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

type fakeBlobs map[string][]byte

var errNoBlob = errors.New("no such blob")

func (f fakeBlobs) ReadBlob(container, blob, _ string) ([]byte, string, error) {
	data, ok := f[container+"/"+blob]
	if !ok {
		return nil, "", errNoBlob
	}

	return data, "application/x-blob", nil
}

const blobBase = "http://127.0.0.1:10000/" + azureauth.DefaultAccountName + "/"

func withBlobs(b fakeBlobs) envOpt {
	return func(h *azurefileHandler) {
		h.Blobs = b
		h.BlobPort = 10000
	}
}

func TestSDK_CopyFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "file to file", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := sc.NewRootDirectoryClient().NewFileClient("src.txt")
			_, err := src.Create(ctx(t), 5, &file.CreateOptions{
				HTTPHeaders: &file.HTTPHeaders{ContentType: new("text/plain")},
				Metadata:    map[string]*string{"From": new("src")},
			})
			require.NoError(t, err)
			upload(t, src, 0, "hello")

			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			copied, err := dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)
			assert.Equal(t, file.CopyStatusTypeSuccess, *copied.CopyStatus)
			assert.NotEmpty(t, copied.CopyID)

			assert.Equal(t, "hello", downloadAll(t, dst))

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "text/plain", *props.ContentType)
			assert.Equal(t, "src", md(props.Metadata, "from"))
			assert.Equal(t, *copied.CopyID, *props.CopyID)
			assert.Equal(t, file.CopyStatusTypeSuccess, *props.CopyStatus)
			assert.Equal(t, src.URL(), *props.CopySource)
			assert.Equal(t, "5/5", *props.CopyProgress)
		}},
		{name: "metadata override", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src.txt", 1)
			_, err := src.SetMetadata(ctx(t), &file.SetMetadataOptions{Metadata: map[string]*string{"A": new("1")}})
			require.NoError(t, err)

			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err = dst.StartCopyFromURL(
				ctx(t),
				src.URL(),
				&file.StartCopyFromURLOptions{Metadata: map[string]*string{"B": new("2")}},
			)
			require.NoError(t, err)

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "2", md(props.Metadata, "b"))
			assert.Empty(t, md(props.Metadata, "a"))
		}},
		{name: "source permission carried over", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			const sddl = "O:S-1-5-32-544G:S-1-5-32-544D:(A;;FA;;;SY)"

			src := sc.NewRootDirectoryClient().NewFileClient("src.txt")
			_, err := src.Create(ctx(t), 1, &file.CreateOptions{Permissions: &file.Permissions{Permission: new(sddl)}})
			require.NoError(t, err)

			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err = dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)

			perm, err := sc.GetPermission(ctx(t), *props.FilePermissionKey, nil)
			require.NoError(t, err)
			assert.Equal(t, sddl, *perm.Permission)
		}},
		{name: "overwrites existing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src.txt", 3)
			upload(t, src, 0, "new")
			dst := newFile(t, sc, "dst.txt", 10)
			upload(t, dst, 0, "old-old-ol")

			_, err := dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)
			assert.Equal(t, "new", downloadAll(t, dst))
		}},
		{name: "missing source", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err := dst.StartCopyFromURL(ctx(t), sc.NewRootDirectoryClient().NewFileClient("ghost").URL(), nil)
			requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
		}},
		{name: "foreign url refused", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err := dst.StartCopyFromURL(ctx(t), "https://example.com/some/file", nil)
			requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
		}},
		{name: "expired sas refused", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src.txt", 1)
			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err := dst.StartCopyFromURL(ctx(t), src.URL()+"?sv=2022-11-02&se=2001-01-01T00%3A00%3A00Z&sig=x", nil)
			requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
		}},
		{name: "readonly destination", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src.txt", 1)
			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			_, err := dst.Create(
				ctx(t),
				1,
				&file.CreateOptions{
					SMBProperties: &file.SMBProperties{Attributes: &file.NTFSFileAttributes{ReadOnly: true}},
				},
			)
			require.NoError(t, err)

			_, err = dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			requireCode(t, err, fileerror.ReadOnlyAttribute)

			_, err = dst.StartCopyFromURL(ctx(t), src.URL(), &file.StartCopyFromURLOptions{
				CopyFileSMBInfo: &file.CopyFileSMBInfo{IgnoreReadOnly: new(true)},
			})
			require.NoError(t, err)
		}},
		{name: "abort after completion", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := newFile(t, sc, "src.txt", 1)
			dst := sc.NewRootDirectoryClient().NewFileClient("dst.txt")
			copied, err := dst.StartCopyFromURL(ctx(t), src.URL(), nil)
			require.NoError(t, err)

			_, err = dst.AbortCopy(ctx(t), *copied.CopyID, nil)
			requireCode(t, err, fileerror.Code("NoPendingCopyOperation"))

			_, err = dst.AbortCopy(ctx(t), "other-id", nil)
			requireCode(t, err, fileerror.Code("CopyIdMismatch"))

			_, err = src.AbortCopy(ctx(t), "any", nil)
			requireCode(t, err, fileerror.Code("NoPendingCopyOperation"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			tt.check(t, sc)
		})
	}
}

func TestSDK_CopyFromBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "start copy", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := sc.NewRootDirectoryClient().NewFileClient("from-blob.bin")
			_, err := dst.StartCopyFromURL(ctx(t), blobBase+"cont/data.bin", nil)
			require.NoError(t, err)

			assert.Equal(t, "blob-bytes", downloadAll(t, dst))

			props, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "application/x-blob", *props.ContentType)
			assert.Equal(t, int64(10), *props.ContentLength)
			assert.NotEmpty(t, props.ContentMD5)
		}},
		{name: "put range from url", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := newFile(t, sc, "range.bin", 20)
			_, err := dst.UploadRangeFromURL(ctx(t), blobBase+"cont/data.bin", 2, 5, 4, nil)
			require.NoError(t, err)

			resp, err := dst.DownloadStream(
				ctx(t),
				&file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 5, Count: 4}},
			)
			require.NoError(t, err)

			defer resp.Body.Close()

			buf, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "ob-b", string(buf))
		}},
		{name: "missing blob", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := sc.NewRootDirectoryClient().NewFileClient("nope.bin")
			_, err := dst.StartCopyFromURL(ctx(t), blobBase+"cont/missing.bin", nil)
			requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
		}},
		{name: "virtual host blob url", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dst := sc.NewRootDirectoryClient().NewFileClient("vh.bin")
			_, err := dst.StartCopyFromURL(
				ctx(t),
				"http://"+azureauth.DefaultAccountName+".blob.core.windows.net/cont/data.bin",
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, "blob-bytes", downloadAll(t, dst))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, withBlobs(fakeBlobs{"cont/data.bin": []byte("blob-bytes")}))
			sc := e.svc.NewShareClient("blobdst")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			tt.check(t, sc)
		})
	}
}

func TestSDK_BlobSourceUnwired(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	dst := sc.NewRootDirectoryClient().NewFileClient("x.bin")
	_, err := dst.StartCopyFromURL(ctx(t), "http://127.0.0.1:10000/devstoreaccount1/c/b", nil)
	requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
}

func TestSDK_PutRangeFromFileURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client, src *file.Client)
		name  string
	}{
		{name: "copies a window", check: func(t *testing.T, sc *share.Client, src *file.Client) {
			t.Helper()

			dst := newFile(t, sc, "dst.bin", 10)
			_, err := dst.UploadRangeFromURL(ctx(t), src.URL(), 3, 0, 4, nil)
			require.NoError(t, err)
			assert.Equal(t, "3456\x00\x00\x00\x00\x00\x00", downloadAll(t, dst))
		}},
		{name: "source out of range", check: func(t *testing.T, sc *share.Client, src *file.Client) {
			t.Helper()

			dst := newFile(t, sc, "dst.bin", 10)
			_, err := dst.UploadRangeFromURL(ctx(t), src.URL(), 8, 0, 4, nil)
			requireCode(t, err, fileerror.InvalidRange)
		}},
		{name: "destination too small", check: func(t *testing.T, sc *share.Client, src *file.Client) {
			t.Helper()

			dst := newFile(t, sc, "dst.bin", 2)
			_, err := dst.UploadRangeFromURL(ctx(t), src.URL(), 0, 0, 4, nil)
			requireCode(t, err, fileerror.InvalidRange)
		}},
		{name: "foreign source", check: func(t *testing.T, sc *share.Client, _ *file.Client) {
			t.Helper()

			dst := newFile(t, sc, "dst.bin", 4)
			_, err := dst.UploadRangeFromURL(ctx(t), "https://example.com/a/b/c", 0, 0, 4, nil)
			requireCode(t, err, fileerror.Code("CannotVerifyCopySource"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			src := newFile(t, sc, "src.bin", 10)
			upload(t, src, 0, "0123456789")

			tt.check(t, sc, src)
		})
	}
}
