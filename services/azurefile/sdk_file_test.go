package azurefile_test

import (
	"bytes"
	"crypto/md5"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func upload(t *testing.T, fc *file.Client, off int64, s string) {
	t.Helper()

	_, err := fc.UploadRange(ctx(t), off, newReadSeekCloser(s), nil)
	require.NoError(t, err)
}

func newFile(t *testing.T, sc *share.Client, path string, size int64) *file.Client {
	t.Helper()

	var fc *file.Client

	dir, name := splitLast(path)
	if dir == "" {
		fc = sc.NewRootDirectoryClient().NewFileClient(name)
	} else {
		fc = sc.NewDirectoryClient(dir).NewFileClient(name)
	}

	_, err := fc.Create(ctx(t), size, nil)
	require.NoError(t, err)

	return fc
}

func splitLast(p string) (string, string) {
	if before, after, ok := strings.CutLast(p, "/"); ok {
		return before, after
	}

	return "", p
}

func TestSDK_FileCreateAndProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "create get", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("doc.txt")
			created, err := fc.Create(ctx(t), 2048, &file.CreateOptions{
				HTTPHeaders: &file.HTTPHeaders{
					ContentType: new("text/plain"), ContentEncoding: new("gzip"), ContentLanguage: new("en"),
					CacheControl: new("no-cache"), ContentDisposition: new("inline"),
				},
				Metadata: map[string]*string{"Tag": new("a")},
			})
			require.NoError(t, err)
			assert.Equal(t, "Archive", *created.FileAttributes)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int64(2048), *props.ContentLength)
			assert.Equal(t, "text/plain", *props.ContentType)
			assert.Equal(t, "gzip", *props.ContentEncoding)
			assert.Equal(t, "en", *props.ContentLanguage)
			assert.Equal(t, "no-cache", *props.CacheControl)
			assert.Equal(t, "inline", *props.ContentDisposition)
			assert.Equal(t, "a", md(props.Metadata, "tag"))
			assert.Equal(t, "File", *props.FileType)
			assert.Equal(t, *created.ETag, *props.ETag)
		}},
		{name: "default content type", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "plain.bin", 1)
			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "application/octet-stream", *props.ContentType)
		}},
		{name: "parent missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("nodir").NewFileClient("f").Create(ctx(t), 1, nil)
			requireCode(t, err, fileerror.ParentNotFound)
		}},
		{name: "overwrite resets content", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "over.txt", 5)
			upload(t, fc, 0, "hello")

			_, err := fc.Create(ctx(t), 3, nil)
			require.NoError(t, err)
			assert.Equal(t, "\x00\x00\x00", downloadAll(t, fc))
		}},
		{name: "directory in file slot", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("adir").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewRootDirectoryClient().NewFileClient("adir").Create(ctx(t), 1, nil)
			requireCode(t, err, fileerror.ResourceTypeMismatch)

			_, err = sc.NewRootDirectoryClient().NewFileClient("adir").GetProperties(ctx(t), nil)
			require.Error(t, err)
		}},
		{name: "missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("ghost")
			_, err := fc.GetProperties(ctx(t), nil)
			require.Error(t, err)
			assert.True(t, fileerror.HasCode(err, fileerror.ResourceNotFound) || strings.Contains(err.Error(), "404"))

			_, err = fc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)

			_, err = fc.DownloadStream(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "set http headers replaces and resizes", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "h.txt", 10)
			upload(t, fc, 0, "0123456789")

			_, err := fc.SetHTTPHeaders(ctx(t), &file.SetHTTPHeadersOptions{
				FileContentLength: new(int64(4)),
				HTTPHeaders:       &file.HTTPHeaders{ContentType: new("text/csv")},
			})
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int64(4), *props.ContentLength)
			assert.Equal(t, "text/csv", *props.ContentType)
			assert.Equal(t, "0123", downloadAll(t, fc))

			_, err = fc.Resize(ctx(t), 6, nil)
			require.NoError(t, err)
			assert.Equal(t, "0123\x00\x00", downloadAll(t, fc))
		}},
		{name: "set metadata", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "m.txt", 1)
			_, err := fc.SetMetadata(ctx(t), &file.SetMetadataOptions{Metadata: map[string]*string{"X": new("y")}})
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "y", md(props.Metadata, "x"))
		}},
		{name: "smb properties and permission", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			created := time.Date(2021, 2, 3, 4, 5, 6, 0, time.UTC)
			fc := sc.NewRootDirectoryClient().NewFileClient("smb.txt")
			_, err := fc.Create(ctx(t), 1, &file.CreateOptions{
				SMBProperties: &file.SMBProperties{
					Attributes: &file.NTFSFileAttributes{ReadOnly: true, Archive: true}, CreationTime: &created,
				},
				Permissions: &file.Permissions{Permission: new("O:S-1-5-32-544G:S-1-5-32-544D:(A;;FA;;;SY)")},
			})
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "ReadOnly|Archive", *props.FileAttributes)
			assert.True(t, created.Equal(*props.FileCreationTime))
			assert.NotEmpty(t, props.FilePermissionKey)
			assert.NotEmpty(t, props.ID)
			assert.NotEmpty(t, props.ParentID)

			_, err = fc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ReadOnlyAttribute)

			_, err = fc.SetHTTPHeaders(ctx(t), &file.SetHTTPHeadersOptions{
				SMBProperties: &file.SMBProperties{Attributes: &file.NTFSFileAttributes{None: true}},
			})
			require.NoError(t, err)

			_, err = fc.Delete(ctx(t), nil)
			require.NoError(t, err)
		}},
		{name: "invalid attributes", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewRootDirectoryClient().NewFileClient("bad.txt").Create(ctx(t), 1, &file.CreateOptions{
				SMBProperties: &file.SMBProperties{Attributes: &file.NTFSFileAttributes{Directory: true}},
			})
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
		{name: "delete", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "gone.txt", 1)
			_, err := fc.Delete(ctx(t), nil)
			require.NoError(t, err)

			_, err = fc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "invalid size", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewRootDirectoryClient().NewFileClient("big").Create(ctx(t), int64(5)<<40, nil)
			requireCode(t, err, fileerror.InvalidHeaderValue)
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

func TestSDK_FileRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "upload download range", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "r.txt", 20)
			upload(t, fc, 5, "hello")

			resp, err := fc.DownloadStream(
				ctx(t),
				&file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 5, Count: 5}},
			)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(body))
			assert.Equal(t, int64(5), *resp.ContentLength)
			assert.Equal(t, "bytes 5-9/20", *resp.ContentRange)
		}},
		{name: "full download zero filled", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "z.txt", 6)
			upload(t, fc, 2, "ab")
			assert.Equal(t, "\x00\x00ab\x00\x00", downloadAll(t, fc))
		}},
		{name: "range md5", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "md5.txt", 10)
			upload(t, fc, 0, "0123456789")

			resp, err := fc.DownloadStream(ctx(t), &file.DownloadStreamOptions{
				Range: file.HTTPRange{Offset: 2, Count: 4}, RangeGetContentMD5: new(true),
			})
			require.NoError(t, err)

			defer resp.Body.Close()

			sum := md5.Sum([]byte("2345"))
			assert.Equal(t, sum[:], resp.ContentMD5)
		}},
		{name: "range md5 needs range", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "md5b.txt", 4)
			upload(t, fc, 0, "abcd")

			_, err := fc.DownloadStream(ctx(t), &file.DownloadStreamOptions{RangeGetContentMD5: new(true)})
			require.Error(t, err)
		}},
		{name: "past end", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "end.txt", 4)
			upload(t, fc, 0, "abcd")

			_, err := fc.DownloadStream(
				ctx(t),
				&file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 10, Count: 2}},
			)
			requireCode(t, err, fileerror.InvalidRange)

			_, err = fc.UploadRange(ctx(t), 2, newReadSeekCloser("toolong"), nil)
			requireCode(t, err, fileerror.InvalidRange)
		}},
		{name: "clear range", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "clr.txt", 8)
			upload(t, fc, 0, "abcdefgh")

			_, err := fc.ClearRange(ctx(t), file.HTTPRange{Offset: 2, Count: 4}, nil)
			require.NoError(t, err)
			assert.Equal(t, "ab\x00\x00\x00\x00gh", downloadAll(t, fc))

			ranges, err := fc.GetRangeList(ctx(t), nil)
			require.NoError(t, err)
			require.Len(t, ranges.Ranges, 2)
			assert.Equal(t, [2]int64{0, 1}, [2]int64{*ranges.Ranges[0].Start, *ranges.Ranges[0].End})
			assert.Equal(t, [2]int64{6, 7}, [2]int64{*ranges.Ranges[1].Start, *ranges.Ranges[1].End})
			assert.Equal(t, int64(8), *ranges.FileContentLength)
		}},
		{name: "md5 mismatch", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "bad.md5", 4)
			bogus := md5.Sum([]byte("other"))

			_, err := fc.UploadRange(ctx(t), 0, newReadSeekCloser("abcd"), &file.UploadRangeOptions{
				TransactionalValidation: file.TransferValidationTypeMD5(bogus[:]),
			})
			requireCode(t, err, fileerror.MD5Mismatch)
		}},
		{name: "upload returns md5", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "ok.md5", 4)
			sum := md5.Sum([]byte("abcd"))

			resp, err := fc.UploadRange(ctx(t), 0, newReadSeekCloser("abcd"), &file.UploadRangeOptions{
				TransactionalValidation: file.TransferValidationTypeMD5(sum[:]),
			})
			require.NoError(t, err)
			assert.Equal(t, sum[:], resp.ContentMD5)
		}},
		{name: "range list window", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "win.bin", 100)
			upload(t, fc, 10, "aaaa")
			upload(t, fc, 50, "bbbb")

			ranges, err := fc.GetRangeList(
				ctx(t),
				&file.GetRangeListOptions{Range: file.HTTPRange{Offset: 40, Count: 30}},
			)
			require.NoError(t, err)
			require.Len(t, ranges.Ranges, 1)
			assert.Equal(t, int64(50), *ranges.Ranges[0].Start)
		}},
		{name: "adjacent writes merge", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := newFile(t, sc, "merge.bin", 12)
			upload(t, fc, 0, "aaaa")
			upload(t, fc, 8, "cccc")
			upload(t, fc, 4, "bbbb")

			ranges, err := fc.GetRangeList(ctx(t), nil)
			require.NoError(t, err)
			require.Len(t, ranges.Ranges, 1)
			assert.Equal(t, "aaaabbbbcccc", downloadAll(t, fc))
		}},
		{name: "large sparse file", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			const size = int64(1) << 40

			fc := newFile(t, sc, "sparse.bin", size)
			upload(t, fc, size-4, "tail")

			resp, err := fc.DownloadStream(
				ctx(t),
				&file.DownloadStreamOptions{Range: file.HTTPRange{Offset: size - 4, Count: 4}},
			)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "tail", string(body))

			stats, err := sc.GetStatistics(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, size, *stats.ShareUsageBytes)
		}},
		{name: "streamed download beyond range cap", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			const size = int64(9) << 20

			fc := newFile(t, sc, "stream.bin", size)
			upload(t, fc, 4<<20, "middle")

			resp, err := fc.DownloadStream(ctx(t), nil)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, size, *resp.ContentLength)

			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Len(t, data, int(size))
			assert.Equal(t, "middle", string(data[4<<20:4<<20+6]))
			assert.Zero(t, data[0])
			assert.Zero(t, data[size-1])
		}},
		{name: "upload buffer and download buffer", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("big.bin")
			_, err := fc.Create(ctx(t), 9<<20, nil)
			require.NoError(t, err)

			payload := bytes.Repeat([]byte("0123456789abcdef"), (9<<20)/16)
			require.NoError(t, fc.UploadBuffer(ctx(t), payload, &file.UploadBufferOptions{ChunkSize: 4 << 20}))

			out := make([]byte, len(payload))
			n, err := fc.DownloadBuffer(ctx(t), out, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(len(payload)), n)
			assert.True(t, bytes.Equal(payload, out))
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

func TestSDK_RangeListDiff(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	fc := newFile(t, sc, "diff.bin", 30)
	upload(t, fc, 0, "aaaaaaaaaa")
	upload(t, fc, 20, "cccccccccc")

	first, err := sc.CreateSnapshot(ctx(t), nil)
	require.NoError(t, err)

	upload(t, fc, 2, "XX")
	_, err = fc.ClearRange(ctx(t), file.HTTPRange{Offset: 20, Count: 10}, nil)
	require.NoError(t, err)
	upload(t, fc, 12, "dd")

	ranges, err := fc.GetRangeList(ctx(t), &file.GetRangeListOptions{PrevShareSnapshot: first.Snapshot})
	require.NoError(t, err)

	changed := make([][2]int64, 0, len(ranges.Ranges))
	cleared := make([][2]int64, 0, len(ranges.ClearRanges))

	for _, r := range ranges.Ranges {
		changed = append(changed, [2]int64{*r.Start, *r.End})
	}

	for _, r := range ranges.ClearRanges {
		cleared = append(cleared, [2]int64{*r.Start, *r.End})
	}

	assert.Equal(t, [][2]int64{{2, 3}, {12, 13}}, changed)
	assert.Equal(t, [][2]int64{{20, 29}}, cleared)

	_, err = fc.GetRangeList(ctx(t), &file.GetRangeListOptions{PrevShareSnapshot: new("2000-01-01T00:00:00.0000000Z")})
	requireCode(t, err, fileerror.ShareNotFound)
}

func TestSDK_FileLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, fc *file.Client, lc *lease.FileClient)
		name  string
	}{
		{name: "guards writes", check: func(t *testing.T, fc *file.Client, lc *lease.FileClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeLeased, *props.LeaseState)
			assert.Equal(t, lease.StatusTypeLocked, *props.LeaseStatus)
			assert.Equal(t, lease.DurationTypeInfinite, *props.LeaseDuration)

			_, err = fc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))

			_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("x"), nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))

			_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("x"), &file.UploadRangeOptions{
				LeaseAccessConditions: &file.LeaseAccessConditions{LeaseID: lc.LeaseID()},
			})
			require.NoError(t, err)

			_, err = fc.Delete(
				ctx(t),
				&file.DeleteOptions{LeaseAccessConditions: &file.LeaseAccessConditions{LeaseID: lc.LeaseID()}},
			)
			require.NoError(t, err)
		}},
		{name: "wrong id", check: func(t *testing.T, fc *file.Client, lc *lease.FileClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			_, err = fc.SetMetadata(
				ctx(t),
				&file.SetMetadataOptions{LeaseAccessConditions: &file.LeaseAccessConditions{LeaseID: new("nope")}},
			)
			requireCode(t, err, fileerror.Code("LeaseIdMismatchWithFileOperation"))
		}},
		{name: "lease id without lease", check: func(t *testing.T, fc *file.Client, _ *lease.FileClient) {
			t.Helper()

			_, err := fc.SetMetadata(
				ctx(t),
				&file.SetMetadataOptions{LeaseAccessConditions: &file.LeaseAccessConditions{LeaseID: new("nope")}},
			)
			requireCode(t, err, fileerror.Code("LeaseNotPresentWithFileOperation"))
		}},
		{name: "change break release", check: func(t *testing.T, fc *file.Client, lc *lease.FileClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			_, err = lc.Change(ctx(t), "7f0f3a4e-0000-4000-8000-000000000003", nil)
			require.NoError(t, err)

			_, err = lc.Break(ctx(t), nil)
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeBroken, *props.LeaseState)

			_, err = fc.SetMetadata(ctx(t), nil)
			require.NoError(t, err, "a broken lease no longer guards the file")

			_, err = lc.Release(ctx(t), nil)
			require.NoError(t, err)
		}},
		{name: "already leased", check: func(t *testing.T, fc *file.Client, lc *lease.FileClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			other, err := lease.NewFileClient(fc, nil)
			require.NoError(t, err)
			_, err = other.Acquire(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseAlreadyPresent"))
		}},
		{name: "recreate keeps lease", check: func(t *testing.T, fc *file.Client, lc *lease.FileClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			_, err = fc.Create(ctx(t), 4, nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))

			_, err = fc.Create(
				ctx(t),
				4,
				&file.CreateOptions{LeaseAccessConditions: &file.LeaseAccessConditions{LeaseID: lc.LeaseID()}},
			)
			require.NoError(t, err)

			props, err := fc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeLeased, *props.LeaseState)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			fc := newFile(t, sc, "leased.txt", 4)

			lc, err := lease.NewFileClient(fc, nil)
			require.NoError(t, err)

			tt.check(t, fc, lc)
		})
	}
}

func TestSDK_FileRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "moves content and properties", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("dst").Create(ctx(t), nil)
			require.NoError(t, err)

			src := newFile(t, sc, "src.txt", 5)
			upload(t, src, 0, "hello")

			_, err = src.Rename(ctx(t), "dst/renamed.txt", nil)
			require.NoError(t, err)

			_, err = src.GetProperties(ctx(t), nil)
			require.Error(t, err)

			dst := sc.NewDirectoryClient("dst").NewFileClient("renamed.txt")
			assert.Equal(t, "hello", downloadAll(t, dst))
		}},
		{name: "exists without replace", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			a := newFile(t, sc, "a.txt", 1)
			newFile(t, sc, "b.txt", 1)

			_, err := a.Rename(ctx(t), "b.txt", nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)
		}},
		{name: "replace if exists", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			a := newFile(t, sc, "a.txt", 3)
			upload(t, a, 0, "new")
			b := newFile(t, sc, "b.txt", 3)
			upload(t, b, 0, "old")

			_, err := a.Rename(ctx(t), "b.txt", &file.RenameOptions{ReplaceIfExists: new(true)})
			require.NoError(t, err)
			assert.Equal(t, "new", downloadAll(t, b))
		}},
		{name: "readonly destination", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			a := newFile(t, sc, "a.txt", 1)
			b := sc.NewRootDirectoryClient().NewFileClient("b.txt")
			_, err := b.Create(
				ctx(t),
				1,
				&file.CreateOptions{
					SMBProperties: &file.SMBProperties{Attributes: &file.NTFSFileAttributes{ReadOnly: true}},
				},
			)
			require.NoError(t, err)

			_, err = a.Rename(ctx(t), "b.txt", &file.RenameOptions{ReplaceIfExists: new(true)})
			requireCode(t, err, fileerror.ReadOnlyAttribute)

			a2 := newFile(t, sc, "a2.txt", 1)
			_, err = a2.Rename(
				ctx(t),
				"b.txt",
				&file.RenameOptions{ReplaceIfExists: new(true), IgnoreReadOnly: new(true)},
			)
			require.NoError(t, err)
		}},
		{name: "directory source type mismatch", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("adir").Create(ctx(t), nil)
			require.NoError(t, err)

			fcDir := sc.NewRootDirectoryClient().NewFileClient("adir")
			_, err = fcDir.Rename(ctx(t), "other", nil)
			requireCode(t, err, fileerror.ResourceTypeMismatch)
		}},
		{name: "leased source", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			a := newFile(t, sc, "a.txt", 1)
			lc, err := lease.NewFileClient(a, nil)
			require.NoError(t, err)
			_, err = lc.Acquire(ctx(t), nil)
			require.NoError(t, err)

			_, err = a.Rename(ctx(t), "moved.txt", nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))

			_, err = a.Rename(
				ctx(t),
				"moved.txt",
				&file.RenameOptions{
					SourceLeaseAccessConditions: &file.SourceLeaseAccessConditions{SourceLeaseID: lc.LeaseID()},
				},
			)
			require.NoError(t, err)
		}},
		{name: "overrides metadata and content type", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			a := newFile(t, sc, "a.txt", 1)
			_, err := a.Rename(ctx(t), "b.txt", &file.RenameOptions{
				Metadata: map[string]*string{"N": new("1")}, ContentType: new("text/x"),
			})
			require.NoError(t, err)

			props, err := sc.NewRootDirectoryClient().NewFileClient("b.txt").GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "1", md(props.Metadata, "n"))
			assert.Equal(t, "text/x", *props.ContentType)
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

func TestSDK_FileHandles(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	fc := newFile(t, sc, "h.txt", 1)

	handles, err := fc.ListHandles(ctx(t), nil)
	require.NoError(t, err)
	assert.Empty(t, handles.Handles)

	closed, err := fc.ForceCloseHandles(ctx(t), "*", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(0), *closed.NumberOfHandlesClosed)

	_, err = sc.NewRootDirectoryClient().NewFileClient("nope").ListHandles(ctx(t), nil)
	requireCode(t, err, fileerror.ResourceNotFound)
}
