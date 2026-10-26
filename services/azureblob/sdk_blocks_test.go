package azureblob_test

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func blockID(n int) string {
	return base64.StdEncoding.EncodeToString([]byte("block-" + strconv.Itoa(1000+n)))
}

func downloadAll(t *testing.T, bc *blob.Client) string {
	t.Helper()

	resp, err := bc.DownloadStream(t.Context(), nil)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestSDK_BlockBlob_StageAndCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		listType      blockblob.BlockListType
		wantCommitted int
		wantStaged    int
	}{
		{name: "committed", listType: blockblob.BlockListTypeCommitted, wantCommitted: 2},
		{name: "uncommitted", listType: blockblob.BlockListTypeUncommitted, wantStaged: 1},
		{name: "all", listType: blockblob.BlockListTypeAll, wantCommitted: 2, wantStaged: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			bb := env.cc.NewBlockBlobClient("big.bin")

			for i, data := range []string{"hello ", "world", "!!"} {
				_, err := bb.StageBlock(ctx, blockID(i), body(data), nil)
				require.NoError(t, err)
			}

			_, err := bb.CommitBlockList(ctx, []string{blockID(0), blockID(1)}, &blockblob.CommitBlockListOptions{
				HTTPHeaders: &blob.HTTPHeaders{BlobContentType: new("text/plain")},
				Metadata:    map[string]*string{"k": new("v")},
			})
			require.NoError(t, err)
			assert.Equal(t, "hello world", downloadAll(t, bb.BlobClient()))

			_, err = bb.StageBlock(ctx, blockID(5), body("xyz"), nil)
			require.NoError(t, err)

			list, err := bb.GetBlockList(ctx, tt.listType, nil)
			require.NoError(t, err)
			require.Len(t, list.CommittedBlocks, tt.wantCommitted)
			require.Len(t, list.UncommittedBlocks, tt.wantStaged)

			if tt.wantCommitted > 0 {
				assert.Equal(t, blockID(0), *list.CommittedBlocks[0].Name)
				assert.EqualValues(t, 6, *list.CommittedBlocks[0].Size)
				assert.EqualValues(t, 11, *list.BlobContentLength)
			}

			if tt.wantStaged > 0 {
				assert.Equal(t, blockID(5), *list.UncommittedBlocks[0].Name)
				assert.EqualValues(t, 3, *list.UncommittedBlocks[0].Size)
			}

			props, err := bb.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, "text/plain", *props.ContentType)
			assert.Equal(t, "v", *props.Metadata["K"])
		})
	}
}

func TestSDK_BlockBlob_CommitSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ids      func() []string
		wantCode bloberror.Code
		want     string
	}{
		{
			name: "latest_prefers_uncommitted",
			ids:  func() []string { return nil },
			want: "new",
		},
		{
			name:     "missing_block",
			ids:      func() []string { return []string{blockID(9)} },
			wantCode: bloberror.InvalidBlockList,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			bb := env.cc.NewBlockBlobClient("c.bin")

			_, err := bb.StageBlock(ctx, blockID(0), body("AA"), nil)
			require.NoError(t, err)
			_, err = bb.CommitBlockList(ctx, []string{blockID(0)}, nil)
			require.NoError(t, err)

			_, err = bb.StageBlock(ctx, blockID(0), body("new"), nil)
			require.NoError(t, err)

			if tt.wantCode != "" {
				_, err = bb.CommitBlockList(ctx, tt.ids(), nil)
				requireStorageErr(t, err, tt.wantCode, 400)

				return
			}

			_, err = bb.CommitBlockList(ctx, []string{blockID(0)}, nil)
			require.NoError(t, err)
			assert.Equal(t, "new", downloadAll(t, bb.BlobClient()))
		})
	}
}

func TestSDK_BlockBlob_CommittedReuse(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	ctx := t.Context()
	bb := env.cc.NewBlockBlobClient("reuse.bin")

	for i, data := range []string{"one", "two"} {
		_, err := bb.StageBlock(ctx, blockID(i), body(data), nil)
		require.NoError(t, err)
	}

	_, err := bb.CommitBlockList(ctx, []string{blockID(0), blockID(1)}, nil)
	require.NoError(t, err)

	_, err = bb.StageBlock(ctx, blockID(2), body("three"), nil)
	require.NoError(t, err)

	// SDK CommitBlockList emits Latest for every id: committed 1, latest 3, repeated 0.
	_, err = bb.CommitBlockList(ctx, []string{blockID(1), blockID(2), blockID(0), blockID(0)}, nil)
	require.NoError(t, err)
	assert.Equal(t, "twothreeoneone", downloadAll(t, bb.BlobClient()))

	list, err := bb.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
	require.NoError(t, err)
	assert.Len(t, list.CommittedBlocks, 4)
	assert.Empty(t, list.UncommittedBlocks, "uncommitted blocks are discarded by a commit")
}

func TestSDK_BlockBlob_BlockIDRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		first    string
		second   string
		wantCode bloberror.Code
		status   int
	}{
		{name: "equal_length", first: "AAAA", second: "BBBB"},
		{
			name:     "unequal_length",
			first:    "AAAA",
			second:   "AAAAAAAA",
			wantCode: bloberror.InvalidBlobOrBlock,
			status:   400,
		},
		{
			name:     "not_base64",
			first:    "AAAA",
			second:   "!!notbase64",
			wantCode: bloberror.InvalidQueryParameterValue,
			status:   400,
		},
		{name: "too_long", first: base64.StdEncoding.EncodeToString(make([]byte, 65)), second: "",
			wantCode: bloberror.InvalidQueryParameterValue, status: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			bb := env.cc.NewBlockBlobClient("ids.bin")

			_, err := bb.StageBlock(ctx, tt.first, body("a"), nil)
			if tt.second == "" {
				requireStorageErr(t, err, tt.wantCode, tt.status)

				return
			}

			require.NoError(t, err)

			_, err = bb.StageBlock(ctx, tt.second, body("b"), nil)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, tt.wantCode, tt.status)
		})
	}
}

func TestSDK_BlockBlob_UncommittedExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		want    int
	}{
		{name: "just_under_7d", advance: azureblob.UncommittedBlockTTL - time.Minute, want: 1},
		{name: "at_7d", advance: azureblob.UncommittedBlockTTL, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			bb := env.cc.NewBlockBlobClient("gc.bin")

			_, err := bb.StageBlock(ctx, blockID(0), body("x"), nil)
			require.NoError(t, err)

			env.clock.Advance(tt.advance)

			list, err := bb.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
			if tt.want == 0 {
				requireStorageErr(t, err, bloberror.BlobNotFound, 404)

				_, err = bb.CommitBlockList(ctx, []string{blockID(0)}, nil)
				requireStorageErr(t, err, bloberror.InvalidBlockList, 400)

				return
			}

			require.NoError(t, err)
			assert.Len(t, list.UncommittedBlocks, tt.want)
		})
	}
}

func TestSDK_BlockBlob_GetBlockListMissingBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		listType blockblob.BlockListType
		staged   bool
		wantErr  bool
	}{
		{name: "committed_missing", listType: blockblob.BlockListTypeCommitted, staged: true, wantErr: true},
		{name: "uncommitted_staged", listType: blockblob.BlockListTypeUncommitted, staged: true},
		{name: "uncommitted_nothing", listType: blockblob.BlockListTypeUncommitted, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			bb := env.cc.NewBlockBlobClient("nope.bin")

			if tt.staged {
				_, err := bb.StageBlock(ctx, blockID(0), body("x"), nil)
				require.NoError(t, err)
			}

			list, err := bb.GetBlockList(ctx, tt.listType, nil)
			if tt.wantErr {
				requireStorageErr(t, err, bloberror.BlobNotFound, 404)

				return
			}

			require.NoError(t, err)
			assert.Len(t, list.UncommittedBlocks, 1)

			_, err = bb.GetProperties(ctx, nil)
			requireStatus(t, err, 404)
		})
	}
}

func TestSDK_BlockBlob_TransactionalValidation(t *testing.T) {
	t.Parallel()

	sum := md5.Sum([]byte("payload"))
	bad := md5.Sum([]byte("other"))

	tests := []struct {
		name     string
		validate blob.TransferValidationType
		wantCode bloberror.Code
	}{
		{name: "md5_ok", validate: blob.TransferValidationTypeMD5(sum[:])},
		{name: "md5_bad", validate: blob.TransferValidationTypeMD5(bad[:]), wantCode: bloberror.MD5Mismatch},
		{name: "crc64_ok", validate: blob.TransferValidationTypeComputeCRC64()},
		{name: "crc64_bad", validate: blob.TransferValidationTypeCRC64(12345), wantCode: "Crc64Mismatch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb := env.cc.NewBlockBlobClient("v.bin")

			resp, err := bb.StageBlock(t.Context(), blockID(0), body("payload"),
				&blockblob.StageBlockOptions{TransactionalValidation: tt.validate})
			if tt.wantCode != "" {
				requireStorageErr(t, err, tt.wantCode, 400)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, sum[:], resp.ContentMD5)
			assert.NotEmpty(t, resp.ContentCRC64)
		})
	}
}

func TestSDK_BlockBlob_StageBlockFromURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src      func(env *sdkEnv) string
		name     string
		want     string
		wantCode bloberror.Code
		srcRange blob.HTTPRange
		status   int
	}{
		{name: "whole", want: "0123456789"},
		{name: "range", srcRange: blob.HTTPRange{Offset: 2, Count: 4}, want: "2345"},
		{
			name: "foreign_account", wantCode: "CannotVerifyCopySource", status: 404,
			src: func(env *sdkEnv) string { return env.url + "-other/sdkenv/src.bin" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			ctx := t.Context()
			src := env.cc.NewBlockBlobClient("src.bin")
			_, err := src.UploadBuffer(ctx, []byte("0123456789"), nil)
			require.NoError(t, err)

			srcURL := src.URL()
			if tt.src != nil {
				srcURL = tt.src(env)
			}

			dst := env.cc.NewBlockBlobClient("dst.bin")

			_, err = dst.StageBlockFromURL(
				ctx,
				blockID(0),
				srcURL,
				&blockblob.StageBlockFromURLOptions{Range: tt.srcRange},
			)
			if tt.wantCode != "" {
				requireStorageErr(t, err, tt.wantCode, tt.status)

				return
			}

			require.NoError(t, err)
			_, err = dst.CommitBlockList(ctx, []string{blockID(0)}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, downloadAll(t, dst.BlobClient()))
		})
	}
}

func TestBackend_BlockStatePersists(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	ctx := t.Context()
	bb := env.cc.NewBlockBlobClient("p.bin")

	_, err := bb.StageBlock(ctx, blockID(0), body("abc"), nil)
	require.NoError(t, err)
	_, err = bb.CommitBlockList(ctx, []string{blockID(0)}, nil)
	require.NoError(t, err)
	_, err = bb.StageBlock(ctx, blockID(1), body("def"), nil)
	require.NoError(t, err)

	snap := env.backend.Snapshot(ctx)

	restored := azureblob.NewInMemoryBackendWithClock(env.clock.Now)
	require.NoError(t, restored.Restore(ctx, snap))

	list, err := restored.GetBlockList("sdkenv", "p.bin", "", azureblob.BlockListAll)
	require.NoError(t, err)
	require.Len(t, list.Committed, 1)
	require.Len(t, list.Uncommitted, 1)
	assert.Equal(t, blockID(1), list.Uncommitted[0].Name)

	_, err = restored.CommitBlockList("sdkenv", "p.bin", []azureblob.BlockRef{
		{ID: blockID(0), Kind: azureblob.BlockKindCommitted}, {ID: blockID(1), Kind: azureblob.BlockKindUncommitted},
	}, azureblob.BlobAttrs{})
	require.NoError(t, err)

	_, data, err := restored.GetBlobAt("sdkenv", "p.bin", "")
	require.NoError(t, err)
	assert.Equal(t, "abcdef", string(data))
}
