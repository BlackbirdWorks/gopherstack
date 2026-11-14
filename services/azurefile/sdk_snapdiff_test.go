package azurefile_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_RangeListDiffIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, sc *share.Client) *file.Client
		opts   func(prev string) *file.GetRangeListOptions
		code   fileerror.Code
		name   string
		ranges int
	}{
		{
			name: "renamed with support", ranges: 1,
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				renamed, err := sc.NewRootDirectoryClient().NewFileClient("a").Rename(ctx(t), "b", nil)
				require.NoError(t, err)
				_ = renamed
				fc := sc.NewRootDirectoryClient().NewFileClient("b")
				upload(t, fc, 0, "zz")

				return fc
			},
			opts: func(prev string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{PrevShareSnapshot: &prev, SupportRename: new(true)}
			},
		},
		{
			name: "renamed without support", code: "FileChangedSincePreviousSnapshot",
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				_, err := sc.NewRootDirectoryClient().NewFileClient("a").Rename(ctx(t), "b", nil)
				require.NoError(t, err)

				return sc.NewRootDirectoryClient().NewFileClient("b")
			},
			opts: func(prev string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{PrevShareSnapshot: &prev}
			},
		},
		{
			name: "deleted and recreated", code: "FileChangedSincePreviousSnapshot",
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				fc := sc.NewRootDirectoryClient().NewFileClient("a")
				_, err := fc.Delete(ctx(t), nil)
				require.NoError(t, err)
				_, err = fc.Create(ctx(t), 10, nil)
				require.NoError(t, err)

				return fc
			},
			opts: func(prev string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{PrevShareSnapshot: &prev}
			},
		},
		{
			name: "overwritten by create", code: "FileChangedSincePreviousSnapshot",
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				fc := sc.NewRootDirectoryClient().NewFileClient("a")
				_, err := fc.Create(ctx(t), 10, nil)
				require.NoError(t, err)

				return fc
			},
			opts: func(prev string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{PrevShareSnapshot: &prev}
			},
		},
		{
			name: "absent from snapshot", code: fileerror.ResourceNotFound,
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				return newFile(t, sc, "new.bin", 10)
			},
			opts: func(prev string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{PrevShareSnapshot: &prev}
			},
		},
		{
			name: "rename flag without prev", code: fileerror.InvalidHeaderValue,
			mutate: func(t *testing.T, sc *share.Client) *file.Client {
				t.Helper()

				return sc.NewRootDirectoryClient().NewFileClient("a")
			},
			opts: func(string) *file.GetRangeListOptions {
				return &file.GetRangeListOptions{SupportRename: new(true)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			fc := newFile(t, sc, "a", 10)
			upload(t, fc, 0, "aaaa")

			snap, err := sc.CreateSnapshot(ctx(t), nil)
			require.NoError(t, err)

			target := tt.mutate(t, sc)
			ranges, err := target.GetRangeList(ctx(t), tt.opts(*snap.Snapshot))

			if tt.code != "" {
				requireCode(t, err, tt.code)

				return
			}

			require.NoError(t, err)
			assert.Len(t, ranges.Ranges, tt.ranges)
		})
	}
}
