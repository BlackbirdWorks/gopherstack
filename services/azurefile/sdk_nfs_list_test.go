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

func listRoot(t *testing.T, sc *share.Client) directory.ListFilesAndDirectoriesSegmentResponse {
	t.Helper()

	pager := sc.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(&directory.ListFilesAndDirectoriesOptions{
		Include: directory.ListFilesInclude{
			Timestamps:    true,
			ETag:          true,
			Attributes:    true,
			PermissionKey: true,
		},
		IncludeExtendedInfo: new(true),
	})

	page, err := pager.NextPage(ctx(t))
	require.NoError(t, err)

	return page.ListFilesAndDirectoriesSegmentResponse
}

func TestSDK_NFSListAndRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "list omits smb properties", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			root := sc.NewRootDirectoryClient()
			_, err := root.NewSubdirectoryClient("d").Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = root.NewFileClient("f").Create(ctx(t), 5, nil)
			require.NoError(t, err)

			seg := listRoot(t, sc)
			require.Len(t, seg.Segment.Files, 1)
			require.Len(t, seg.Segment.Directories, 1)

			f, d := seg.Segment.Files[0], seg.Segment.Directories[0]
			assert.NotEmpty(t, *f.ID)
			assert.NotEmpty(t, *d.ID)
			assert.EqualValues(t, 5, *f.Properties.ContentLength)
			assert.Nil(t, f.Properties.ETag)
			assert.Nil(t, f.Properties.CreationTime)
			assert.Nil(t, f.Attributes)
			assert.Nil(t, f.PermissionKey)
			assert.Nil(t, d.Properties)
			assert.Nil(t, d.Attributes)
		}},
		{name: "rename refused", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			root := sc.NewRootDirectoryClient()
			fc := root.NewFileClient("f")
			_, err := fc.Create(ctx(t), 5, nil)
			require.NoError(t, err)

			_, err = fc.Rename(ctx(t), "g", nil)
			requireCode(t, err, fileerror.InvalidURI)

			_, err = root.NewSubdirectoryClient("d").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = root.NewSubdirectoryClient("d").Rename(ctx(t), "e", nil)
			requireCode(t, err, fileerror.InvalidURI)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.check(t, newNFSShare(t))
		})
	}
}

func TestSDK_SMBListAlwaysReturnsIDs(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	_, err := sc.NewRootDirectoryClient().NewFileClient("f").Create(ctx(t), 1, &file.CreateOptions{})
	require.NoError(t, err)

	pager := sc.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil)
	page, err := pager.NextPage(ctx(t))
	require.NoError(t, err)
	require.Len(t, page.Segment.Files, 1)
	assert.NotEmpty(t, *page.Segment.Files[0].ID)
	assert.NotEmpty(t, *page.DirectoryID)
}

func TestSDK_RenameRefusedOnShareSnapshot(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	fc := newFile(t, sc, "a", 4)
	snap, err := sc.CreateSnapshot(ctx(t), nil)
	require.NoError(t, err)

	sn, err := sc.WithSnapshot(*snap.Snapshot)
	require.NoError(t, err)

	_, err = sn.NewRootDirectoryClient().NewFileClient("a").Rename(ctx(t), "b", nil)
	requireCode(t, err, fileerror.InvalidQueryParameterValue)
	_ = fc
}
