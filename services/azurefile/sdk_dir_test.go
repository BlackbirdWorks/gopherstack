package azurefile_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newShare(t *testing.T) *share.Client {
	t.Helper()

	e := newEnv(t)
	sc := e.svc.NewShareClient("dirshare")
	_, err := sc.Create(ctx(t), nil)
	require.NoError(t, err)

	return sc
}

func TestSDK_Directory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "create get delete", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("docs")
			created, err := dc.Create(
				ctx(t),
				&directory.CreateOptions{Metadata: map[string]*string{"Owner": new("me")}},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, created.ETag)
			assert.Equal(t, "Directory", *created.FileAttributes)
			assert.NotEmpty(t, created.FilePermissionKey)

			props, err := dc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "me", md(props.Metadata, "owner"))
			assert.Equal(t, *created.FileCreationTime, *props.FileCreationTime)
			assert.NotEmpty(t, props.ID)

			_, err = dc.Delete(ctx(t), nil)
			require.NoError(t, err)

			_, err = dc.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "duplicate", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("dup")
			_, err := dc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = dc.Create(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)
		}},
		{name: "case insensitive", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("MixedCase").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("mixedcase").GetProperties(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("MIXEDCASE").Create(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)
		}},
		{name: "parent missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("no/such/parent").Create(ctx(t), nil)
			requireCode(t, err, fileerror.ParentNotFound)
		}},
		{name: "nested", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("a").Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = sc.NewDirectoryClient("a/b").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("a/b").GetProperties(ctx(t), nil)
			require.NoError(t, err)
		}},
		{name: "invalid name", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("bad*name").Create(ctx(t), nil)
			requireCode(t, err, fileerror.InvalidFileOrDirectoryPathName)
		}},
		{name: "not empty", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("full")
			_, err := dc.Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = dc.NewFileClient("f.txt").Create(ctx(t), 1, nil)
			require.NoError(t, err)

			_, err = dc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.DirectoryNotEmpty)
		}},
		{name: "delete missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("ghost").Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "file in directory slot", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewRootDirectoryClient().NewFileClient("afile").Create(ctx(t), 1, nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("afile").GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceTypeMismatch)
		}},
		{name: "set metadata", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("meta")
			_, err := dc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = dc.SetMetadata(ctx(t), &directory.SetMetadataOptions{Metadata: map[string]*string{"K": new("V")}})
			require.NoError(t, err)

			props, err := dc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "V", md(props.Metadata, "k"))
		}},
		{name: "set properties smb", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("smb")
			_, err := dc.Create(ctx(t), nil)
			require.NoError(t, err)

			created := time.Date(2020, 5, 6, 7, 8, 9, 0, time.UTC)
			_, err = dc.SetProperties(ctx(t), &directory.SetPropertiesOptions{
				FileSMBProperties: &file.SMBProperties{
					Attributes:   &file.NTFSFileAttributes{Hidden: true},
					CreationTime: &created,
				},
				FilePermissions: &file.Permissions{Permission: new("O:S-1-5-32-544G:S-1-5-32-544D:(A;;FA;;;SY)")},
			})
			require.NoError(t, err)

			props, err := dc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "Hidden|Directory", *props.FileAttributes)
			assert.True(t, created.Equal(*props.FileCreationTime))

			perm, err := sc.GetPermission(ctx(t), *props.FilePermissionKey, nil)
			require.NoError(t, err)
			assert.Equal(t, "O:S-1-5-32-544G:S-1-5-32-544D:(A;;FA;;;SY)", *perm.Permission)
		}},
		{name: "permission and key conflict", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("both").Create(ctx(t), &directory.CreateOptions{
				FilePermissions: &file.Permissions{Permission: new("O:SY"), PermissionKey: new("k")},
			})
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
		{name: "root directory", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			root := sc.NewRootDirectoryClient()
			props, err := root.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "Directory", *props.FileAttributes)

			_, err = root.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.InvalidInput)
		}},
		{name: "handles are empty", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			dc := sc.NewDirectoryClient("h")
			_, err := dc.Create(ctx(t), nil)
			require.NoError(t, err)

			handles, err := dc.ListHandles(ctx(t), nil)
			require.NoError(t, err)
			assert.Empty(t, handles.Handles)

			closed, err := dc.ForceCloseHandles(ctx(t), "*", nil)
			require.NoError(t, err)
			assert.Equal(t, int32(0), *closed.NumberOfHandlesClosed)

			_, err = sc.NewDirectoryClient("absent").ForceCloseHandles(ctx(t), "*", nil)
			requireCode(t, err, fileerror.ResourceNotFound)
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

func TestSDK_DirectoryListing(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, sc *share.Client) {
		t.Helper()

		for _, d := range []string{"zdir", "adir"} {
			_, err := sc.NewDirectoryClient(d).Create(ctx(t), nil)
			require.NoError(t, err)
		}

		for i, f := range []string{"b.txt", "a.txt", "c.log"} {
			_, err := sc.NewRootDirectoryClient().NewFileClient(f).Create(ctx(t), int64(10*(i+1)), nil)
			require.NoError(t, err)
		}
	}

	tests := []struct {
		opts      *directory.ListFilesAndDirectoriesOptions
		extra     func(t *testing.T, dirs []*directory.Directory, files []*directory.File)
		name      string
		wantDirs  []string
		wantFiles []string
	}{
		{
			name: "directories first then sorted files", wantDirs: []string{"adir", "zdir"},
			wantFiles: []string{"a.txt", "b.txt", "c.log"},
		},
		{
			name: "prefix", opts: &directory.ListFilesAndDirectoriesOptions{Prefix: new("a")},
			wantDirs: []string{"adir"}, wantFiles: []string{"a.txt"},
		},
		{
			name: "paged", opts: &directory.ListFilesAndDirectoriesOptions{MaxResults: new(int32(2))},
			wantDirs: []string{"adir", "zdir"}, wantFiles: []string{"a.txt", "b.txt", "c.log"},
		},
		{
			name:      "content length present",
			wantDirs:  []string{"adir", "zdir"},
			wantFiles: []string{"a.txt", "b.txt", "c.log"},
			extra: func(t *testing.T, _ []*directory.Directory, files []*directory.File) {
				t.Helper()

				sizes := map[string]int64{}
				for _, f := range files {
					sizes[*f.Name] = *f.Properties.ContentLength
				}

				assert.Equal(t, map[string]int64{"a.txt": 20, "b.txt": 10, "c.log": 30}, sizes)
			},
		},
		{
			name: "include everything",
			opts: &directory.ListFilesAndDirectoriesOptions{
				Include: directory.ListFilesInclude{
					Timestamps:    true,
					ETag:          true,
					Attributes:    true,
					PermissionKey: true,
				},
				IncludeExtendedInfo: new(true),
			},
			wantDirs: []string{"adir", "zdir"}, wantFiles: []string{"a.txt", "b.txt", "c.log"},
			extra: func(t *testing.T, dirs []*directory.Directory, files []*directory.File) {
				t.Helper()

				assert.Equal(t, "Directory", *dirs[0].Attributes)
				assert.NotEmpty(t, dirs[0].ID)
				assert.NotEmpty(t, dirs[0].PermissionKey)
				assert.NotEmpty(t, files[0].Properties.ETag)
				assert.NotNil(t, files[0].Properties.CreationTime)
				assert.NotNil(t, files[0].Properties.LastWriteTime)
				assert.NotNil(t, files[0].Properties.LastModified)
				assert.Equal(t, "Archive", *files[0].Attributes)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			setup(t, sc)

			var (
				dirs  []*directory.Directory
				files []*directory.File
			)

			pager := sc.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(tt.opts)
			for pager.More() {
				page, err := pager.NextPage(ctx(t))
				require.NoError(t, err)

				dirs = append(dirs, page.Segment.Directories...)
				files = append(files, page.Segment.Files...)
			}

			gotDirs := make([]string, 0, len(dirs))
			gotFiles := make([]string, 0, len(files))
			for _, d := range dirs {
				gotDirs = append(gotDirs, *d.Name)
			}

			for _, f := range files {
				gotFiles = append(gotFiles, *f.Name)
			}

			assert.Equal(t, tt.wantDirs, gotDirs)
			assert.Equal(t, tt.wantFiles, gotFiles)

			if tt.extra != nil {
				tt.extra(t, dirs, files)
			}
		})
	}
}

func TestSDK_ListInSnapshotAndMissing(t *testing.T) {
	t.Parallel()

	sc := newShare(t)
	_, err := sc.NewRootDirectoryClient().NewFileClient("old.txt").Create(ctx(t), 1, nil)
	require.NoError(t, err)

	snap, err := sc.CreateSnapshot(ctx(t), nil)
	require.NoError(t, err)

	_, err = sc.NewRootDirectoryClient().NewFileClient("new.txt").Create(ctx(t), 1, nil)
	require.NoError(t, err)

	list := func(c *share.Client) []string {
		var names []string

		pager := c.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil)
		for pager.More() {
			page, pageErr := pager.NextPage(ctx(t))
			require.NoError(t, pageErr)

			for _, f := range page.Segment.Files {
				names = append(names, *f.Name)
			}
		}

		return names
	}

	snapShare, err := sc.WithSnapshot(*snap.Snapshot)
	require.NoError(t, err)

	assert.Equal(t, []string{"old.txt"}, list(snapShare))
	assert.Equal(t, []string{"new.txt", "old.txt"}, list(sc))

	pager := sc.NewDirectoryClient("missing").NewListFilesAndDirectoriesPager(nil)
	_, err = pager.NextPage(ctx(t))
	requireCode(t, err, fileerror.ResourceNotFound)
}

func TestSDK_DirectoryRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "moves tree", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			src := sc.NewDirectoryClient("src")
			_, err := src.Create(ctx(t), &directory.CreateOptions{Metadata: map[string]*string{"M": new("1")}})
			require.NoError(t, err)
			_, err = src.NewFileClient("child.txt").Create(ctx(t), 3, nil)
			require.NoError(t, err)

			_, err = src.Rename(ctx(t), "moved", nil)
			require.NoError(t, err)

			dst := sc.NewDirectoryClient("moved")

			_, err = src.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ResourceNotFound)

			props, err := dst.NewFileClient("child.txt").GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), *props.ContentLength)

			dprops, err := dst.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "1", md(dprops.Metadata, "m"))
		}},
		{name: "destination exists", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("one").Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = sc.NewDirectoryClient("two").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("one").Rename(ctx(t), "two", nil)
			requireCode(t, err, fileerror.ResourceAlreadyExists)
		}},
		{name: "into itself", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("loop").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("loop").Rename(ctx(t), "loop/inner", nil)
			requireCode(t, err, fileerror.InvalidInput)
		}},
		{name: "source missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("nope").Rename(ctx(t), "elsewhere", nil)
			requireCode(t, err, fileerror.ResourceNotFound)
		}},
		{name: "parent missing", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.NewDirectoryClient("here").Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.NewDirectoryClient("here").Rename(ctx(t), "no/parent", nil)
			requireCode(t, err, fileerror.ParentNotFound)
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
