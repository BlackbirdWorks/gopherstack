package azurefile_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

func newShareEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t)
	_, err := e.svc.NewShareClient("share1").Create(ctx(t), nil)
	require.NoError(t, err)

	return e
}

func TestShareTreeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []azurefile.ShareTree
		want  azurefile.ShareTree
	}{
		{
			name: "create",
			steps: []azurefile.ShareTree{{
				Dirs:  []string{"d", "d/e"},
				Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B"), "d/e/c.txt": nil},
			}},
			want: azurefile.ShareTree{
				Dirs:  []string{"d", "d/e"},
				Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B"), "d/e/c.txt": {}},
			},
		},
		{
			name: "update-and-remove",
			steps: []azurefile.ShareTree{
				{Dirs: []string{"d"}, Files: map[string][]byte{"a.txt": []byte("A"), "d/b.txt": []byte("B")}},
				{Files: map[string][]byte{"a.txt": []byte("A2"), "new.txt": []byte("N")}},
			},
			want: azurefile.ShareTree{Files: map[string][]byte{"a.txt": []byte("A2"), "new.txt": []byte("N")}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newShareEnv(t)

			for _, step := range tc.steps {
				require.NoError(t, e.h.Backend.ApplyShare(azureauth.DefaultAccountName, "SHARE1", step))
			}

			got, err := e.h.Backend.ExportShare(azureauth.DefaultAccountName, "share1")
			require.NoError(t, err)
			assert.Equal(t, tc.want.Dirs, got.Dirs)
			assert.Len(t, got.Files, len(tc.want.Files))

			for p, data := range tc.want.Files {
				assert.Equal(t, string(data), string(got.Files[p]), p)
			}
		})
	}
}

func TestShareTreeVisibleThroughDataPlaneAndKeepsETag(t *testing.T) {
	t.Parallel()

	e := newShareEnv(t)
	tree := azurefile.ShareTree{Files: map[string][]byte{"a.txt": []byte("hello")}}

	require.NoError(t, e.h.Backend.ApplyShare(azureauth.DefaultAccountName, "share1", tree))

	fc := e.svc.NewShareClient("share1").NewRootDirectoryClient().NewFileClient("a.txt")

	props, err := fc.GetProperties(ctx(t), nil)
	require.NoError(t, err)

	dl, err := fc.DownloadStream(ctx(t), nil)
	require.NoError(t, err)

	body, err := io.ReadAll(dl.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))

	require.NoError(t, e.h.Backend.ApplyShare(azureauth.DefaultAccountName, "share1", tree))

	again, err := fc.GetProperties(ctx(t), nil)
	require.NoError(t, err)
	assert.Equal(t, *props.ETag, *again.ETag, "an unchanged file is left alone")
}

func TestShareTreeMissingShare(t *testing.T) {
	t.Parallel()

	e := newShareEnv(t)

	_, err := e.h.Backend.ExportShare(azureauth.DefaultAccountName, "nope")
	require.ErrorIs(t, err, azurefile.ErrShareNotFound)
	require.ErrorIs(t, e.h.Backend.ApplyShare("other", "share1", azurefile.ShareTree{}), azurefile.ErrShareNotFound)
}
