package gcpartifactregistry_test

import (
	"encoding/json"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func fileName(repo, id string) string { return repoName(repo) + "/files/" + id }

func listFiles(t *testing.T, e *env, req *artifactregistrypb.ListFilesRequest) []*artifactregistrypb.File {
	t.Helper()

	var out []*artifactregistrypb.File

	it := e.client.ListFiles(t.Context(), req)

	for {
		f, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, f)
	}
}

func fileNames(files []*artifactregistrypb.File) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.GetName())
	}

	return out
}

func configDigest(t *testing.T, img ocitest.Image) string {
	t.Helper()

	var doc struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
	}

	require.NoError(t, json.Unmarshal(img.Body, &doc))

	return doc.Config.Digest
}

func TestDockerFiles(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "files", dockerRepo())

	c := e.docker(t)
	app := pushTo(c, "files", "app", "v1")
	other := pushTo(c, "files", "other", "v1")

	appCfg, appLayer := configDigest(t, app), ocitest.Digest(app.Layer)
	owner := pkgName("files", "app") + "/versions/" + app.Digest
	parent := repoName("files")

	t.Run("list shape", func(t *testing.T) {
		t.Parallel()

		files := listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, Filter: `owner="` + owner + `"`})
		require.ElementsMatch(t, []string{
			fileName("files", app.Digest), fileName("files", appCfg), fileName("files", appLayer),
		}, fileNames(files))

		for _, f := range files {
			assert.Equal(t, owner, f.GetOwner())
			require.Len(t, f.GetHashes(), 1)
			assert.Equal(t, artifactregistrypb.Hash_SHA256, f.GetHashes()[0].GetType())
			assert.NotNil(t, f.GetCreateTime())
		}

		got, err := e.client.GetFile(t.Context(), &artifactregistrypb.GetFileRequest{Name: fileName("files", appLayer)})
		require.NoError(t, err)
		assert.EqualValues(t, len(app.Layer), got.GetSizeBytes())
		assert.Equal(t, ocitest.Digest(app.Layer), "sha256:"+hexOf(got.GetHashes()[0].GetValue()))

		manifest, err := e.client.GetFile(
			t.Context(),
			&artifactregistrypb.GetFileRequest{Name: fileName("files", app.Digest)},
		)
		require.NoError(t, err)
		assert.EqualValues(t, len(app.Body), manifest.GetSizeBytes())
	})

	t.Run("filters", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			filter string
			want   int
		}{
			{"all", "", 6},
			{"name exact", `name="` + fileName("files", appLayer) + `"`, 1},
			{"name wildcard", `name="` + parent + `/files/sha256:*"`, 6},
			{"name no match", `name="` + parent + `/files/nope*"`, 0},
			{"owner other", `owner="` + pkgName("files", "other") + `/versions/` + other.Digest + `"`, 3},
			{"owner and name", `owner="` + owner + `" AND name="*` + appCfg + `"`, 1},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				got := listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, Filter: tt.filter})
				assert.Len(t, got, tt.want)
			})
		}

		_, err := e.client.ListFiles(t.Context(), &artifactregistrypb.ListFilesRequest{
			Parent: parent, Filter: "bogus", PageSize: 1,
		}).Next()
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("order and paging", func(t *testing.T) {
		t.Parallel()

		asc := fileNames(listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, OrderBy: "name"}))
		desc := fileNames(listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, OrderBy: "name desc"}))
		require.Len(t, asc, 6)
		assert.True(t, sortedAsc(asc))
		assert.Equal(t, reversed(asc), desc)

		paged := fileNames(listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, PageSize: 2}))
		assert.Equal(t, asc, paged)

		_, err := e.client.ListFiles(t.Context(), &artifactregistrypb.ListFilesRequest{
			Parent: parent, OrderBy: "size_bytes",
		}).Next()
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()

		_, err := e.client.GetFile(
			t.Context(),
			&artifactregistrypb.GetFileRequest{Name: fileName("files", "sha256:00")},
		)
		assert.Equal(t, codes.NotFound, status.Code(err))

		_, err = e.client.ListFiles(t.Context(), &artifactregistrypb.ListFilesRequest{Parent: repoName("ghost")}).Next()
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("delete refused on docker", func(t *testing.T) {
		t.Parallel()

		_, err := e.client.DeleteFile(t.Context(), &artifactregistrypb.DeleteFileRequest{
			Name: fileName("files", app.Digest),
		})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestUpdateFileAnnotations(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "ann", dockerRepo())

	img := pushTo(e.docker(t), "ann", "app", "v1")
	name := fileName("ann", img.Digest)

	tests := []struct {
		input map[string]string
		want  map[string]string
		name  string
		mask  []string
		code  codes.Code
	}{
		{name: "set", mask: []string{"annotations"}, input: map[string]string{"k": "v", "external.link": "https://x"},
			want: map[string]string{"k": "v", "external.link": "https://x"}},
		{name: "replace", mask: []string{"annotations"}, input: map[string]string{"z": "1"},
			want: map[string]string{"z": "1"}},
		{name: "clear", mask: []string{"annotations"}},
		{name: "bad mask", mask: []string{"size_bytes"}, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		got, err := e.client.UpdateFile(t.Context(), &artifactregistrypb.UpdateFileRequest{
			File:       &artifactregistrypb.File{Name: name, Annotations: tt.input},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
		})
		require.Equal(t, tt.code, status.Code(err), tt.name)

		if tt.code == codes.OK {
			assert.Equal(t, tt.want, got.GetAnnotations(), tt.name)

			again, getErr := e.client.GetFile(t.Context(), &artifactregistrypb.GetFileRequest{Name: name})
			require.NoError(t, getErr)
			assert.Equal(t, tt.want, again.GetAnnotations(), tt.name)
		}
	}

	_, err := e.client.UpdateFile(t.Context(), &artifactregistrypb.UpdateFileRequest{
		File: &artifactregistrypb.File{
			Name:        name,
			Annotations: map[string]string{"team": "core", "external.link": "https://e.com/f"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"annotations"}},
	})
	require.NoError(t, err)

	filters := []struct {
		filter string
		want   int
	}{
		{"annotations.team:core", 1},
		{"annotations.team", 1},
		{"annotations.team=other", 0},
		{"annotations.absent", 0},
		{"annotations.`external.link`:`https://e.com/*`", 1},
	}

	for _, f := range filters {
		got := listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: repoName("ann"), Filter: f.filter})
		assert.Len(t, got, f.want, f.filter)
	}

	_, err = e.client.UpdateFile(t.Context(), &artifactregistrypb.UpdateFileRequest{
		File: &artifactregistrypb.File{Name: fileName("ann", "sha256:00")},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
