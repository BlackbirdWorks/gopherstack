package gcpartifactregistry_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func kfpRepo() *artifactregistrypb.Repository {
	return &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_KFP}
}

func pipelineSpec(name string) []byte {
	return []byte("pipelineInfo:\n  name: " + name + "\nschemaVersion: 2.1.0\ncomponents: {}\n")
}

type kfpUpload struct {
	host        string
	description string
	tags        string
	content     []byte
	noContent   bool
}

func kfpPost(t *testing.T, e *env, repo string, u kfpUpload) (int, string) {
	t.Helper()

	var body bytes.Buffer

	mw := multipart.NewWriter(&body)
	if u.tags != "" {
		require.NoError(t, mw.WriteField("tags", u.tags))
	}

	if !u.noContent {
		fw, err := mw.CreateFormFile("content", "pipeline.yaml")
		require.NoError(t, err)
		_, err = fw.Write(u.content)
		require.NoError(t, err)
	}

	require.NoError(t, mw.Close())

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		"http://127.0.0.1:"+e.port+"/kfp/"+project+"/"+repo, &body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer anything")

	if u.description != "" {
		req.Header.Set("Description", u.description)
	}

	if u.host != "" {
		req.Host = u.host
		req.URL.Path = "/" + project + "/" + repo
	}

	resp, err := testHTTP.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, strings.TrimSpace(string(out))
}

func TestKFPUploadValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		upload kfpUpload
		want   int
	}{
		{name: "ok", upload: kfpUpload{content: pipelineSpec("hello-world"), tags: "v1,latest"}, want: http.StatusOK},
		{
			name:   "snake case info",
			upload: kfpUpload{content: []byte("pipeline_info:\n  name: snake-name\n")},
			want:   http.StatusOK,
		},
		{name: "no content", upload: kfpUpload{noContent: true}, want: http.StatusBadRequest},
		{name: "short name", upload: kfpUpload{content: pipelineSpec("abc")}, want: http.StatusBadRequest},
		{name: "uppercase name", upload: kfpUpload{content: pipelineSpec("Hello-World")}, want: http.StatusBadRequest},
		{name: "not a spec", upload: kfpUpload{content: []byte("just: text")}, want: http.StatusBadRequest},
		{
			name:   "bad tag",
			upload: kfpUpload{content: pipelineSpec("tag-check"), tags: "a b"},
			want:   http.StatusBadRequest,
		},
		{
			name:   "nine tags",
			upload: kfpUpload{content: pipelineSpec("tag-count"), tags: "a,b,c,d,e,f,g,h,i"},
			want:   http.StatusBadRequest,
		},
	}

	e := newEnv(t, "")
	e.createRepo(t, "kfp-validate", kfpRepo())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := kfpPost(t, e, "kfp-validate", tt.upload)
			assert.Equal(t, tt.want, code, body)
		})
	}
}

func TestKFPUploadAndDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "kfp", kfpRepo())

	ctx := t.Context()
	spec := pipelineSpec("hello-world")
	digest := "sha256:" + hexSum(spec)

	code, body := kfpPost(t, e, "kfp", kfpUpload{content: spec, tags: "v1,latest", description: "first"})
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, "hello-world/"+digest, body)

	pkgName := repoName("kfp") + "/packages/hello-world"
	verName := pkgName + "/versions/" + digest

	t.Run("reads", func(t *testing.T) {
		t.Run("api resources", func(t *testing.T) {
			t.Parallel()

			pkg, err := e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkgName})
			require.NoError(t, err)
			assert.Equal(t, "hello-world", pkg.GetDisplayName())

			ver, err := e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: verName})
			require.NoError(t, err)
			assert.Equal(t, "first", ver.GetDescription())

			tags := e.client.ListTags(ctx, &artifactregistrypb.ListTagsRequest{Parent: pkgName})
			got := map[string]string{}

			for {
				tag, nerr := tags.Next()
				if errors.Is(nerr, iterator.Done) {
					break
				}

				require.NoError(t, nerr)

				got[tag.GetName()] = tag.GetVersion()
			}

			assert.Equal(t, map[string]string{pkgName + "/tags/v1": verName, pkgName + "/tags/latest": verName}, got)

			repo, err := e.client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: repoName("kfp")})
			require.NoError(t, err)
			assert.EqualValues(t, len(spec), repo.GetSizeBytes())
		})

		t.Run("download", func(t *testing.T) {
			t.Parallel()

			base := "http://127.0.0.1:" + e.port + "/kfp/" + project + "/kfp/hello-world/"

			for _, ref := range []string{digest, "v1", "latest"} {
				status, got := getBody(t, base+ref)
				assert.Equal(t, http.StatusOK, status, ref)
				assert.Equal(t, spec, got, ref)
			}

			status, _ := getBody(t, base+"nope")
			assert.Equal(t, http.StatusNotFound, status)
		})
	})

	t.Run("tag moves to new version", func(t *testing.T) {
		t.Parallel()

		next := append(pipelineSpec("hello-world"), []byte("# v2\n")...)
		st, resp := kfpPost(t, e, "kfp", kfpUpload{content: next, tags: "latest"})
		require.Equal(t, http.StatusOK, st)
		assert.Equal(t, "hello-world/sha256:"+hexSum(next), resp)

		tag, err := e.client.GetTag(ctx, &artifactregistrypb.GetTagRequest{Name: pkgName + "/tags/latest"})
		require.NoError(t, err)
		assert.Equal(t, pkgName+"/versions/sha256:"+hexSum(next), tag.GetVersion())

		status, got := getBody(t, "http://127.0.0.1:"+e.port+"/kfp/"+project+"/kfp/hello-world/v1")
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, spec, got, "v1 still points at the first version")
	})
}

func TestKFPDeleteVersionDropsItsTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "kfp-del", kfpRepo())

	first, second := pipelineSpec("deleted-pipe"), append(pipelineSpec("deleted-pipe"), []byte("# 2\n")...)
	for _, up := range []kfpUpload{{content: first, tags: "v1"}, {content: second, tags: "latest"}} {
		st, body := kfpPost(t, e, "kfp-del", up)
		require.Equal(t, http.StatusOK, st, body)
	}

	pkg := repoName("kfp-del") + "/packages/deleted-pipe"
	op, err := e.client.DeleteVersion(t.Context(), &artifactregistrypb.DeleteVersionRequest{
		Name: pkg + "/versions/sha256:" + hexSum(second), Force: true,
	})
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	_, err = e.client.GetTag(t.Context(), &artifactregistrypb.GetTagRequest{Name: pkg + "/tags/latest"})
	assert.Equal(t, codes.NotFound, status.Code(err), "latest is not re-pointed in KFP repositories")

	_, err = e.client.GetTag(t.Context(), &artifactregistrypb.GetTagRequest{Name: pkg + "/tags/v1"})
	require.NoError(t, err)

	code, _ := getBody(t, "http://127.0.0.1:"+e.port+"/kfp/"+project+"/kfp-del/deleted-pipe/sha256:"+hexSum(second))
	assert.Equal(t, http.StatusNotFound, code)
}

func TestKFPHostedAndZip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "kfp-host", kfpRepo())

	var zbuf bytes.Buffer

	zw := zip.NewWriter(&zbuf)
	fw, err := zw.Create("pipeline.yaml")
	require.NoError(t, err)
	_, err = fw.Write(pipelineSpec("zipped-pipeline"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	code, body := kfpPost(
		t,
		e,
		"kfp-host",
		kfpUpload{content: zbuf.Bytes(), tags: "z", host: "us-central1-kfp.pkg.dev"},
	)
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, "zipped-pipeline/sha256:"+hexSum(zbuf.Bytes()), body)

	code, body = kfpPost(
		t,
		e,
		"kfp-host",
		kfpUpload{content: pipelineSpec("wrong-place"), host: "europe-west1-kfp.pkg.dev"},
	)
	assert.Equal(t, http.StatusNotFound, code, body)
}

func TestKFPRepositoriesAreStandardOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	_, err := e.client.CreateRepository(t.Context(), &artifactregistrypb.CreateRepositoryRequest{
		Parent: loc, RepositoryId: "kfp-virtual", Repository: &artifactregistrypb.Repository{
			Format: artifactregistrypb.Repository_KFP, Mode: artifactregistrypb.Repository_VIRTUAL_REPOSITORY,
		},
	})
	require.Error(t, err)
}

func hexSum(b []byte) string { return strings.TrimPrefix(sha256Of(b), "sha256:") }
