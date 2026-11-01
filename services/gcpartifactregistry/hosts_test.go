package gcpartifactregistry_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (e *env) callHost(t *testing.T, host, method, path, ctype string, body []byte) httpResult {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://127.0.0.1:"+e.port+path,
		strings.NewReader(string(body)))
	require.NoError(t, err)

	req.Host = host
	req.Header.Set("Authorization", "Bearer anything")

	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}

	client := &http.Client{
		Transport:     httpclient.NewTransport(),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return httpResult{code: resp.StatusCode, body: string(raw), header: resp.Header}
}

func TestPackageHostRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)
	e.formatRepo(t, "py", artifactregistrypb.Repository_PYTHON)
	e.formatRepo(t, "jars", artifactregistrypb.Repository_MAVEN)

	const (
		npmHost   = "us-central1-npm.pkg.dev"
		pyHost    = "us-central1-python.pkg.dev"
		mavenHost = "us-central1-maven.pkg.dev"
	)

	npmRoot, pyRoot, mvnRoot := "/"+project+"/web/", "/"+project+"/py/", "/"+project+"/jars/"
	wheel := []byte("wheel-bytes")
	sum := sha256.Sum256(wheel)
	ctype, upload := twineBody(t, map[string]string{
		":action": "file_upload", "name": "My_Package", "version": "1.0.0", "sha256_digest": hex.EncodeToString(sum[:]),
	}, "My_Package-1.0.0-py3-none-any.whl", wheel)

	pom := []byte("<project/>")
	jar := "/com/acme/app/1.0/app-1.0.pom"

	tests := []struct {
		check  func(*testing.T, httpResult)
		name   string
		host   string
		method string
		path   string
		ctype  string
		body   []byte
		code   int
	}{
		{name: "npm publish", host: npmHost, method: http.MethodPut, path: npmRoot + "left-pad",
			body: npmPublishBody(t, "left-pad", "1.0.0", "latest", []byte("tgz")), code: http.StatusOK},
		{name: "npm packument", host: npmHost, method: http.MethodGet, path: npmRoot + "left-pad", code: http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Contains(t, r.body, "http://"+npmHost+npmRoot+"left-pad/-/left-pad-1.0.0.tgz")
			}},
		{name: "npm tarball", host: npmHost, method: http.MethodGet,
			path: npmRoot + "left-pad/-/left-pad-1.0.0.tgz",
			code: http.StatusOK, check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Equal(t, "tgz", r.body)
			}},
		{name: "npm wrong location", host: "europe-west1-npm.pkg.dev", method: http.MethodGet,
			path: npmRoot + "left-pad", code: http.StatusNotFound},
		{name: "npm host on python repo", host: npmHost, method: http.MethodGet, path: pyRoot + "simple/",
			code: http.StatusNotFound},
		{name: "npm host with port", host: npmHost + ":443", method: http.MethodGet, path: npmRoot + "left-pad",
			code: http.StatusOK},
		{name: "python upload", host: pyHost, method: http.MethodPost, path: pyRoot, ctype: ctype, body: upload,
			code: http.StatusOK},
		{name: "python simple", host: pyHost, method: http.MethodGet, path: pyRoot + "simple/my-package/",
			code: http.StatusOK, check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Contains(t, r.body, "#sha256="+hex.EncodeToString(sum[:]))
			}},
		{name: "python redirect", host: pyHost, method: http.MethodGet, path: pyRoot + "simple/My_Package/",
			code: http.StatusMovedPermanently, check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Equal(t, pyRoot+"simple/my-package/", r.header.Get("Location"))
			}},
		{name: "python wheel", host: pyHost, method: http.MethodGet,
			path: pyRoot + "packages/my-package/1.0.0/My_Package-1.0.0-py3-none-any.whl", code: http.StatusOK},
		{name: "maven put", host: mavenHost, method: http.MethodPut, path: mvnRoot + jar[1:], body: pom,
			code: http.StatusCreated},
		{name: "maven get", host: mavenHost, method: http.MethodGet, path: mvnRoot + jar[1:], code: http.StatusOK,
			check: func(t *testing.T, r httpResult) { t.Helper(); assert.Equal(t, string(pom), r.body) }},
		{name: "unknown repo", host: mavenHost, method: http.MethodGet, path: "/" + project + "/ghost/x",
			code: http.StatusNotFound},
		{name: "path form still works", host: "127.0.0.1", method: http.MethodGet,
			path: "/maven/" + project + "/jars/" + jar[1:],
			code: http.StatusOK},
	}

	for _, tt := range tests {
		r := e.callHost(t, tt.host, tt.method, tt.path, tt.ctype, tt.body)
		require.Equal(t, tt.code, r.code, "%s: %s", tt.name, r.body)

		if tt.check != nil {
			tt.check(t, r)
		}
	}
}

func TestDockerHostRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "images", dockerRepo())

	tests := []struct {
		name string
		host string
		code int
	}{
		{"matching location", "us-central1-docker.pkg.dev", http.StatusOK},
		{"wrong location", "europe-west1-docker.pkg.dev", http.StatusNotFound},
		{"plain host", "127.0.0.1", http.StatusOK},
	}

	for _, tt := range tests {
		r := e.callHost(t, tt.host, http.MethodGet, "/v2/"+project+"/images/_catalog", "", nil)
		assert.Equal(t, tt.code, r.code, "%s: %s", tt.name, r.body)
	}
}

func TestNPMTagResources(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)

	root := "/npm/" + project + "/web/"
	pkg := repoName("web") + "/packages/left-pad"
	v1, v2 := pkg+"/versions/1.0.0", pkg+"/versions/2.0.0"

	require.Equal(t, http.StatusOK, e.call(t, http.MethodPut, root+"left-pad", "",
		npmPublishBody(t, "left-pad", "1.0.0", "latest", []byte("one"))).code)
	require.Equal(t, http.StatusOK, e.call(t, http.MethodPut, root+"left-pad", "",
		npmPublishBody(t, "left-pad", "2.0.0", "beta", []byte("two"))).code)

	listTags := func(filter string) map[string]string {
		it := e.client.ListTags(t.Context(), &artifactregistrypb.ListTagsRequest{Parent: pkg, Filter: filter})
		out := map[string]string{}

		for {
			tag, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return out
			}

			require.NoError(t, err)

			out[strings.TrimPrefix(tag.GetName(), pkg+"/tags/")] = tag.GetVersion()
		}
	}

	distTags := func() string { return e.call(t, http.MethodGet, root+"-/package/left-pad/dist-tags", "", nil).body }

	assert.Equal(t, map[string]string{"latest": v1, "beta": v2}, listTags(""))
	assert.Equal(t, map[string]string{"beta": v2}, listTags(`version="`+v2+`"`))

	got, err := e.client.GetTag(t.Context(), &artifactregistrypb.GetTagRequest{Name: pkg + "/tags/beta"})
	require.NoError(t, err)
	assert.Equal(t, v2, got.GetVersion())

	_, err = e.client.CreateTag(t.Context(), &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "next", Tag: &artifactregistrypb.Tag{Version: v1},
	})
	require.NoError(t, err)
	assert.Contains(t, distTags(), `"next":"1.0.0"`)

	_, err = e.client.CreateTag(t.Context(), &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "next", Tag: &artifactregistrypb.Tag{Version: v1},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.CreateTag(t.Context(), &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "ghost", Tag: &artifactregistrypb.Tag{Version: pkg + "/versions/9.9.9"},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	upd, err := e.client.UpdateTag(t.Context(), &artifactregistrypb.UpdateTagRequest{
		Tag: &artifactregistrypb.Tag{Name: pkg + "/tags/next", Version: v2},
	})
	require.NoError(t, err)
	assert.Equal(t, v2, upd.GetVersion())
	assert.Contains(t, distTags(), `"next":"2.0.0"`)

	require.NoError(t, e.client.DeleteTag(t.Context(), &artifactregistrypb.DeleteTagRequest{Name: pkg + "/tags/next"}))
	assert.NotContains(t, distTags(), "next")
	assert.Equal(t, codes.NotFound, status.Code(e.client.DeleteTag(t.Context(),
		&artifactregistrypb.DeleteTagRequest{Name: pkg + "/tags/next"})))

	put := e.call(t, http.MethodPut, root+"-/package/left-pad/dist-tags/rc", "", []byte(`"1.0.0"`))
	require.Equal(t, http.StatusOK, put.code)
	assert.Equal(t, v1, listTags("")["rc"])
}
