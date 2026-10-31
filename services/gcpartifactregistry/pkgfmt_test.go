package gcpartifactregistry_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

type httpResult struct {
	header http.Header
	body   string
	code   int
}

func (e *env) call(t *testing.T, method, path, ctype string, body []byte) httpResult {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://127.0.0.1:"+e.port+path, bytes.NewReader(body))
	require.NoError(t, err)

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

func (e *env) formatRepo(t *testing.T, id string, f artifactregistrypb.Repository_Format) {
	t.Helper()
	e.createRepo(t, id, &artifactregistrypb.Repository{Format: f})
}

func (e *env) packageIDs(t *testing.T, repo string) []string {
	t.Helper()

	it := e.client.ListPackages(t.Context(), &artifactregistrypb.ListPackagesRequest{Parent: repoName(repo)})

	var out []string

	for {
		p, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, strings.TrimPrefix(p.GetName(), repoName(repo)+"/packages/"))
	}
}

func (e *env) versionIDs(t *testing.T, repo, pkg string) []string {
	t.Helper()

	it := e.client.ListVersions(t.Context(), &artifactregistrypb.ListVersionsRequest{
		Parent: repoName(repo) + "/packages/" + pkg,
	})

	var out []string

	for {
		v, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, v.GetName()[strings.LastIndex(v.GetName(), "/")+1:])
	}
}

func (e *env) fileIDs(t *testing.T, repo string) []string {
	t.Helper()

	it := e.client.ListFiles(t.Context(), &artifactregistrypb.ListFilesRequest{Parent: repoName(repo)})

	var out []string

	for {
		f, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, strings.TrimPrefix(f.GetName(), repoName(repo)+"/files/"))
	}
}

func (e *env) denyDownloads(t *testing.T, repo string) {
	t.Helper()

	_, err := e.client.CreateRule(t.Context(), &artifactregistrypb.CreateRuleRequest{
		Parent: repoName(repo), RuleId: "deny",
		Rule: &artifactregistrypb.Rule{
			Action: artifactregistrypb.Rule_DENY, Operation: artifactregistrypb.Rule_DOWNLOAD,
		},
	})
	require.NoError(t, err)
}

func npmPublishBody(t *testing.T, name, version, tag string, tarball []byte) []byte {
	t.Helper()

	base := name[strings.LastIndex(name, "/")+1:]
	file := base + "-" + version + ".tgz"
	attachment := name + "-" + version + ".tgz"
	sum := sha1.Sum(tarball)

	doc := map[string]any{
		"_id": name, "name": name, "description": "d",
		"dist-tags": map[string]string{tag: version},
		"versions": map[string]any{version: map[string]any{
			"name": name, "version": version, "_id": name + "@" + version,
			"dist": map[string]string{
				"shasum":  hex.EncodeToString(sum[:]),
				"tarball": "http://registry.npmjs.org/" + name + "/-/" + file,
			},
		}},
		"_attachments": map[string]any{attachment: map[string]any{
			"content_type": "application/octet-stream",
			"data":         base64.StdEncoding.EncodeToString(tarball), "length": len(tarball),
		}},
	}

	raw, err := json.Marshal(doc)
	require.NoError(t, err)

	return raw
}

func TestNPMProtocol(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)
	e.formatRepo(t, "images", artifactregistrypb.Repository_DOCKER)

	root := "/npm/" + project + "/web/"
	tar1, tar2, tar3 := []byte("tarball-one"), []byte("tarball-two"), []byte("tarball-scoped")

	tests := []struct {
		check  func(*testing.T, httpResult)
		name   string
		method string
		path   string
		body   []byte
		code   int
	}{
		{
			name:   "publish scoped",
			method: http.MethodPut,
			path:   root + "@acme%2Flib",
			body:   npmPublishBody(t, "@acme/lib", "1.0.0", "latest", tar3),
			code:   http.StatusOK,
			check:  nil,
		},
		{
			name:   "publish plain",
			method: http.MethodPut,
			path:   root + "left-pad",
			body:   npmPublishBody(t, "left-pad", "1.0.0", "latest", tar1),
			code:   http.StatusOK,
			check:  nil,
		},
		{
			name:   "publish next version",
			method: http.MethodPut,
			path:   root + "left-pad",
			body:   npmPublishBody(t, "left-pad", "1.1.0-beta.1", "next", tar2),
			code:   http.StatusOK,
			check:  nil,
		},
		{
			name:   "republish conflicts",
			method: http.MethodPut,
			path:   root + "left-pad",
			body:   npmPublishBody(t, "left-pad", "1.0.0", "latest", tar1),
			code:   http.StatusConflict,
			check:  nil,
		},
		{
			name:   "name mismatch",
			method: http.MethodPut,
			path:   root + "left-pad",
			body:   npmPublishBody(t, "other", "1.0.0", "latest", tar1),
			code:   http.StatusBadRequest,
			check:  nil,
		},
		{
			name:   "bad version",
			method: http.MethodPut,
			path:   root + "bad",
			body:   npmPublishBody(t, "bad", "one", "latest", tar1),
			code:   http.StatusBadRequest,
			check:  nil,
		},
		{
			name:   "wrong repo format",
			method: http.MethodPut,
			path:   "/npm/" + project + "/images/left-pad",
			body:   npmPublishBody(t, "left-pad", "1.0.0", "latest", tar1),
			code:   http.StatusNotFound,
			check:  nil,
		},
		{
			name:   "packument",
			method: http.MethodGet,
			path:   root + "left-pad",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()

				doc := decodeJSON(t, []byte(r.body))
				assert.Equal(t, "left-pad", doc["name"])
				assert.Equal(t, map[string]any{"latest": "1.0.0", "next": "1.1.0-beta.1"}, doc["dist-tags"])

				v1 := doc["versions"].(map[string]any)["1.0.0"].(map[string]any)
				dist := v1["dist"].(map[string]any)
				sum := sha1.Sum(tar1)
				assert.Equal(t, hex.EncodeToString(sum[:]), dist["shasum"])
				assert.Contains(t, dist["integrity"], "sha512-")
				assert.Contains(t, dist["tarball"], "/npm/"+project+"/web/left-pad/-/left-pad-1.0.0.tgz")
				assert.Len(t, doc["time"], 4)
			},
		},
		{
			name:   "scoped packument",
			method: http.MethodGet,
			path:   root + "@acme/lib",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Contains(t, r.body, "/npm/"+project+"/web/@acme/lib/-/lib-1.0.0.tgz")
			},
		},
		{
			name:   "tarball",
			method: http.MethodGet,
			path:   root + "left-pad/-/left-pad-1.0.0.tgz",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Equal(t, string(tar1), r.body)
			},
		},
		{
			name:   "scoped tarball",
			method: http.MethodGet,
			path:   root + "@acme/lib/-/lib-1.0.0.tgz",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.Equal(t, string(tar3), r.body)
			},
		},
		{
			name:   "missing package",
			method: http.MethodGet,
			path:   root + "ghost",
			body:   nil,
			code:   http.StatusNotFound,
			check:  nil,
		},
		{
			name:   "missing tarball",
			method: http.MethodGet,
			path:   root + "left-pad/-/left-pad-9.9.9.tgz",
			body:   nil,
			code:   http.StatusNotFound,
			check:  nil,
		},
		{name: "ping", method: http.MethodGet, path: root + "-/ping", body: nil, code: http.StatusOK, check: nil},
		{
			name:   "tag list",
			method: http.MethodGet,
			path:   root + "-/package/left-pad/dist-tags",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.JSONEq(t, `{"latest":"1.0.0","next":"1.1.0-beta.1"}`, r.body)
			},
		},
		{
			name:   "tag add",
			method: http.MethodPut,
			path:   root + "-/package/left-pad/dist-tags/stable",
			body:   []byte(`"1.0.0"`),
			code:   http.StatusOK,
			check:  nil,
		},
		{
			name:   "tag add unknown version",
			method: http.MethodPut,
			path:   root + "-/package/left-pad/dist-tags/x",
			body:   []byte(`"7.0.0"`),
			code:   http.StatusNotFound,
			check:  nil,
		},
		{
			name:   "tag delete",
			method: http.MethodDelete,
			path:   root + "-/package/left-pad/dist-tags/next",
			body:   nil,
			code:   http.StatusOK,
			check:  nil,
		},
		{
			name:   "tag delete missing",
			method: http.MethodDelete,
			path:   root + "-/package/left-pad/dist-tags/next",
			body:   nil,
			code:   http.StatusNotFound,
			check:  nil,
		},
		{
			name:   "tags after",
			method: http.MethodGet,
			path:   root + "-/package/left-pad/dist-tags",
			body:   nil,
			code:   http.StatusOK,
			check: func(t *testing.T, r httpResult) {
				t.Helper()
				assert.JSONEq(t, `{"latest":"1.0.0","stable":"1.0.0"}`, r.body)
			},
		},
	}

	for _, tt := range tests {
		r := e.call(t, tt.method, tt.path, "application/json", tt.body)
		require.Equal(t, tt.code, r.code, "%s: %s", tt.name, r.body)

		if tt.check != nil {
			tt.check(t, r)
		}
	}

	assert.Equal(t, []string{"@acme%2Flib", "left-pad"}, e.packageIDs(t, "web"))
	assert.ElementsMatch(t, []string{"1.0.0", "1.1.0-beta.1"}, e.versionIDs(t, "web", "left-pad"))
	assert.ElementsMatch(t, []string{
		"@acme%2Flib%2F-%2Flib-1.0.0.tgz",
		"left-pad%2F-%2Fleft-pad-1.0.0.tgz",
		"left-pad%2F-%2Fleft-pad-1.1.0-beta.1.tgz",
	}, e.fileIDs(t, "web"))

	t.Run("unpublish", func(t *testing.T) {
		t.Parallel()

		unpub := map[string]any{
			"name": "left-pad", "dist-tags": map[string]string{"latest": "1.0.0"},
			"versions": map[string]any{"1.0.0": map[string]any{}},
		}
		raw, _ := json.Marshal(unpub)

		require.Equal(
			t,
			http.StatusOK,
			e.call(t, http.MethodPut, root+"left-pad/-rev/1-x", "application/json", raw).code,
		)
		require.Equal(t, http.StatusOK,
			e.call(t, http.MethodDelete, root+"left-pad/-/left-pad-1.1.0-beta.1.tgz/-rev/2-x", "", nil).code)
		assert.Equal(t, []string{"1.0.0"}, e.versionIDs(t, "web", "left-pad"))

		require.Equal(t, http.StatusOK, e.call(t, http.MethodDelete, root+"@acme%2Flib/-rev/1-x", "", nil).code)
		assert.Equal(t, []string{"left-pad"}, e.packageIDs(t, "web"))
		assert.Equal(t, http.StatusNotFound, e.call(t, http.MethodGet, root+"@acme/lib", "", nil).code)
	})
}

func TestNPMDownloadRuleAndLatestRepoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)

	root := "/npm/" + project + "/web/"
	for _, v := range []string{"1.0.0", "1.1.0"} {
		require.Equal(
			t,
			http.StatusOK,
			e.call(
				t,
				http.MethodPut,
				root+"pkg",
				"application/json",
				npmPublishBody(t, "pkg", v, "latest", []byte(v)),
			).code,
		)
	}

	// An API delete of the latest version repoints latest at the remaining one.
	_, err := e.client.DeleteVersion(t.Context(), &artifactregistrypb.DeleteVersionRequest{
		Name: repoName("web") + "/packages/pkg/versions/1.1.0",
	})
	require.NoError(t, err)

	r := e.call(t, http.MethodGet, root+"-/package/pkg/dist-tags", "", nil)
	assert.JSONEq(t, `{"latest":"1.0.0"}`, r.body)

	e.denyDownloads(t, "web")
	assert.Equal(t, http.StatusForbidden, e.call(t, http.MethodGet, root+"pkg/-/pkg-1.0.0.tgz", "", nil).code)
	assert.Equal(t, http.StatusOK, e.call(t, http.MethodGet, root+"pkg", "", nil).code)
}

func twineBody(t *testing.T, fields map[string]string, filename string, content []byte) (string, []byte) {
	t.Helper()

	var buf bytes.Buffer

	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		require.NoError(t, mw.WriteField(k, v))
	}

	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="content"; filename=%q`, filename))
	h.Set("Content-Type", "application/octet-stream")

	part, err := mw.CreatePart(h)
	require.NoError(t, err)
	_, err = part.Write(content)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	return mw.FormDataContentType(), buf.Bytes()
}

func TestPythonProtocol(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "py", artifactregistrypb.Repository_PYTHON)

	root := "/python/" + project + "/py/"
	wheel := []byte("wheel-bytes")
	sum := sha256.Sum256(wheel)

	upload := func(fields map[string]string, filename string, content []byte) httpResult {
		ctype, body := twineBody(t, fields, filename, content)

		return e.call(t, http.MethodPost, root, ctype, body)
	}

	good := map[string]string{
		":action": "file_upload", "protocol_version": "1", "metadata_version": "2.1",
		"name": "My_Package", "version": "1.0.0", "requires_python": ">=3.8",
		"sha256_digest": hex.EncodeToString(sum[:]),
	}

	tests := []struct {
		name     string
		fields   map[string]string
		filename string
		code     int
	}{
		{"upload wheel", good, "My_Package-1.0.0-py3-none-any.whl", http.StatusOK},
		{"upload sdist", map[string]string{
			":action": "file_upload", "name": "My_Package", "version": "1.0.0",
		}, "my_package-1.0.0.tar.gz", http.StatusOK},
		{"duplicate file", good, "My_Package-1.0.0-py3-none-any.whl", http.StatusConflict},
		{"bad action", map[string]string{":action": "submit", "name": "x", "version": "1"}, "x-1.tar.gz",
			http.StatusBadRequest},
		{"digest mismatch", map[string]string{
			":action": "file_upload", "name": "x", "version": "1", "sha256_digest": strings.Repeat("0", 64),
		}, "x-1.tar.gz", http.StatusBadRequest},
		{"bad filename", map[string]string{":action": "file_upload", "name": "x", "version": "1"}, "x-1.exe",
			http.StatusBadRequest},
	}

	for _, tt := range tests {
		r := upload(tt.fields, tt.filename, wheel)
		require.Equal(t, tt.code, r.code, "%s: %s", tt.name, r.body)
	}

	assert.Equal(t, []string{"my-package"}, e.packageIDs(t, "py"))
	assert.Equal(t, []string{"1.0.0"}, e.versionIDs(t, "py", "my-package"))
	assert.Len(t, e.fileIDs(t, "py"), 2)

	idx := e.call(t, http.MethodGet, root+"simple/", "", nil)
	require.Equal(t, http.StatusOK, idx.code)
	assert.Contains(t, idx.header.Get("Content-Type"), "text/html")
	assert.Contains(t, idx.body, `<a href="my-package/">my-package</a>`)

	redirect := e.call(t, http.MethodGet, root+"simple/My_Package/", "", nil)
	assert.Equal(t, http.StatusMovedPermanently, redirect.code)
	assert.Equal(t, root+"simple/my-package/", redirect.header.Get("Location"))

	page := e.call(t, http.MethodGet, root+"simple/my-package/", "", nil)
	require.Equal(t, http.StatusOK, page.code)
	assert.Contains(t, page.body, "#sha256="+hex.EncodeToString(sum[:]))
	assert.Contains(t, page.body, `data-requires-python="&gt;=3.8"`)
	assert.Contains(t, page.body, ">My_Package-1.0.0-py3-none-any.whl</a>")
	assert.Equal(t, http.StatusNotFound, e.call(t, http.MethodGet, root+"simple/ghost/", "", nil).code)

	dl := e.call(t, http.MethodGet, root+"packages/my-package/1.0.0/My_Package-1.0.0-py3-none-any.whl", "", nil)
	assert.Equal(t, http.StatusOK, dl.code)
	assert.Equal(t, string(wheel), dl.body)

	e.denyDownloads(t, "py")
	assert.Equal(t, http.StatusForbidden,
		e.call(t, http.MethodGet, root+"packages/my-package/1.0.0/My_Package-1.0.0-py3-none-any.whl", "", nil).code)
}

func TestMavenProtocol(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "jars", &artifactregistrypb.Repository{
		Format: artifactregistrypb.Repository_MAVEN,
		FormatConfig: &artifactregistrypb.Repository_MavenConfig{
			MavenConfig: &artifactregistrypb.Repository_MavenRepositoryConfig{
				VersionPolicy: artifactregistrypb.Repository_MavenRepositoryConfig_RELEASE,
			},
		},
	})
	e.createRepo(t, "snaps", &artifactregistrypb.Repository{
		Format: artifactregistrypb.Repository_MAVEN,
		FormatConfig: &artifactregistrypb.Repository_MavenConfig{
			MavenConfig: &artifactregistrypb.Repository_MavenRepositoryConfig{
				VersionPolicy:           artifactregistrypb.Repository_MavenRepositoryConfig_SNAPSHOT,
				AllowSnapshotOverwrites: true,
			},
		},
	})

	root := "/maven/" + project + "/jars/"
	jar := []byte("jar-bytes")
	jarSum := sha1.Sum(jar)
	base := root + "com/acme/tools/lib/1.0/lib-1.0"

	tests := []struct {
		name   string
		method string
		path   string
		want   string
		body   []byte
		code   int
	}{
		{
			name:   "metadata before",
			method: http.MethodGet,
			path:   root + "com/acme/tools/lib/maven-metadata.xml",
			body:   nil,
			code:   http.StatusNotFound,
			want:   "",
		},
		{name: "put jar", method: http.MethodPut, path: base + ".jar", body: jar, code: http.StatusCreated, want: ""},
		{
			name:   "put jar sha1",
			method: http.MethodPut,
			path:   base + ".jar.sha1",
			body:   []byte(hex.EncodeToString(jarSum[:])),
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "checksum without file",
			method: http.MethodPut,
			path:   base + ".war.sha1",
			body:   []byte("x"),
			code:   http.StatusNotFound,
			want:   "",
		},
		{
			name:   "put pom",
			method: http.MethodPut,
			path:   base + ".pom",
			body:   []byte("<project/>"),
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "put metadata",
			method: http.MethodPut,
			path:   root + "com/acme/tools/lib/maven-metadata.xml",
			body:   []byte("<metadata/>"),
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "release overwrite",
			method: http.MethodPut,
			path:   base + ".jar",
			body:   []byte("other"),
			code:   http.StatusConflict,
			want:   "",
		},
		{
			name:   "snapshot in release repo",
			method: http.MethodPut,
			path:   root + "com/acme/tools/lib/2.0-SNAPSHOT/lib-2.0-SNAPSHOT.jar",
			body:   jar,
			code:   http.StatusBadRequest,
			want:   "",
		},
		{
			name:   "bad layout",
			method: http.MethodPut,
			path:   root + "lib.jar",
			body:   jar,
			code:   http.StatusBadRequest,
			want:   "",
		},
		{
			name:   "name mismatch",
			method: http.MethodPut,
			path:   root + "com/acme/tools/lib/1.0/other-1.0.jar",
			body:   jar,
			code:   http.StatusBadRequest,
			want:   "",
		},
		{
			name:   "get jar",
			method: http.MethodGet,
			path:   base + ".jar",
			body:   nil,
			code:   http.StatusOK,
			want:   string(jar),
		},
		{
			name:   "get pom",
			method: http.MethodGet,
			path:   base + ".pom",
			body:   nil,
			code:   http.StatusOK,
			want:   "<project/>",
		},
		{
			name:   "get sha1",
			method: http.MethodGet,
			path:   base + ".jar.sha1",
			body:   nil,
			code:   http.StatusOK,
			want:   hex.EncodeToString(jarSum[:]),
		},
		{name: "head jar", method: http.MethodHead, path: base + ".jar", body: nil, code: http.StatusOK, want: ""},
		{name: "missing", method: http.MethodGet, path: base + ".zip", body: nil, code: http.StatusNotFound, want: ""},
		{
			name:   "second release",
			method: http.MethodPut,
			path:   root + "com/acme/tools/lib/1.1/lib-1.1.jar",
			body:   []byte("j2"),
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "snapshot repo rejects release",
			method: http.MethodPut,
			path:   "/maven/" + project + "/snaps/g/a/1.0/a-1.0.jar",
			body:   jar,
			code:   http.StatusBadRequest,
			want:   "",
		},
		{
			name:   "snapshot put",
			method: http.MethodPut,
			path:   "/maven/" + project + "/snaps/g/a/1.0-SNAPSHOT/a-1.0-20240101.120000-1.jar",
			body:   jar,
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "snapshot overwrite allowed",
			method: http.MethodPut,
			path:   "/maven/" + project + "/snaps/g/a/1.0-SNAPSHOT/a-1.0-20240101.120000-1.jar",
			body:   []byte("v2"),
			code:   http.StatusCreated,
			want:   "",
		},
		{
			name:   "snapshot overwritten",
			method: http.MethodGet,
			path:   "/maven/" + project + "/snaps/g/a/1.0-SNAPSHOT/a-1.0-20240101.120000-1.jar",
			body:   nil,
			code:   http.StatusOK,
			want:   "v2",
		},
	}

	for _, tt := range tests {
		r := e.call(t, tt.method, tt.path, "", tt.body)
		require.Equal(t, tt.code, r.code, "%s: %s", tt.name, r.body)

		if tt.want != "" {
			assert.Equal(t, tt.want, r.body, tt.name)
		}
	}

	meta := e.call(t, http.MethodGet, root+"com/acme/tools/lib/maven-metadata.xml", "", nil)
	require.Equal(t, http.StatusOK, meta.code)
	assert.Contains(t, meta.body, "<groupId>com.acme.tools</groupId>")
	assert.Contains(t, meta.body, "<artifactId>lib</artifactId>")
	assert.Contains(t, meta.body, "<release>1.1</release>")
	assert.Contains(t, meta.body, "<version>1.0</version>")

	msum := sha1.Sum([]byte(meta.body))
	msha := e.call(t, http.MethodGet, root+"com/acme/tools/lib/maven-metadata.xml.sha1", "", nil)
	assert.Equal(t, hex.EncodeToString(msum[:]), msha.body)

	assert.Equal(t, []string{"com.acme.tools:lib"}, e.packageIDs(t, "jars"))
	assert.Equal(t, []string{"1.0", "1.1"}, e.versionIDs(t, "jars", "com.acme.tools:lib"))
	assert.ElementsMatch(t, []string{
		"com%2Facme%2Ftools%2Flib%2F1.0%2Flib-1.0.jar", "com%2Facme%2Ftools%2Flib%2F1.0%2Flib-1.0.pom",
		"com%2Facme%2Ftools%2Flib%2F1.1%2Flib-1.1.jar",
	}, e.fileIDs(t, "jars"))

	e.denyDownloads(t, "jars")
	assert.Equal(t, http.StatusForbidden, e.call(t, http.MethodGet, base+".jar", "", nil).code)
}

func TestPackageFormatsPersist(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "web", artifactregistrypb.Repository_NPM)
	e.formatRepo(t, "py", artifactregistrypb.Repository_PYTHON)
	e.formatRepo(t, "jars", artifactregistrypb.Repository_MAVEN)

	require.Equal(t, http.StatusOK, e.call(t, http.MethodPut, "/npm/"+project+"/web/pkg", "application/json",
		npmPublishBody(t, "pkg", "1.0.0", "latest", []byte("tgz"))).code)

	ctype, body := twineBody(
		t,
		map[string]string{":action": "file_upload", "name": "p", "version": "1"},
		"p-1.tar.gz",
		[]byte("sd"),
	)
	require.Equal(t, http.StatusOK, e.call(t, http.MethodPost, "/python/"+project+"/py/", ctype, body).code)
	require.Equal(t, http.StatusCreated,
		e.call(t, http.MethodPut, "/maven/"+project+"/jars/g/a/1/a-1.jar", "", []byte("jar")).code)

	snap := e.handler.Snapshot(context.Background())

	restored := newEnv(t, "")
	require.NoError(t, restored.handler.Restore(context.Background(), snap))

	pk := restored.call(t, http.MethodGet, "/npm/"+project+"/web/pkg", "", nil)
	require.Equal(t, http.StatusOK, pk.code)
	assert.Contains(t, pk.body, `"latest":"1.0.0"`)
	assert.Equal(t, "tgz", restored.call(t, http.MethodGet, "/npm/"+project+"/web/pkg/-/pkg-1.0.0.tgz", "", nil).body)
	assert.Equal(t, "sd",
		restored.call(t, http.MethodGet, "/python/"+project+"/py/packages/p/1/p-1.tar.gz", "", nil).body)
	assert.Equal(t, "jar", restored.call(t, http.MethodGet, "/maven/"+project+"/jars/g/a/1/a-1.jar", "", nil).body)
	assert.Equal(t, []string{"pkg"}, restored.packageIDs(t, "web"))

	repo, err := restored.client.GetRepository(
		t.Context(),
		&artifactregistrypb.GetRepositoryRequest{Name: repoName("jars")},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 3, repo.GetSizeBytes())

	_ = gcpartifactregistry.NewInMemoryBackend
}
