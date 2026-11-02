package gcpartifactregistry_test

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // Maven .sha1 checksum
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type apb = artifactregistrypb.RemoteRepositoryConfig

func remoteRepo(format artifactregistrypb.Repository_Format, cfg *apb) *artifactregistrypb.Repository {
	return &artifactregistrypb.Repository{
		Format: format, Mode: artifactregistrypb.Repository_REMOTE_REPOSITORY,
		ModeConfig: &artifactregistrypb.Repository_RemoteRepositoryConfig{RemoteRepositoryConfig: cfg},
	}
}

func npmUpstream(uri string) *apb {
	return &apb{RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_NpmRepository_{
		NpmRepository: &artifactregistrypb.RemoteRepositoryConfig_NpmRepository{
			Upstream: &artifactregistrypb.RemoteRepositoryConfig_NpmRepository_CustomRepository_{
				CustomRepository: &artifactregistrypb.RemoteRepositoryConfig_NpmRepository_CustomRepository{Uri: uri},
			},
		},
	}}
}

func pythonUpstream(uri string) *apb {
	return &apb{RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_PythonRepository_{
		PythonRepository: &artifactregistrypb.RemoteRepositoryConfig_PythonRepository{
			Upstream: &artifactregistrypb.RemoteRepositoryConfig_PythonRepository_CustomRepository_{
				CustomRepository: &artifactregistrypb.RemoteRepositoryConfig_PythonRepository_CustomRepository{Uri: uri},
			},
		},
	}}
}

func mavenUpstream(uri string) *apb {
	return &apb{RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_MavenRepository_{
		MavenRepository: &artifactregistrypb.RemoteRepositoryConfig_MavenRepository{
			Upstream: &artifactregistrypb.RemoteRepositoryConfig_MavenRepository_CustomRepository_{
				CustomRepository: &artifactregistrypb.RemoteRepositoryConfig_MavenRepository_CustomRepository{Uri: uri},
			},
		},
	}}
}

func aptUpstream(uri string) *apb {
	return &apb{RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_{
		AptRepository: &artifactregistrypb.RemoteRepositoryConfig_AptRepository{
			Upstream: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_CustomRepository_{
				CustomRepository: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_CustomRepository{Uri: uri},
			},
		},
	}}
}

func versionIDs(t *testing.T, e *env, repo, pkg string) []string {
	t.Helper()

	var out []string

	it := e.client.ListVersions(t.Context(), &artifactregistrypb.ListVersionsRequest{
		Parent: repoName(repo) + "/packages/" + pkg,
	})

	for {
		v, err := it.Next()
		if err != nil {
			return out
		}

		out = append(out, v.GetName()[strings.LastIndex(v.GetName(), "/")+1:])
	}
}

func TestRemoteNPMRepository(t *testing.T) {
	t.Parallel()

	tarball := tarGz(t, map[string]string{"package/package.json": `{"name":"left-pad","version":"1.3.0","description":"pads"}`})

	var tarballHits atomic.Int32

	var up *httptest.Server

	up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/left-pad":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "left-pad", "dist-tags": map[string]string{"latest": "1.3.0"},
				"versions": map[string]any{"1.3.0": map[string]any{
					"name": "left-pad", "version": "1.3.0",
					"dist": map[string]any{"tarball": up.URL + "/left-pad/-/left-pad-1.3.0.tgz"},
				}},
			})
		case "/left-pad/-/left-pad-1.3.0.tgz":
			tarballHits.Add(1)
			_, _ = w.Write(tarball)
		default:
			http.NotFound(w, r)
		}
	}))

	e := newEnv(t, "")
	e.createRepo(t, "npm-remote", remoteRepo(artifactregistrypb.Repository_NPM, npmUpstream(up.URL)))

	base := "http://127.0.0.1:" + e.port + "/npm/" + project + "/npm-remote"

	code, body := getBody(t, base+"/left-pad")
	assert.Equal(t, http.StatusBadGateway, code, "upstream access is off by default")
	assert.Contains(t, string(body), "disabled")

	e.handler.Backend.SetRemoteUpstreams(true)

	code, body = getBody(t, base+"/left-pad")
	require.Equal(t, http.StatusOK, code)

	var doc struct {
		Versions map[string]struct {
			Dist struct{ Tarball string } `json:"dist"`
		} `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(body, &doc))
	assert.Equal(t, base+"/left-pad/-/left-pad-1.3.0.tgz", doc.Versions["1.3.0"].Dist.Tarball)

	code, got := getBody(t, doc.Versions["1.3.0"].Dist.Tarball)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, tarball, got)
	assert.Equal(t, []string{"1.3.0"}, versionIDs(t, e, "npm-remote", "left-pad"), "the fetched tarball is cached as a version")

	up.Close()

	code, got = getBody(t, doc.Versions["1.3.0"].Dist.Tarball)
	assert.Equal(t, http.StatusOK, code, "served from the cache once the upstream is gone")
	assert.Equal(t, tarball, got)
	assert.Equal(t, int32(1), tarballHits.Load())

	code, _ = getBody(t, base+"/left-pad/-/left-pad-9.9.9.tgz")
	assert.NotEqual(t, http.StatusOK, code)
}

func TestRemotePythonRepository(t *testing.T) {
	t.Parallel()

	wheel := []byte("fake wheel bytes")

	var up *httptest.Server

	up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/simple/requests/":
			_, _ = w.Write([]byte(`<html><body><a href="` + up.URL + `/files/requests-2.0.0-py3-none-any.whl#sha256=` + sum256(wheel) +
				`" data-requires-python="&gt;=3.8">requests-2.0.0-py3-none-any.whl</a></body></html>`))
		case "/files/requests-2.0.0-py3-none-any.whl":
			_, _ = w.Write(wheel)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(up.Close)

	e := newEnv(t, "")
	e.handler.Backend.SetRemoteUpstreams(true)
	e.createRepo(t, "py-remote", remoteRepo(artifactregistrypb.Repository_PYTHON, pythonUpstream(up.URL)))

	base := "http://127.0.0.1:" + e.port + "/python/" + project + "/py-remote"

	code, page := getBody(t, base+"/simple/Requests/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(page), base[len("http://127.0.0.1:"+e.port):]+"/packages/requests/2.0.0/requests-2.0.0-py3-none-any.whl#sha256="+sum256(wheel))
	assert.Contains(t, string(page), `data-requires-python="&gt;=3.8"`)

	code, got := getBody(t, base+"/packages/requests/2.0.0/requests-2.0.0-py3-none-any.whl")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, wheel, got)
	assert.Equal(t, []string{"2.0.0"}, versionIDs(t, e, "py-remote", "requests"))

	code, _ = getBody(t, base+"/packages/requests/9.0.0/requests-9.0.0.tar.gz")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestRemoteMavenRepository(t *testing.T) {
	t.Parallel()

	jar := []byte("jar bytes")

	var hits atomic.Int32

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/com/acme/lib/1.0/lib-1.0.jar":
			hits.Add(1)
			_, _ = w.Write(jar)
		case "/com/acme/lib/maven-metadata.xml":
			_, _ = w.Write([]byte("<metadata><artifactId>lib</artifactId></metadata>"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(up.Close)

	e := newEnv(t, "")
	e.handler.Backend.SetRemoteUpstreams(true)
	e.createRepo(t, "mvn-remote", remoteRepo(artifactregistrypb.Repository_MAVEN, mavenUpstream(up.URL)))

	base := "http://127.0.0.1:" + e.port + "/maven/" + project + "/mvn-remote"

	code, got := getBody(t, base+"/com/acme/lib/1.0/lib-1.0.jar")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, jar, got)

	getBody(t, base+"/com/acme/lib/1.0/lib-1.0.jar")
	assert.Equal(t, int32(1), hits.Load(), "the second read comes from the cache")

	sum := sha1.Sum(jar) //nolint:gosec // Maven .sha1 checksum
	code, got = getBody(t, base+"/com/acme/lib/1.0/lib-1.0.jar.sha1")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, hex.EncodeToString(sum[:]), string(got))

	code, got = getBody(t, base+"/com/acme/lib/maven-metadata.xml")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(got), "<artifactId>lib</artifactId>")

	assert.Equal(t, []string{"1.0"}, versionIDs(t, e, "mvn-remote", "com.acme:lib"))

	code, _ = getBody(t, base+"/com/acme/lib/2.0/lib-2.0.jar")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestRemoteAptRepository(t *testing.T) {
	t.Parallel()

	deb := makeDeb(t, "tool", "1.0", "amd64")

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dists/stable/Release":
			_, _ = w.Write([]byte("Suite: stable\n"))
		case "/pool/main/t/tool/tool_1.0_amd64.deb":
			_, _ = w.Write(deb)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(up.Close)

	e := newEnv(t, "")
	e.handler.Backend.SetRemoteUpstreams(true)
	e.createRepo(t, "apt-remote", remoteRepo(artifactregistrypb.Repository_APT, aptUpstream(up.URL)))

	base := "http://127.0.0.1:" + e.port + "/apt/remote/" + project + "/apt-remote"

	code, got := getBody(t, base+"/dists/stable/Release")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Suite: stable\n", string(got))

	code, got = getBody(t, base+"/pool/main/t/tool/tool_1.0_amd64.deb")
	require.Equal(t, http.StatusOK, code)
	assert.True(t, bytes.Equal(deb, got))
	assert.Equal(t, []string{"1.0"}, versionIDs(t, e, "apt-remote", "tool"))
}

func virtualRepo(format artifactregistrypb.Repository_Format, ups ...[2]any) *artifactregistrypb.Repository {
	var policies []*artifactregistrypb.UpstreamPolicy
	for i, u := range ups {
		policies = append(policies, &artifactregistrypb.UpstreamPolicy{
			Id: "up" + string(rune('a'+i)), Repository: repoName(u[0].(string)), Priority: int32(u[1].(int)),
		})
	}

	return &artifactregistrypb.Repository{
		Format: format, Mode: artifactregistrypb.Repository_VIRTUAL_REPOSITORY,
		ModeConfig: &artifactregistrypb.Repository_VirtualRepositoryConfig{
			VirtualRepositoryConfig: &artifactregistrypb.VirtualRepositoryConfig{UpstreamPolicies: policies},
		},
	}
}

func putMaven(t *testing.T, url string, body []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := testHTTP.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestVirtualMavenRepositoryMergesByPriority(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	mvn := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_MAVEN}
	e.createRepo(t, "low", mvn)
	e.createRepo(t, "high", mvn)
	e.createRepo(t, "virt", virtualRepo(artifactregistrypb.Repository_MAVEN, [2]any{"low", 1}, [2]any{"high", 10}))

	root := "http://127.0.0.1:" + e.port + "/maven/" + project + "/"
	art := "/com/acme/lib/1.0/lib-1.0.jar"

	putMaven(t, root+"low"+art, []byte("from low"))
	putMaven(t, root+"low/com/acme/only-low/1.0/only-low-1.0.jar", []byte("only low"))

	code, got := getBody(t, root+"virt"+art)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "from low", string(got), "a lower-priority upstream answers when the higher one lacks the artifact")

	putMaven(t, root+"high"+art, []byte("from high"))

	_, got = getBody(t, root+"virt"+art)
	assert.Equal(t, "from high", string(got), "the greater priority value wins")

	_, got = getBody(t, root+"virt/com/acme/only-low/1.0/only-low-1.0.jar")
	assert.Equal(t, "only low", string(got), "reads merge across upstreams")

	code, _ = getBody(t, root+"virt/com/acme/none/1.0/none-1.0.jar")
	assert.Equal(t, http.StatusNotFound, code)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, root+"virt"+art, strings.NewReader("x"))
	require.NoError(t, err)

	resp, err := testHTTP.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "virtual repositories are read-only")
}

func TestVirtualNPMRewritesTarballURLs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "npm-a", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_NPM})
	e.createRepo(t, "npm-virt", virtualRepo(artifactregistrypb.Repository_NPM, [2]any{"npm-a", 1}))

	tarball := tarGz(t, map[string]string{"package/package.json": `{"name":"pkg","version":"1.0.0"}`})
	publishNPM(t, e, "npm-a", "pkg", "1.0.0", tarball)

	base := "http://127.0.0.1:" + e.port + "/npm/" + project + "/npm-virt"

	code, body := getBody(t, base+"/pkg")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), base+"/pkg/-/pkg-1.0.0.tgz", "tarball URLs point at the virtual repository")

	code, got := getBody(t, base+"/pkg/-/pkg-1.0.0.tgz")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, tarball, got)
}

func publishNPM(t *testing.T, e *env, repo, name, version string, tarball []byte) {
	t.Helper()

	url := "http://127.0.0.1:" + e.port + "/npm/" + project + "/" + repo + "/" + name

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url,
		bytes.NewReader(npmPublishBody(t, name, version, "latest", tarball)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := testHTTP.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
