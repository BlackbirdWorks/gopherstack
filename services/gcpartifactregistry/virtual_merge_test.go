package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVirtualNPMMergesPackuments(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	npm := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_NPM}
	e.createRepo(t, "npm-low", npm)
	e.createRepo(t, "npm-high", npm)
	e.createRepo(
		t,
		"npm-virt",
		virtualRepo(artifactregistrypb.Repository_NPM, [2]any{"npm-low", 1}, [2]any{"npm-high", 10}),
	)

	pkg := func(version string) []byte {
		return tarGz(t, map[string]string{"package/package.json": `{"name":"pkg","version":"` + version + `"}`})
	}

	publishNPM(t, e, "npm-low", "pkg", "1.0.0", pkg("1.0.0"))
	publishNPM(t, e, "npm-high", "pkg", "2.0.0", pkg("2.0.0"))

	code, body := getBody(t, "http://127.0.0.1:"+e.port+"/npm/"+project+"/npm-virt/pkg")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), `"1.0.0"`)
	assert.Contains(t, string(body), `"2.0.0"`)
	assert.Contains(t, string(body), "npm-virt/pkg/-/pkg-2.0.0.tgz")
}

func TestVirtualMavenMergesMetadata(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	mvn := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_MAVEN}
	e.createRepo(t, "m-low", mvn)
	e.createRepo(t, "m-high", mvn)
	e.createRepo(
		t,
		"m-virt",
		virtualRepo(artifactregistrypb.Repository_MAVEN, [2]any{"m-low", 1}, [2]any{"m-high", 10}),
	)

	root := "http://127.0.0.1:" + e.port + "/maven/" + project + "/"
	putMaven(t, root+"m-low/com/acme/lib/1.0/lib-1.0.jar", []byte("one"))
	putMaven(t, root+"m-high/com/acme/lib/2.0/lib-2.0.jar", []byte("two"))

	code, body := getBody(t, root+"m-virt/com/acme/lib/maven-metadata.xml")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), "<version>1.0</version>")
	assert.Contains(t, string(body), "<version>2.0</version>")
	assert.Contains(t, string(body), "<latest>2.0</latest>")
}

func TestVirtualPythonMergesSimplePages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.formatRepo(t, "py-low", artifactregistrypb.Repository_PYTHON)
	e.formatRepo(t, "py-high", artifactregistrypb.Repository_PYTHON)
	e.createRepo(
		t,
		"py-virt",
		virtualRepo(artifactregistrypb.Repository_PYTHON, [2]any{"py-low", 1}, [2]any{"py-high", 10}),
	)

	upload := func(repo, version, filename string) {
		ctype, body := twineBody(t, map[string]string{
			":action": "file_upload", "name": "lib", "version": version,
		}, filename, []byte("data-"+version))
		r := e.call(t, http.MethodPost, "/python/"+project+"/"+repo+"/", ctype, body)
		require.Equal(t, http.StatusOK, r.code, r.body)
	}

	upload("py-low", "1.0", "lib-1.0.tar.gz")
	upload("py-high", "2.0", "lib-2.0.tar.gz")

	page := e.call(t, http.MethodGet, "/python/"+project+"/py-virt/simple/lib/", "", nil)
	require.Equal(t, http.StatusOK, page.code)
	assert.Contains(t, page.body, ">lib-1.0.tar.gz</a>")
	assert.Contains(t, page.body, ">lib-2.0.tar.gz</a>")
	assert.Contains(t, page.body, "py-virt/packages/lib/1.0/lib-1.0.tar.gz")
}
