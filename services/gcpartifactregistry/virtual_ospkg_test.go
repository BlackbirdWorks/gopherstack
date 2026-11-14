package gcpartifactregistry_test

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVirtualAptMergesUpstreamIndices(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	apt := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT}
	e.createRepo(t, "low", apt)
	e.createRepo(t, "high", apt)
	e.createRepo(t, "virt", virtualRepo(artifactregistrypb.Repository_APT, [2]any{"low", 1}, [2]any{"high", 10}))

	svc := restService(t, e)
	uploadApt(t, svc, "low", makeDebWithPayload(t, "shared", "1.0", "amd64", "low payload"))
	uploadApt(t, svc, "low", makeDeb(t, "only-low", "1.0", "amd64"))
	uploadApt(t, svc, "high", makeDebWithPayload(t, "shared", "1.0", "amd64", "high payload"))
	uploadApt(t, svc, "high", makeDeb(t, "only-high", "2.0", "amd64"))

	base := "http://127.0.0.1:" + e.port + "/apt/projects/" + project
	dists := base + "/dists/virt/"

	release := string(fetchOK(t, dists+"Release"))
	assert.Equal(t, "virt", fieldOf(release, "Codename"))

	packages := string(fetchOK(t, dists+"main/binary-amd64/Packages"))
	assert.Equal(t, 3, strings.Count(packages, "Package: "), "shared once, plus one package from each upstream")
	assert.Contains(t, packages, "Package: only-low")
	assert.Contains(t, packages, "Package: only-high")
	assert.Contains(t, packages, "Filename: pool/high/main/s/shared/shared_1.0_amd64.deb", "higher priority wins")
	assert.NotContains(t, packages, "pool/low/main/s/shared")

	assert.Contains(t, release, sum256([]byte(packages))+" "+padSizeOf(len(packages))+" main/binary-amd64/Packages")

	keys := readKey(t, fetchOK(t, dists+"apt-key.asc"))
	block, rest := clearsign.Decode(fetchOK(t, dists+"InRelease"))
	require.NotNil(t, block)
	assert.Empty(t, bytes.TrimSpace(rest))

	_, err := block.VerifySignature(keys, nil)
	require.NoError(t, err, "signed with the virtual repository's key")

	got := fetchOK(t, base+"/pool/high/main/s/shared/shared_1.0_amd64.deb")
	assert.Contains(t, string(got), "debian-binary")
	assert.Equal(t, http.StatusNotFound, statusOf(t, base+"/pool/nope/main/x/x/x_1_amd64.deb"))
}

func statusOf(t *testing.T, url string) int {
	t.Helper()

	code, _ := getBody(t, url)

	return code
}

func TestVirtualYumMergesUpstreamIndices(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	yum := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_YUM}
	e.createRepo(t, "low", yum)
	e.createRepo(t, "high", yum)
	e.createRepo(t, "virt", virtualRepo(artifactregistrypb.Repository_YUM, [2]any{"low", 1}, [2]any{"high", 10}))

	svc := restService(t, e)

	for repo, name := range map[string]string{"low": "pkg-low", "high": "pkg-high"} {
		_, err := yumUpload(svc, repo).Media(bytes.NewReader(makeRPM(t, name, "1.0", "1", "x86_64"))).
			Context(t.Context()).Do()
		require.NoError(t, err)
	}

	base := "http://127.0.0.1:" + e.port + "/yum/projects/" + project + "/virt/"

	var md struct {
		Data []struct {
			Type     string `xml:"type,attr"`
			Location struct {
				Href string `xml:"href,attr"`
			} `xml:"location"`
		} `xml:"data"`
	}

	require.NoError(t, xml.Unmarshal(fetchOK(t, base+"repodata/repomd.xml"), &md))

	for _, d := range md.Data {
		if d.Type != "primary" {
			continue
		}

		primary := string(gunzip(t, fetchOK(t, base+d.Location.Href)))
		assert.Contains(t, primary, "<name>pkg-low</name>")
		assert.Contains(t, primary, "<name>pkg-high</name>")
		assert.Contains(t, primary, `packages="2"`)
	}

	keys := readKey(t, fetchOK(t, base+"repodata/repomd.xml.key"))
	assert.NotEmpty(t, keys)
	assert.NotEmpty(t, fetchOK(t, base+"repodata/repomd.xml.asc"))
	assert.NotEmpty(t, fetchOK(t, base+"Packages/pkg-low-1.0-1.x86_64.rpm"))
}

func padSizeOf(n int) string {
	s := strconv.Itoa(n)

	return strings.Repeat(" ", 16-len(s)) + s
}
