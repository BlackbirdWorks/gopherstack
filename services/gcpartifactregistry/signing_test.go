package gcpartifactregistry_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
)

func fetchOK(t *testing.T, url string) []byte {
	t.Helper()

	code, body := getBody(t, url)
	require.Equal(t, http.StatusOK, code, url)

	return body
}

func readKey(t *testing.T, armored []byte) openpgp.EntityList {
	t.Helper()

	keys, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armored))
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	return keys
}

func uploadAptTo(t *testing.T, e *env, repo, name, version string) {
	t.Helper()

	uploadApt(t, restService(t, e), repo, makeDeb(t, name, version, "amd64"))
}

func TestAptReleaseIsSigned(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-sign", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})
	e.createRepo(t, "apt-other", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})
	uploadAptTo(t, e, "apt-sign", "hello", "1.0-1")
	uploadAptTo(t, e, "apt-other", "hello", "1.0-1")

	dists := "http://127.0.0.1:" + e.port + "/apt/projects/" + project + "/dists/apt-sign/"
	release := fetchOK(t, dists+"Release")
	armoredKey := fetchOK(t, dists+"apt-key.asc")
	keys := readKey(t, armoredKey)

	t.Run("InRelease", func(t *testing.T) {
		t.Parallel()

		block, rest := clearsign.Decode(fetchOK(t, dists+"InRelease"))
		require.NotNil(t, block)
		assert.Empty(t, bytes.TrimSpace(rest))
		assert.Equal(t, strings.TrimSpace(string(release)), strings.TrimSpace(string(block.Plaintext)))

		_, err := block.VerifySignature(keys, nil)
		require.NoError(t, err)
	})

	t.Run("Release.gpg", func(t *testing.T) {
		t.Parallel()

		_, err := openpgp.CheckArmoredDetachedSignature(keys, bytes.NewReader(release),
			bytes.NewReader(fetchOK(t, dists+"Release.gpg")), nil)
		require.NoError(t, err)
	})

	t.Run("tampered release fails", func(t *testing.T) {
		t.Parallel()

		_, err := openpgp.CheckArmoredDetachedSignature(keys, bytes.NewReader(append([]byte("x"), release...)),
			bytes.NewReader(fetchOK(t, dists+"Release.gpg")), nil)
		require.Error(t, err)
	})

	t.Run("binary key matches armored key", func(t *testing.T) {
		t.Parallel()

		bin, err := openpgp.ReadKeyRing(bytes.NewReader(fetchOK(t, dists+"apt-key.gpg")))
		require.NoError(t, err)
		require.NotEmpty(t, bin)
		assert.Equal(t, keys[0].PrimaryKey.Fingerprint, bin[0].PrimaryKey.Fingerprint)
	})

	t.Run("key is stable and per repository", func(t *testing.T) {
		t.Parallel()

		again := readKey(t, fetchOK(t, dists+"apt-key.asc"))
		assert.Equal(t, keys[0].PrimaryKey.Fingerprint, again[0].PrimaryKey.Fingerprint)

		other := readKey(
			t,
			fetchOK(t, "http://127.0.0.1:"+e.port+"/apt/projects/"+project+"/dists/apt-other/apt-key.asc"),
		)
		assert.NotEqual(t, keys[0].PrimaryKey.Fingerprint, other[0].PrimaryKey.Fingerprint)

		_, err := openpgp.CheckArmoredDetachedSignature(other, bytes.NewReader(release),
			bytes.NewReader(fetchOK(t, dists+"Release.gpg")), nil)
		require.Error(t, err, "another repository's key must not verify")
	})
}

func TestSigningKeySurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-keep", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})
	uploadAptTo(t, e, "apt-keep", "hello", "1.0-1")

	url := "http://127.0.0.1:" + e.port + "/apt/projects/" + project + "/dists/apt-keep/apt-key.asc"
	before := readKey(t, fetchOK(t, url))

	snap := e.handler.Backend.Snapshot(t.Context())
	require.NoError(t, e.handler.Backend.Restore(t.Context(), snap))

	after := readKey(t, fetchOK(t, url))
	assert.Equal(t, before[0].PrimaryKey.Fingerprint, after[0].PrimaryKey.Fingerprint)
	assert.NotContains(t, string(fetchOK(t, url)), "PRIVATE")
}

func TestYumRepomdIsSigned(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "yum-sign", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_YUM})

	_, err := restService(t, e).Projects.Locations.Repositories.YumArtifacts.
		Upload(repoName("yum-sign"), &arapi.UploadYumArtifactRequest{}).
		Media(bytes.NewReader(makeRPM(t, "hello", "1.0", "1", "noarch"))).Context(t.Context()).Do()
	require.NoError(t, err)

	base := "http://127.0.0.1:" + e.port + "/yum/projects/" + project + "/yum-sign/repodata/"
	repomd := fetchOK(t, base+"repomd.xml")
	keys := readKey(t, fetchOK(t, base+"repomd.xml.key"))

	_, err = openpgp.CheckArmoredDetachedSignature(keys, bytes.NewReader(repomd),
		bytes.NewReader(fetchOK(t, base+"repomd.xml.asc")), nil)
	require.NoError(t, err)
}
