package gcpartifactregistry_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/api/iterator"
)

func getBody(t *testing.T, url string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, body
}

func gunzip(t *testing.T, data []byte) []byte {
	t.Helper()

	zr, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out, err := io.ReadAll(zr)
	require.NoError(t, err)

	return out
}

func sum256(b []byte) string {
	s := sha256.Sum256(b)

	return hex.EncodeToString(s[:])
}

func uploadApt(t *testing.T, svc *arapi.Service, repo string, deb []byte) *arapi.UploadAptArtifactResponse {
	t.Helper()

	res, err := svc.Projects.Locations.Repositories.AptArtifacts.Upload(repoName(repo), &arapi.UploadAptArtifactRequest{}).
		Media(bytes.NewReader(deb)).Context(t.Context()).Do()
	require.NoError(t, err)
	require.NotNil(t, res.Operation)
	assert.True(t, res.Operation.Done)

	var out arapi.UploadAptArtifactResponse
	require.NoError(t, jsonUnmarshal(res.Operation.Response, &out))

	return &out
}

func TestAptRepositoryServesUploadedPackages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-repo", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})

	svc := restService(t, e)
	hello := makeDeb(t, "hello", "1.0-1", "amd64")
	tools := makeDeb(t, "tools-all", "2.1", "all")

	got := uploadApt(t, svc, "apt-repo", hello)
	require.Len(t, got.AptArtifacts, 1)
	assert.Equal(t, "hello", got.AptArtifacts[0].PackageName)
	assert.Equal(t, "amd64", got.AptArtifacts[0].Architecture)
	assert.Equal(t, "main", got.AptArtifacts[0].Component)
	assert.Equal(t, "BINARY", got.AptArtifacts[0].PackageType)

	uploadApt(t, svc, "apt-repo", tools)

	_, err := svc.Projects.Locations.Repositories.AptArtifacts.Upload(repoName("apt-repo"), &arapi.UploadAptArtifactRequest{}).
		Media(strings.NewReader("not a deb")).Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, httpCode(err))

	pkgs := e.client.ListPackages(t.Context(), &artifactregistrypb.ListPackagesRequest{Parent: repoName("apt-repo")})
	var names []string

	for {
		p, nerr := pkgs.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		names = append(names, p.GetDisplayName())
	}

	assert.ElementsMatch(t, []string{"hello", "tools-all"}, names)

	base := "http://127.0.0.1:" + e.port + "/apt/projects/" + project

	code, release := getBody(t, base+"/dists/apt-repo/Release")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "apt-repo", fieldOf(string(release), "Codename"))
	assert.Equal(t, "amd64", fieldOf(string(release), "Architectures"), "arch all adds no architecture")

	_, packages := getBody(t, base+"/dists/apt-repo/main/binary-amd64/Packages")
	_, packagesGz := getBody(t, base+"/dists/apt-repo/main/binary-amd64/Packages.gz")
	assert.Equal(t, packages, gunzip(t, packagesGz))

	stanzas := strings.Split(strings.TrimSpace(string(packages)), "\n\n")
	require.Len(t, stanzas, 2, "the amd64 package and the arch-all package")

	byName := map[string]string{}
	for _, s := range stanzas {
		byName[fieldOf(s, "Package")] = s
	}

	h := byName["hello"]
	assert.Equal(t, "1.0-1", fieldOf(h, "Version"))
	assert.Equal(t, "libc6", fieldOf(h, "Depends"))
	assert.Equal(t, sum256(hello), fieldOf(h, "SHA256"))
	assert.Equal(t, "pool/apt-repo/main/h/hello/hello_1.0-1_amd64.deb", fieldOf(h, "Filename"))
	assert.Contains(t, h, "\n long description line")

	for _, idx := range []struct {
		name string
		data []byte
	}{{"main/binary-amd64/Packages", packages}, {"main/binary-amd64/Packages.gz", packagesGz}} {
		assert.Contains(t, string(release), sum256(idx.data)+" ", idx.name)
		assert.Contains(t, string(release), " "+idx.name+"\n", idx.name)
	}

	code, deb := getBody(t, base+"/"+fieldOf(h, "Filename"))
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, hello, deb)

	code, _ = getBody(t, base+"/dists/apt-repo/main/binary-amd64/Nope")
	assert.Equal(t, http.StatusNotFound, code)

	hostBase := "http://127.0.0.1:" + e.port
	req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, hostBase+"/projects/"+project+"/dists/apt-repo/Release", nil)
	require.NoError(t, rerr)

	req.Host = "us-central1-apt.pkg.dev"

	resp, rerr := http.DefaultClient.Do(req)
	require.NoError(t, rerr)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the pkg.dev host form serves the same repository")
}

func TestYumRepositoryServesUploadedPackages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "yum-repo", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_YUM})

	svc := restService(t, e)
	rpm := makeRPM(t, "hello", "1.0", "2", "x86_64")

	res, err := svc.Projects.Locations.Repositories.YumArtifacts.Upload(repoName("yum-repo"), &arapi.UploadYumArtifactRequest{}).
		Media(bytes.NewReader(rpm)).Context(t.Context()).Do()
	require.NoError(t, err)

	var up arapi.UploadYumArtifactResponse
	require.NoError(t, jsonUnmarshal(res.Operation.Response, &up))
	require.Len(t, up.YumArtifacts, 1)
	assert.Equal(t, "hello", up.YumArtifacts[0].PackageName)
	assert.Equal(t, "x86_64", up.YumArtifacts[0].Architecture)

	_, err = svc.Projects.Locations.Repositories.YumArtifacts.Upload(repoName("yum-repo"), &arapi.UploadYumArtifactRequest{}).
		Media(strings.NewReader("not an rpm")).Context(t.Context()).Do()
	require.Error(t, err)

	base := "http://127.0.0.1:" + e.port + "/yum/projects/" + project + "/yum-repo"

	code, repomd := getBody(t, base+"/repodata/repomd.xml")
	require.Equal(t, http.StatusOK, code)

	var md struct {
		Data []struct {
			Type         string `xml:"type,attr"`
			Checksum     string `xml:"checksum"`
			OpenChecksum string `xml:"open-checksum"`
			Location     struct {
				Href string `xml:"href,attr"`
			} `xml:"location"`
			Size int `xml:"size"`
		} `xml:"data"`
	}
	require.NoError(t, xml.Unmarshal(repomd, &md))
	require.Len(t, md.Data, 3)

	var primaryRaw []byte

	for _, d := range md.Data {
		code, gz := getBody(t, base+"/"+d.Location.Href)
		require.Equal(t, http.StatusOK, code, d.Type)
		assert.Equal(t, d.Checksum, sum256(gz), d.Type)
		assert.Equal(t, len(gz), d.Size, d.Type)

		raw := gunzip(t, gz)
		assert.Equal(t, d.OpenChecksum, sum256(raw), d.Type)

		if d.Type == "primary" {
			primaryRaw = raw
		}
	}

	var primary struct {
		Packages []struct {
			Name    string `xml:"name"`
			Arch    string `xml:"arch"`
			Version struct {
				Ver string `xml:"ver,attr"`
				Rel string `xml:"rel,attr"`
			} `xml:"version"`
			Checksum string `xml:"checksum"`
			Location struct {
				Href string `xml:"href,attr"`
			} `xml:"location"`
			Format struct {
				License  string `xml:"license"`
				Requires struct {
					Entries []struct {
						Name  string `xml:"name,attr"`
						Flags string `xml:"flags,attr"`
					} `xml:"entry"`
				} `xml:"requires"`
			} `xml:"format"`
		} `xml:"package"`
	}
	require.NoError(t, xml.Unmarshal(primaryRaw, &primary))
	require.Len(t, primary.Packages, 1)

	pkg := primary.Packages[0]
	assert.Equal(t, "hello", pkg.Name)
	assert.Equal(t, "x86_64", pkg.Arch)
	assert.Equal(t, "1.0", pkg.Version.Ver)
	assert.Equal(t, "2", pkg.Version.Rel)
	assert.Equal(t, sum256(rpm), strings.TrimSpace(pkg.Checksum))
	assert.Equal(t, "MIT", pkg.Format.License)

	var reqNames []string
	for _, r := range pkg.Format.Requires.Entries {
		reqNames = append(reqNames, r.Name+":"+r.Flags)
	}

	assert.Contains(t, reqNames, "bash:GE")

	code, got := getBody(t, base+"/"+pkg.Location.Href)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, rpm, got)
}
