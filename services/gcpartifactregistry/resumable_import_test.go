package gcpartifactregistry_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
)

type memStore map[string][]byte

var errNoObject = errors.New("no such object")

func (m memStore) ReadObject(bucket, object string) ([]byte, error) {
	if d, ok := m[bucket+"/"+object]; ok {
		return d, nil
	}

	return nil, errNoObject
}

func (m memStore) ListObjects(bucket, prefix string) ([]string, error) {
	var out []string

	for k := range m {
		if o, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(o, prefix) {
			out = append(out, o)
		}
	}

	sort.Strings(out)

	return out, nil
}

func TestResumableUploads(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-res", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})
	e.createRepo(t, "gen-res", genericRepo())

	svc := restService(t, e)
	deb := makeDeb(t, "resumable", "3.0", "amd64")

	res, err := svc.Projects.Locations.Repositories.AptArtifacts.Upload(repoName("apt-res"), &arapi.UploadAptArtifactRequest{}).
		ResumableMedia(t.Context(), bytes.NewReader(deb), int64(len(deb)), "application/octet-stream").
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.True(t, res.Operation.Done)

	payload := bytes.Repeat([]byte("0123456789abcdef"), 4096)

	gen, err := svc.Projects.Locations.Repositories.GenericArtifacts.Upload(repoName("gen-res"),
		&arapi.UploadGenericArtifactRequest{PackageId: "blob", VersionId: "1.0", Filename: "blob.bin"}).
		ResumableMedia(t.Context(), bytes.NewReader(payload), int64(len(payload)), "application/octet-stream").
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.True(t, gen.Operation.Done)

	got, err := download(t, svc, repoName("gen-res")+"/files/blob:1.0:blob.bin")
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestResumableChunkProtocol(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "gen-chunks", genericRepo())

	endpoint := "http://127.0.0.1:" + e.port + "/upload/v1/" + repoName("gen-chunks") + "/genericArtifacts:create"
	payload := []byte("chunked resumable content")

	do := func(method, target string, body []byte, hdr map[string]string) *http.Response {
		req, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
		require.NoError(t, err)

		for k, v := range hdr {
			req.Header.Set(k, v)
		}

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		return resp
	}

	init := do(http.MethodPost, endpoint+"?uploadType=resumable",
		[]byte(`{"packageId":"chunky","versionId":"1","filename":"c.txt"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, init.StatusCode)

	loc := init.Header.Get("Location")
	u, err := url.Parse(loc)
	require.NoError(t, err)
	assert.Equal(t, "resumable", u.Query().Get("uploadType"))
	require.NotEmpty(t, u.Query().Get("upload_id"))

	total := strconv.Itoa(len(payload))
	first := do(http.MethodPut, loc, payload[:10], map[string]string{"Content-Range": "bytes 0-9/" + total})
	assert.Equal(t, 308, first.StatusCode)
	assert.Equal(t, "bytes=0-9", first.Header.Get("Range"))

	status := do(http.MethodPut, loc, nil, map[string]string{"Content-Range": "bytes */" + total})
	assert.Equal(t, 308, status.StatusCode)
	assert.Equal(t, "bytes=0-9", status.Header.Get("Range"), "a status query reports the persisted bytes")

	gap := do(http.MethodPut, loc, payload[15:], map[string]string{"Content-Range": "bytes 15-" + strconv.Itoa(len(payload)-1) + "/" + total})
	assert.Equal(t, 308, gap.StatusCode, "a chunk that skips bytes is not appended")

	last := do(http.MethodPut, loc, payload[10:], map[string]string{"Content-Range": "bytes 10-" + strconv.Itoa(len(payload)-1) + "/" + total})
	assert.Equal(t, http.StatusOK, last.StatusCode)

	code, _ := getBody(t, "http://127.0.0.1:"+e.port+"/v1/"+repoName("gen-chunks")+"/files/chunky:1:c.txt:download?alt=media")
	assert.Equal(t, http.StatusOK, code)

	unknown := do(http.MethodPut, endpoint+"?uploadType=resumable&upload_id=nope", payload, nil)
	assert.Equal(t, http.StatusNotFound, unknown.StatusCode)
}

func TestImportAptAndYumFromStorage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-imp", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})
	e.createRepo(t, "yum-imp", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_YUM})

	store := memStore{
		"bkt/debs/a_1.0_amd64.deb": makeDeb(t, "a", "1.0", "amd64"),
		"bkt/debs/b_2.0_amd64.deb": makeDeb(t, "b", "2.0", "amd64"),
		"bkt/debs/junk.deb":        []byte("junk"),
		"bkt/rpms/c.rpm":           makeRPM(t, "c", "3.0", "1", "noarch"),
	}
	e.handler.Backend.SetObjectStore(store)

	ctx := t.Context()

	op, err := e.client.ImportAptArtifacts(ctx, &artifactregistrypb.ImportAptArtifactsRequest{
		Parent: repoName("apt-imp"),
		Source: &artifactregistrypb.ImportAptArtifactsRequest_GcsSource{GcsSource: &artifactregistrypb.ImportAptArtifactsGcsSource{
			Uris: []string{"gs://bkt/debs/*.deb"}, UseWildcards: true,
		}},
	})
	require.NoError(t, err)

	resp, err := op.Wait(ctx)
	require.NoError(t, err)
	assert.Len(t, resp.GetAptArtifacts(), 2)
	require.Len(t, resp.GetErrors(), 1)
	assert.Equal(t, "gs://bkt/debs/junk.deb", resp.GetErrors()[0].GetGcsSource().GetUris()[0])
	assert.NotZero(t, resp.GetErrors()[0].GetError().GetCode())

	_, err = op.Metadata()
	require.NoError(t, err)

	_, packages := getBody(t, "http://127.0.0.1:"+e.port+"/apt/projects/"+project+"/dists/apt-imp/main/binary-amd64/Packages")
	assert.Contains(t, string(packages), "Package: a\n")
	assert.Contains(t, string(packages), "Package: b\n")

	yop, err := e.client.ImportYumArtifacts(ctx, &artifactregistrypb.ImportYumArtifactsRequest{
		Parent: repoName("yum-imp"),
		Source: &artifactregistrypb.ImportYumArtifactsRequest_GcsSource{GcsSource: &artifactregistrypb.ImportYumArtifactsGcsSource{
			Uris: []string{"gs://bkt/rpms/c.rpm", "gs://bkt/rpms/missing.rpm"},
		}},
	})
	require.NoError(t, err)

	yresp, err := yop.Wait(ctx)
	require.NoError(t, err)
	assert.Len(t, yresp.GetYumArtifacts(), 1)
	assert.Len(t, yresp.GetErrors(), 1)

	_, err = e.client.ImportAptArtifacts(ctx, &artifactregistrypb.ImportAptArtifactsRequest{
		Parent: repoName("yum-imp"),
		Source: &artifactregistrypb.ImportAptArtifactsRequest_GcsSource{GcsSource: &artifactregistrypb.ImportAptArtifactsGcsSource{
			Uris: []string{"gs://bkt/debs/a_1.0_amd64.deb"},
		}},
	})
	require.Error(t, err, "importing into a repository of another format is refused")
}
