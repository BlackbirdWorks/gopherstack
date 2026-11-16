package ociartifacts_test

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func TestRepositoryLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetLimits(2, 100, 1<<40)

	e.createRepo(t, "one", nil)
	e.createRepo(t, "two", nil)

	_, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
		CompartmentId: new(testCompartment), DisplayName: new("three"),
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	resp := e.registry(t).Do(http.MethodPut, "/v2/"+testNamespace+"/four/manifests/v1", []byte("{}"),
		map[string]string{"Content-Type": ocitest.ManifestMediaType})
	assert.Equal(t, http.StatusForbidden, resp.Status)
}

func TestManifestStorageLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		images int
		bytes  int64
		retag  bool
		want   int
	}{
		{"image count", 1, 1 << 40, false, http.StatusForbidden},
		{"same image new tag", 1, 1 << 40, true, http.StatusCreated},
		{"byte total", 100, 1, false, http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			c := e.registry(t)
			name := testNamespace + "/lim"
			first := c.PushImage(name, "v1")

			e.backend.SetLimits(500, tt.images, tt.bytes)

			body, ref := []byte(`{"schemaVersion":2,"mediaType":"`+ocitest.ManifestMediaType+`"}`), "v2"
			if tt.retag {
				body = first.Body
			}

			resp := c.Do(http.MethodPut, "/v2/"+name+"/manifests/"+ref, body,
				map[string]string{"Content-Type": ocitest.ManifestMediaType})
			assert.Equal(t, tt.want, resp.Status, string(resp.Body))
		})
	}
}

func TestConcurrentPushesRespectImageLimit(t *testing.T) {
	t.Parallel()

	const pushers = 24

	e := newEnv(t, envOpts{})
	e.createRepo(t, "race", nil)

	c := e.registry(t)
	name := testNamespace + "/race"
	cfg := []byte(`{"architecture":"amd64","os":"linux"}`)
	cfgDigest := c.PushBlob(name, cfg, 0)
	bodies := make([][]byte, pushers)

	for i := range bodies {
		layer := []byte("layer " + strconv.Itoa(i))
		bodies[i] = []byte(fmt.Sprintf(
			`{"schemaVersion":2,"mediaType":%q,"config":{"mediaType":"application/vnd.docker.container.image.v1+json",`+
				`"size":%d,"digest":%q},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip",`+
				`"size":%d,"digest":%q}]}`,
			ocitest.ManifestMediaType, len(cfg), cfgDigest, len(layer), c.PushBlob(name, layer, 0)))
	}

	e.backend.SetLimits(500, 1, 1<<40)

	statuses := make(chan int, pushers)

	var wg sync.WaitGroup

	for i, body := range bodies {
		wg.Go(func() {
			statuses <- c.Do(http.MethodPut, "/v2/"+name+"/manifests/v"+strconv.Itoa(i), body,
				map[string]string{"Content-Type": ocitest.ManifestMediaType}).Status
		})
	}

	wg.Wait()
	close(statuses)

	created := 0

	for st := range statuses {
		if st == http.StatusCreated {
			created++
		}
	}

	assert.Equal(t, 1, created)
}
