package gcp_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

const artifactRegistryFixture = `
resource "google_artifact_registry_repository" "docker" {
  location      = "us-central1"
  repository_id = "tf-docker"
  description   = "terraform docker repository"
  format        = "DOCKER"

  labels = {
    env = "test"
  }

  docker_config {
    immutable_tags = true
  }
}

resource "google_artifact_registry_repository" "maven" {
  location      = "us-central1"
  repository_id = "tf-maven"
  format        = "MAVEN"
}

output "docker_id" {
  value = google_artifact_registry_repository.docker.id
}

output "docker_registry" {
  value = google_artifact_registry_repository.docker.registry_uri
}
`

func TestTerraformGCP_ArtifactRegistry(t *testing.T) {
	t.Parallel()

	r := newRunner(t, artifactRegistryFixture)

	r.lifecycleThen(func() {
		assert.Equal(t, "projects/gopherstack-test/locations/us-central1/repositories/tf-docker", r.output("docker_id"))

		// Push through the Docker Registry API on the same port and see it in the Artifact Registry API.
		c := ocitest.Client{
			T: t, Base: "http://localhost:" + hostArtifactPort,
			Prepare: func(req *http.Request) {
				req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("oauth2accesstoken:fake")))
			},
		}

		img := c.PushImage("gopherstack-test/tf-docker/app", "v1")

		// immutable_tags: pushing the same tag again is refused.
		again := c.Do(http.MethodPut, "/v2/gopherstack-test/tf-docker/app/manifests/v1", img.Body,
			map[string]string{"Content-Type": ocitest.ManifestMediaType})
		assert.Equal(t, http.StatusConflict, again.Status)

		got := c.Do(http.MethodGet, "/v2/gopherstack-test/tf-docker/app/manifests/v1", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.Equal(t, img.Body, got.Body)

		api := ocitest.Client{T: t, Base: "http://localhost:" + hostArtifactPort}
		images := api.Do(http.MethodGet,
			"/v1/projects/gopherstack-test/locations/us-central1/repositories/tf-docker/dockerImages", nil, nil)
		require.Equal(t, http.StatusOK, images.Status)
		assert.Contains(t, string(images.Body), img.Digest)
	})
}
