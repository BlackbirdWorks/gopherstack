package gcpartifactregistry_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"slices"
	"testing"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1"
	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

const (
	project = "test-project"
	loc     = "projects/" + project + "/locations/us-central1"
)

type env struct {
	client  *artifactregistry.Client
	handler *gcpartifactregistry.Handler
	port    string
	opts    []option.ClientOption
}

func newEnv(t *testing.T, dataDir string) *env {
	t.Helper()

	b := gcpartifactregistry.NewInMemoryBackend()
	b.SetDataDir(dataDir)
	b.SetContext(t.Context())

	h := gcpartifactregistry.NewHandler(b)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))

	_, port, err := net.SplitHostPort(h.Addr())
	require.NoError(t, err)

	b.SetRegistryAddress("localhost", port)
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := artifactregistry.NewClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, opts: opts, port: port}
}

func dockerRepo(mut ...func(*artifactregistrypb.Repository)) *artifactregistrypb.Repository {
	r := &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER, Description: "images"}
	for _, m := range mut {
		m(r)
	}

	return r
}

// createRepo creates a repository and waits for its operation.
func (e *env) createRepo(t *testing.T, id string, repo *artifactregistrypb.Repository) *artifactregistrypb.Repository {
	t.Helper()

	op, err := e.client.CreateRepository(t.Context(), &artifactregistrypb.CreateRepositoryRequest{
		Parent: loc, RepositoryId: id, Repository: repo,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func (e *env) docker(t *testing.T) ocitest.Client {
	t.Helper()

	return ocitest.Client{
		T: t, Base: "http://127.0.0.1:" + e.port,
		Prepare: func(r *http.Request) {
			r.Header.Set(
				"Authorization",
				"Basic "+base64.StdEncoding.EncodeToString([]byte("oauth2accesstoken:anything")),
			)
		},
	}
}

func repoName(id string) string { return loc + "/repositories/" + id }

func decodeJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	return out
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func sortedAsc(s []string) bool { return slices.IsSorted(s) }

func reversed(s []string) []string {
	out := slices.Clone(s)
	slices.Reverse(out)

	return out
}
