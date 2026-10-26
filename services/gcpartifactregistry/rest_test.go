package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	base := "/v1/projects/p/locations/us-central1"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create", Method: http.MethodPost, Path: base + "/repositories?repositoryId=rest1",
			Body: `{"format":"DOCKER","description":"via rest","labels":{"a":"b"}}`, Status: 200,
			Contains: []string{
				`"name":"projects/p/locations/us-central1/operations/operation-`, `"done":true`,
				`"@type":"type.googleapis.com/google.devtools.artifactregistry.v1.Repository"`,
			},
		},
		{
			Name: "get", Method: http.MethodGet, Path: base + "/repositories/rest1", Status: 200,
			Contains: []string{
				`"name":"projects/p/locations/us-central1/repositories/rest1"`, `"format":"DOCKER"`,
				`"mode":"STANDARD_REPOSITORY"`, `"registryUri":"localhost:` + e.port + `/p/rest1"`,
			},
		},
		{
			Name: "list", Method: http.MethodGet, Path: base + "/repositories?pageSize=1", Status: 200,
			Contains: []string{`"repositories":[`, `rest1`},
		},
		{
			Name: "patch", Method: http.MethodPatch, Path: base + "/repositories/rest1?updateMask=description",
			Body: `{"description":"patched"}`, Status: 200, Contains: []string{`"description":"patched"`},
		},
		{
			Name: "duplicate", Method: http.MethodPost, Path: base + "/repositories?repositoryId=rest1",
			Body: `{"format":"DOCKER"}`, Status: 409, Contains: []string{`"status":"ALREADY_EXISTS"`},
		},
		{
			Name: "invalid", Method: http.MethodPost, Path: base + "/repositories?repositoryId=rest2", Body: `{}`,
			Status: 400, Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "missing", Method: http.MethodGet, Path: base + "/repositories/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "packages", Method: http.MethodGet, Path: base + "/repositories/rest1/packages", Status: 200,
			Contains: []string{`{}`},
		},
		{
			Name: "docker images", Method: http.MethodGet, Path: base + "/repositories/rest1/dockerImages", Status: 200,
			Contains: []string{`{}`},
		},
		{
			Name: "delete", Method: http.MethodDelete, Path: base + "/repositories/rest1", Status: 200,
			Contains: []string{`"done":true`},
		},
		{
			Name: "unknown operation", Method: http.MethodGet, Path: base + "/operations/none", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}

func TestScopedOperationLookup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")

	op, err := e.client.CreateRepository(t.Context(), &artifactregistrypb.CreateRepositoryRequest{
		Parent: loc, RepositoryId: "optest", Repository: dockerRepo(),
	})
	require.NoError(t, err)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{{
		Name: "get operation by its scoped name", Method: http.MethodGet, Path: "/v1/" + op.Name(), Status: 200,
		Contains: []string{`"done":true`, `"response"`},
	}})
}

func TestIAMPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "iamrepo", dockerRepo())

	empty, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: repoName("iamrepo")})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())

	set, err := e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: repoName("iamrepo"),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{{
			Role: "roles/artifactregistry.reader", Members: []string{"user:a@example.com"},
		}}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, set.GetEtag())

	got, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: repoName("iamrepo")})
	require.NoError(t, err)
	assert.Equal(t, "roles/artifactregistry.reader", got.GetBindings()[0].GetRole())
	assert.Equal(t, set.GetEtag(), got.GetEtag())

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: repoName("iamrepo"), Policy: &iampb.Policy{Etag: []byte("stale")},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	_, err = e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: repoName("ghost")})
	assert.Equal(t, codes.NotFound, status.Code(err))

	perms, err := e.client.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
		Resource: repoName("iamrepo"), Permissions: []string{"artifactregistry.repositories.get"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"artifactregistry.repositories.get"}, perms.GetPermissions())
}
