package gcpartifactregistry_test

import (
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestRepositoryLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")

	created := e.createRepo(t, "lifecycle", dockerRepo(func(r *artifactregistrypb.Repository) {
		r.Labels = map[string]string{"team": "a"}
	}))
	assert.Equal(t, repoName("lifecycle"), created.GetName())
	assert.Equal(t, artifactregistrypb.Repository_STANDARD_REPOSITORY, created.GetMode())
	assert.NotNil(t, created.GetCreateTime())

	got, err := e.client.GetRepository(
		t.Context(),
		&artifactregistrypb.GetRepositoryRequest{Name: repoName("lifecycle")},
	)
	require.NoError(t, err)
	assert.Equal(t, "images", got.GetDescription())
	assert.Equal(t, map[string]string{"team": "a"}, got.GetLabels())
	assert.Equal(t, "localhost:"+e.port+"/"+project+"/lifecycle", got.GetRegistryUri())

	upd, err := e.client.UpdateRepository(t.Context(), &artifactregistrypb.UpdateRepositoryRequest{
		Repository: &artifactregistrypb.Repository{
			Name: repoName("lifecycle"), Description: "new", Labels: map[string]string{"team": "b"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "new", upd.GetDescription())
	assert.Equal(t, map[string]string{"team": "a"}, upd.GetLabels(), "unmasked fields are untouched")

	upd, err = e.client.UpdateRepository(t.Context(), &artifactregistrypb.UpdateRepositoryRequest{
		Repository: &artifactregistrypb.Repository{Name: repoName("lifecycle"), Labels: map[string]string{"x": "y"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"x": "y"}, upd.GetLabels())

	_, err = e.client.UpdateRepository(t.Context(), &artifactregistrypb.UpdateRepositoryRequest{
		Repository: &artifactregistrypb.Repository{
			Name:   repoName("lifecycle"),
			Format: artifactregistrypb.Repository_MAVEN,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"format"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	e.createRepo(t, "second", dockerRepo())

	it := e.client.ListRepositories(t.Context(), &artifactregistrypb.ListRepositoriesRequest{Parent: loc, PageSize: 1})

	var names []string

	for {
		r, nextErr := it.Next()
		if nextErr != nil {
			break
		}

		names = append(names, r.GetName())
	}

	assert.Equal(t, []string{repoName("lifecycle"), repoName("second")}, names)

	op, err := e.client.DeleteRepository(
		t.Context(),
		&artifactregistrypb.DeleteRepositoryRequest{Name: repoName("lifecycle")},
	)
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	_, err = e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: repoName("lifecycle")})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestCreateRepositoryErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "exists", dockerRepo())

	tests := []struct {
		req  *artifactregistrypb.CreateRepositoryRequest
		name string
		want codes.Code
	}{
		{name: "duplicate", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, RepositoryId: "exists", Repository: dockerRepo()}, want: codes.AlreadyExists},
		{name: "duplicate in other location", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent:       "projects/" + project + "/locations/europe-west1",
			RepositoryId: "exists",
			Repository:   dockerRepo(),
		}, want: codes.AlreadyExists},
		{name: "bad id", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, RepositoryId: "Bad_ID", Repository: dockerRepo()}, want: codes.InvalidArgument},
		{name: "empty id", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, Repository: dockerRepo()}, want: codes.InvalidArgument},
		{name: "missing repository", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, RepositoryId: "norepo"}, want: codes.InvalidArgument},
		{name: "missing format", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent:       loc,
			RepositoryId: "noformat",
			Repository:   &artifactregistrypb.Repository{},
		}, want: codes.InvalidArgument},
		{name: "bad parent", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: "projects/p", RepositoryId: "badparent", Repository: dockerRepo()}, want: codes.InvalidArgument},
		{name: "all locations", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent:       "projects/p/locations/-",
			RepositoryId: "allloc",
			Repository:   dockerRepo(),
		}, want: codes.InvalidArgument},
		{name: "docker config on maven", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, RepositoryId: "mvn", Repository: &artifactregistrypb.Repository{
				Format: artifactregistrypb.Repository_MAVEN,
				FormatConfig: &artifactregistrypb.Repository_DockerConfig{
					DockerConfig: &artifactregistrypb.Repository_DockerRepositoryConfig{ImmutableTags: true},
				},
			}}, want: codes.InvalidArgument},
		{name: "virtual without config", req: &artifactregistrypb.CreateRepositoryRequest{
			Parent: loc, RepositoryId: "virt", Repository: &artifactregistrypb.Repository{
				Format: artifactregistrypb.Repository_DOCKER, Mode: artifactregistrypb.Repository_VIRTUAL_REPOSITORY,
			}}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateRepository(t.Context(), tt.req)
			assert.Equal(t, tt.want, status.Code(err), "%v", err)
		})
	}
}

func TestNonDockerRepository(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "maven", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_MAVEN})

	_, err := e.client.GetPackage(t.Context(), &artifactregistrypb.GetPackageRequest{
		Name: repoName("maven") + "/packages/x",
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	got, err := e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: repoName("maven")})
	require.NoError(t, err)
	assert.Empty(t, got.GetRegistryUri(), "only DOCKER repositories have a registry_uri")
}

func TestInvalidNames(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{name: "repository", call: func() error {
			_, err := e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: "nope"})

			return err
		}, want: codes.InvalidArgument},
		{name: "package", call: func() error {
			_, err := e.client.GetPackage(t.Context(), &artifactregistrypb.GetPackageRequest{Name: repoName("r")})

			return err
		}, want: codes.InvalidArgument},
		{name: "missing repo for package", call: func() error {
			_, err := e.client.GetPackage(t.Context(), &artifactregistrypb.GetPackageRequest{
				Name: repoName("r") + "/packages/p",
			})

			return err
		}, want: codes.NotFound},
		{name: "negative page size", call: func() error {
			_, err := e.client.ListRepositories(t.Context(), &artifactregistrypb.ListRepositoriesRequest{
				Parent: loc, PageSize: -1,
			}).Next()

			return err
		}, want: codes.InvalidArgument},
		{name: "filter", call: func() error {
			_, err := e.client.ListRepositories(t.Context(), &artifactregistrypb.ListRepositoriesRequest{
				Parent: loc, Filter: "labels.a=b",
			}).Next()

			return err
		}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, status.Code(tt.call()))
		})
	}
}
