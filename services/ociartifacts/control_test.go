package ociartifacts_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateContainerRepositoryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		display  string
		compart  string
		wantCode string
		wantHTTP int
	}{
		{name: "simple", display: "app", compart: testCompartment},
		{name: "path", display: "team/app-1/api_v2.x", compart: testCompartment},
		{name: "uppercase", display: "App", compart: testCompartment, wantCode: "InvalidParameter", wantHTTP: 400},
		{
			name:     "trailing slash",
			display:  "app/",
			compart:  testCompartment,
			wantCode: "InvalidParameter",
			wantHTTP: 400,
		},
		{name: "leading dash", display: "-app", compart: testCompartment, wantCode: "InvalidParameter", wantHTTP: 400},
		{name: "double slash", display: "a//b", compart: testCompartment, wantCode: "InvalidParameter", wantHTTP: 400},
		{name: "space", display: "my app", compart: testCompartment, wantCode: "InvalidParameter", wantHTTP: 400},
		{
			name:     "too long",
			display:  strings.Repeat("a", 256),
			compart:  testCompartment,
			wantCode: "InvalidParameter",
			wantHTTP: 400,
		},
		{name: "no compartment", display: "app", compart: "", wantCode: "MissingParameter", wantHTTP: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			display := tt.display

			resp, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
				CompartmentId: new(tt.compart), DisplayName: new(display)})

			if tt.wantCode != "" {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantCode, se.GetCode())
				assert.Equal(t, tt.wantHTTP, se.GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, display, *resp.DisplayName)
			assert.Equal(t, testNamespace, *resp.Namespace)
			assert.Equal(t, artifacts.ContainerRepositoryLifecycleStateAvailable, resp.LifecycleState)
			assert.False(t, *resp.IsPublic)
			assert.False(t, *resp.IsImmutable)
			assert.Zero(t, *resp.ImageCount)
			assert.NotEmpty(t, *resp.Id)
			assert.NotEmpty(t, *resp.Etag)
		})
	}
}

func TestContainerRepositoryLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})

	repo := e.createRepo(t, "svc/api", func(d *artifacts.CreateContainerRepositoryDetails) {
		d.IsImmutable, d.IsPublic = new(true), new(true)
		d.FreeformTags = map[string]string{"env": "dev"}
		d.Readme = &artifacts.ContainerRepositoryReadme{
			Content: new("# hi"),
			Format:  artifacts.ContainerRepositoryReadmeFormatMarkdown,
		}
	})
	assert.Equal(t, testUser, *repo.CreatedBy)
	assert.Equal(t, "# hi", *repo.Readme.Content)

	_, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
		CompartmentId: new(testCompartment), DisplayName: new("svc/api")})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "dev"}, got.FreeformTags)
	assert.True(t, *got.IsPublic)

	_, err = e.client.UpdateContainerRepository(t.Context(), artifacts.UpdateContainerRepositoryRequest{
		RepositoryId: repo.Id, IfMatch: new("wrong"),
		IsPublic: new(false),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.client.UpdateContainerRepository(t.Context(), artifacts.UpdateContainerRepositoryRequest{
		RepositoryId: repo.Id,
		IfMatch:      got.Etag,
		IsPublic:     new(false),
		IsImmutable:  new(false),
		Readme: &artifacts.ContainerRepositoryReadme{
			Content: new("plain"),
			Format:  artifacts.ContainerRepositoryReadmeFormatPlain,
		},
	})
	require.NoError(t, err)
	assert.False(t, *upd.IsPublic)
	assert.False(t, *upd.IsImmutable)
	assert.Equal(t, artifacts.ContainerRepositoryReadmeFormatPlain, upd.Readme.Format)
	assert.Equal(t, map[string]string{"env": "dev"}, upd.FreeformTags)

	_, err = e.client.ChangeContainerRepositoryCompartment(
		t.Context(),
		artifacts.ChangeContainerRepositoryCompartmentRequest{
			RepositoryId:  repo.Id,
			CompartmentId: new(otherCompart),
		},
	)
	require.NoError(t, err)

	moved, err := e.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *moved.CompartmentId)

	_, err = e.client.DeleteContainerRepository(t.Context(), artifacts.DeleteContainerRepositoryRequest{
		RepositoryId: repo.Id, IfMatch: new("stale")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteContainerRepository(
		t.Context(),
		artifacts.DeleteContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)

	_, err = e.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	se := svcErr(t, err)
	assert.Equal(t, "NotAuthorizedOrNotFound", se.GetCode())
	assert.Equal(t, http.StatusNotFound, se.GetHTTPStatusCode())
}

func TestUpdateRepositoryReadmeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		readme *artifacts.ContainerRepositoryReadme
		code   string
	}{
		{
			"bad format",
			&artifacts.ContainerRepositoryReadme{Content: new("x"), Format: "TEXT_HTML"},
			"InvalidParameter",
		},
		{
			"no content",
			&artifacts.ContainerRepositoryReadme{Format: artifacts.ContainerRepositoryReadmeFormatPlain},
			"MissingParameter",
		},
		{"no format", &artifacts.ContainerRepositoryReadme{Content: new("x")}, "MissingParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			repo := e.createRepo(t, "r", nil)

			_, err := e.client.UpdateContainerRepository(t.Context(), artifacts.UpdateContainerRepositoryRequest{
				RepositoryId: repo.Id,
				Readme:       tt.readme,
			})
			assert.Equal(t, tt.code, svcErr(t, err).GetCode())
		})
	}
}

func TestListContainerRepositories(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "alpha", nil)
	e.clock.Advance(1_000_000_000)
	e.createRepo(t, "beta", func(d *artifacts.CreateContainerRepositoryDetails) { d.IsPublic = new(true) })
	e.clock.Advance(1_000_000_000)
	e.createRepo(
		t,
		"gamma",
		func(d *artifacts.CreateContainerRepositoryDetails) { d.CompartmentId = new(otherCompart) },
	)

	names := func(items []artifacts.ContainerRepositorySummary) []string {
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, *it.DisplayName)
		}

		return out
	}

	tests := []struct {
		name string
		req  artifacts.ListContainerRepositoriesRequest
		want []string
	}{
		{
			"default newest first",
			artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment)},
			[]string{"beta", "alpha"},
		},
		{"by name asc", artifacts.ListContainerRepositoriesRequest{
			CompartmentId: new(testCompartment), SortBy: artifacts.ListContainerRepositoriesSortByDisplayname,
		}, []string{"alpha", "beta"}},
		{"by name desc", artifacts.ListContainerRepositoriesRequest{
			CompartmentId: new(testCompartment), SortBy: artifacts.ListContainerRepositoriesSortByDisplayname,
			SortOrder: artifacts.ListContainerRepositoriesSortOrderDesc,
		}, []string{"beta", "alpha"}},
		{
			"public only",
			artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment), IsPublic: new(true)},
			[]string{"beta"},
		},
		{
			"display name",
			artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment), DisplayName: new("ALPHA")},
			[]string{"alpha"},
		},
		{
			"other compartment",
			artifacts.ListContainerRepositoriesRequest{CompartmentId: new(otherCompart)},
			[]string{"gamma"},
		},
		{
			"deleted state",
			artifacts.ListContainerRepositoriesRequest{
				CompartmentId:  new(testCompartment),
				LifecycleState: new("DELETED"),
			},
			[]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.ListContainerRepositories(t.Context(), tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, names(resp.Items))
			assert.Equal(t, len(tt.want), *resp.RepositoryCount)
		})
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		var got []string

		page := (*string)(nil)

		for range 5 {
			resp, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
				CompartmentId: new(testCompartment), Limit: new(1), Page: page,
			})
			require.NoError(t, err)

			got = append(got, names(resp.Items)...)

			if resp.OpcNextPage == nil {
				assert.Zero(t, *resp.RemainingItemsCount)

				break
			}

			assert.Equal(t, 1, *resp.RemainingItemsCount)

			page = resp.OpcNextPage
		}

		assert.Equal(t, []string{"beta", "alpha"}, got)
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		status, body := rawDo(t, http.MethodGet, e.base+"/20160918/container/repositories", "")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, body, "MissingParameter")

		_, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
			CompartmentId: new(testCompartment), Page: new("!!not-a-token"),
		})
		assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
	})
}

func TestContainerConfiguration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	got, err := e.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Equal(t, testNamespace, *got.Namespace)
	assert.True(t, *got.IsRepositoryCreatedOnFirstPush)

	_, err = e.client.UpdateContainerConfiguration(t.Context(), artifacts.UpdateContainerConfigurationRequest{
		CompartmentId: new(testCompartment), IfMatch: new("stale"),
		IsRepositoryCreatedOnFirstPush: new(false),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.client.UpdateContainerConfiguration(t.Context(), artifacts.UpdateContainerConfigurationRequest{
		CompartmentId: new(testCompartment), IfMatch: got.Etag,
		IsRepositoryCreatedOnFirstPush: new(false),
	})
	require.NoError(t, err)
	assert.False(t, *upd.IsRepositoryCreatedOnFirstPush)

	again, err := e.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.False(t, *again.IsRepositoryCreatedOnFirstPush)

	status, body := rawDo(t, http.MethodGet, e.base+"/20160918/container/configuration", "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")
}

func TestControlPlaneAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opts     envOpts
		wantCode int
	}{
		{"accept any when not enforced", envOpts{}, http.StatusOK},
		{"signed by registered key", envOpts{enforce: true, register: true}, http.StatusOK},
		{"unknown key rejected", envOpts{enforce: true}, http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)

			_, err := e.client.GetContainerConfiguration(
				t.Context(),
				artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
			)
			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	t.Run("unsigned rejected when enforced", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{enforce: true, register: true})
		status, body := rawDo(
			t,
			http.MethodGet,
			e.base+"/20160918/container/configuration?compartmentId="+testCompartment,
			"",
		)
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Contains(t, body, "NotAuthenticated")
	})
}

func TestUnknownRouteAndBadBody(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		want   int
	}{
		{"no route", http.MethodGet, "/20160918/nope", "", "NotFound", http.StatusNotFound},
		{
			"bad json",
			http.MethodPost,
			"/20160918/container/repositories",
			"{",
			"CannotParseRequest",
			http.StatusBadRequest,
		},
		{
			"empty body",
			http.MethodPost,
			"/20160918/container/repositories",
			"",
			"MissingParameter",
			http.StatusBadRequest,
		},
		{"wrong method", http.MethodPatch, "/20160918/container/repositories", "{}", "NotFound", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.want, status)
			assert.Contains(t, body, tt.code)
		})
	}
}
