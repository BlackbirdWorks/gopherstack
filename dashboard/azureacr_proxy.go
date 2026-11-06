package dashboard

import (
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/labstack/echo/v5"

	acrbackend "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

const (
	azureACRProxyPrefix = "/dashboard/api/v1/azure/acr"
	acrMaxTags          = 200
	acrMask             = "••••••••"
)

//nolint:gochecknoglobals // immutable allow-list patterns
var (
	acrRepoRe = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$`)
	acrTagRe  = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
)

type acrRegistryJSON struct {
	CreatedAt    time.Time `json:"createdAt"`
	Name         string    `json:"name"`
	LoginServer  string    `json:"loginServer"`
	Repositories int       `json:"repositoryCount"`
	AdminEnabled bool      `json:"adminEnabled"`
}

type acrRepoJSON struct {
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	Name      string    `json:"name"`
	Manifests int       `json:"manifestCount"`
	Tags      int       `json:"tagCount"`
}

type acrManifestJSON struct {
	Created      time.Time `json:"created"`
	Digest       string    `json:"digest"`
	MediaType    string    `json:"mediaType"`
	Architecture string    `json:"architecture"`
	OS           string    `json:"os"`
	Tags         []string  `json:"tags"`
	Size         int64     `json:"sizeBytes"`
	Children     int       `json:"childManifests"`
	Index        bool      `json:"index"`
}

type acrTagJSON struct {
	Updated time.Time `json:"updated"`
	acrManifestJSON
	Name string `json:"name"`
}

// registerAzureACRRoutes serves the Container Registry page from the backend in-process: repositories, tags and
// manifests are read-only except for tag deletion, and the admin password leaves only through the reveal POST.
func (h *DashboardHandler) registerAzureACRRoutes() {
	api := &azureACRAPI{}
	if h.config.AzureACROps != nil {
		api.h = h.config.AzureACROps
	}

	r := h.SubRouter
	p := azureACRProxyPrefix + "/registries"
	r.Add(http.MethodGet, p, api.guard(api.registries))
	r.Add(http.MethodGet, p+"/:reg/repositories", api.guard(api.repositories))
	r.Add(http.MethodGet, p+"/:reg/tags", api.guard(api.tags))
	r.Add(http.MethodGet, p+"/:reg/manifests", api.guard(api.manifests))
	r.Add(http.MethodDelete, p+"/:reg/tags", api.guard(api.deleteTag))
	r.Add(http.MethodGet, p+"/:reg/credentials", api.guard(api.credentials))
	r.Add(http.MethodPost, p+"/:reg/credentials/reveal", api.guard(api.reveal))
}

type azureACRAPI struct {
	h *acrbackend.Handler
}

func (a *azureACRAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.h == nil {
			return cloudUnavailable(c, "Azure Container Registry")
		}

		return next(c)
	}
}

func (a *azureACRAPI) fail(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, acrbackend.ErrRegistryNotFound), errors.Is(err, acrbackend.ErrNotFound):
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	case errors.Is(err, acrbackend.ErrDenied), errors.Is(err, acrbackend.ErrAdminDisabled):
		return cloudJSONFail(c, http.StatusConflict, "FAILED_PRECONDITION", err.Error())
	default:
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
	}
}

func (a *azureACRAPI) registries(c *echo.Context) error {
	names := a.h.Backend.Names()
	out := make([]acrRegistryJSON, 0, len(names))

	for _, n := range names {
		info, ok := a.h.Backend.Registry(n)
		if !ok {
			continue
		}

		out = append(out, acrRegistryJSON{
			Name: info.Name, CreatedAt: info.CreatedAt, AdminEnabled: info.AdminEnabled,
			Repositories: len(a.h.Backend.Repositories(n)), LoginServer: info.Name + ".azurecr.localhost",
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"registries": out})
}

func (a *azureACRAPI) known(c *echo.Context) (string, bool) {
	reg := c.Param("reg")
	if _, ok := a.h.Backend.Registry(reg); !ok {
		_ = a.fail(c, acrbackend.ErrRegistryNotFound)

		return "", false
	}

	return reg, true
}

func (a *azureACRAPI) repoParam(c *echo.Context) (string, bool) {
	repo := c.QueryParam("repo")
	if len(repo) > 255 || !acrRepoRe.MatchString(repo) {
		_ = cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid repository name")

		return "", false
	}

	return repo, true
}

func (a *azureACRAPI) repositories(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	repos := a.h.Backend.Repositories(reg)
	out := make([]acrRepoJSON, 0, len(repos))

	for _, r := range repos {
		out = append(out, acrRepoJSON{
			Name: r.Name, Created: r.Created, Updated: r.Updated, Manifests: r.ManifestCount, Tags: r.TagCount,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"repositories": out})
}

// detail describes a manifest; a manifest that cannot be read keeps just its digest.
func (a *azureACRAPI) detail(c *echo.Context, reg, repo, digest string) acrManifestJSON {
	out := acrManifestJSON{Digest: digest, Tags: []string{}}

	d, err := a.h.Backend.DescribeManifest(c.Request().Context(), reg, repo, digest)
	if err != nil {
		return out
	}

	out.MediaType, out.Size, out.Architecture, out.OS = d.MediaType, d.Size, d.Architecture, d.OS
	out.Index, out.Children = d.Index, d.Children

	return out
}

func (a *azureACRAPI) tags(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	repo, ok := a.repoParam(c)
	if !ok {
		return nil
	}

	tags, found := a.h.Backend.Tags(reg, repo)
	if !found {
		return a.fail(c, acrbackend.ErrNotFound)
	}

	if len(tags) > acrMaxTags {
		tags = tags[:acrMaxTags]
	}

	cache := map[string]acrManifestJSON{}
	out := make([]acrTagJSON, 0, len(tags))

	for _, t := range tags {
		m, seen := cache[t.Digest]
		if !seen {
			m = a.detail(c, reg, repo, t.Digest)
			cache[t.Digest] = m
		}

		m.Created = t.Created
		out = append(out, acrTagJSON{Name: t.Name, Updated: t.Updated, acrManifestJSON: m})
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": out})
}

func (a *azureACRAPI) manifests(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	repo, ok := a.repoParam(c)
	if !ok {
		return nil
	}

	list, found := a.h.Backend.Manifests(reg, repo)
	if !found {
		return a.fail(c, acrbackend.ErrNotFound)
	}

	if len(list) > acrMaxTags {
		list = list[:acrMaxTags]
	}

	out := make([]acrManifestJSON, 0, len(list))

	for _, m := range list {
		d := a.detail(c, reg, repo, m.Digest)
		d.Created = m.Created
		d.Tags = append(d.Tags, m.Tags...)
		out = append(out, d)
	}

	return c.JSON(http.StatusOK, map[string]any{"manifests": out})
}

func (a *azureACRAPI) deleteTag(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	repo, ok := a.repoParam(c)
	if !ok {
		return nil
	}

	tag := c.QueryParam("tag")
	if !acrTagRe.MatchString(tag) {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid tag name")
	}

	if err := a.h.Backend.DeleteTag(c.Request().Context(), reg, repo, tag); err != nil {
		return a.fail(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (a *azureACRAPI) credentials(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	info, _ := a.h.Backend.Registry(reg)

	return c.JSON(http.StatusOK, map[string]any{
		"adminEnabled": info.AdminEnabled, "username": info.Name, "passwords": []string{acrMask, acrMask},
	})
}

// reveal is the only route that sends the admin passwords out.
func (a *azureACRAPI) reveal(c *echo.Context) error {
	reg, ok := a.known(c)
	if !ok {
		return nil
	}

	creds, err := a.h.Backend.Credentials(reg)
	if err != nil {
		return a.fail(c, err)
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, map[string]string{
		"username": creds.Username, "password": creds.Password1, "password2": creds.Password2,
	})
}
