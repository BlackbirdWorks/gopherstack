package azurecontainerregistry_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func strList(t *testing.T, v any) []string {
	t.Helper()

	raw, ok := v.([]any)
	require.True(t, ok, "not a list: %v", v)

	out := make([]string, 0, len(raw))
	for _, x := range raw {
		s, _ := x.(string)
		out = append(out, s)
	}

	return out
}

func TestACRCatalogAndRepo(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "apicatalog", true)
	c := e.client(t, "apicatalog", basic(creds.Username, creds.Password1))

	for _, r := range []string{"alpha", "team/beta", "gamma"} {
		c.PushImage(r, "v1")
	}

	c.PushImage("alpha", "v2")

	t.Run("catalog pages", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, []string{"alpha", "gamma", "team/beta"}, c.Names("/acr/v1/_catalog", "repositories", 2))
	})

	t.Run("repo attributes", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/alpha", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		doc := decode(t, got.Body)
		assert.Equal(t, "alpha", doc["imageName"])
		assert.InDelta(t, 2, doc["manifestCount"], 0)
		assert.InDelta(t, 2, doc["tagCount"], 0)
		assert.Equal(t, host("apicatalog"), doc["registry"])
		assert.Equal(t, map[string]any{
			"deleteEnabled": true, "writeEnabled": true, "readEnabled": true, "listEnabled": true,
		}, doc["changeableAttributes"])
	})

	t.Run("nested repo name", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/team/beta", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.Equal(t, "team/beta", decode(t, got.Body)["imageName"])
	})

	t.Run("unknown repo", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/missing", nil, nil).Status)
		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/missing/_tags", nil, nil).Status)
	})

	t.Run("unauthenticated", func(t *testing.T) {
		t.Parallel()

		got := e.client(t, "apicatalog", nil).Do(http.MethodGet, "/acr/v1/_catalog", nil, nil)
		assert.Equal(t, http.StatusUnauthorized, got.Status)
		assert.Contains(t, got.Header.Get("WWW-Authenticate"), "registry:catalog:*")
	})
}

func TestACRTagsAndManifests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "apitags", true)
	c := e.client(t, "apitags", basic(creds.Username, creds.Password1))

	first := c.PushImage("app", "one")
	second := c.PushImage("app", "two")

	t.Run("list tags", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_tags", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		doc := decode(t, got.Body)
		assert.Equal(t, "app", doc["imageName"])

		tags, _ := doc["tags"].([]any)
		require.Len(t, tags, 2)
		assert.Equal(t, "one", tags[0].(map[string]any)["name"])
		assert.Equal(t, first.Digest, tags[0].(map[string]any)["digest"])
	})

	t.Run("tag order and paging", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_tags?orderby=timedesc&n=1", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		tags, _ := decode(t, got.Body)["tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "two", tags[0].(map[string]any)["name"])
		assert.Contains(t, got.Header.Get("Link"), `rel="next"`)
	})

	t.Run("filter by digest", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_tags?digest="+second.Digest, nil, nil)
		tags, _ := decode(t, got.Body)["tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "two", tags[0].(map[string]any)["name"])
	})

	t.Run("tag attributes", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_tags/one", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		tag, _ := decode(t, got.Body)["tag"].(map[string]any)
		assert.Equal(t, "one", tag["name"])
		assert.Equal(t, false, tag["signed"])
		assert.NotEmpty(t, tag["createdTime"])
	})

	t.Run("manifests", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_manifests", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		list, _ := decode(t, got.Body)["manifests"].([]any)
		require.Len(t, list, 2)

		m, _ := list[0].(map[string]any)
		assert.Equal(t, "amd64", m["architecture"])
		assert.Equal(t, "linux", m["os"])
		assert.Equal(t, ocitest.ManifestMediaType, m["mediaType"])
		assert.Greater(t, m["imageSize"], float64(100))
		assert.NotEmpty(t, m["tags"])
	})

	t.Run("manifest attributes", func(t *testing.T) {
		t.Parallel()

		got := c.Do(http.MethodGet, "/acr/v1/app/_manifests/"+first.Digest, nil, nil)
		require.Equal(t, http.StatusOK, got.Status)

		m, _ := decode(t, got.Body)["manifest"].(map[string]any)
		assert.Equal(t, first.Digest, m["digest"])
		assert.Equal(t, []any{"one"}, m["tags"])
	})

	t.Run("unknown tag", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/app/_tags/zzz", nil, nil).Status)
	})
}

func patch(enabledKey string, value bool) []byte {
	raw, _ := json.Marshal(map[string]any{"value": map[string]any{enabledKey: value}})

	return raw
}

func TestACRLocksAndDeletes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "apilocks", true)
	c := e.client(t, "apilocks", basic(creds.Username, creds.Password1))

	t.Run("delete locked tag", func(t *testing.T) {
		t.Parallel()

		img := c.PushImage("locked", "keep")

		require.Equal(t, http.StatusOK,
			c.Do(http.MethodPatch, "/acr/v1/locked/_tags/keep", patch("deleteEnabled", false), nil).Status)

		assert.Equal(t, http.StatusForbidden, c.Do(http.MethodDelete, "/acr/v1/locked/_tags/keep", nil, nil).Status)
		assert.Equal(
			t,
			http.StatusForbidden,
			c.Do(http.MethodDelete, "/v2/locked/manifests/"+img.Digest, nil, nil).Status,
		)

		require.Equal(t, http.StatusOK,
			c.Do(http.MethodPatch, "/acr/v1/locked/_tags/keep", patch("deleteEnabled", true), nil).Status)
		assert.Equal(t, http.StatusOK, c.Do(http.MethodDelete, "/acr/v1/locked/_tags/keep", nil, nil).Status)
		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/locked/_tags/keep", nil, nil).Status)
	})

	t.Run("write locked tag", func(t *testing.T) {
		t.Parallel()

		img := c.PushImage("wlocked", "pin")

		require.Equal(t, http.StatusOK,
			c.Do(http.MethodPatch, "/acr/v1/wlocked/_tags/pin", patch("writeEnabled", false), nil).Status)

		put := c.Do(
			http.MethodPut,
			"/v2/wlocked/manifests/pin",
			img.Body,
			map[string]string{"Content-Type": ocitest.ManifestMediaType},
		)
		assert.Equal(t, http.StatusForbidden, put.Status)
	})

	t.Run("read locked manifest", func(t *testing.T) {
		t.Parallel()

		img := c.PushImage("rlocked", "v")

		require.Equal(t, http.StatusOK,
			c.Do(http.MethodPatch, "/acr/v1/rlocked/_manifests/"+img.Digest, patch("readEnabled", false), nil).Status)
		assert.Equal(
			t,
			http.StatusForbidden,
			c.Do(http.MethodGet, "/v2/rlocked/manifests/"+img.Digest, nil, nil).Status,
		)
	})

	t.Run("list locked repo", func(t *testing.T) {
		t.Parallel()

		c.PushImage("hidden", "v")
		require.Equal(
			t,
			http.StatusOK,
			c.Do(http.MethodPatch, "/acr/v1/hidden", patch("listEnabled", false), nil).Status,
		)

		got := c.Do(http.MethodGet, "/acr/v1/_catalog", nil, nil)
		assert.NotContains(t, string(got.Body), `"hidden"`)
	})

	t.Run("delete manifest by digest", func(t *testing.T) {
		t.Parallel()

		img := c.PushImage("bydigest", "a")

		assert.Equal(
			t,
			http.StatusAccepted,
			c.Do(http.MethodDelete, "/acr/v1/bydigest/_manifests/"+img.Digest, nil, nil).Status,
		)
		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/bydigest", nil, nil).Status)
	})

	t.Run("delete repo", func(t *testing.T) {
		t.Parallel()

		c.PushImage("doomed", "x")
		c.PushImage("doomed", "y")

		got := c.Do(http.MethodDelete, "/acr/v1/doomed", nil, nil)
		require.Equal(t, http.StatusAccepted, got.Status)
		assert.Len(t, strList(t, decode(t, got.Body)["tagsDeleted"]), 2)
		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/v2/doomed/manifests/x", nil, nil).Status)
	})

	t.Run("v2 delete forgets metadata", func(t *testing.T) {
		t.Parallel()

		img := c.PushImage("v2del", "t")
		require.Equal(
			t,
			http.StatusAccepted,
			c.Do(http.MethodDelete, "/v2/v2del/manifests/"+img.Digest, nil, nil).Status,
		)
		assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/acr/v1/v2del/_tags/t", nil, nil).Status)
	})

	t.Run("bad patch body", func(t *testing.T) {
		t.Parallel()

		c.PushImage("badpatch", "t")
		assert.Equal(t, http.StatusBadRequest,
			c.Do(http.MethodPatch, "/acr/v1/badpatch", []byte("{"), nil).Status)
	})

	t.Run("method not allowed", func(t *testing.T) {
		t.Parallel()

		c.PushImage("methods", "t")
		assert.Equal(t, http.StatusMethodNotAllowed, c.Do(http.MethodPost, "/acr/v1/methods/_tags", nil, nil).Status)
		assert.Contains(t, string(c.Do(http.MethodPost, "/acr/v1/_catalog", nil, nil).Body), "UNSUPPORTED")
	})
}
