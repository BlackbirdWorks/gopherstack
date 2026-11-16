package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestAPIMGroupLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parent     string
		parentBody string
		child      string
		childBody  string
		coll       string
	}{
		{
			name: "product groups", parent: "products/p1", parentBody: `{"properties":{"displayName":"P"}}`,
			child: "groups/g1", childBody: `{"properties":{"displayName":"G"}}`, coll: "groups",
		},
		{
			name: "group users", parent: "groups/g1", parentBody: `{"properties":{"displayName":"G"}}`,
			child: "users/u1", coll: "users",
			childBody: `{"properties":{"email":"a@x.io","firstName":"A","lastName":"B"}}`,
		},
	}

	names := func(t *testing.T, body string) []string {
		t.Helper()

		var got struct {
			Value []struct {
				Name string `json:"name"`
			} `json:"value"`
		}

		require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))

		out := make([]string, 0, len(got.Value))
		for _, v := range got.Value {
			out = append(out, v.Name)
		}

		return out
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)

			childID := tt.child[len(tt.coll)+1:]
			link := s.armPath("/" + tt.parent + "/" + tt.coll + "/" + childID)
			list := s.armPath("/" + tt.parent + "/" + tt.coll)

			status, body, _ := s.raw(t, http.MethodPut, s.armPath("/"+tt.parent), tt.parentBody, nil)
			require.Less(t, status, http.StatusBadRequest, body)

			status, _, _ = s.raw(t, http.MethodPut, link, "", nil)
			assert.Equal(t, http.StatusNotFound, status, "the child does not exist yet")

			status, body, _ = s.raw(t, http.MethodPut, s.armPath("/"+tt.child), tt.childBody, nil)
			require.Less(t, status, http.StatusBadRequest, body)

			status, _, _ = s.raw(t, http.MethodHead, link, "", nil)
			assert.Equal(t, http.StatusNotFound, status, "no link yet")

			status, body, _ = s.raw(t, http.MethodGet, list, "", nil)
			require.Equal(t, http.StatusOK, status, body)
			assert.Empty(t, names(t, body))

			status, body, _ = s.raw(t, http.MethodPut, link, "", nil)
			assert.Equal(t, http.StatusCreated, status, body)
			assert.Contains(t, body, `"name":"`+childID+`"`)

			status, _, _ = s.raw(t, http.MethodPut, link, "", nil)
			assert.Equal(t, http.StatusOK, status)

			status, _, _ = s.raw(t, http.MethodHead, link, "", nil)
			assert.Equal(t, http.StatusNoContent, status)

			status, body, _ = s.raw(t, http.MethodGet, list, "", nil)
			require.Equal(t, http.StatusOK, status, body)
			assert.Equal(t, []string{childID}, names(t, body))

			status, _, _ = s.raw(t, http.MethodDelete, s.armPath("/"+tt.child), "", nil)
			require.Less(t, status, http.StatusBadRequest)

			status, body, _ = s.raw(t, http.MethodGet, list, "", nil)
			require.Equal(t, http.StatusOK, status, body)
			assert.Empty(t, names(t, body), "deleting the child drops the link")

			status, body, _ = s.raw(t, http.MethodPut, s.armPath("/"+tt.child), tt.childBody, nil)
			require.Less(t, status, http.StatusBadRequest, body)

			status, _, _ = s.raw(t, http.MethodHead, link, "", nil)
			assert.Equal(t, http.StatusNotFound, status, "a recreated child is not linked again")

			status, _, _ = s.raw(t, http.MethodGet, s.armPath("/"+tt.parent+"x/"+tt.coll), "", nil)
			assert.Equal(t, http.StatusNotFound, status)
		})
	}
}

func TestAPIMLinksPersistence(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	for path, body := range map[string]string{
		"/products/p1": `{"properties":{"displayName":"P"}}`, "/groups/g1": `{"properties":{"displayName":"G"}}`,
	} {
		status, resp, _ := s.raw(t, http.MethodPut, s.armPath(path), body, nil)
		require.Less(t, status, http.StatusBadRequest, resp)
	}

	status, resp, _ := s.raw(t, http.MethodPut, s.armPath("/products/p1/groups/g1"), "", nil)
	require.Equal(t, http.StatusCreated, status, resp)

	fresh := newAPIMStack(t)
	require.NoError(t, fresh.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	status, resp, _ = fresh.raw(t, http.MethodHead, fresh.armPath("/products/p1/groups/g1"), "", nil)
	assert.Equal(t, http.StatusNoContent, status, resp)
}
