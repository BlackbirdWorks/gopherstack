package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const redisCacheKey = "/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg1/" +
	"providers/microsoft.cache/redis/c1"

func TestRedisAccessPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		perms any
		name  string
		path  string
		want  int
	}{
		{name: "custom", path: "readonly", perms: "+get +hget", want: http.StatusOK},
		{name: "with space", path: "my%20policy", perms: "+@read ~*", want: http.StatusOK},
		{name: "builtin immutable", path: "Data%20Owner", perms: "+@all", want: http.StatusBadRequest},
		{name: "bad permissions", path: "p1", perms: "bogus", want: http.StatusBadRequest},
		{name: "empty permissions", path: "p1", perms: "", want: http.StatusBadRequest},
		{name: "missing permissions", path: "p1", perms: nil, want: http.StatusBadRequest},
		{name: "bad name", path: "-bad", perms: "+get", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRedisEnv(t)
			status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
			require.Equal(t, http.StatusCreated, status)

			props := map[string]any{}
			if tt.perms != nil {
				props["permissions"] = tt.perms
			}

			status, body, _ := e.do(t, http.MethodPut, redisBase+"c1/accessPolicies/"+tt.path+redisAPI,
				map[string]any{"properties": props})
			require.Equal(t, tt.want, status, body)

			if tt.want != http.StatusOK {
				return
			}

			assert.Equal(t, "Microsoft.Cache/Redis/accessPolicies", body["type"])
			assert.Equal(t, "Custom", propsOf(body)["type"])
			assert.Equal(t, "Succeeded", propsOf(body)["provisioningState"])

			status, _, _ = e.do(t, http.MethodGet, redisBase+"c1/accessPolicies/"+tt.path+redisAPI, nil)
			assert.Equal(t, http.StatusOK, status)

			_, list, _ := e.do(t, http.MethodGet, redisBase+"c1/accessPolicies"+redisAPI, nil)
			assert.Len(t, list["value"], 4)

			status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1/accessPolicies/"+tt.path+redisAPI, nil)
			assert.Equal(t, http.StatusOK, status)

			status, _, _ = e.do(t, http.MethodGet, redisBase+"c1/accessPolicies/"+tt.path+redisAPI, nil)
			assert.Equal(t, http.StatusNotFound, status)
		})
	}

	t.Run("builtins", func(t *testing.T) {
		t.Parallel()

		e := newRedisEnv(t)
		status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
		require.Equal(t, http.StatusCreated, status)

		status, body, _ := e.do(t, http.MethodGet, redisBase+"c1/accessPolicies/Data%20Reader"+redisAPI, nil)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, "BuiltIn", propsOf(body)["type"])

		status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1/accessPolicies/Data%20Reader"+redisAPI, nil)
		assert.Equal(t, http.StatusBadRequest, status)

		status, _, _ = e.do(t, http.MethodGet, redisBase+"nope/accessPolicies"+redisAPI, nil)
		assert.Equal(t, http.StatusNotFound, status)
	})
}

type assignEnv struct {
	*redisEnv
}

const assignOID = "6497c918-11ad-41e7-1b0f-7c518a87d0b0"

func newAssignEnv(t *testing.T) *assignEnv {
	t.Helper()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	require.Equal(t, http.StatusCreated, status)

	status, _, _ = e.do(t, http.MethodPut, redisBase+"c1/accessPolicies/custom"+redisAPI,
		map[string]any{"properties": map[string]any{"permissions": "+get ~a*"}})
	require.Equal(t, http.StatusOK, status)

	return &assignEnv{e}
}

func (e *assignEnv) put(t *testing.T, name, policy string) (int, map[string]any) {
	t.Helper()

	s, b, _ := e.do(t, http.MethodPut, redisBase+"c1/accessPolicyAssignments/"+name+redisAPI,
		map[string]any{"properties": map[string]any{
			"accessPolicyName": policy, "objectId": assignOID, "objectIdAlias": "app",
		}})

	return s, b
}

func TestRedisAccessPolicyAssignments(t *testing.T) {
	t.Parallel()

	t.Run("put and data plane", func(t *testing.T) {
		t.Parallel()

		e := newAssignEnv(t)
		status, body := e.put(t, "assign1", "custom")
		require.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, "Microsoft.Cache/Redis/accessPolicyAssignments", body["type"])
		assert.Equal(t, "custom", propsOf(body)["accessPolicyName"])

		e.dp.mu.Lock()
		got := e.dp.specs[redisCacheKey].Assignments
		e.dp.mu.Unlock()

		require.Len(t, got, 1)
		assert.Equal(t, assignOID, got[0].ObjectID)
		assert.Equal(t, "app", got[0].Alias)
		assert.Equal(t, "+get ~a*", got[0].Permissions)

		status, _ = e.put(t, "assign2", "DATA reader")
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		e := newAssignEnv(t)
		status, _ := e.put(t, "assign3", "nosuchpolicy")
		assert.Equal(t, http.StatusBadRequest, status)

		status, _ = e.put(t, "ab", "custom")
		assert.Equal(t, http.StatusBadRequest, status)

		status, _, _ = e.do(t, http.MethodPut, redisBase+"c1/accessPolicyAssignments/assign4"+redisAPI,
			map[string]any{"properties": map[string]any{"accessPolicyName": "custom"}})
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("policy in use", func(t *testing.T) {
		t.Parallel()

		e := newAssignEnv(t)
		status, _ := e.put(t, "assign1", "custom")
		require.Equal(t, http.StatusOK, status)

		status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1/accessPolicies/custom"+redisAPI, nil)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("list and delete", func(t *testing.T) {
		t.Parallel()

		e := newAssignEnv(t)
		status, _ := e.put(t, "assign5", "custom")
		require.Equal(t, http.StatusOK, status)

		_, list, _ := e.do(t, http.MethodGet, redisBase+"c1/accessPolicyAssignments"+redisAPI, nil)
		assert.Len(t, list["value"], 1)

		status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1/accessPolicyAssignments/assign5"+redisAPI, nil)
		assert.Equal(t, http.StatusOK, status)

		status, _, _ = e.do(t, http.MethodGet, redisBase+"c1/accessPolicyAssignments/assign5"+redisAPI, nil)
		assert.Equal(t, http.StatusNotFound, status)

		e.dp.mu.Lock()
		defer e.dp.mu.Unlock()

		assert.Empty(t, e.dp.specs[redisCacheKey].Assignments)
	})
}
