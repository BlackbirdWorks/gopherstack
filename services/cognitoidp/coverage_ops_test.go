package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unmarshalBody(t *testing.T, rec *httptest.ResponseRecorder, v any) error {
	t.Helper()

	return json.Unmarshal(rec.Body.Bytes(), v)
}

func TestCognito_ListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "list-pool",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, unmarshalBody(t, rec, &resp))
	poolID := resp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "listuser",
	})

	rec = doCognitoRequest(t, h, "ListUsers", map[string]any{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCognito_ListUsersInGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "group-pool",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, unmarshalBody(t, rec, &resp))
	poolID := resp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "my-group",
	})

	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "my-group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
