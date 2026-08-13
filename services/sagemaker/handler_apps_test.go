package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AppLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain and user profile first.
	recDomain := doSageMakerRequest(
		t,
		h,
		"CreateDomain",
		map[string]any{"DomainName": "app-domain", "DefaultUserSettings": map[string]any{}},
	)
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	recUser := doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
	})
	require.Equal(t, http.StatusOK, recUser.Code)

	// Create app.
	recCreate := doSageMakerRequest(t, h, "CreateApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["AppArn"])

	// Describe app.
	recDesc := doSageMakerRequest(t, h, "DescribeApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List apps.
	recList := doSageMakerRequest(t, h, "ListApps", map[string]any{"DomainIdEquals": domainID})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Apps"].([]any), 1)

	// Delete app.
	recDelete := doSageMakerRequest(t, h, "DeleteApp", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "app-user",
		"AppType":         "JupyterServer",
		"AppName":         "my-app",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_CreateApp_RequiresOwner(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateApp", map[string]any{
		"DomainId": "d-1",
		"AppType":  "JupyterServer",
		"AppName":  "orphan-app",
	})
	assert.Equal(
		t,
		http.StatusBadRequest,
		rec.Code,
		"CreateApp must reject an app with no UserProfileName or SpaceName",
	)

	rec = doSageMakerRequest(t, h, "CreateApp", map[string]any{
		"DomainId":        "d-1",
		"UserProfileName": "u",
		"SpaceName":       "s",
		"AppType":         "JupyterServer",
		"AppName":         "both-app",
	})
	assert.Equal(
		t,
		http.StatusBadRequest,
		rec.Code,
		"CreateApp must reject an app with both UserProfileName and SpaceName",
	)
}

func TestHandler_CreateApp_SpaceOwned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName":          "space-app-domain",
		"DefaultUserSettings": map[string]any{},
	})
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": domainID, "SpaceName": "app-space"})

	// A real client that has no UserProfile — only a Space — must still be
	// able to launch an app (CreateAppInput.SpaceName is a real, previously
	// unmodeled, alternative to UserProfileName).
	recCreate := doSageMakerRequest(t, h, "CreateApp", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "app-space",
		"AppType":   "JupyterServer",
		"AppName":   "space-app",
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	recDesc := doSageMakerRequest(t, h, "DescribeApp", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "app-space",
		"AppType":   "JupyterServer",
		"AppName":   "space-app",
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "app-space", descOut["SpaceName"])
	assert.Nil(t, descOut["UserProfileName"])

	recDelete := doSageMakerRequest(t, h, "DeleteApp", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "app-space",
		"AppType":   "JupyterServer",
		"AppName":   "space-app",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_ListApps_FiltersAndMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName":          "list-apps-domain",
		"DefaultUserSettings": map[string]any{},
	})
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{"DomainId": domainID, "UserProfileName": "u1"})
	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{"DomainId": domainID, "UserProfileName": "u2"})

	apps := []struct{ userProfile, appName string }{
		{"u1", "app-1"},
		{"u1", "app-2"},
		{"u2", "app-3"},
	}
	for _, a := range apps {
		rec := doSageMakerRequest(t, h, "CreateApp", map[string]any{
			"DomainId":        domainID,
			"UserProfileName": a.userProfile,
			"AppType":         "JupyterServer",
			"AppName":         a.appName,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	t.Run("userProfileNameEquals narrows the result set", func(t *testing.T) {
		t.Parallel()

		rec := doSageMakerRequest(t, h, "ListApps", map[string]any{
			"DomainIDEquals":        domainID,
			"UserProfileNameEquals": "u1",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out["Apps"].([]any), 2)
	})

	t.Run("maxResults caps the page", func(t *testing.T) {
		t.Parallel()

		rec := doSageMakerRequest(t, h, "ListApps", map[string]any{
			"DomainIDEquals": domainID,
			"MaxResults":     1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out["Apps"].([]any), 1, "MaxResults must cap the page, not just be parsed and ignored")
		assert.NotEmpty(t, out["NextToken"])
	})
}
