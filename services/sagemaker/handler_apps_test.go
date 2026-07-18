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
		map[string]any{"DomainName": "app-domain"},
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
