package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UserProfileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain first.
	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "up-domain"})
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	// Create user profile.
	recCreate := doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe user profile.
	recDesc := doSageMakerRequest(t, h, "DescribeUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List user profiles.
	recList := doSageMakerRequest(
		t,
		h,
		"ListUserProfiles",
		map[string]any{"DomainIdEquals": domainID},
	)
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["UserProfiles"].([]any), 1)

	// Delete user profile.
	recDelete := doSageMakerRequest(t, h, "DeleteUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_UserProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recDomain := doSageMakerRequest(
		t,
		h,
		"CreateDomain",
		map[string]any{"DomainName": "up-notfound-domain"},
	)
	require.Equal(t, http.StatusOK, recDomain.Code)

	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID := domainOut["DomainId"].(string)

	for _, op := range []string{"DescribeUserProfile", "DeleteUserProfile"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{
				"DomainId":        domainID,
				"UserProfileName": "nonexistent",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UpdateUserProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "my-domain",
		"AuthMode":   "SSO",
	})

	var domainResp map[string]any
	rec := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &domainResp))
	domains := domainResp["Domains"].([]any)
	require.Len(t, domains, 1)
	domainID := domains[0].(map[string]any)["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})

	rec = doSageMakerRequest(t, h, "UpdateUserProfile", map[string]any{
		"DomainId":        domainID,
		"UserProfileName": "my-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["UserProfileArn"])
}

func TestHandler_UpdateUserProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateUserProfile", map[string]any{
		"DomainId":        "d-nonexistent",
		"UserProfileName": "no-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
