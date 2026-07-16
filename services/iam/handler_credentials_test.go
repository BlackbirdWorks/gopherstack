package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ServiceSpecificCredential_CreateListDelete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-handler-user", "/", "")

	// Create.
	req := iamRequest("CreateServiceSpecificCredential", map[string]string{
		"UserName":    "ssc-handler-user",
		"ServiceName": "codecommit.amazonaws.com",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	createBody := rec.Body.String()
	assert.Contains(t, createBody, "ServiceSpecificCredentialId")
	assert.Contains(t, createBody, "Active")

	// List.
	req2 := iamRequest("ListServiceSpecificCredentials", map[string]string{
		"UserName": "ssc-handler-user",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "codecommit.amazonaws.com")

	// Extract credential ID from create response.
	var createResp struct {
		XMLName xml.Name `xml:"CreateServiceSpecificCredentialResponse"`
		Result  struct {
			SSC struct {
				ID string `xml:"ServiceSpecificCredentialId"`
			} `xml:"ServiceSpecificCredential"`
		} `xml:"CreateServiceSpecificCredentialResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	credID := createResp.Result.SSC.ID
	require.NotEmpty(t, credID)

	// Delete.
	req3 := iamRequest("DeleteServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-handler-user",
		"ServiceSpecificCredentialId": credID,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify deleted.
	req4 := iamRequest("ListServiceSpecificCredentials", map[string]string{
		"UserName": "ssc-handler-user",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.NotContains(t, rec4.Body.String(), credID)
}

func TestHandler_ServiceSpecificCredential_UpdateStatus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-update-status-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-update-status-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	req := iamRequest("UpdateServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-update-status-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
		"Status":                      "Inactive",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	creds, _ := b.ListServiceSpecificCredentials("ssc-update-status-user", "")
	require.Len(t, creds, 1)
	assert.Equal(t, "Inactive", creds[0].Status)
}

func TestHandler_ResetServiceSpecificCredential_ChangesPassword(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssc-reset-user", "/", "")

	cred, err := b.CreateServiceSpecificCredential("ssc-reset-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	req := iamRequest("ResetServiceSpecificCredential", map[string]string{
		"UserName":                    "ssc-reset-user",
		"ServiceSpecificCredentialId": cred.ServiceSpecificCredentialID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	// Response field is ServicePassword (not ServiceSpecificCredentialPassword).
	assert.Contains(t, body, "ServicePassword")
}

func TestCreateServiceSpecificCredential_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		name        string
		userName    string
		serviceName string
		wantErr     bool
	}{
		{
			name: "create_credential_success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:    "alice",
			serviceName: "codecommit.amazonaws.com",
		},
		{
			name:        "user_not_found_returns_error",
			setup:       func(_ *iam.InMemoryBackend) {},
			userName:    "ghost",
			serviceName: "codecommit.amazonaws.com",
			wantErr:     true,
		},
		{
			name: "empty_service_name_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			userName:    "bob",
			serviceName: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			cred, err := b.CreateServiceSpecificCredential(tt.userName, tt.serviceName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, cred)
			assert.Equal(t, tt.userName, cred.UserName)
			assert.Equal(t, tt.serviceName, cred.ServiceName)
			assert.NotEmpty(t, cred.ServiceSpecificCredentialID)
			assert.NotEmpty(t, cred.ServicePassword)
			assert.Equal(t, "Active", cred.Status)
		})
	}
}
