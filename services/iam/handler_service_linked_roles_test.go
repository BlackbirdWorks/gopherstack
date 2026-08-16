package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestDeleteServiceLinkedRole_RemovesRole(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Create a service-linked role.
	iamCall(t, e, h, "CreateServiceLinkedRole", map[string]string{
		"AWSServiceName": "elasticloadbalancing.amazonaws.com",
	})

	// Discover the role name from ListRoles.
	listBefore := iamCall(t, e, h, "ListRoles", nil)
	require.Equal(t, http.StatusOK, listBefore.Code)
	roleName := extractFirstXMLTag(listBefore.Body.String(), "RoleName")
	require.NotEmpty(t, roleName, "service-linked role should appear in ListRoles")

	// Delete it.
	delRec := iamCall(t, e, h, "DeleteServiceLinkedRole", map[string]string{"RoleName": roleName})
	require.Equal(t, http.StatusOK, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "DeletionTaskId",
		"response must include a DeletionTaskId")

	// Verify role is gone.
	listAfter := iamCall(t, e, h, "ListRoles", nil)
	require.Equal(t, http.StatusOK, listAfter.Code)
	assert.NotContains(t, listAfter.Body.String(), roleName,
		"service-linked role must be removed from the backend")
}

func TestDeleteServiceLinkedRole_Idempotent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Deleting a non-existent role must not error (AWS async semantics).
	rec := iamCall(t, e, h, "DeleteServiceLinkedRole", map[string]string{"RoleName": "NonExistentRole"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeletionTaskId")
}

func TestCreateServiceLinkedRole_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(b *iam.InMemoryBackend)
		name           string
		serviceName    string
		description    string
		customSuffix   string
		wantRolePrefix string
		wantErr        bool
	}{
		{
			name:           "create_role_success",
			setup:          func(_ *iam.InMemoryBackend) {},
			serviceName:    "elasticloadbalancing.amazonaws.com",
			wantRolePrefix: "AWSServiceRoleFor",
		},
		{
			name:           "create_role_with_suffix",
			setup:          func(_ *iam.InMemoryBackend) {},
			serviceName:    "ec2.amazonaws.com",
			customSuffix:   "MyApp",
			wantRolePrefix: "AWSServiceRoleFor",
		},
		{
			name:        "empty_service_name_returns_error",
			setup:       func(_ *iam.InMemoryBackend) {},
			serviceName: "",
			wantErr:     true,
		},
		{
			name: "duplicate_role_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateServiceLinkedRole("elasticloadbalancing.amazonaws.com", "", "")
			},
			serviceName: "elasticloadbalancing.amazonaws.com",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			role, err := b.CreateServiceLinkedRole(tt.serviceName, tt.description, tt.customSuffix)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, role)
			assert.Contains(t, role.RoleName, tt.wantRolePrefix)
			assert.NotEmpty(t, role.Arn)
		})
	}
}

func TestHandler_ServiceLinkedRole_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateServiceLinkedRole", map[string]string{
		"AWSServiceName": "elasticloadbalancing.amazonaws.com",
		"Description":    "ELB service role",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "elasticloadbalancing")
}
