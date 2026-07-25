package cognitoidentity_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// errBoom is a static test-only sentinel standing in for an unmodeled/unexpected error
// that resolveErrorType has never seen before (err113 forbids ad hoc errors.New calls).
var errBoom = errors.New("boom")

// TestResolveErrorType locks the sentinel-error -> AWS wire exception-type/HTTP-status
// mapping used by the handler's error path. Every case here must match a real case in
// aws-sdk-go-v2/service/cognitoidentity's deserializers.go errorCode switch, or (for the
// catch-all) the generic modeled 500 the SDK recognizes on every operation.
func TestResolveErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "resource_not_found",
			err:        cognitoidentity.ErrIdentityPoolNotFound,
			wantType:   "ResourceNotFoundException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "resource_conflict_pool_name",
			err:        cognitoidentity.ErrIdentityPoolAlreadyExists,
			wantType:   "ResourceConflictException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_parameter",
			err:        cognitoidentity.ErrInvalidParameter,
			wantType:   "InvalidParameterException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_authorized_is_403",
			err:        cognitoidentity.ErrNotAuthorized,
			wantType:   "NotAuthorizedException",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "resource_conflict_generic",
			err:        cognitoidentity.ErrResourceConflict,
			wantType:   "ResourceConflictException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "developer_user_already_registered",
			err:        cognitoidentity.ErrDeveloperUserAlreadyRegistered,
			wantType:   "DeveloperUserAlreadyRegisteredException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_identity_pool_configuration",
			err:        cognitoidentity.ErrInvalidIdentityPoolConfiguration,
			wantType:   "InvalidIdentityPoolConfigurationException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unmodeled_error_falls_back_to_internal_error_exception",
			err:        errBoom,
			wantType:   "InternalErrorException",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotType, gotStatus := cognitoidentity.ResolveErrorType(tt.err)
			assert.Equal(t, tt.wantType, gotType)
			assert.Equal(t, tt.wantStatus, gotStatus)
		})
	}
}
