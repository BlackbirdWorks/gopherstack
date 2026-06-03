package appsync_test

// AWS-accuracy audit batch-2 tests (go-jhqo).
//
// Covers: GetResolver non-existent API, DeleteResolver non-existent API,
// GetFunction non-existent API, DeleteFunction non-existent API.
// Each operation previously returned a sub-resource "not found" error when the
// parent API ID did not exist; AWS returns NotFoundException for the API itself.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// ---- GetResolver non-existent API ----

func TestGetResolver_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiID     string
		typeName  string
		fieldName string
	}{
		{
			name:      "unknown_api_returns_api_not_found",
			apiID:     "no-such-api",
			typeName:  "Query",
			fieldName: "getUser",
		},
		{
			name:      "empty_backend_returns_api_not_found",
			apiID:     "ghost-api-id",
			typeName:  "Mutation",
			fieldName: "createUser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.GetResolver(tt.apiID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

// ---- DeleteResolver non-existent API ----

func TestDeleteResolver_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiID     string
		typeName  string
		fieldName string
	}{
		{
			name:      "unknown_api_returns_api_not_found",
			apiID:     "no-such-api",
			typeName:  "Query",
			fieldName: "getUser",
		},
		{
			name:      "resolver_not_reachable_via_bad_api",
			apiID:     "ghost-api",
			typeName:  "Mutation",
			fieldName: "deleteUser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.DeleteResolver(tt.apiID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

// ---- GetFunction non-existent API ----

func TestGetFunction_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		functionID string
	}{
		{
			name:       "unknown_api_returns_api_not_found",
			apiID:      "no-such-api",
			functionID: "fn-abc123",
		},
		{
			name:       "empty_backend_returns_api_not_found",
			apiID:      "ghost-api",
			functionID: "fn-xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.GetFunction(tt.apiID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

// ---- DeleteFunction non-existent API ----

func TestDeleteFunction_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		functionID string
	}{
		{
			name:       "unknown_api_returns_api_not_found",
			apiID:      "no-such-api",
			functionID: "fn-abc123",
		},
		{
			name:       "empty_backend_returns_api_not_found",
			apiID:      "ghost-api",
			functionID: "fn-xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.DeleteFunction(tt.apiID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

// ---- Positive path: sub-resource ops succeed when API exists ----

func TestGetResolver_ExistingAPI_ReturnsResolverNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		typeName  string
		fieldName string
	}{
		{
			name:      "resolver_absent_on_valid_api",
			typeName:  "Query",
			fieldName: "missingField",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GetResolver(api.APIID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			// Error must mention the resolver, not "api not found".
			assert.NotContains(t, err.Error(), "api "+api.APIID)
		})
	}
}

func TestGetFunction_ExistingAPI_ReturnsFunctionNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		functionID string
	}{
		{
			name:       "function_absent_on_valid_api",
			functionID: "fn-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GetFunction(api.APIID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			// Error must mention the function, not "api not found".
			assert.NotContains(t, err.Error(), "api "+api.APIID)
		})
	}
}
