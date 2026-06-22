package lambda_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestLambda_VersionIndex_ResolveQualifier verifies that resolveQualifier uses the version
// index for O(1) lookups: an existing version number resolves (returns ErrLambdaUnavailable,
// not ErrVersionNotFound), while a missing version number returns ErrVersionNotFound.
func TestLambda_VersionIndex_ResolveQualifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs    error
		wantNotErrIs error
		name         string
		qualifier    string
	}{
		{
			name:      "existing_version_resolves",
			qualifier: "1",
			// No docker available in unit tests → runtime creation fails with ErrLambdaUnavailable,
			// but that means resolveQualifier succeeded (the version was found).
			wantErrIs:    lambda.ErrLambdaUnavailable,
			wantNotErrIs: lambda.ErrVersionNotFound,
		},
		{
			name:         "existing_version_2_resolves",
			qualifier:    "2",
			wantErrIs:    lambda.ErrLambdaUnavailable,
			wantNotErrIs: lambda.ErrVersionNotFound,
		},
		{
			name:         "existing_version_3_resolves",
			qualifier:    "3",
			wantErrIs:    lambda.ErrLambdaUnavailable,
			wantNotErrIs: lambda.ErrVersionNotFound,
		},
		{
			name:      "missing_version_returns_not_found",
			qualifier: "999",
			wantErrIs: lambda.ErrVersionNotFound,
		},
	}

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(),
		"000000000000", "us-east-1",
	)
	closeBackend(t, backend)

	fnName := "version-index-fn"
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

	// Publish 3 versions.
	for range 3 {
		_, err := backend.PublishVersion(fnName, "test version")
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := backend.InvokeFunctionWithQualifier(
				t.Context(),
				fnName,
				tt.qualifier,
				lambda.InvocationTypeRequestResponse,
				[]byte(`{}`),
			)

			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErrIs,
				"expected error to be %v, got %v", tt.wantErrIs, err)

			if tt.wantNotErrIs != nil {
				assert.NotErrorIs(t, err, tt.wantNotErrIs,
					"error should not be %v, got %v", tt.wantNotErrIs, err)
			}
		})
	}
}

// TestLambda_GetVersion_UsesIndex verifies that GetVersion uses the versionIndex for O(1) lookups.
func TestLambda_GetVersion_UsesIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		wantErr     error
		wantVersion string
	}{
		{
			name:        "existing_version_2",
			version:     "2",
			wantVersion: "2",
		},
		{
			name:    "missing_version_99",
			version: "99",
			wantErr: lambda.ErrVersionNotFound,
		},
		{
			name:        "latest_returns_live_config",
			version:     "$LATEST",
			wantVersion: "$LATEST",
		},
		{
			name:    "function_not_found",
			version: "1",
			wantErr: lambda.ErrFunctionNotFound,
		},
	}

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(),
		"000000000000", "us-east-1",
	)
	closeBackend(t, backend)

	fnName := "get-version-fn"
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

	// Publish 3 versions.
	for range 3 {
		_, err := backend.PublishVersion(fnName, "test version")
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use a non-existent function for the function-not-found case.
			lookupName := fnName
			if errors.Is(tt.wantErr, lambda.ErrFunctionNotFound) {
				lookupName = "nonexistent-fn"
			}

			v, err := backend.GetVersion(lookupName, tt.version)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, v.Version)
		})
	}
}
