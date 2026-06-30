package sts_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestRefinement1_SessionNameCharacterValidation verifies that RoleSessionName
// with invalid characters is rejected.
func TestRefinement1_SessionNameCharacterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionName string
		wantErr     bool
	}{
		{
			name:        "valid alphanumeric",
			sessionName: "my-session",
			wantErr:     false,
		},
		{
			name:        "valid with allowed special chars",
			sessionName: "sess+=,.@abc",
			wantErr:     false,
		},
		{
			name:        "invalid colon (not allowed per AWS)",
			sessionName: "sess:abc",
			wantErr:     true,
		},
		{
			name:        "invalid space character",
			sessionName: "my session",
			wantErr:     true,
		},
		{
			name:        "invalid slash",
			sessionName: "my/session",
			wantErr:     true,
		},
		{
			name:        "invalid dollar sign",
			sessionName: "my$session",
			wantErr:     true,
		},
		{
			name:        "invalid semicolon",
			sessionName: "my;session",
			wantErr:     true,
		},
		{
			name:        "too short (1 char)",
			sessionName: "a",
			wantErr:     true,
		},
		{
			name:        "too long (65 chars)",
			sessionName: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::000000000000:role/TestRole",
				RoleSessionName: tt.sessionName,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_RoleArnValidation verifies that invalid ARNs are rejected.
func TestRefinement1_RoleArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{
			name:    "valid iam role arn",
			roleArn: "arn:aws:iam::000000000000:role/TestRole",
			wantErr: false,
		},
		{
			name:    "malformed too few parts",
			roleArn: "short/role",
			wantErr: true,
		},
		{
			name:    "malformed no arn prefix",
			roleArn: "aws:iam::000000000000:role/TestRole:extra",
			wantErr: true,
		},
		{
			name:    "empty arn",
			roleArn: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         tt.roleArn,
				RoleSessionName: "valid-session",
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_OperationCounters verifies per-operation call counters
// are incremented when operations are invoked.
func TestRefinement1_OperationCounters(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	ctx := t.Context()

	// Before any operations, metrics should all be zero.
	initialMetrics := h.SessionMetrics()
	assert.Equal(t, int64(0), initialMetrics.OpsAssumeRole)
	assert.Equal(t, int64(0), initialMetrics.OpsGetSessionToken)
	assert.Equal(t, int64(0), initialMetrics.TotalSessionsCreated)

	// Call AssumeRole.
	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::000000000000:role/CounterRole",
		RoleSessionName: "counter-session",
	})
	require.NoError(t, err)

	// Call GetSessionToken.
	_, err = backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 3600})
	require.NoError(t, err)

	// Call GetCallerIdentity.
	_, err = backend.GetCallerIdentity("", "")
	require.NoError(t, err)

	// Call GetFederationToken.
	_, err = backend.GetFederationToken(&sts.GetFederationTokenInput{Name: "fed-user"})
	require.NoError(t, err)

	_ = ctx

	// Check metrics reflect the calls.
	metrics := h.SessionMetrics()
	assert.Equal(t, int64(1), metrics.OpsAssumeRole, "AssumeRole counter")
	assert.Equal(t, int64(1), metrics.OpsGetSessionToken, "GetSessionToken counter")
	assert.Equal(t, int64(1), metrics.OpsGetCallerIdentity, "GetCallerIdentity counter")
	assert.Equal(t, int64(1), metrics.OpsGetFederationToken, "GetFederationToken counter")
	assert.Equal(t, int64(3), metrics.TotalSessionsCreated, "3 sessions created (1 role + 1 session + 1 fed)")
}

// TestRefinement1_TotalSessionsCreated verifies the lifetime session counter is
// correct across multiple session-issuing operations.
func TestRefinement1_TotalSessionsCreated(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)

	calls := []func() error{
		func() error {
			_, e := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::000000000000:role/R1",
				RoleSessionName: "s1",
			})

			return e
		},
		func() error {
			_, e := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 3600})

			return e
		},
		func() error {
			_, e := backend.GetFederationToken(&sts.GetFederationTokenInput{Name: "fed1"})

			return e
		},
		func() error {
			_, e := backend.AssumeRoot(&sts.AssumeRootInput{
				TargetPrincipal: "000000000000",
				TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			})

			return e
		},
	}

	for _, call := range calls {
		require.NoError(t, call())
	}

	metrics := h.SessionMetrics()
	assert.Equal(t, int64(len(calls)), metrics.TotalSessionsCreated)
}

// TestRefinement1_ConcurrentSessions verifies that concurrent session creation
// does not cause data races or missed counter increments.
func TestRefinement1_ConcurrentSessions(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)

	const workers = 20

	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::000000000000:role/ConcurrencyRole",
				RoleSessionName: "sess-concurrent",
			})
			assert.NoError(t, err)
		})
	}

	wg.Wait()

	metrics := h.SessionMetrics()
	assert.Equal(t, int64(workers), metrics.TotalSessionsCreated)
	assert.Equal(t, int64(workers), metrics.OpsAssumeRole)
	assert.Equal(t, workers, metrics.ActiveSessions)
}

// TestRefinement1_JWTPayloadParsingSharedHelper verifies that the shared
// parseJWTPayloadClaims path correctly handles malformed tokens without panicking.
func TestRefinement1_JWTPayloadParsingSharedHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		want  string // expected sub, or placeholder
	}{
		{
			name: "valid JWT with sub",
			// payload encodes {"sub":"web-identity-user","iss":"...","aud":"test-aud"}
			token: "eyJhbGciOiJub25lIn0" +
				".eyJzdWIiOiJ3ZWItaWRlbnRpdHktdXNlciIsImlzcyI6Imh0dHBzOi8vaWRwLmV4YW1wbGUuY29tIiwiYXVkIjoidGVzdC1hdWQifQ" +
				".",
			want: "web-identity-user",
		},
		{
			name:  "invalid token returns placeholder",
			token: "not-a-jwt",
			want:  "WebIdentitySubject",
		},
		{
			name:  "empty token returns placeholder",
			token: "",
			want:  "WebIdentitySubject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			resp, err := backend.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::000000000000:role/WIRole",
				RoleSessionName:  "wi-session",
				WebIdentityToken: tt.token,
			})
			if tt.token == "" {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(
				t,
				tt.want,
				resp.AssumeRoleWithWebIdentityResult.SubjectFromWebIdentityToken,
			)
		})
	}
}
