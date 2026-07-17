package appconfigdata_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// --- StartConfigurationSession ---

func TestHandler_StartConfigurationSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(h *appconfigdata.Handler)
		body       []byte
		wantStatus int
		wantToken  bool
	}{
		{
			name: "success_with_active_deployment",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "my-profile", `{}`, "application/json"))
			},
			body: []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "no_active_deployment_returns_404",
			body: []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_application",
			body: []byte(
				`{"EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_environment",
			body: []byte(
				`{"ApplicationIdentifier":"my-app","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_profile",
			body:       []byte(`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       []byte(`not-json`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "poll_interval_too_low_returns_400",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":5}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "poll_interval_zero_is_accepted",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":0}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "poll_interval_exactly_minimum_is_accepted",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":15}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "whitespace_trimmed_from_identifiers",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "my-profile", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"  my-app  ",` +
				`"EnvironmentIdentifier":"prod",` +
				`"ConfigurationProfileIdentifier":"my-profile"}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantToken {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["InitialConfigurationToken"])
			}
		})
	}
}

// TestHandler_NoActiveDeployment verifies that starting a session for a non-existent profile
// returns 404.
func TestHandler_NoActiveDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// No SetConfiguration call — no deployment.
	body := []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env","ConfigurationProfileIdentifier":"p"}`)
	rec := doRequest(t, h, http.MethodPost, "/configurationsessions", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Poll interval ---

func TestHandler_PollIntervalValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int
		wantStatus int
	}{
		{name: "below_minimum_rejected", interval: 5, wantStatus: http.StatusBadRequest},
		{name: "at_minimum_accepted", interval: 15, wantStatus: http.StatusCreated},
		{name: "above_minimum_accepted", interval: 60, wantStatus: http.StatusCreated},
		{name: "zero_uses_default", interval: 0, wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))

			bodyJSON, err := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": tt.interval,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PollIntervalDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "profile", `{}`)
	token := startSession(t, h, "app", "env", "profile")

	cfgRec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, cfgRec.Code)
	assert.Equal(t, "30", cfgRec.Header().Get("Next-Poll-Interval-In-Seconds"))
	assert.NotEmpty(t, cfgRec.Header().Get("ETag"))
	assert.NotEmpty(t, cfgRec.Header().Get("Content-Length"))
}

// TestHandler_PollIntervalHonored verifies that Next-Poll-Interval-In-Seconds always echoes
// the session's declared RequiredMinimumPollIntervalInSeconds, whether that value is above
// or *below* the service default (30s). A prior bug took the larger of the declared interval
// and the default, so a session declaring the AWS-allowed minimum of 15s incorrectly got back
// "30" instead of "15".
func TestHandler_PollIntervalHonored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantInterval string
		interval     int
	}{
		{name: "above_default_interval_honored", interval: 60, wantInterval: "60"},
		{name: "below_default_interval_honored", interval: 15, wantInterval: "15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedProfile(t, h, "app", "env", "profile", `{}`)

			sessionBody, err := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "profile",
				"RequiredMinimumPollIntervalInSeconds": tt.interval,
			})
			require.NoError(t, err)

			sessionRec := doRequest(t, h, http.MethodPost, "/configurationsessions", sessionBody)
			require.Equal(t, http.StatusCreated, sessionRec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(sessionRec.Body.Bytes(), &resp))
			token := resp["InitialConfigurationToken"]

			cfgRec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
			require.Equal(t, http.StatusOK, cfgRec.Code)
			assert.Equal(t, tt.wantInterval, cfgRec.Header().Get("Next-Poll-Interval-In-Seconds"))
		})
	}
}

// TestHandler_StartSession_IdentifierLength verifies that identifiers exceeding 128 chars
// (the real AWS "Identifier" shape's max, per the service model) are rejected with
// BadRequestException.
func TestHandler_StartSession_IdentifierLength(t *testing.T) {
	t.Parallel()

	longID := strings.Repeat("x", 129)

	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "application_too_long",
			body: func() []byte {
				b, _ := json.Marshal(map[string]string{
					"ApplicationIdentifier":          longID,
					"EnvironmentIdentifier":          "env",
					"ConfigurationProfileIdentifier": "p",
				})

				return b
			}(),
		},
		{
			name: "environment_too_long",
			body: func() []byte {
				b, _ := json.Marshal(map[string]string{
					"ApplicationIdentifier":          "app",
					"EnvironmentIdentifier":          longID,
					"ConfigurationProfileIdentifier": "p",
				})

				return b
			}(),
		},
		{
			name: "profile_too_long",
			body: func() []byte {
				b, _ := json.Marshal(map[string]string{
					"ApplicationIdentifier":          "app",
					"EnvironmentIdentifier":          "env",
					"ConfigurationProfileIdentifier": longID,
				})

				return b
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			errType, _ := decodeErrorBody(t, rec.Body.String())
			assert.Equal(t, "BadRequestException", errType)
		})
	}
}

// TestHandler_StartSession_MaxPollInterval verifies that RequiredMinimumPollIntervalInSeconds
// values above 86400 are rejected (AWS-defined upper bound).
func TestHandler_StartSession_MaxPollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int
		wantStatus int
	}{
		{name: "at_max_accepted", interval: 86400, wantStatus: http.StatusCreated},
		{name: "above_max_rejected", interval: 86401, wantStatus: http.StatusBadRequest},
		{name: "large_value_rejected", interval: 999999, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))

			bodyJSON, err := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": tt.interval,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				errType, _ := decodeErrorBody(t, rec.Body.String())
				assert.Equal(t, "BadRequestException", errType)
			}
		})
	}
}

// TestHandler_StartSession_WhitespaceOnlyIdentifiers verifies that identifiers consisting
// only of whitespace are rejected after trimming, the same as empty identifiers.
func TestHandler_StartSession_WhitespaceOnlyIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "whitespace_app",
			body: []byte(
				`{"ApplicationIdentifier":"   ","EnvironmentIdentifier":"env","ConfigurationProfileIdentifier":"p"}`,
			),
		},
		{
			name: "whitespace_env",
			body: []byte(
				`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"  ","ConfigurationProfileIdentifier":"p"}`,
			),
		},
		{
			name: "whitespace_profile",
			body: []byte(
				`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env","ConfigurationProfileIdentifier":"   "}`,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			errType, _ := decodeErrorBody(t, rec.Body.String())
			assert.Equal(t, "BadRequestException", errType)
		})
	}
}

// TestHandler_StartSession_ExactMaxIdentifierLength verifies the boundary: identifiers of
// exactly 128 chars are accepted; 129 chars are rejected. 128 is the real AWS "Identifier"
// shape's max (verified against the service model), not an arbitrary gopherstack choice.
func TestHandler_StartSession_ExactMaxIdentifierLength(t *testing.T) {
	t.Parallel()

	validID := strings.Repeat("a", 128)
	invalidID := strings.Repeat("a", 129)

	h := newTestHandler(t)
	require.NoError(t, h.Backend.SetConfiguration(validID, validID, validID, `{}`, "application/json"))

	t.Run("exactly_128_accepted", func(t *testing.T) {
		t.Parallel()

		bodyJSON, err := json.Marshal(map[string]string{
			"ApplicationIdentifier":          validID,
			"EnvironmentIdentifier":          validID,
			"ConfigurationProfileIdentifier": validID,
		})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
		assert.Equal(t, http.StatusCreated, rec.Code)
	})

	t.Run("129_rejected", func(t *testing.T) {
		t.Parallel()

		bodyJSON, err := json.Marshal(map[string]string{
			"ApplicationIdentifier":          invalidID,
			"EnvironmentIdentifier":          "env",
			"ConfigurationProfileIdentifier": "p",
		})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errType, _ := decodeErrorBody(t, rec.Body.String())
		assert.Equal(t, "BadRequestException", errType)
	})
}

// TestHandler_StartSession_PollInterval_Boundary checks boundary values for poll interval.
func TestHandler_StartSession_PollInterval_Boundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int
		wantStatus int
	}{
		{name: "zero_accepted", interval: 0, wantStatus: http.StatusCreated},
		{name: "1_rejected", interval: 1, wantStatus: http.StatusBadRequest},
		{name: "14_rejected", interval: 14, wantStatus: http.StatusBadRequest},
		{name: "15_accepted", interval: 15, wantStatus: http.StatusCreated},
		{name: "16_accepted", interval: 16, wantStatus: http.StatusCreated},
		{name: "300_accepted", interval: 300, wantStatus: http.StatusCreated},
		{name: "86399_accepted", interval: 86399, wantStatus: http.StatusCreated},
		{name: "86400_accepted", interval: 86400, wantStatus: http.StatusCreated},
		{name: "86401_rejected", interval: 86401, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))

			bodyJSON, err := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": tt.interval,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
			assert.Equal(t, tt.wantStatus, rec.Code, "interval=%d", tt.interval)
		})
	}
}

// --- Backend: StartSession ---

func TestBackend_StartSession_RequiresDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *appconfigdata.InMemoryBackend)
		name    string
	}{
		{
			name:    "no_profile_returns_ErrNoActiveDeployment",
			wantErr: appconfigdata.ErrNoActiveDeployment,
		},
		{
			name: "profile_exists_succeeds",
			setup: func(b *appconfigdata.InMemoryBackend) {
				require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))
			},
			wantErr: nil,
		},
		{
			name: "wrong_profile_key_still_fails",
			setup: func(b *appconfigdata.InMemoryBackend) {
				require.NoError(t, b.SetConfiguration("other-app", "env", "p", `{}`, "application/json"))
			},
			wantErr: appconfigdata.ErrNoActiveDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			_, err := b.StartSession("app", "env", "p", 0)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_TokenHasHighEntropy(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Token format: <64-hex-random>.<16-hex-mac>
	// Total length should be at least 64 chars (32 random bytes in hex).
	assert.GreaterOrEqual(t, len(token), 64, "token must have >= 64 chars (32 random bytes in hex)")
	assert.Contains(t, token, ".", "token must contain MAC separator")
}

func TestBackend_TokenFamilyID(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	sess := b.LookupSession(token)
	require.NotNil(t, sess)
	assert.NotEmpty(t, sess.TokenFamilyID, "session must have a non-empty family ID")

	// Poll and verify the family ID is preserved.
	_, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	sess2 := b.LookupSession(nextToken)
	require.NotNil(t, sess2)
	assert.Equal(t, sess.TokenFamilyID, sess2.TokenFamilyID, "family ID must persist across rotation")
}

// TestBackend_StartSession_TokenFamilyID verifies that sessions share a family ID across rotations.
func TestBackend_StartSession_TokenFamilyID(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	tok, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	sess := b.LookupSession(tok)
	require.NotNil(t, sess)
	familyID := sess.TokenFamilyID
	require.NotEmpty(t, familyID)

	// Poll — token rotates, family must be preserved.
	_, _, nextTok, _, _, err := b.GetLatestConfiguration(tok)
	require.NoError(t, err)

	sess2 := b.LookupSession(nextTok)
	require.NotNil(t, sess2)
	assert.Equal(t, familyID, sess2.TokenFamilyID, "token family must be preserved across rotations")
}
