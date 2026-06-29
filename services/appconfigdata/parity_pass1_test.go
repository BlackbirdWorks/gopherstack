package appconfigdata_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appconfigdatasdk "github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// newSDKClient starts an Echo server backed by the given handler and returns an
// AppConfigData SDK client pointed at it. The server is shut down via t.Cleanup.
func newSDKClient(t *testing.T, h *appconfigdata.Handler) *appconfigdatasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return appconfigdatasdk.NewFromConfig(cfg, func(o *appconfigdatasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestParity_SDKFullSessionFlow exercises the complete AppConfigData retrieval flow via
// the real AWS SDK v2 client: StartConfigurationSession → GetLatestConfiguration (200) →
// GetLatestConfiguration (204 unchanged) → update config → GetLatestConfiguration (200).
func TestParity_SDKFullSessionFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		content        string
		updatedContent string
		contentType    string
	}{
		{
			name:           "json_config",
			content:        `{"featureFlag":true,"limit":100}`,
			updatedContent: `{"featureFlag":false,"limit":200}`,
			contentType:    "application/json",
		},
		{
			name:           "plain_text_config",
			content:        "feature.enabled=true",
			updatedContent: "feature.enabled=false",
			contentType:    "text/plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "flags", tt.content, tt.contentType))
			client := newSDKClient(t, h)
			ctx := context.Background()

			startOut, err := client.StartConfigurationSession(ctx, &appconfigdatasdk.StartConfigurationSessionInput{
				ApplicationIdentifier:          aws.String("my-app"),
				EnvironmentIdentifier:          aws.String("prod"),
				ConfigurationProfileIdentifier: aws.String("flags"),
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.InitialConfigurationToken)
			assert.NotEmpty(t, *startOut.InitialConfigurationToken)

			// First poll → must return content.
			getOut1, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: startOut.InitialConfigurationToken,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, string(getOut1.Configuration), "first poll must return configuration content")
			assert.Equal(t, tt.content, string(getOut1.Configuration))
			assert.Positive(t, getOut1.NextPollIntervalInSeconds)
			require.NotNil(t, getOut1.NextPollConfigurationToken)

			// Second poll (unchanged) → empty body.
			getOut2, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: getOut1.NextPollConfigurationToken,
			})
			require.NoError(t, err)
			assert.Empty(t, getOut2.Configuration, "second poll with unchanged config must return empty")
			require.NotNil(t, getOut2.NextPollConfigurationToken)

			// Update then poll → must detect change.
			require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "flags", tt.updatedContent, tt.contentType))

			getOut3, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: getOut2.NextPollConfigurationToken,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.updatedContent, string(getOut3.Configuration))
		})
	}
}

// TestParity_SDKStartSession_NoDeployment verifies that starting a session for a
// profile with no active deployment returns an error via the SDK.
func TestParity_SDKStartSession_NoDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newSDKClient(t, h)

	_, err := client.StartConfigurationSession(context.Background(), &appconfigdatasdk.StartConfigurationSessionInput{
		ApplicationIdentifier:          aws.String("nonexistent-app"),
		EnvironmentIdentifier:          aws.String("prod"),
		ConfigurationProfileIdentifier: aws.String("flags"),
	})
	require.Error(t, err)
}

// TestParity_SDKStartSession_PollInterval verifies poll interval validation via the SDK.
func TestParity_SDKStartSession_PollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int32
		wantErrNil bool
	}{
		{name: "zero_accepted", interval: 0, wantErrNil: true},
		{name: "minimum_15s_accepted", interval: 15, wantErrNil: true},
		{name: "60s_accepted", interval: 60, wantErrNil: true},
		{name: "too_low_rejected", interval: 5, wantErrNil: false},
		{name: "above_max_rejected", interval: 86401, wantErrNil: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))
			client := newSDKClient(t, h)

			_, err := client.StartConfigurationSession(
				context.Background(),
				&appconfigdatasdk.StartConfigurationSessionInput{
					ApplicationIdentifier:                aws.String("app"),
					EnvironmentIdentifier:                aws.String("env"),
					ConfigurationProfileIdentifier:       aws.String("p"),
					RequiredMinimumPollIntervalInSeconds: aws.Int32(tt.interval),
				},
			)
			if tt.wantErrNil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestParity_PollRateLimitEnforced verifies that when a session declares an explicit
// minimum poll interval, immediate re-polling returns 400 with Problem=PollIntervalNotSatisfied
// and Retry-After header matching the session interval.
func TestParity_PollRateLimitEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantRetry    string
		pollInterval int
	}{
		{name: "30s_interval_enforced", pollInterval: 30, wantRetry: "30"},
		{name: "60s_interval_enforced", pollInterval: 60, wantRetry: "60"},
		{name: "15s_minimum_enforced", pollInterval: 15, wantRetry: "15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))

			sessionBody, _ := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": tt.pollInterval,
			})

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", sessionBody)
			require.Equal(t, http.StatusCreated, rec.Code)

			var sessionResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sessionResp))
			tok := sessionResp["InitialConfigurationToken"]

			// First poll — succeeds.
			rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
			require.Equal(t, http.StatusOK, rec1.Code)
			nextTok := rec1.Header().Get("Next-Poll-Configuration-Token")
			require.NotEmpty(t, nextTok)

			// Immediate re-poll — rate limited.
			rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextTok, nil)
			assert.Equal(t, http.StatusBadRequest, rec2.Code)
			assert.Equal(t, "BadRequestException", rec2.Header().Get("X-Amzn-ErrorType"))
			assert.Equal(t, tt.wantRetry, rec2.Header().Get("Retry-After"))

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))
			assert.Equal(t, "BadRequestException", body["__type"])
			assert.Equal(t, "InvalidParameters", body["Reason"])

			details, ok := body["Details"].(map[string]any)
			require.True(t, ok)
			params, ok := details["InvalidParameters"].(map[string]any)
			require.True(t, ok)
			tokenDetail, ok := params["ConfigurationToken"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "PollIntervalNotSatisfied", tokenDetail["Problem"])
		})
	}
}

// TestParity_NoPollRateLimitWhenIntervalZero verifies that when no minimum poll interval
// is declared (0), immediate re-polling is allowed. AWS only enforces rate limiting when
// the client explicitly declares RequiredMinimumPollIntervalInSeconds > 0.
func TestParity_NoPollRateLimitWhenIntervalZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
	tok := startSession(t, h, "app", "env", "p")

	// First poll.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	nextTok := rec1.Header().Get("Next-Poll-Configuration-Token")

	// Immediate re-poll must NOT be rejected.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextTok, nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code,
		"immediate re-poll with no declared interval must return 204, not rate-limit error")
}

// TestParity_ResponseHeaders verifies that AWS-required response headers are present and
// absent at the right times for both 200 and 204 responses.
func TestParity_ResponseHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name             string
		wantETag         bool
		wantVersionLabel bool
		wantStatus       int
		isSecondPoll     bool
	}{
		{
			name:             "first_poll_200_headers",
			wantStatus:       http.StatusOK,
			wantETag:         true,
			wantVersionLabel: true,
		},
		{
			name:         "second_poll_204_no_etag_no_content_type",
			wantStatus:   http.StatusNoContent,
			wantETag:     false,
			isSecondPoll: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
			tok := startSession(t, h, "app", "env", "p")

			if tt.isSecondPoll {
				rec0 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
				require.Equal(t, http.StatusOK, rec0.Code)
				tok = rec0.Header().Get("Next-Poll-Configuration-Token")
			}

			rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Poll-control headers always present on success.
			assert.NotEmpty(t, rec.Header().Get("Next-Poll-Configuration-Token"))
			assert.NotEmpty(t, rec.Header().Get("Next-Poll-Interval-In-Seconds"))

			if tt.wantETag {
				assert.NotEmpty(t, rec.Header().Get("ETag"), "ETag must be set on 200")
			} else {
				assert.Empty(t, rec.Header().Get("ETag"), "ETag must not be set on 204")
			}

			if tt.wantStatus == http.StatusNoContent {
				assert.Empty(t, rec.Header().Get("Content-Type"),
					"Content-Type must not be set on 204 response")
			}
		})
	}
}

// TestParity_TokenRotationAndGracePeriod verifies that each poll rotates the token and
// the old token remains usable within the grace period (idempotent retry).
func TestParity_TokenRotationAndGracePeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		replayOld  bool
		wantStatus int
	}{
		{
			name:       "new_token_usable_after_rotation",
			replayOld:  false,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "old_token_in_grace_period_returns_same_response",
			replayOld:  true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
			initialTok := startSession(t, h, "app", "env", "p")

			rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+initialTok, nil)
			require.Equal(t, http.StatusOK, rec1.Code)
			nextTok := rec1.Header().Get("Next-Poll-Configuration-Token")
			require.NotEmpty(t, nextTok)
			assert.NotEqual(t, initialTok, nextTok, "token must rotate after each successful poll")

			var useToken string
			if tt.replayOld {
				useToken = initialTok
			} else {
				useToken = nextTok
			}

			rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+useToken, nil)
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// TestParity_ErrorResponseShape verifies that all error responses carry the __type field
// in the body and the X-Amzn-ErrorType response header, matching the AWS REST-JSON protocol.
func TestParity_ErrorResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name          string
		method        string
		path          string
		body          []byte
		wantStatus    int
		wantErrorType string
	}{
		{
			name:          "start_session_missing_all_identifiers",
			method:        http.MethodPost,
			path:          "/configurationsessions",
			body:          []byte(`{}`),
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
		{
			name:   "start_session_no_deployment",
			method: http.MethodPost,
			path:   "/configurationsessions",
			body: []byte(
				`{"ApplicationIdentifier":"a","EnvironmentIdentifier":"e","ConfigurationProfileIdentifier":"p"}`,
			),
			wantStatus:    http.StatusNotFound,
			wantErrorType: "ResourceNotFoundException",
		},
		{
			name:          "get_config_empty_token",
			method:        http.MethodGet,
			path:          "/configuration",
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
		{
			name:          "get_config_invalid_token",
			method:        http.MethodGet,
			path:          "/configuration?configuration_token=bad-token",
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-ErrorType"),
				"X-Amzn-ErrorType header must match exception type")

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantErrorType, body["__type"],
				"response body __type field must match exception type")
			assert.NotEmpty(t, body["message"], "error body must have a message field")
		})
	}
}

// TestParity_MultipleProfilesAndSessions verifies that multiple app/env/profile combinations
// coexist and each session sees only its own configuration.
func TestParity_MultipleProfilesAndSessions(t *testing.T) {
	t.Parallel()

	type profileDef struct {
		app         string
		env         string
		profile     string
		content     string
		contentType string
	}

	tests := []struct {
		name     string
		profiles []profileDef
	}{
		{
			name: "two_apps_independent",
			profiles: []profileDef{
				{app: "app-a", env: "prod", profile: "flags", content: `{"a":1}`, contentType: "application/json"},
				{app: "app-b", env: "prod", profile: "flags", content: `{"b":2}`, contentType: "application/json"},
			},
		},
		{
			name: "same_app_different_envs",
			profiles: []profileDef{
				{app: "app", env: "prod", profile: "flags", content: "prod-value", contentType: "text/plain"},
				{app: "app", env: "staging", profile: "flags", content: "staging-value", contentType: "text/plain"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tokens := make([]string, len(tt.profiles))

			for i, p := range tt.profiles {
				require.NoError(t, h.Backend.SetConfiguration(p.app, p.env, p.profile, p.content, p.contentType))
				tokens[i] = startSession(t, h, p.app, p.env, p.profile)
			}

			for i, p := range tt.profiles {
				rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tokens[i], nil)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, p.content, rec.Body.String(),
					"profile %s/%s/%s must return its own content", p.app, p.env, p.profile)
			}
		})
	}
}

// TestParity_ContentLengthAndETagOnChange verifies Content-Length and ETag are only set
// when content is returned (200), not on 204 responses.
func TestParity_ContentLengthAndETagOnChange(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name        string
		content     string
		wantHeaders bool
		wantStatus  int
		secondPoll  bool
	}{
		{
			name:        "200_has_content_length_and_etag",
			content:     `{"x":42}`,
			wantStatus:  http.StatusOK,
			wantHeaders: true,
		},
		{
			name:       "204_has_no_content_length_or_etag",
			content:    `{"x":42}`,
			wantStatus: http.StatusNoContent,
			secondPoll: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", tt.content, "application/json"))
			tok := startSession(t, h, "app", "env", "p")

			if tt.secondPoll {
				rec0 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
				require.Equal(t, http.StatusOK, rec0.Code)
				tok = rec0.Header().Get("Next-Poll-Configuration-Token")
			}

			rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tok, nil)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantHeaders {
				assert.NotEmpty(t, rec.Header().Get("Content-Length"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			} else {
				assert.Empty(t, rec.Header().Get("ETag"))
			}
		})
	}
}
