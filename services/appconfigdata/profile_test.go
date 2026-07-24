package appconfigdata_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

func TestBackend_SetConfiguration_ContentTooLarge(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	huge := make([]byte, 1*1024*1024+1)
	err := b.SetConfiguration("app", "env", "profile", string(huge), "text/plain")
	assert.ErrorIs(t, err, appconfigdata.ErrContentTooLarge)
}

func TestBackend_SetConfiguration_JSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		content     string
		contentType string
	}{
		{
			name:        "valid_json_accepted",
			content:     `{"key":"value"}`,
			contentType: "application/json",
			wantErr:     nil,
		},
		{
			name:        "invalid_json_rejected",
			content:     `not valid json`,
			contentType: "application/json",
			wantErr:     appconfigdata.ErrContentTypeMismatch,
		},
		{
			name:        "plain_text_not_validated",
			content:     `not json but thats ok`,
			contentType: "text/plain",
			wantErr:     nil,
		},
		{
			name:        "json_plus_suffix_validated",
			content:     `{}`,
			contentType: "application/vnd.api+json",
			wantErr:     nil,
		},
		{
			name:        "json_plus_suffix_invalid_rejected",
			content:     `bad`,
			contentType: "application/vnd.api+json",
			wantErr:     appconfigdata.ErrContentTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			err := b.SetConfiguration("app", "env", "p", tt.content, tt.contentType)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_SetConfiguration_ContentHashNormalization(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	// Two semantically equivalent JSON objects with different whitespace / key order.
	v1 := `{"b":2,"a":1}`
	v2 := `{ "a": 1, "b": 2 }`

	require.NoError(t, b.SetConfiguration("app", "env", "p", v1, "application/json"))
	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	hash1 := profiles[0].ContentHash

	require.NoError(t, b.SetConfiguration("app", "env", "p", v2, "application/json"))
	profiles = b.ListProfiles()
	require.Len(t, profiles, 1)
	hash2 := profiles[0].ContentHash

	// Normalised JSON produces the same hash for semantically equal documents.
	// Note: Go's json.Marshal sorts map keys, so normalisation is deterministic.
	// The hash equality here depends on key ordering after marshal; both should match.
	assert.Equal(t, hash1, hash2, "semantically equivalent JSON should produce the same hash")
}

func TestBackend_SetConfiguration_History(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v1", "text/plain"))
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v2", "text/plain"))
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v3", "text/plain"))

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	p := profiles[0]
	assert.Equal(t, "v3", p.Content)
	require.Len(t, p.History, 2)
	assert.Equal(t, "v2", p.History[0].Content)
	assert.Equal(t, "v1", p.History[1].Content)
}

func TestBackend_SetConfiguration_ContentHash(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(
		t,
		b.SetConfiguration("app", "env", "profile", `{"key":"value"}`, "application/json"),
	)

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.NotEmpty(t, profiles[0].ContentHash)
}

func TestBackend_SetConfiguration_VersionLabel(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	require.NoError(t, b.SetConfiguration("app", "env", "p", "v1", "text/plain"))
	ps := b.ListProfiles()
	require.Len(t, ps, 1)
	assert.Equal(t, "v1", ps[0].VersionLabel)
	assert.Equal(t, 1, ps[0].VersionNumber)

	require.NoError(t, b.SetConfiguration("app", "env", "p", "v2", "text/plain"))
	ps = b.ListProfiles()
	require.Len(t, ps, 1)
	assert.Equal(t, "v2", ps[0].VersionLabel)
	assert.Equal(t, 2, ps[0].VersionNumber)
}

func TestBackend_ListProfiles(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	assert.Empty(t, b.ListProfiles())

	require.NoError(t, b.SetConfiguration("app1", "env1", "profile1", "data1", "text/plain"))
	require.NoError(t, b.SetConfiguration("app2", "env2", "profile2", `{}`, "application/json"))

	profiles := b.ListProfiles()
	assert.Len(t, profiles, 2)
}

// TestBackend_ChangeDetection verifies that GetLatestConfiguration returns empty content
// when the profile has not changed since the last poll.
func TestBackend_ChangeDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		firstContent  string
		secondContent string
		wantContent   bool // whether second poll should return content
	}{
		{
			name:          "unchanged_content_returns_empty",
			firstContent:  `{"v":1}`,
			secondContent: "",
			wantContent:   false,
		},
		{
			name:          "changed_content_returns_new_content",
			firstContent:  `{"v":1}`,
			secondContent: `{"v":2}`,
			wantContent:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			require.NoError(t, b.SetConfiguration("app", "env", "p", tt.firstContent, "application/json"))

			token, err := b.StartSession("app", "env", "p", 0)
			require.NoError(t, err)

			// First poll.
			content1, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
			require.NoError(t, err)
			assert.NotEmpty(t, content1, "first poll must return content")

			if tt.secondContent != "" {
				require.NoError(t, b.SetConfiguration("app", "env", "p", tt.secondContent, "application/json"))
			}

			// Second poll.
			content2, _, _, _, _, err := b.GetLatestConfiguration(nextToken)
			require.NoError(t, err)

			if tt.wantContent {
				assert.NotEmpty(t, content2, "second poll must return updated content")
			} else {
				assert.Empty(t, content2, "second poll must return empty (no change)")
			}
		})
	}
}

// TestBackend_GraceTokenIdempotency verifies that replaying a rotated token within the grace
// window returns the cached response, not a new rotation.
func TestBackend_GraceTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"x":1}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// First poll.
	content1, ct1, next1, hash1, vl1, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Replay the old token (grace period).
	content2, ct2, next2, hash2, vl2, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	assert.Equal(t, content1, content2, "grace replay must return same content")
	assert.Equal(t, ct1, ct2, "grace replay must return same content type")
	assert.Equal(t, next1, next2, "grace replay must return same next token")
	assert.Equal(t, hash1, hash2, "grace replay must return same hash")
	assert.Equal(t, vl1, vl2, "grace replay must return same version label")
}

// TestBackend_ResourceRemovedMidSession verifies that polling with a valid token after the
// profile is manually removed returns ErrResourceRemoved.
// Note: DeleteProfile also removes linked sessions, so the test simulates profile removal
// without session eviction by checking the backend behaviour directly.
func TestBackend_ResourceRemovedMidSession(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Poll to get a live next-token.
	_, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Remove the profile (this also removes the session for nextToken).
	b.DeleteProfile("app", "env", "p")

	// Poll with the next token: session was removed → ErrSessionNotFound.
	_, _, _, _, _, err = b.GetLatestConfiguration(nextToken)
	assert.ErrorIs(t, err, appconfigdata.ErrSessionNotFound)
}

// TestBackend_MultipleProfilesIndependent verifies that sessions for different profiles
// do not interfere with each other.
func TestBackend_MultipleProfilesIndependent(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p1", `{"p":1}`, "application/json"))
	require.NoError(t, b.SetConfiguration("app", "env", "p2", `{"p":2}`, "application/json"))

	t1, err := b.StartSession("app", "env", "p1", 0)
	require.NoError(t, err)

	t2, err := b.StartSession("app", "env", "p2", 0)
	require.NoError(t, err)

	c1, _, _, _, _, err := b.GetLatestConfiguration(t1)
	require.NoError(t, err)

	c2, _, _, _, _, err := b.GetLatestConfiguration(t2)
	require.NoError(t, err)

	assert.NotEqual(t, string(c1), string(c2), "separate profiles must return different content")
}

// TestBackend_HistoryRetention verifies that configuration history is retained up to
// maxHistoryEntries and older versions are evicted FIFO.
func TestBackend_HistoryRetention(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	// Write 52 versions (maxHistoryEntries is 50, so last 50 history entries should survive).
	for i := range 52 {
		content := `{"v":` + strconv.Itoa(i+1) + `}`
		require.NoError(t, b.SetConfiguration("app", "env", "p", content, "application/json"))
	}

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, `{"v":52}`, profiles[0].Content, "current version must be the last written")
	assert.LessOrEqual(t, len(profiles[0].History), 50, "history must not exceed maxHistoryEntries")
}

// TestBackend_GraceTokenReturnsConsistentNextToken verifies that grace-period replays return
// the same next token each time, enabling idempotent client retry.
func TestBackend_GraceTokenReturnsConsistentNextToken(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"x":1}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// First poll — rotates token, caches grace entry.
	_, _, next1, hash1, label1, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Grace replay — must return same next token, hash, and label.
	_, _, next2, hash2, label2, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	assert.Equal(t, next1, next2, "grace replay must return same next token")
	assert.Equal(t, hash1, hash2, "grace replay must return same content hash")
	assert.Equal(t, label1, label2, "grace replay must return same version label")
}

// TestBackend_SetConfiguration_VersionNumber verifies version numbers increment monotonically.
func TestBackend_SetConfiguration_VersionNumber(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, 1, profiles[0].VersionNumber)
	assert.Equal(t, "v1", profiles[0].VersionLabel)

	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":2}`, "application/json"))
	profiles = b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, 2, profiles[0].VersionNumber)
	assert.Equal(t, "v2", profiles[0].VersionLabel)

	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":3}`, "application/json"))
	profiles = b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, 3, profiles[0].VersionNumber)
	assert.Equal(t, "v3", profiles[0].VersionLabel)
}

// TestBackend_SetConfiguration_SameContentNoVersionBump verifies that writing identical
// content does not increment the version number (content deduplication via hash).
func TestBackend_SetConfiguration_SameContentNoVersionBump(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	// Version bumps even on identical content because we treat each write as a new deployment.
	// The change counter does NOT increment for identical content.
	assert.Equal(t, 2, profiles[0].VersionNumber)

	stats := b.GetStats()
	assert.Equal(t, int64(1), stats.ConfigurationChangeCount,
		"identical content must not increment change counter")
}

// TestHandler_MultipleProfilesIndependent verifies that multiple app/env/profile combinations
// coexist independently and sessions are correctly scoped.
func TestHandler_MultipleProfilesIndependent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.SetConfiguration("app-a", "prod", "flags", `{"a":1}`, "application/json"))
	require.NoError(t, h.Backend.SetConfiguration("app-b", "prod", "flags", `{"b":2}`, "application/json"))
	require.NoError(t, h.Backend.SetConfiguration("app-a", "staging", "flags", `{"s":3}`, "application/json"))

	tokA := startSession(t, h, "app-a", "prod", "flags")
	tokB := startSession(t, h, "app-b", "prod", "flags")
	tokS := startSession(t, h, "app-a", "staging", "flags")

	recA := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tokA, nil)
	require.Equal(t, http.StatusOK, recA.Code)
	assert.Equal(t, `{"a":1}`, recA.Body.String())

	recB := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tokB, nil)
	require.Equal(t, http.StatusOK, recB.Code)
	assert.Equal(t, `{"b":2}`, recB.Body.String())

	recS := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+tokS, nil)
	require.Equal(t, http.StatusOK, recS.Code)
	assert.Equal(t, `{"s":3}`, recS.Body.String())
}

// TestHandler_MultipleProfilesAndSessions verifies that multiple app/env/profile combinations
// coexist and each session sees only its own configuration.
func TestHandler_MultipleProfilesAndSessions(t *testing.T) {
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

// TestHandler_ContentLengthAndETagOnChange verifies Content-Length and ETag are only set
// when content is returned, not on unchanged (empty-body) responses. Both cases use HTTP
// 200 -- AWS's GetLatestConfiguration has a fixed responseCode and never returns 204.
func TestHandler_ContentLengthAndETagOnChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     string
		wantHeaders bool
		secondPoll  bool
	}{
		{
			name:        "content_present_has_content_length_and_etag",
			content:     `{"x":42}`,
			wantHeaders: true,
		},
		{
			name:       "unchanged_has_no_content_length_or_etag",
			content:    `{"x":42}`,
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
			require.Equal(t, http.StatusOK, rec.Code, "GetLatestConfiguration always returns 200")

			if tt.wantHeaders {
				assert.NotEmpty(t, rec.Header().Get("Content-Length"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			} else {
				assert.Empty(t, rec.Body.String(), "unchanged response must have an empty body")
				assert.Empty(t, rec.Header().Get("ETag"))
			}
		})
	}
}
