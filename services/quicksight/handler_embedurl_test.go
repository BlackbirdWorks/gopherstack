package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- Embed URL generation: each op, plus distinctness/expiry-derivation and errors ----

func TestQuickSight_GenerateEmbedUrlForAnonymousUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Missing AuthorizedResourceArns -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/anonymous-user"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	// Unknown namespace -> 404.
	badNsRec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/anonymous-user"), map[string]any{
		"Namespace":              "nosuchns",
		"AuthorizedResourceArns": []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
	})
	assert.Equal(t, http.StatusNotFound, badNsRec.Code)

	rec1 := doRequest(t, h, http.MethodPost, accountPath("/embed-url/anonymous-user"), map[string]any{
		"AuthorizedResourceArns": []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		"ExperienceConfiguration": map[string]any{
			"Dashboard": map[string]any{"InitialDashboardId": "dash1"},
		},
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	body1 := parseBody(t, rec1)
	url1, _ := body1["EmbedUrl"].(string)
	assert.Contains(t, url1, "dashboards/dash1")
	assert.NotEmpty(t, body1["AnonymousUserArn"])

	// A second call for the same dashboard produces a distinct single-use URL.
	rec2 := doRequest(t, h, http.MethodPost, accountPath("/embed-url/anonymous-user"), map[string]any{
		"AuthorizedResourceArns": []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		"ExperienceConfiguration": map[string]any{
			"Dashboard": map[string]any{"InitialDashboardId": "dash1"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	url2 := parseBody(t, rec2)["EmbedUrl"].(string)
	assert.NotEqual(t, url1, url2, "each embed URL call must mint a fresh token")
}

func TestQuickSight_GenerateEmbedUrlForRegisteredUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Missing UserArn -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/registered-user"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	// A well-formed QuickSight user ARN that doesn't resolve to a real user -> 404.
	unknownUserRec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/registered-user"), map[string]any{
		"UserArn": "arn:aws:quicksight:us-east-1:000000000000:user/default/nosuchuser",
	})
	assert.Equal(t, http.StatusNotFound, unknownUserRec.Code)

	registerRec := doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
		"UserName": "embeduser", "Email": "embeduser@example.com",
		"IdentityType": "QUICKSIGHT", "UserRole": "READER",
	})
	require.Equal(t, http.StatusOK, registerRec.Code)

	rec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/registered-user"), map[string]any{
		"UserArn": "arn:aws:quicksight:us-east-1:000000000000:user/default/embeduser",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, parseBody(t, rec)["EmbedUrl"])
}

func TestQuickSight_GenerateEmbedUrlForRegisteredUserWithIdentity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/embed-url/registered-user-with-identity"), map[string]any{
		"ExperienceConfiguration": map[string]any{"QuickSightConsole": map[string]any{}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, parseBody(t, rec)["EmbedUrl"])
}

func TestQuickSight_GetDashboardEmbedUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDashRec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{
		"Name": "Dashboard1",
	})
	require.Equal(t, http.StatusOK, createDashRec.Code)

	// Missing required creds-type -> validation error.
	invalidRec := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/embed-url"), nil)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	// Unknown dashboard -> 404.
	missingRec := doRequest(
		t, h, http.MethodGet, accountPath("/dashboards/notexist/embed-url")+"?creds-type=QUICKSIGHT", nil,
	)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	rec := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/embed-url")+"?creds-type=QUICKSIGHT", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	url, _ := parseBody(t, rec)["EmbedUrl"].(string)
	assert.Contains(t, url, "dashboards/dash1")
}

func TestQuickSight_GetSessionEmbedUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, accountPath("/session-embed-url")+"?entry-point=/start/dashboards", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	url, _ := parseBody(t, rec)["EmbedUrl"].(string)
	assert.Contains(t, url, "start/dashboards")
}

// ---- Embed URL tests ---- //nolint:godot // existing issue.
func TestQuickSight_EmbedURLs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// GenerateEmbedUrlForRegisteredUser requires an existing registered user.
	setupRec := doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
		"UserName": "embeduser1", "Email": "embeduser1@example.com",
		"IdentityType": "QUICKSIGHT", "UserRole": "READER",
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:   "generate embed url for anonymous user",
			method: http.MethodPost,
			path:   accountPath("/embed-url/anonymous-user"),
			body: map[string]any{
				"AuthorizedResourceArns": []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
				"ExperienceConfiguration": map[string]any{
					"Dashboard": map[string]any{"InitialDashboardId": "dash1"},
				},
			},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:   "generate embed url for registered user",
			method: http.MethodPost,
			path:   accountPath("/embed-url/registered-user"),
			body: map[string]any{
				"UserArn": "arn:aws:quicksight:us-east-1:000000000000:user/default/embeduser1",
			},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "generate embed url for registered user with identity",
			method:     http.MethodPost,
			path:       accountPath("/embed-url/registered-user-with-identity"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "get session embed url",
			method:     http.MethodGet,
			path:       accountPath("/session-embed-url"),
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

// ---- GetIdentityContext ----

func TestQuickSight_GetIdentityContext(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      testNamespace,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	ctx1, ok := body["Context"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, ctx1)
	assert.NotContains(t, body, "IdentityContextDomains")

	// Each call mints a fresh token.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      testNamespace,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	ctx2 := parseBody(t, rec)["Context"].(string) //nolint:forcetypeassert // asserted above
	assert.NotEqual(t, ctx1, ctx2)

	// Email/UserName identifiers require a Namespace.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"Email": "u1@example.com"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing UserIdentifier entirely.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Unknown namespace.
	rec = doRequest(t, h, http.MethodPost, accountPath("/identity-context"), map[string]any{
		"UserIdentifier": map[string]any{"UserName": "u1"},
		"Namespace":      "no-such-ns",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestEmbedURLsAreUnique verifies that embed URL operations return distinct URLs per call.
func TestEmbedURLsAreUnique(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	doRequest(t, h, http.MethodPost, accountPath("/dashboards/d1"), map[string]any{"Name": "Dash"})

	embedPaths := []string{
		accountPath("/dashboards/d1/embed-url") + "?creds-type=QUICKSIGHT",
		"/accounts/000000000000/session-embed-url",
	}

	for _, path := range embedPaths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			rec1 := doRequest(t, h, http.MethodGet, path, nil)
			rec2 := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec1.Code)
			require.Equal(t, http.StatusOK, rec2.Code)

			url1, _ := parseBody(t, rec1)["EmbedUrl"].(string)
			url2, _ := parseBody(t, rec2)["EmbedUrl"].(string)
			assert.NotEmpty(t, url1)
			assert.NotEqual(t, url1, url2, "each embed URL call must return a unique URL")
		})
	}
}
