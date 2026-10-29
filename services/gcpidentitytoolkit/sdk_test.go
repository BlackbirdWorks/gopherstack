package gcpidentitytoolkit_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/auth"
	"firebase.google.com/go/v4/auth/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// sub returns an env on the same server with its own project, so subtests never see each other's accounts.
func (e *env) sub(t *testing.T) *env {
	t.Helper()

	out := *e
	out.project = "sub-" + strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))

	return &out
}

func (e *env) sdk(t *testing.T) *auth.Client {
	t.Helper()

	app, err := firebase.NewApp(
		context.Background(),
		&firebase.Config{ProjectID: e.project},
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)

	c, err := app.Auth(context.Background())
	require.NoError(t, err)

	return c
}

func (e *env) signInToken(t *testing.T, email, pw string) result {
	t.Helper()

	r := e.client(t, "signInWithPassword", map[string]any{"email": email, "password": pw, "returnSecureToken": true})
	require.Equal(t, http.StatusOK, r.status, r.body)

	return r
}

// TestAdminSDK drives the server through firebase.google.com/go/v4/auth in FIREBASE_AUTH_EMULATOR_HOST mode.
// t.Setenv forbids t.Parallel on this parent; its subtests run in parallel.
func TestAdminSDK(t *testing.T) { //nolint:tparallel // t.Setenv forbids t.Parallel on the parent
	root := newEnv(t)

	u, err := url.Parse(root.url)
	require.NoError(t, err)
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", u.Host)

	run := func(t *testing.T, f func(*testing.T, *env, *auth.Client)) {
		t.Helper()

		e := root.sub(t)
		e.hc = testClient(t)
		f(t, e, e.sdk(t))
	}

	// These own the shared clock (revocation compares second-resolution timestamps), so they run first and alone.
	for _, tt := range []struct {
		run  func(*testing.T, *env, *auth.Client)
		name string
	}{
		{name: "revoke and disable", run: sdkRevokeDisable},
		{name: "session cookie", run: sdkSessionCookie},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root.clock.Set(skewedNow())
			run(t, tt.run)
		})
	}

	for _, tt := range []struct {
		run  func(*testing.T, *env, *auth.Client)
		name string
	}{
		{name: "users crud", run: sdkUsersCRUD},
		{name: "list and query", run: sdkListQuery},
		{name: "tokens and claims", run: sdkTokensClaims},
		{name: "custom token", run: sdkCustomToken},
		{name: "import hashes", run: sdkImportHashes},
		{name: "tenants", run: sdkTenants},
		{name: "email links", run: sdkEmailLinks},
		{name: "delete users", run: sdkDeleteUsers},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			run(t, tt.run)
		})
	}
}

func sdkUsersCRUD(t *testing.T, _ *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	created, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID("sdk-u1").Email("Sdk@Example.com").Password("secret1").
		DisplayName("Sdk User").PhotoURL("http://x/p.png").PhoneNumber("+15555550123").EmailVerified(true))
	require.NoError(t, err)
	assert.Equal(t, "sdk-u1", created.UID)
	assert.Equal(t, "sdk@example.com", created.Email)
	assert.True(t, created.EmailVerified)
	assert.Equal(t, "Sdk User", created.DisplayName)
	assert.Equal(t, "+15555550123", created.PhoneNumber)
	assert.NotZero(t, created.UserMetadata.CreationTimestamp)
	require.Len(t, created.ProviderUserInfo, 2)
	assert.Equal(t, "password", created.ProviderUserInfo[0].ProviderID)

	_, err = c.CreateUser(ctx, (&auth.UserToCreate{}).UID("sdk-u1"))
	assert.True(t, auth.IsUIDAlreadyExists(err), err)

	_, err = c.CreateUser(ctx, (&auth.UserToCreate{}).Email("sdk@example.com"))
	assert.True(t, auth.IsEmailAlreadyExists(err), err)

	_, err = c.CreateUser(ctx, (&auth.UserToCreate{}).PhoneNumber("+15555550123"))
	assert.True(t, auth.IsPhoneNumberAlreadyExists(err), err)

	byMail, err := c.GetUserByEmail(ctx, "sdk@example.com")
	require.NoError(t, err)
	assert.Equal(t, "sdk-u1", byMail.UID)

	byPhone, err := c.GetUserByPhoneNumber(ctx, "+15555550123")
	require.NoError(t, err)
	assert.Equal(t, "sdk-u1", byPhone.UID)

	byProv, err := c.GetUserByProviderUID(ctx, "password", "sdk@example.com")
	require.NoError(t, err)
	assert.Equal(t, "sdk-u1", byProv.UID)

	updated, err := c.UpdateUser(ctx, "sdk-u1", (&auth.UserToUpdate{}).DisplayName("").PhotoURL("").
		Email("new@example.com").Disabled(true).PhoneNumber(""))
	require.NoError(t, err)
	assert.Empty(t, updated.DisplayName)
	assert.Empty(t, updated.PhotoURL)
	assert.Empty(t, updated.PhoneNumber)
	assert.Equal(t, "new@example.com", updated.Email)
	assert.True(t, updated.Disabled)
	assert.Len(t, updated.ProviderUserInfo, 1)

	_, err = c.GetUser(ctx, "missing")
	assert.True(t, auth.IsUserNotFound(err), err)

	require.NoError(t, c.DeleteUser(ctx, "sdk-u1"))
	assert.True(t, auth.IsUserNotFound(c.DeleteUser(ctx, "sdk-u1")))

	res, err := c.GetUsers(
		ctx,
		[]auth.UserIdentifier{auth.UIDIdentifier{UID: "nope"}, auth.EmailIdentifier{Email: "x@example.com"}},
	)
	require.NoError(t, err)
	assert.Empty(t, res.Users)
	assert.Len(t, res.NotFound, 2)
}

func sdkListQuery(t *testing.T, _ *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	for _, id := range []string{"l1", "l2", "l3"} {
		_, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID(id).Email(id+"@example.com").Password("secret1"))
		require.NoError(t, err)
	}

	it := c.Users(ctx, "")
	seen := map[string]string{}

	for {
		u, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)
		seen[u.UID] = u.PasswordHash
	}

	assert.Len(t, seen, 3)

	for _, h := range seen {
		assert.NotEmpty(t, h, "admin export returns the real hash")
	}

	q, err := c.QueryUsers(ctx, &auth.QueryUsersRequest{Expression: []*auth.Expression{{Email: "l2@example.com"}}})
	require.NoError(t, err)
	assert.EqualValues(t, 1, q.Count)
	require.Len(t, q.Users, 1)
	assert.Equal(t, "l2", q.Users[0].UID)
}

func sdkTokensClaims(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := c.CreateUser(
		ctx,
		(&auth.UserToCreate{}).UID("t1").Email("t1@example.com").Password("secret1").DisplayName("T One"),
	)
	require.NoError(t, err)
	require.NoError(t, c.SetCustomUserClaims(ctx, "t1", map[string]any{"role": "admin", "level": 7}))

	in := e.signInToken(t, "t1@example.com", "secret1")

	tok, err := c.VerifyIDToken(ctx, in.str("idToken"))
	require.NoError(t, err)
	assert.Equal(t, "t1", tok.UID)
	assert.Equal(t, "admin", tok.Claims["role"])
	assert.EqualValues(t, 7, tok.Claims["level"])
	assert.Equal(t, "password", tok.Firebase.SignInProvider)
	assert.Equal(t, "https://securetoken.google.com/"+e.project, tok.Issuer)
	assert.Equal(t, "t1@example.com", tok.Claims["email"])
	assert.Equal(t, "T One", tok.Claims["name"])
	assert.Equal(t, map[string]any{"email": []any{"t1@example.com"}}, tok.Firebase.Identities)

	_, err = c.VerifyIDToken(ctx, "garbage")
	assert.True(t, auth.IsIDTokenInvalid(err), err)

	rec, err := c.GetUser(ctx, "t1")
	require.NoError(t, err)
	assert.Equal(t, "admin", rec.CustomClaims["role"])

	require.NoError(t, c.SetCustomUserClaims(ctx, "t1", nil))

	rec, err = c.GetUser(ctx, "t1")
	require.NoError(t, err)
	assert.Empty(t, rec.CustomClaims)
}

func sdkRevokeDisable(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID("r1").Email("r1@example.com").Password("secret1"))
	require.NoError(t, err)

	in := e.signInToken(t, "r1@example.com", "secret1")

	_, err = c.VerifyIDTokenAndCheckRevoked(ctx, in.str("idToken"))
	require.NoError(t, err)

	require.NoError(t, c.RevokeRefreshTokens(ctx, "r1"))

	_, err = c.VerifyIDTokenAndCheckRevoked(ctx, in.str("idToken"))
	assert.True(t, auth.IsIDTokenRevoked(err), err)

	_, err = c.VerifyIDToken(ctx, in.str("idToken"))
	assert.True(t, auth.IsIDTokenRevoked(err), "emulator mode always checks revocation: %v", err)

	rec, err := c.GetUser(ctx, "r1")
	require.NoError(t, err)
	assert.NotZero(t, rec.TokensValidAfterMillis)

	e.clock.Advance(3 * time.Minute)

	fresh := e.signInToken(t, "r1@example.com", "secret1")
	_, err = c.VerifyIDTokenAndCheckRevoked(ctx, fresh.str("idToken"))
	require.NoError(t, err)

	_, err = c.UpdateUser(ctx, "r1", (&auth.UserToUpdate{}).Disabled(true))
	require.NoError(t, err)

	_, err = c.VerifyIDTokenAndCheckRevoked(ctx, fresh.str("idToken"))
	assert.True(t, auth.IsUserDisabled(err), err)

	r := e.client(t, "signInWithPassword", map[string]any{"email": "r1@example.com", "password": "secret1"})
	assert.Contains(t, r.errMsg(), "USER_DISABLED")
}

func sdkCustomToken(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	ct, err := c.CustomTokenWithClaims(ctx, "custom-1", map[string]any{"team": "blue"})
	require.NoError(t, err)

	r := e.client(t, "signInWithCustomToken", map[string]any{"token": ct, "returnSecureToken": true})
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Equal(t, true, r.body["isNewUser"])

	tok, err := c.VerifyIDToken(ctx, r.str("idToken"))
	require.NoError(t, err)
	assert.Equal(t, "custom-1", tok.UID)
	assert.Equal(t, "blue", tok.Claims["team"])
	assert.Equal(t, "custom", tok.Firebase.SignInProvider)

	again := e.client(t, "signInWithCustomToken", map[string]any{"token": ct})
	assert.NotEqual(t, true, again.body["isNewUser"])

	refreshed := e.do(
		t,
		http.MethodPost,
		"/v1/token?key=k",
		"",
		map[string]any{"grant_type": "refresh_token", "refresh_token": r.str("refreshToken")},
	)
	require.Equal(t, http.StatusOK, refreshed.status, refreshed.body)

	tok, err = c.VerifyIDToken(ctx, refreshed.str("id_token"))
	require.NoError(t, err)
	assert.Equal(t, "blue", tok.Claims["team"], "developer claims survive a refresh")

	bad := e.client(t, "signInWithCustomToken", map[string]any{"token": "x.y.z"})
	assert.Contains(t, bad.errMsg(), "INVALID_CUSTOM_TOKEN")
}

func sdkImportHashes(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	bc, err := bcrypt.GenerateFromPassword([]byte("secret1"), bcrypt.MinCost)
	require.NoError(t, err)

	res, err := c.ImportUsers(ctx, []*auth.UserToImport{
		(&auth.UserToImport{}).UID("bc1").Email("bc1@example.com").PasswordHash(bc),
	}, auth.WithHash(hash.Bcrypt{}))
	require.NoError(t, err)
	assert.Equal(t, 1, res.SuccessCount)

	in := e.client(t, "signInWithPassword", map[string]any{"email": "bc1@example.com", "password": "secret1"})
	require.Equal(t, http.StatusOK, in.status, in.body)

	salt := []byte("salty-salt")
	key := []byte("signer-key")
	sep := []byte{0x07}
	h := firebaseScrypt(t, "secret1", salt, sep, key, 12, 8)

	res, err = c.ImportUsers(ctx, []*auth.UserToImport{
		(&auth.UserToImport{}).UID("sc1").Email("sc1@example.com").PasswordHash(h).PasswordSalt(salt),
		(&auth.UserToImport{}).UID("bc1").Email("dup@example.com"),
	}, auth.WithHash(hash.Scrypt{Key: key, SaltSeparator: sep, Rounds: 8, MemoryCost: 12}))
	require.NoError(t, err)
	assert.Equal(t, 1, res.SuccessCount)
	require.Len(t, res.Errors, 1)
	assert.Equal(t, 1, res.Errors[0].Index)

	in = e.client(t, "signInWithPassword", map[string]any{"email": "sc1@example.com", "password": "secret1"})
	assert.Equal(t, http.StatusOK, in.status, in.body)
}

func sdkTenants(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	tm := c.TenantManager

	a, err := tm.CreateTenant(
		ctx,
		(&auth.TenantToCreate{}).DisplayName("tenant-a").AllowPasswordSignUp(true).EnableEmailLinkSignIn(true),
	)
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", a.DisplayName)
	assert.True(t, a.AllowPasswordSignUp)
	assert.True(t, strings.HasPrefix(a.ID, "tenant-a-"), a.ID)

	b, err := tm.CreateTenant(ctx, (&auth.TenantToCreate{}).DisplayName("tenant-b").AllowPasswordSignUp(true))
	require.NoError(t, err)

	got, err := tm.Tenant(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, a.ID, got.ID)

	upd, err := tm.UpdateTenant(ctx, a.ID, (&auth.TenantToUpdate{}).DisplayName("tenant-a2").EnableAnonymousUsers(true))
	require.NoError(t, err)
	assert.Equal(t, "tenant-a2", upd.DisplayName)
	assert.True(t, upd.EnableAnonymousUsers)
	assert.True(t, upd.AllowPasswordSignUp, "unmasked fields survive the patch")

	ta, err := tm.AuthForTenant(a.ID)
	require.NoError(t, err)

	tb, err := tm.AuthForTenant(b.ID)
	require.NoError(t, err)

	ua, err := ta.CreateUser(ctx, (&auth.UserToCreate{}).UID("same").Email("same@example.com").Password("secret1"))
	require.NoError(t, err)
	assert.Equal(t, a.ID, ua.TenantID)

	_, err = tb.CreateUser(ctx, (&auth.UserToCreate{}).UID("same").Email("same@example.com").Password("secret2"))
	require.NoError(t, err, "tenants are separate namespaces")

	_, err = c.GetUser(ctx, "same")
	assert.True(t, auth.IsUserNotFound(err), "project level does not see tenant users")

	_, err = ta.CreateUser(ctx, (&auth.UserToCreate{}).Email("same@example.com"))
	assert.True(t, auth.IsEmailAlreadyExists(err))

	in := e.client(
		t,
		"signInWithPassword",
		map[string]any{"email": "same@example.com", "password": "secret1", "tenantId": a.ID},
	)
	require.Equal(t, http.StatusOK, in.status, in.body)

	wrong := e.client(
		t,
		"signInWithPassword",
		map[string]any{"email": "same@example.com", "password": "secret1", "tenantId": b.ID},
	)
	assert.Equal(t, http.StatusBadRequest, wrong.status)

	tok, err := ta.VerifyIDToken(ctx, in.str("idToken"))
	require.NoError(t, err)
	assert.Equal(t, a.ID, tok.Firebase.Tenant)

	_, err = tb.VerifyIDToken(ctx, in.str("idToken"))
	require.Error(t, err)

	_, err = tm.AuthForTenant("ghost")
	require.NoError(t, err)

	gone, err := tm.Tenant(ctx, "ghost-tenant")
	assert.Nil(t, gone)
	assert.True(t, auth.IsTenantNotFound(err), err)

	ids := map[string]bool{}
	it := tm.Tenants(ctx, "")

	for {
		tn, nErr := it.Next()
		if errors.Is(nErr, iterator.Done) {
			break
		}

		require.NoError(t, nErr)
		ids[tn.ID] = true
	}

	assert.Equal(t, map[string]bool{a.ID: true, b.ID: true}, ids)

	require.NoError(t, tm.DeleteTenant(ctx, a.ID))

	_, err = tm.Tenant(ctx, a.ID)
	assert.True(t, auth.IsTenantNotFound(err))

	_, err = tb.GetUser(ctx, "same")
	assert.NoError(t, err, "deleting one tenant leaves others intact")
}

func sdkSessionCookie(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID("sc").Email("sc@example.com").Password("secret1"))
	require.NoError(t, err)

	in := e.signInToken(t, "sc@example.com", "secret1")

	cookie, err := c.SessionCookie(ctx, in.str("idToken"), 6*3600*1e9)
	require.NoError(t, err)

	tok, err := c.VerifySessionCookieAndCheckRevoked(ctx, cookie)
	require.NoError(t, err)
	assert.Equal(t, "sc", tok.UID)
	assert.Equal(t, "https://session.firebase.google.com/"+e.project, tok.Issuer)

	require.NoError(t, c.RevokeRefreshTokens(ctx, "sc"))

	_, err = c.VerifySessionCookieAndCheckRevoked(ctx, cookie)
	assert.True(t, auth.IsSessionCookieRevoked(err), err)

	_, err = c.SessionCookie(ctx, in.str("idToken"), 1)
	assert.Error(t, err)
}

func sdkEmailLinks(t *testing.T, e *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID("em").Email("em@example.com").Password("secret1"))
	require.NoError(t, err)

	link, err := c.PasswordResetLink(ctx, "em@example.com")
	require.NoError(t, err)

	lu, err := url.Parse(link)
	require.NoError(t, err)
	assert.Equal(t, "resetPassword", lu.Query().Get("mode"))

	done := e.client(
		t,
		"resetPassword",
		map[string]any{"oobCode": lu.Query().Get("oobCode"), "newPassword": "changed1"},
	)
	require.Equal(t, http.StatusOK, done.status, done.body)

	e.signInToken(t, "em@example.com", "changed1")

	vlink, err := c.EmailVerificationLink(ctx, "em@example.com")
	require.NoError(t, err)

	vu, err := url.Parse(vlink)
	require.NoError(t, err)

	r := e.client(t, "update", map[string]any{"oobCode": vu.Query().Get("oobCode")})
	require.Equal(t, http.StatusOK, r.status, r.body)

	rec, err := c.GetUser(ctx, "em")
	require.NoError(t, err)
	assert.True(t, rec.EmailVerified)

	_, err = c.PasswordResetLink(ctx, "ghost@example.com")
	assert.True(t, auth.IsEmailNotFound(err), err)

	slink, err := c.EmailSignInLink(
		ctx,
		"new-link@example.com",
		&auth.ActionCodeSettings{URL: "https://app.example.com/finish", HandleCodeInApp: true},
	)
	require.NoError(t, err)

	su, err := url.Parse(slink)
	require.NoError(t, err)

	in := e.client(
		t,
		"signInWithEmailLink",
		map[string]any{"email": "new-link@example.com", "oobCode": su.Query().Get("oobCode")},
	)
	require.Equal(t, http.StatusOK, in.status, in.body)
	assert.Equal(t, true, in.body["isNewUser"])
}

func sdkDeleteUsers(t *testing.T, _ *env, c *auth.Client) {
	t.Helper()

	ctx := t.Context()

	for _, id := range []string{"d1", "d2"} {
		_, err := c.CreateUser(ctx, (&auth.UserToCreate{}).UID(id))
		require.NoError(t, err)
	}

	res, err := c.DeleteUsers(ctx, []string{"d1", "d2", "never-existed"})
	require.NoError(t, err)
	assert.Equal(t, 3, res.SuccessCount)
	assert.Zero(t, res.FailureCount)

	_, err = c.GetUser(ctx, "d1")
	assert.True(t, auth.IsUserNotFound(err))
}
