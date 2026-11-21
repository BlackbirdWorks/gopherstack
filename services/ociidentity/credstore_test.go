package ociidentity_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

type credCase struct {
	create func(t *testing.T, e *env, user, token, text string) (id, secret string, err error)
	remove func(t *testing.T, e *env, user, id string)
	name   string
}

func credCases() []credCase {
	return []credCase{
		{
			name: "auth token",
			create: func(t *testing.T, e *env, user, token, text string) (string, string, error) {
				t.Helper()

				r, err := e.client.CreateAuthToken(t.Context(), identity.CreateAuthTokenRequest{
					UserId: &user, OpcRetryToken: new(token), Description: &text,
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.Token, nil
			},
			remove: func(t *testing.T, e *env, user, id string) {
				t.Helper()

				_, err := e.client.DeleteAuthToken(
					t.Context(),
					identity.DeleteAuthTokenRequest{UserId: &user, AuthTokenId: &id},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "secret key",
			create: func(t *testing.T, e *env, user, token, text string) (string, string, error) {
				t.Helper()

				r, err := e.client.CreateCustomerSecretKey(t.Context(), identity.CreateCustomerSecretKeyRequest{
					UserId: &user, OpcRetryToken: new(token),
					DisplayName: &text,
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.Key, nil
			},
			remove: func(t *testing.T, e *env, user, id string) {
				t.Helper()

				_, err := e.client.DeleteCustomerSecretKey(t.Context(),
					identity.DeleteCustomerSecretKeyRequest{UserId: &user, CustomerSecretKeyId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "smtp credential",
			create: func(t *testing.T, e *env, user, token, text string) (string, string, error) {
				t.Helper()

				r, err := e.client.CreateSmtpCredential(t.Context(), identity.CreateSmtpCredentialRequest{
					UserId: &user, OpcRetryToken: new(token),
					Description: &text,
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.Password, nil
			},
			remove: func(t *testing.T, e *env, user, id string) {
				t.Helper()

				_, err := e.client.DeleteSmtpCredential(t.Context(),
					identity.DeleteSmtpCredentialRequest{UserId: &user, SmtpCredentialId: &id})
				require.NoError(t, err)
			},
		},
	}
}

func TestCredentialRetryToken(t *testing.T) {
	t.Parallel()

	for _, tc := range credCases() {
		t.Run(tc.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			u := e.mkUser(t, "u")

			id1, sec1, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err)
			require.NotEmpty(t, sec1)

			id2, sec2, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
			assert.Equal(t, sec1, sec2)

			id3, sec3, err := tc.create(t, e, *u.Id, "other", "second")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id3)
			assert.NotEqual(t, sec1, sec3)

			_, _, err = tc.create(t, e, *u.Id, "third", "third")
			assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

			_, replay, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err, "a replay is not a new credential, so the per-user limit does not apply")
			assert.Equal(t, sec1, replay)
		})

		t.Run(tc.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			u := e.mkUser(t, "u")

			id, _, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err)
			tc.remove(t, e, *u.Id, id)

			_, _, err = tc.create(t, e, *u.Id, "tok", "first")
			se := svcErr(t, err)
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidatedRetryToken", se.GetCode())
		})

		t.Run(tc.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			u := e.mkUser(t, "u")

			id1, _, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, _, err := tc.create(t, e, *u.Id, "tok", "first")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2, "an expired token is a new request")
		})
	}
}

func TestAPIKeyAndUIPasswordRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "pw")
	kp := newKeyPair(t)

	up := func() (identity.UploadApiKeyResponse, error) {
		return e.client.UploadApiKey(ctx, identity.UploadApiKeyRequest{
			UserId: u.Id, OpcRetryToken: new("k"), Key: new(kp.pubPEM),
		})
	}

	k1, err := up()
	require.NoError(t, err)

	k2, err := up()
	require.NoError(t, err, "same token replays instead of 409")
	assert.Equal(t, *k1.Fingerprint, *k2.Fingerprint)

	lst, err := e.client.ListApiKeys(ctx, identity.ListApiKeysRequest{UserId: u.Id})
	require.NoError(t, err)
	assert.Len(t, lst.Items, 1)

	reset := func(token string) (string, error) {
		r, rerr := e.client.CreateOrResetUIPassword(ctx, identity.CreateOrResetUIPasswordRequest{
			UserId: u.Id, OpcRetryToken: new(token),
		})
		if rerr != nil {
			return "", rerr
		}

		return *r.Password, nil
	}

	p1, err := reset("a")
	require.NoError(t, err)

	p1again, err := reset("a")
	require.NoError(t, err)
	assert.Equal(t, p1, p1again)

	p2, err := reset("b")
	require.NoError(t, err)
	assert.NotEqual(t, p1, p2)

	_, err = reset("a")
	assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode(), "the password was replaced since")

	_, ok := e.backend.VerifyUIPassword("pw", p1)
	assert.False(t, ok)

	id, ok := e.backend.VerifyUIPassword("pw", p2)
	assert.True(t, ok)
	assert.Equal(t, *u.Id, id)
}

func TestStoredCredentialsSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	u := e.mkUser(t, "store")
	cases := credCases()

	_, secretKey, err := cases[1].create(t, e, *u.Id, "", "k")
	require.NoError(t, err)

	smtpID, smtpPW, err := cases[2].create(t, e, *u.Id, "tok", "s")
	require.NoError(t, err)

	pw, err := e.client.CreateOrResetUIPassword(t.Context(), identity.CreateOrResetUIPasswordRequest{UserId: u.Id})
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), *pw.Password, "the console password is kept only as a digest")
	assert.NotContains(t, string(snap), smtpPW, "the SMTP password is kept only as a digest")

	fresh := oci.NewInMemoryBackend()
	fresh.SetClock(e.clock.Now)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	keys, err := e.client.ListCustomerSecretKeys(t.Context(), identity.ListCustomerSecretKeysRequest{UserId: u.Id})
	require.NoError(t, err)
	require.Len(t, keys.Items, 1)

	owner, secret, ok := fresh.CustomerSecretKey(*keys.Items[0].Id)
	assert.True(t, ok)
	assert.Equal(t, *u.Id, owner)
	assert.Equal(t, secretKey, secret)

	smtps, err := e.client.ListSmtpCredentials(t.Context(), identity.ListSmtpCredentialsRequest{UserId: u.Id})
	require.NoError(t, err)
	require.Len(t, smtps.Items, 1)
	assert.Equal(t, smtpID, *smtps.Items[0].Id)

	_, ok = fresh.VerifySMTPCredential(*smtps.Items[0].Username, smtpPW)
	assert.True(t, ok)

	_, ok = fresh.VerifySMTPCredential(*smtps.Items[0].Username, smtpPW+"x")
	assert.False(t, ok)

	_, ok = fresh.VerifyUIPassword("store", *pw.Password)
	assert.True(t, ok)

	_, _, err = cases[0].create(t, e, *u.Id, "auth", "a")
	require.NoError(t, err)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), e.backend.Snapshot(t.Context())))
	assert.NotContains(t, string(e.backend.Snapshot(t.Context())), "\"authReplay\"")

	h := oci.NewHandler(restored)
	h.Port = 0
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.WithoutCancel(t.Context())) })

	e2 := &env{
		backend: restored,
		handler: h,
		clock:   e.clock,
		kp:      e.kp,
		base:    fmt.Sprintf("http://localhost:%d", h.Port),
	}
	e2.client = e2.clientFor(t, "ocid1.user.oc1..bootstrap", e.kp)

	_, _, err = cases[0].create(t, e2, *u.Id, "auth", "a")
	assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode(), "the auth token plaintext is not persisted")

	_, _, err = cases[2].create(t, e2, *u.Id, "tok", "s")
	assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode(), "the SMTP plaintext is not persisted")
}
