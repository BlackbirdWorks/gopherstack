package gcpidentitytoolkit

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	evBeforeCreate   = "beforeCreate"
	evBeforeSignIn   = "beforeSignIn"
	blockTimeout     = 7 * time.Second
	blockMaxResponse = 1 << 20
	blockJWTTTL      = time.Hour
	eventTypePrefix  = "providers/cloud.auth/eventTypes/user."
)

//nolint:gochecknoglobals // shared client with its own cloned transport
var blockingClient = httpclient.New(blockTimeout)

type blockingResult struct {
	UserRecord *struct {
		DisplayName   *string        `json:"displayName"`
		PhotoURL      *string        `json:"photoURL"`
		EmailVerified *bool          `json:"emailVerified"`
		Disabled      *bool          `json:"disabled"`
		CustomClaims  map[string]any `json:"customClaims"`
		SessionClaims map[string]any `json:"sessionClaims"`
		UpdateMask    string         `json:"updateMask"`
	} `json:"userRecord"`
}

func (u *userRec) blockingRecord(sc *scope) map[string]any {
	providers := make([]map[string]any, 0, len(u.Providers))
	for _, p := range u.Providers {
		providers = append(providers, map[string]any{
			"uid": p.RawID, claimEmail: p.Email, "display_name": p.DisplayName, "photo_url": p.PhotoURL,
			"provider_id": p.ProviderID, "phone_number": p.Phone,
		})
	}

	var custom map[string]any
	if u.CustomAttrs != "" {
		_ = json.Unmarshal([]byte(u.CustomAttrs), &custom, jsonx.V1())
	}

	rec := map[string]any{
		"uid": u.UID, claimEmail: u.Email, "email_verified": u.EmailVerified, "display_name": u.DisplayName,
		"photo_url": u.PhotoURL, "phone_number": u.Phone, "disabled": u.Disabled, "provider_data": providers,
		"custom_claims": custom,
		"metadata": map[string]any{
			"creation_time":     time.UnixMilli(u.CreatedAt).UTC().Format(time.RFC3339),
			"last_sign_in_time": time.UnixMilli(u.LastLoginAt).UTC().Format(time.RFC3339),
		},
	}

	if sc.tenant != rootTenantKey {
		rec["tenant_id"] = sc.tenant
	}

	return rec
}

func (b *InMemoryBackend) blockingJWTLocked(
	sc *scope,
	uri, event, method string,
	u *userRec,
	a *idpAssertion,
) (string, error) {
	now := b.now()
	claims := jwt.MapClaims{
		"iss": securetokenIssuer + sc.project, "aud": uri, "iat": now.Unix(), "exp": now.Add(blockJWTTTL).Unix(),
		claimSub: u.UID, "event_id": uuid.NewString(), "event_type": eventTypePrefix + event + ":" + method,
		"sign_in_method": method, "locale": "en", "user_record": u.blockingRecord(sc),
	}

	fwd := sc.proj.Config.BlockingFunctions.ForwardInboundCredentials
	if a != nil && fwd != nil {
		if fwd.IdToken {
			claims["oauth_id_token"] = a.idToken
		}

		if fwd.AccessToken {
			claims["oauth_access_token"] = a.accessToken
		}

		claims["raw_user_info"] = a.raw
	}

	return b.signJWTLocked(claims)
}

func blockingFailure(detail string) *APIError {
	return idtk("BLOCKING_FUNCTION_ERROR_RESPONSE", "((HTTP Cloud Function returned an error:\n"+detail+"\n))")
}

func postBlocking(uri, token string) (*blockingResult, error) {
	body, err := json.Marshal(map[string]any{"data": map[string]any{"jwt": token}}, jsonx.V1())
	if err != nil {
		return nil, blockingFailure(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), blockTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(body))
	if err != nil {
		return nil, blockingFailure(err.Error())
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := blockingClient.Do(req)
	if err != nil {
		return nil, blockingFailure(err.Error())
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, blockMaxResponse))
	if err != nil {
		return nil, blockingFailure(err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, blockingFailure(strings.TrimSpace(string(raw)))
	}

	var out blockingResult
	if err = json.Unmarshal(raw, &out, jsonx.V1()); err != nil {
		return nil, blockingFailure("response is not JSON")
	}

	return &out, nil
}

// blockLocked runs the project's blocking function for event, with the backend lock released for the call. It
// returns the session claims the function asked for.
func (b *InMemoryBackend) blockLocked(
	sc *scope, event, method string, u *userRec, a *idpAssertion,
) (map[string]any, error) {
	bf := sc.proj.Config.BlockingFunctions
	if bf == nil || bf.Triggers[event].FunctionUri == "" {
		return nil, nil //nolint:nilnil // no trigger configured
	}

	uri := bf.Triggers[event].FunctionUri

	token, err := b.blockingJWTLocked(sc, uri, event, method, u, a)
	if err != nil {
		return nil, err
	}

	b.mu.Unlock()
	res, err := postBlocking(uri, token)
	b.mu.Lock("blockLocked")

	if err != nil {
		return nil, err
	}

	if cur, ok := sc.users[u.UID]; !ok || cur != u {
		return nil, idtk("USER_NOT_FOUND")
	}

	return applyBlockingResult(u, res)
}

func applyBlockingResult(u *userRec, res *blockingResult) (map[string]any, error) {
	r := res.UserRecord
	if r == nil {
		return nil, nil //nolint:nilnil // function made no changes
	}

	var session map[string]any

	for field := range strings.SplitSeq(r.UpdateMask, ",") {
		switch strings.TrimSpace(field) {
		case "displayName":
			u.DisplayName = deref(r.DisplayName)
		case "photoURL":
			u.PhotoURL = deref(r.PhotoURL)
		case "emailVerified":
			u.EmailVerified = r.EmailVerified != nil && *r.EmailVerified
		case "disabled":
			u.Disabled = r.Disabled != nil && *r.Disabled
		case "customClaims":
			raw, _ := json.Marshal(r.CustomClaims, jsonx.V1())

			claims, err := parseClaims(string(raw))
			if err != nil {
				return nil, err
			}

			u.CustomAttrs = claims
		case "sessionClaims":
			session = r.SessionClaims
		}
	}

	return session, nil
}

// createClientUserLocked creates an account for a client sign-in and runs the beforeCreate trigger, removing the
// account again when the function rejects it.
func (b *InMemoryBackend) createClientUserLocked(
	sc *scope, n newUser, method string, a *idpAssertion,
) (*userRec, error) {
	u, err := b.createUserLocked(sc, n, false)
	if err != nil {
		return nil, err
	}

	if _, err = b.blockLocked(sc, evBeforeCreate, method, u, a); err != nil {
		delete(sc.users, u.UID)

		return nil, err
	}

	return u, nil
}
