package azurefunctions

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	msiPath            = "/msi/token"
	msiAPIVersion      = "2019-08-01"
	msiHeader          = "X-Identity-Header"
	settingIdentityEP  = "IDENTITY_ENDPOINT"
	settingIdentityHdr = "IDENTITY_HEADER"
	settingDisableMSI  = "WEBSITE_DISABLE_MSI"
)

// UserIdentity is a user-assigned managed identity of an app.
type UserIdentity struct {
	ResourceID  string
	ClientID    string
	PrincipalID string
}

// Identity is the managed identity of an app.
type Identity struct {
	TenantID          string
	SystemPrincipalID string
	User              []UserIdentity
}

// IdentityResolver returns the managed identity of the named app.
type IdentityResolver func(app string) (Identity, bool)

// TokenIssuer mints an access token for resource on behalf of the service principal objectID of clientID.
type TokenIssuer func(tenant, clientID, objectID, resource string) (string, time.Time, error)

// SetIdentity wires the managed identity lookup and token issuer behind the apps' local token service.
func (b *InMemoryBackend) SetIdentity(r IdentityResolver, t TokenIssuer) {
	b.mu.Lock("SetIdentity")
	defer b.mu.Unlock()

	b.identity, b.issuer = r, t
}

// identityHeader is the per-app secret App Service puts in IDENTITY_HEADER; it is derived from the master key.
func identityHeader(a *appState) string {
	sum := sha256.Sum256([]byte("identity:" + a.MasterKey))

	return hex.EncodeToString(sum[:])
}

// identityEnv returns IDENTITY_ENDPOINT and IDENTITY_HEADER for apps that have a managed identity (Learn:
// Managed identities, "REST endpoint reference"); WEBSITE_DISABLE_MSI turns the token service off.
func (b *InMemoryBackend) identityEnv(a *appState, host string) map[string]string {
	b.mu.RLock("identityEnv")
	resolve, port := b.identity, b.port
	b.mu.RUnlock()

	if resolve == nil || strings.EqualFold(a.Settings[settingDisableMSI], "true") {
		return nil
	}

	if _, ok := resolve(a.key()); !ok {
		return nil
	}

	return map[string]string{
		settingIdentityEP:  "http://" + host + ":" + strconv.Itoa(port) + msiPath,
		settingIdentityHdr: identityHeader(a),
	}
}

func (b *InMemoryBackend) appForIdentityHeader(h string) (appState, bool) {
	b.mu.RLock("appForIdentityHeader")
	defer b.mu.RUnlock()

	for _, a := range b.apps {
		if subtle.ConstantTimeCompare([]byte(h), []byte(identityHeader(&a.appState))) == 1 {
			return a.appState, true
		}
	}

	return appState{}, false
}

type msiResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresOn   string `json:"expires_on"`
	Resource    string `json:"resource"`
	TokenType   string `json:"token_type"`
	ClientID    string `json:"client_id"`
}

type msiError struct {
	Message    string `json:"message"`
	StatusCode int    `json:"statusCode"`
}

func msiFail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	writeBody(w, msiError{StatusCode: code, Message: msg})
}

var (
	errIdentityConflict = errors.New("specify only one of client_id, principal_id, object_id and mi_res_id")
	errNoSystemIdentity = errors.New("the app has no system-assigned identity")
	errNoUserIdentity   = errors.New("the app has no such user-assigned identity")
)

type pickedIdentity struct {
	clientID string
	objectID string
}

// pickIdentity chooses the identity a token request names; no id parameter means the system-assigned one.
func pickIdentity(id Identity, q map[string][]string) (pickedIdentity, error) {
	given := 0

	for _, k := range []string{"client_id", "principal_id", "object_id", "mi_res_id"} {
		if len(q[k]) > 0 {
			given++
		}
	}

	switch {
	case given > 1:
		return pickedIdentity{}, errIdentityConflict
	case given == 0 && id.SystemPrincipalID == "":
		return pickedIdentity{}, errNoSystemIdentity
	case given == 0:
		return pickedIdentity{clientID: id.SystemPrincipalID, objectID: id.SystemPrincipalID}, nil
	}

	idx := slices.IndexFunc(id.User, func(u UserIdentity) bool {
		return matches(q, "client_id", u.ClientID) || matches(q, "principal_id", u.PrincipalID) ||
			matches(q, "object_id", u.PrincipalID) || matches(q, "mi_res_id", u.ResourceID)
	})
	if idx < 0 {
		return pickedIdentity{}, errNoUserIdentity
	}

	return pickedIdentity{clientID: id.User[idx].ClientID, objectID: id.User[idx].PrincipalID}, nil
}

func matches(q map[string][]string, key, value string) bool {
	return first(q, key) != "" && strs.Equal(first(q, key), value)
}

func first(q map[string][]string, k string) string {
	if v := q[k]; len(v) > 0 {
		return v[0]
	}

	return ""
}

func (b *InMemoryBackend) serveMSI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		msiFail(w, http.StatusMethodNotAllowed, "use GET")

		return
	}

	a, ok := b.appForIdentityHeader(r.Header.Get(msiHeader))
	if !ok {
		msiFail(w, http.StatusUnauthorized, "missing or wrong "+msiHeader)

		return
	}

	b.mu.RLock("serveMSI")
	resolve, issue := b.identity, b.issuer
	b.mu.RUnlock()

	q := r.URL.Query()

	id, found := Identity{}, false
	if resolve != nil {
		id, found = resolve(a.key())
	}

	switch {
	case !found || issue == nil:
		msiFail(w, http.StatusBadRequest, "the app has no managed identity")

		return
	case q.Get("api-version") != msiAPIVersion:
		msiFail(w, http.StatusBadRequest, "api-version must be "+msiAPIVersion)

		return
	case q.Get("resource") == "":
		msiFail(w, http.StatusBadRequest, "resource is required")

		return
	}

	picked, err := pickIdentity(id, q)
	if err != nil {
		msiFail(w, http.StatusBadRequest, err.Error())

		return
	}

	token, exp, err := issue(id.TenantID, picked.clientID, picked.objectID, q.Get("resource"))
	if err != nil {
		msiFail(w, http.StatusInternalServerError, "token issuance failed")

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeBody(w, msiResponse{
		AccessToken: token, ExpiresOn: strconv.FormatInt(exp.Unix(), 10), Resource: q.Get("resource"),
		TokenType: "Bearer", ClientID: picked.clientID,
	})
}
