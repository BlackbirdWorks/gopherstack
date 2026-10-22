package bedrockagentcore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	accessTokenTTL = time.Hour
	accessTokenLen = 40
	jwtSegments    = 3
)

func (b *InMemoryBackend) workloadToken(userFrom func(doc) (string, error)) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		in, err := decodeDoc(req)
		if err != nil {
			return nil, 0, err
		}

		name := strOf(in, "workloadName")
		if name == "" {
			return nil, 0, invalid("workloadName is required")
		}

		user, err := userFrom(in)
		if err != nil {
			return nil, 0, err
		}

		sc := b.scopeOf(ctx)

		c := b.ctl()
		if c == nil || !c.WorkloadIdentity(sc, name) {
			return nil, 0, notFound("workload identity %q not found", name)
		}

		tok := b.issueToken(sc, name, user)

		return doc{"workloadAccessToken": tok}, 0, nil
	}
}

func (b *InMemoryBackend) issueToken(sc Scope, workload, user string) string {
	b.mu.Lock("issueToken")
	defer b.mu.Unlock()

	now := b.nowT()

	for _, t := range b.tokens.All() {
		if !now.Before(t.Expires) {
			b.tokens.Delete(t.Token)
		}
	}

	t := &AccessToken{
		Token: awsid.NewN("wat-", accessTokenLen), Account: sc.Account, Region: sc.Region, Workload: workload,
		UserID: user, Expires: now.Add(accessTokenTTL),
	}
	b.tokens.Put(t)

	return t.Token
}

func noUser(doc) (string, error) { return "", nil }

func userFromID(in doc) (string, error) {
	id := strOf(in, "userId")
	if id == "" {
		return "", invalid("userId is required")
	}

	return id, nil
}

// userFromJWT reads the subject of the caller's user token. The signature is not verified: the emulator has no
// access to the identity provider's signing keys.
func userFromJWT(in doc) (string, error) {
	parts := strings.Split(strOf(in, "userToken"), ".")
	if len(parts) != jwtSegments {
		return "", invalid("userToken must be a JWT")
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return "", invalid("userToken payload is not valid base64url")
	}

	var claims struct {
		Sub string `json:"sub"`
	}

	if err = json.Unmarshal(raw, &claims); err != nil || claims.Sub == "" {
		return "", invalid("userToken must carry a sub claim")
	}

	return claims.Sub, nil
}

// authorize validates a workload access token and returns the scope it was issued in.
func (b *InMemoryBackend) authorize(token string) (*AccessToken, error) {
	b.mu.RLock("authorize")
	defer b.mu.RUnlock()

	t, ok := b.tokens.Get(token)
	if !ok || !b.nowT().Before(t.Expires) {
		return nil, unauthorized("the workload identity token is invalid or expired")
	}

	return t, nil
}

func (b *InMemoryBackend) providerFor(t *AccessToken, kind ProviderKind, name string) (ProviderInfo, error) {
	if name == "" {
		return ProviderInfo{}, invalid("resourceCredentialProviderName is required")
	}

	c := b.ctl()
	if c == nil {
		return ProviderInfo{}, internal("the control plane is not wired")
	}

	p, ok := c.Provider(Scope{Account: t.Account, Region: t.Region}, kind, name)
	if !ok {
		return ProviderInfo{}, notFound("credential provider %q not found", name)
	}

	return p, nil
}

func (b *InMemoryBackend) getResourceAPIKey(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	t, err := b.authorize(strOf(in, "workloadIdentityToken"))
	if err != nil {
		return nil, 0, err
	}

	p, err := b.providerFor(t, ProviderAPIKey, strOf(in, "resourceCredentialProviderName"))
	if err != nil {
		return nil, 0, err
	}

	key, ok := b.ctl().SecretValue(p.SecretARN, p.SecretJSONID)
	if !ok {
		return nil, 0, notFound("the API key of provider %q is unavailable", p.Name)
	}

	return doc{"apiKey": key}, 0, nil
}
