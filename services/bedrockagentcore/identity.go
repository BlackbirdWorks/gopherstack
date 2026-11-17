package bedrockagentcore

import (
	"context"
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

type userResolver func(context.Context, Scope, string, doc) (string, error)

func (b *InMemoryBackend) workloadToken(userFrom userResolver) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		in, err := decodeDoc(req)
		if err != nil {
			return nil, 0, err
		}

		name := strOf(in, "workloadName")
		if name == "" {
			return nil, 0, invalid("workloadName is required")
		}

		sc := b.scopeOf(ctx)

		user, err := userFrom(ctx, sc, name, in)
		if err != nil {
			return nil, 0, err
		}

		c := b.ctl()
		if c == nil || !c.WorkloadIdentity(sc, name) {
			return nil, 0, notFound("workload identity %q not found", name)
		}

		tok := b.issueToken(sc, name, user, strOf(in, "userToken"))

		return doc{"workloadAccessToken": tok}, 0, nil
	}
}

func (b *InMemoryBackend) issueToken(sc Scope, workload, user, userJWT string) string {
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
		UserID: user, UserJWT: userJWT, Expires: now.Add(accessTokenTTL),
	}
	b.tokens.Put(t)

	return t.Token
}

func noUser(context.Context, Scope, string, doc) (string, error) { return "", nil }

func userFromID(_ context.Context, _ Scope, _ string, in doc) (string, error) {
	id := strOf(in, "userId")
	if id == "" {
		return "", invalid("userId is required")
	}

	return id, nil
}

// userFromJWT verifies the signature of the caller's user token against its issuer's JWKS and returns the subject.
func (b *InMemoryBackend) userFromJWT(ctx context.Context, sc Scope, workload string, in doc) (string, error) {
	token := strOf(in, "userToken")
	if len(strings.Split(token, ".")) != jwtSegments {
		return "", invalid("userToken must be a JWT")
	}

	c := b.ctl()
	if c == nil {
		return "", internal("the control plane is not wired")
	}

	sub, err := c.VerifyWorkloadJWT(ctx, sc, workload, token)
	if err != nil {
		return "", invalid("userToken is not a valid signed token: %v", err)
	}

	return sub, nil
}

// authorize validates a workload access token and returns the scope it was issued in.
func (b *InMemoryBackend) authorize(token string) (*AccessToken, error) {
	b.mu.RLock("authorize")
	defer b.mu.RUnlock()

	t, ok := b.tokens.Get(token)
	if !ok || !b.nowT().Before(t.Expires) {
		return nil, unauthorized("the workload identity token is invalid or expired")
	}

	cp := *t

	return &cp, nil
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
