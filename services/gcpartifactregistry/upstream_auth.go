package gcpartifactregistry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// SecretResolver reads a Secret Manager version payload; the in-process Secret Manager implements it.
type SecretResolver interface {
	AccessSecretVersion(name string) ([]byte, error)
}

// SetSecretResolver lets remote repositories resolve upstream_credentials password secret versions.
func (b *InMemoryBackend) SetSecretResolver(s SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	defer b.mu.Unlock()

	b.secrets = s
}

var errUpstreamCredentials = errors.New("upstream credentials")

type upstreamAuthKey struct{}

// upstreamAuth is the Basic credential for requests to one upstream host. The password is never logged or
// returned in a response.
type upstreamAuth struct {
	host, user, pass string
}

func (a upstreamAuth) String() string { return "upstreamAuth(" + a.host + ")" }

// withUpstreamAuth resolves the repository's upstream_credentials, if any, and attaches them to ctx for fetches
// to the upstream's host.
func (b *InMemoryBackend) withUpstreamAuth(ctx context.Context, rc repoCtx, base string) (context.Context, error) {
	creds := rc.pb.GetRemoteRepositoryConfig().GetUpstreamCredentials().GetUsernamePasswordCredentials()
	if creds == nil || creds.GetUsername() == "" {
		return ctx, nil
	}

	b.mu.RLock("withUpstreamAuth")
	res := b.secrets
	b.mu.RUnlock()

	if res == nil {
		return ctx, fmt.Errorf("%w: Secret Manager is not available", errUpstreamCredentials)
	}

	if creds.GetPasswordSecretVersion() == "" {
		return ctx, fmt.Errorf("%w: password_secret_version is not set", errUpstreamCredentials)
	}

	pass, err := res.AccessSecretVersion(creds.GetPasswordSecretVersion())
	if err != nil {
		return ctx, fmt.Errorf("%w: cannot access %s", errUpstreamCredentials, creds.GetPasswordSecretVersion())
	}

	u, err := url.Parse(base)
	if err != nil {
		return ctx, fmt.Errorf("%w: bad upstream URI", errUpstreamCredentials)
	}

	return context.WithValue(
		ctx,
		upstreamAuthKey{},
		upstreamAuth{host: u.Host, user: creds.GetUsername(), pass: string(pass)},
	), nil
}

// allowAuthHost lets the credential also go to host, the token realm of a Docker registry.
func allowAuthHost(ctx context.Context, host string) context.Context {
	if a, ok := ctx.Value(upstreamAuthKey{}).(upstreamAuth); ok {
		a.host = host

		return context.WithValue(ctx, upstreamAuthKey{}, a)
	}

	return ctx
}

func applyUpstreamAuth(ctx context.Context, req *http.Request) {
	if a, ok := ctx.Value(upstreamAuthKey{}).(upstreamAuth); ok && a.host == req.URL.Host &&
		req.Header.Get("Authorization") == "" {
		req.SetBasicAuth(a.user, a.pass)
	}
}
