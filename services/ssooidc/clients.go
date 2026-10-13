package ssooidc

import (
	"crypto/subtle"
	"net/url"
	"slices"
	"strings"
	"time"
)

// RegisterClientInput carries the RegisterClient request members.
type RegisterClientInput struct {
	ClientName   string
	ClientType   string
	IssuerURL    string
	EntitledApp  string
	GrantTypes   []string
	RedirectURIs []string
	Scopes       []string
}

func supportedGrant(g string) bool {
	return g == grantDeviceCode || g == grantAuthCode || g == grantRefreshToken
}

func loopbackHost(host string) bool {
	return host == "127.0.0.1" || host == "localhost" || host == "::1"
}

func validRedirectURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Fragment != "" {
		return false
	}

	return u.Scheme == schemeHTTPS || (u.Scheme == "http" && loopbackHost(u.Hostname()))
}

func validateRegistration(in *RegisterClientInput) *APIError {
	if strings.TrimSpace(in.ClientName) == "" {
		return errInvalidRequest("clientName is required")
	}

	if in.ClientType != clientTypePublic {
		return errInvalidClientMetadata("clientType must be public")
	}

	for _, g := range in.GrantTypes {
		if !supportedGrant(g) {
			return errUnsupportedGrant("Unsupported grant type: " + g)
		}
	}

	for _, s := range in.Scopes {
		if strings.TrimSpace(s) == "" {
			return errInvalidScope("scopes must not contain blank values")
		}
	}

	for _, r := range in.RedirectURIs {
		if !validRedirectURI(r) {
			return errInvalidRedirectURI("Unsupported redirect URI: " + r)
		}
	}

	if slices.Contains(in.GrantTypes, grantAuthCode) && len(in.RedirectURIs) == 0 {
		return errInvalidRedirectURI("redirectUris is required for the authorization_code grant")
	}

	return nil
}

// RegisterClient registers a public client.
func (b *InMemoryBackend) RegisterClient(in *RegisterClientInput) (*ClientRegistration, *APIError) {
	if err := validateRegistration(in); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	c := &Client{
		ClientID:     randomString(clientIDBytes),
		ClientSecret: randomString(clientSecretBytes),
		ClientName:   in.ClientName,
		IssuerURL:    in.IssuerURL,
		EntitledApp:  in.EntitledApp,
		GrantTypes:   slices.Clone(in.GrantTypes),
		RedirectURIs: slices.Clone(in.RedirectURIs),
		Scopes:       slices.Clone(in.Scopes),
		IssuedAt:     now,
		ExpiresAt:    now.Add(clientSecretTTL),
	}

	b.mu.Lock("RegisterClient")
	defer b.mu.Unlock()

	b.clients.Put(c)

	return &ClientRegistration{
		ClientID:              c.ClientID,
		ClientSecret:          c.ClientSecret,
		ClientIDIssuedAt:      c.IssuedAt.Unix(),
		ClientSecretExpiresAt: c.ExpiresAt.Unix(),
	}, nil
}

// clientLocked authenticates id/secret; an empty secret is allowed only when secretRequired is false.
func (b *InMemoryBackend) clientLocked(id, secret string, secretRequired bool, now time.Time) (*Client, *APIError) {
	if id == "" || (secretRequired && secret == "") {
		return nil, errInvalidRequest("clientId and clientSecret are required")
	}

	c, ok := b.clients.Get(id)
	if !ok {
		return nil, errInvalidClient("Client ID not found")
	}

	if secretRequired && subtle.ConstantTimeCompare([]byte(secret), []byte(c.ClientSecret)) != 1 {
		return nil, errInvalidClient("Client secret is invalid")
	}

	if now.After(c.ExpiresAt) {
		return nil, errInvalidClient("Client secret has expired")
	}

	return c, nil
}

// allowsGrant reports whether the client registered grant; without grantTypes only the device grant is allowed.
func (c *Client) allowsGrant(grant string) bool {
	if len(c.GrantTypes) == 0 {
		return grant == grantDeviceCode
	}

	return slices.Contains(c.GrantTypes, grant)
}

// redirectAllowed matches the registered URIs exactly, ignoring the port of loopback URIs.
func (c *Client) redirectAllowed(raw string) bool {
	if slices.Contains(c.RedirectURIs, raw) {
		return true
	}

	got, err := url.Parse(raw)
	if err != nil || !loopbackHost(got.Hostname()) {
		return false
	}

	for _, r := range c.RedirectURIs {
		reg, perr := url.Parse(r)
		if perr == nil && reg.Scheme == got.Scheme && reg.Hostname() == got.Hostname() && reg.Path == got.Path {
			return true
		}
	}

	return false
}
