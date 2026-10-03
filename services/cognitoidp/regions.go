package cognitoidp

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

const (
	arnRegionFields = 6
	arnRegionIndex  = 3
	minRegionParts  = 3
)

// looksLikeRegion reports whether s has the AWS region shape (for example eu-west-1).
func looksLikeRegion(s string) bool {
	parts := strings.Split(s, "-")
	if len(parts) < minRegionParts {
		return false
	}

	_, err := strconv.Atoi(parts[len(parts)-1])

	return err == nil
}

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own janitor running under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(h.Backend.region, func(region string) *Handler {
		nb := NewInMemoryBackend(h.Backend.accountID, region, h.Backend.endpoint)
		nb.inheritTriggerInvoker(h.Backend)

		p := NewHandler(nb, region)

		if h.janitor != nil {
			p.WithJanitor(h.janitor.Interval, h.janitor.TaskTimeout)

			var pctx context.Context

			pctx, p.stop = context.WithCancel(ctx)
			go p.janitor.Run(pctx)
		}

		return p
	})
}

func (b *InMemoryBackend) inheritTriggerInvoker(src *InMemoryBackend) {
	src.mu.RLock("inheritTriggerInvoker")
	defer src.mu.RUnlock()

	b.lambdaInvoker = src.lambdaInvoker
}

// BackendFor returns the backend serving region: the home backend, or the sibling
// for any other region (built on first use).
func (h *Handler) BackendFor(region string) *InMemoryBackend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
}

func (h *Handler) closePeers() {
	for _, p := range h.peers.Drain() {
		if p.stop != nil {
			p.stop()
		}

		p.Backend.Reset()
	}
}

// handlers returns h followed by every regional sibling built so far.
func (h *Handler) handlers() []*Handler {
	return append([]*Handler{h}, h.peers.All()...)
}

// GetJWTPublicKey resolves the signing key across every region's pools.
func (h *Handler) GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error) {
	for _, p := range h.handlers() {
		key, err := p.Backend.GetJWTPublicKey(issuerURL, kid)
		if !errors.Is(err, ErrJWTIssuerUnknown) {
			return key, err
		}
	}

	return nil, ErrJWTIssuerUnknown
}

// ownerHints are the identifiers a request carries that name the region owning its resources.
type ownerHints struct {
	poolID, arn, clientID, token, domain, session string
}

func (h *Handler) byRegionName(region string) *Handler {
	for _, p := range h.handlers() {
		if p.Backend.region == region {
			return p
		}
	}

	return nil
}

func (h *Handler) ownerByHints(hints ownerHints) *Handler {
	if region, _, found := strings.Cut(hints.poolID, "_"); found && looksLikeRegion(region) {
		return h.byRegionName(region)
	}

	if parts := strings.SplitN(hints.arn, ":", arnRegionFields); len(parts) == arnRegionFields {
		return h.byRegionName(parts[arnRegionIndex])
	}

	for _, p := range h.handlers() {
		if p.Backend.ownsAny(hints) {
			return p
		}
	}

	return nil
}

// ownsAny reports whether b holds the client, signing key, domain or auth session named by hints.
func (b *InMemoryBackend) ownsAny(hints ownerHints) bool {
	if hints.domain != "" {
		if _, ok := b.domainPoolID(hints.domain); ok {
			return true
		}
	}

	b.mu.RLock("ownsAny")
	defer b.mu.RUnlock()

	if _, ok := b.clients.Get(hints.clientID); ok && hints.clientID != "" {
		return true
	}

	if _, ok := b.mfaSessions[hints.session]; ok && hints.session != "" {
		return true
	}

	kid := tokenHeaderKID(hints.token)
	if kid == "" {
		return false
	}

	for _, pool := range b.pools.All() {
		if pool.issuer != nil && pool.issuer.keyID == kid {
			return true
		}
	}

	return false
}

// owner picks the handler whose backend owns the request: by the pool, client, token or domain it
// names, else by the request region. Each owner still validates the request exactly as before.
func (h *Handler) owner(c *echo.Context) *Handler {
	if h.peers == nil {
		return h
	}

	r := c.Request()

	if p := h.ownerByHints(h.requestHints(r)); p != nil {
		return p
	}

	if p := h.peers.Get(awsmeta.Region(r.Context())); p != nil {
		return p
	}

	return h
}

func (h *Handler) requestHints(r *http.Request) ownerHints {
	if strings.HasPrefix(r.Header.Get("X-Amz-Target"), cognitoTargetPrefix) {
		return targetHints(r)
	}

	hints := ownerHints{clientID: r.URL.Query().Get("client_id"), domain: r.Host}

	if pool := poolPathPrefix(r.URL.Path, discoverySuffix); pool != "" {
		hints.poolID = pool
	} else if pool = poolPathPrefix(r.URL.Path, jwksPathSuffix); pool != "" {
		hints.poolID = pool
	}

	if user, _, ok := r.BasicAuth(); ok {
		hints.clientID = user
	}

	hints.token = bearerToken(r)

	if hints.clientID == "" && r.Method == http.MethodPost {
		hints.clientID = formClientID(r)
	}

	return hints
}

func formClientID(r *http.Request) string {
	body, err := httputils.ReadBody(r)
	if err != nil || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		return ""
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	return form.Get("client_id")
}

func targetHints(r *http.Request) ownerHints {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return ownerHints{}
	}

	var req struct {
		UserPoolID  string `json:"UserPoolId"`
		ResourceArn string `json:"ResourceArn"`
		ClientID    string `json:"ClientId"`
		AccessToken string `json:"AccessToken"`
		Domain      string `json:"Domain"`
		Session     string `json:"Session"`
	}

	_ = json.Unmarshal(body, &req)

	return ownerHints{
		poolID: req.UserPoolID, arn: req.ResourceArn, clientID: req.ClientID,
		token: req.AccessToken, domain: req.Domain, session: req.Session,
	}
}
