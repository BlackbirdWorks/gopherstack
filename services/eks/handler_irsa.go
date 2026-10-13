package eks

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	pathIRSAPrefix       = "/_gopherstack/eks/"
	pathIRSAOIDC         = pathIRSAPrefix + "oidc/id/"
	suffixOIDCDiscovery  = "/.well-known/openid-configuration"
	suffixOIDCKeys       = "/keys"
	suffixSAToken        = "/serviceaccount-token"
	hostOIDCPrefix       = "oidc.eks."
	opGetOIDCDiscovery   = "GetOIDCDiscovery"
	opGetOIDCKeys        = "GetOIDCKeys"
	opMintServiceAccount = "MintServiceAccountToken"
)

func isOIDCHost(host string) bool {
	return strings.HasPrefix(host, hostOIDCPrefix) && strings.Contains(host, ".amazonaws.com")
}

// irsaPath maps host-style OIDC issuer requests onto the emulator path.
func irsaPath(r *http.Request) string {
	p := r.URL.Path
	if isOIDCHost(r.Host) && strings.HasPrefix(p, "/id/") {
		return pathIRSAOIDC + strings.TrimPrefix(p, "/id/")
	}

	return p
}

func isIRSAPath(p string) bool { return strings.HasPrefix(p, pathIRSAPrefix) }

func parseIRSAPath(method, p string) eksRoute {
	rest := strings.TrimPrefix(p, pathIRSAPrefix)

	if after, ok := strings.CutPrefix(rest, "oidc/id/"); ok && method == http.MethodGet {
		if id, found := strings.CutSuffix(after, suffixOIDCDiscovery); found && id != "" && !strings.Contains(id, "/") {
			return eksRoute{operation: opGetOIDCDiscovery, resourceARN: id}
		}

		if id, found := strings.CutSuffix(after, suffixOIDCKeys); found && id != "" && !strings.Contains(id, "/") {
			return eksRoute{operation: opGetOIDCKeys, resourceARN: id}
		}
	}

	if cluster, ok := strings.CutSuffix(rest, suffixSAToken); ok && method == http.MethodPost &&
		cluster != "" && !strings.Contains(cluster, "/") {
		return eksRoute{operation: opMintServiceAccount, clusterName: cluster}
	}

	return eksRoute{operation: opUnknown}
}

type serviceAccountTokenBody struct {
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name"`
	PodName    string   `json:"podName"`
	Audience   string   `json:"audience"`
	Audiences  []string `json:"audiences"`
	TTLSeconds int64    `json:"ttlSeconds"`
}

func (h *Handler) serveIRSA(c *echo.Context, route eksRoute) error {
	switch route.operation {
	case opGetOIDCDiscovery:
		return h.serveOIDCDocument(c, route.resourceARN, true)
	case opGetOIDCKeys:
		return h.serveOIDCDocument(c, route.resourceARN, false)
	case opMintServiceAccount:
		return h.serveMintToken(c, route.clusterName)
	}

	return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
}

func (h *Handler) serveOIDCDocument(c *echo.Context, id string, discovery bool) error {
	var lastErr error

	for _, b := range h.RegionBackends() {
		var (
			doc any
			err error
		)

		if discovery {
			doc, err = b.OIDCDiscovery(id)
		} else {
			doc, err = b.OIDCJWKS(id)
		}

		if err == nil {
			return c.JSON(http.StatusOK, doc)
		}

		lastErr = err
	}

	return h.handleError(c, lastErr)
}

func (h *Handler) serveMintToken(c *echo.Context, cluster string) error {
	var in serviceAccountTokenBody

	if c.Request().Body != nil {
		if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
			return h.handleError(c, errMsgf(ErrValidation, "invalid request body"))
		}
	}

	req := ServiceAccountTokenRequest{
		Namespace: in.Namespace,
		Name:      in.Name,
		PodName:   in.PodName,
		Audiences: in.Audiences,
		TTL:       time.Duration(in.TTLSeconds) * time.Second,
	}

	if in.Audience != "" {
		req.Audiences = append([]string{in.Audience}, req.Audiences...)
	}

	if in.TTLSeconds == 0 {
		req.TTL = 0
	}

	tok, err := h.mintAcrossRegions(awsmeta.Region(c.Request().Context()), cluster, req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"token":               tok.Token,
		"issuer":              tok.Issuer,
		"expirationTimestamp": tok.ExpiresAt.Format(time.RFC3339),
	})
}

func (h *Handler) mintAcrossRegions(
	region, cluster string, req ServiceAccountTokenRequest,
) (*ServiceAccountToken, error) {
	backends := h.RegionBackends()
	if preferred := h.BackendFor(region); preferred != nil {
		backends = append([]*InMemoryBackend{preferred}, backends...)
	}

	var lastErr error

	for _, b := range backends {
		tok, err := b.MintServiceAccountToken(cluster, req)
		if err == nil {
			return tok, nil
		}

		lastErr = err
	}

	return nil, lastErr
}
