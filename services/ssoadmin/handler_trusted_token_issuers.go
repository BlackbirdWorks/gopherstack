package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// trustedTokenIssuerView is the wire shape for types.TrustedTokenIssuerMetadata
// (the ListTrustedTokenIssuers item type): Name/TrustedTokenIssuerArn/
// TrustedTokenIssuerType only -- no InstanceArn member (gopherstack previously
// invented one here).
type trustedTokenIssuerView struct {
	TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
	Name                   string `json:"Name"`
	TrustedTokenIssuerType string `json:"TrustedTokenIssuerType,omitempty"`
}

func (h *Handler) handleCreateTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerConfiguration *struct {
			OidcJwtConfiguration *struct {
				IssuerURL                  string `json:"IssuerUrl"`
				ClaimAttributePath         string `json:"ClaimAttributePath"`
				IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
				JwksRetrievalOption        string `json:"JwksRetrievalOption"`
			} `json:"OidcJwtConfiguration,omitempty"`
		} `json:"TrustedTokenIssuerConfiguration,omitempty"`
		InstanceArn            string    `json:"InstanceArn"`
		Name                   string    `json:"Name"`
		TrustedTokenIssuerType string    `json:"TrustedTokenIssuerType"`
		Tags                   []tagView `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Name is required")
	}
	if req.TrustedTokenIssuerType == "" {
		req.TrustedTokenIssuerType = ttiDefaultType
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	var cfg *TrustedTokenIssuerConfiguration
	if req.TrustedTokenIssuerConfiguration != nil {
		cfg = &TrustedTokenIssuerConfiguration{}
		if req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidcSrc := req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfg.OidcJwtConfiguration = &OidcJwtConfiguration{
				IssuerURL:                  oidcSrc.IssuerURL,
				ClaimAttributePath:         oidcSrc.ClaimAttributePath,
				IdentityStoreAttributePath: oidcSrc.IdentityStoreAttributePath,
				JwksRetrievalOption:        oidcSrc.JwksRetrievalOption,
			}
		}
	}

	ti, err := h.Backend.CreateTrustedTokenIssuer(req.InstanceArn, req.Name, req.TrustedTokenIssuerType, tags, cfg)
	if err != nil {
		if errors.Is(err, ErrTrustedTokenIssuerAlreadyExists) {
			return writeError(
				c,
				http.StatusBadRequest,
				"ConflictException",
				"trusted token issuer already exists: "+req.Name,
			)
		}

		return handleBackendError(c, err, "failed to create trusted token issuer: "+req.Name)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuerArn": ti.TrustedTokenIssuerArn,
	})
}

func (h *Handler) handleDeleteTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerArn string `json:"TrustedTokenIssuerArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.TrustedTokenIssuerArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "TrustedTokenIssuerArn is required")
	}
	if err := h.Backend.DeleteTrustedTokenIssuer(req.TrustedTokenIssuerArn); err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerArn string `json:"TrustedTokenIssuerArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	issuer, err := h.Backend.DescribeTrustedTokenIssuer(req.TrustedTokenIssuerArn)
	if err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	// Real DescribeTrustedTokenIssuerOutput is flat
	// (Name/TrustedTokenIssuerArn/TrustedTokenIssuerConfiguration/
	// TrustedTokenIssuerType) -- no nested "TrustedTokenIssuer" wrapper, no
	// InstanceArn member, and no Tags member (gopherstack previously invented
	// all three here). Tags are fetched separately via ListTagsForResource,
	// matching every other taggable ssoadmin resource.
	resp := map[string]any{
		"TrustedTokenIssuerArn":  issuer.TrustedTokenIssuerArn,
		keyName:                  issuer.Name,
		"TrustedTokenIssuerType": issuer.TrustedTokenIssuerType,
	}
	if issuer.TrustedTokenIssuerConfiguration != nil {
		cfgMap := map[string]any{}
		if issuer.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidc := issuer.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfgMap["OidcJwtConfiguration"] = map[string]any{
				"IssuerUrl":                  oidc.IssuerURL,
				"ClaimAttributePath":         oidc.ClaimAttributePath,
				"IdentityStoreAttributePath": oidc.IdentityStoreAttributePath,
				"JwksRetrievalOption":        oidc.JwksRetrievalOption,
			}
		}
		resp["TrustedTokenIssuerConfiguration"] = cfgMap
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListTrustedTokenIssuers(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	issuers := h.Backend.ListTrustedTokenIssuers(req.InstanceArn)
	out := make([]trustedTokenIssuerView, 0, len(issuers))
	for _, issuer := range issuers {
		out = append(out, trustedTokenIssuerView{
			TrustedTokenIssuerArn:  issuer.TrustedTokenIssuerArn,
			Name:                   issuer.Name,
			TrustedTokenIssuerType: issuer.TrustedTokenIssuerType,
		})
	}

	page, next := paginateBy(out, req.MaxResults, req.NextToken, func(v trustedTokenIssuerView) string {
		return v.TrustedTokenIssuerArn
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuers": page,
		keyNextToken:          next,
	})
}

func (h *Handler) handleUpdateTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerConfiguration *struct {
			OidcJwtConfiguration *struct {
				ClaimAttributePath         *string `json:"ClaimAttributePath"`
				IdentityStoreAttributePath *string `json:"IdentityStoreAttributePath"`
				JwksRetrievalOption        *string `json:"JwksRetrievalOption"`
			} `json:"OidcJwtConfiguration"`
		} `json:"TrustedTokenIssuerConfiguration"`
		TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
		Name                   string `json:"Name"`
		TrustedTokenIssuerType string `json:"TrustedTokenIssuerType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	// Real UpdateTrustedTokenIssuerInput.TrustedTokenIssuerConfiguration
	// (types.OidcJwtUpdateConfiguration) has no IssuerUrl member at all --
	// an issuer's IssuerUrl is immutable after creation -- and its remaining
	// fields are independently optional, so decode straight into the
	// pointer-scalar update shape rather than reusing Create's type.
	var cfg *TrustedTokenIssuerUpdateConfiguration
	if req.TrustedTokenIssuerConfiguration != nil {
		cfg = &TrustedTokenIssuerUpdateConfiguration{}
		if req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidc := req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfg.OidcJwtConfiguration = &OidcJwtUpdateConfiguration{
				ClaimAttributePath:         oidc.ClaimAttributePath,
				IdentityStoreAttributePath: oidc.IdentityStoreAttributePath,
				JwksRetrievalOption:        oidc.JwksRetrievalOption,
			}
		}
	}

	if _, err := h.Backend.UpdateTrustedTokenIssuer(
		req.TrustedTokenIssuerArn,
		req.Name,
		req.TrustedTokenIssuerType,
		cfg,
	); err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	// Real UpdateTrustedTokenIssuerOutput carries no members at all
	// (gopherstack previously echoed a full invented "TrustedTokenIssuer"
	// object here, including an InstanceArn field that doesn't exist on
	// TrustedTokenIssuerMetadata either); see api_op_UpdateTrustedTokenIssuer.go
	// in the real SDK.
	return writeJSON(c, http.StatusOK, map[string]any{})
}
