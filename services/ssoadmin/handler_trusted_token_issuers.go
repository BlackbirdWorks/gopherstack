package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

type trustedTokenIssuerView struct {
	TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
	Name                   string `json:"Name"`
	InstanceArn            string `json:"InstanceArn"`
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
				http.StatusConflict,
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

	tagList := make([]tagView, 0, len(issuer.Tags))
	for k, v := range issuer.Tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	ttiMap := map[string]any{
		"TrustedTokenIssuerArn":  issuer.TrustedTokenIssuerArn,
		keyName:                  issuer.Name,
		keyInstanceArn:           issuer.InstanceArn,
		"TrustedTokenIssuerType": issuer.TrustedTokenIssuerType,
		keyTags:                  tagList,
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
		ttiMap["TrustedTokenIssuerConfiguration"] = cfgMap
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuer": ttiMap,
	})
}

func (h *Handler) handleListTrustedTokenIssuers(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	issuers := h.Backend.ListTrustedTokenIssuers(req.InstanceArn)
	sort.Slice(
		issuers,
		func(i, j int) bool { return issuers[i].TrustedTokenIssuerArn < issuers[j].TrustedTokenIssuerArn },
	)
	out := make([]trustedTokenIssuerView, 0, len(issuers))
	for _, issuer := range issuers {
		out = append(out, trustedTokenIssuerView{
			TrustedTokenIssuerArn:  issuer.TrustedTokenIssuerArn,
			Name:                   issuer.Name,
			InstanceArn:            issuer.InstanceArn,
			TrustedTokenIssuerType: issuer.TrustedTokenIssuerType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuers": out,
		keyNextToken:          nil,
	})
}

func (h *Handler) handleUpdateTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerConfiguration *struct {
			OidcJwtConfiguration *struct {
				IssuerURL                  string `json:"IssuerUrl"`
				ClaimAttributePath         string `json:"ClaimAttributePath"`
				IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
				JwksRetrievalOption        string `json:"JwksRetrievalOption"`
			} `json:"OidcJwtConfiguration"`
		} `json:"TrustedTokenIssuerConfiguration"`
		TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
		Name                   string `json:"Name"`
		TrustedTokenIssuerType string `json:"TrustedTokenIssuerType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	var cfg *TrustedTokenIssuerConfiguration
	if req.TrustedTokenIssuerConfiguration != nil {
		cfg = &TrustedTokenIssuerConfiguration{}
		if req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidc := req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfg.OidcJwtConfiguration = &OidcJwtConfiguration{
				IssuerURL:                  oidc.IssuerURL,
				ClaimAttributePath:         oidc.ClaimAttributePath,
				IdentityStoreAttributePath: oidc.IdentityStoreAttributePath,
				JwksRetrievalOption:        oidc.JwksRetrievalOption,
			}
		}
	}

	issuer, err := h.Backend.UpdateTrustedTokenIssuer(
		req.TrustedTokenIssuerArn,
		req.Name,
		req.TrustedTokenIssuerType,
		cfg,
	)
	if err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuer": trustedTokenIssuerView{
			TrustedTokenIssuerArn:  issuer.TrustedTokenIssuerArn,
			Name:                   issuer.Name,
			InstanceArn:            issuer.InstanceArn,
			TrustedTokenIssuerType: issuer.TrustedTokenIssuerType,
		},
	})
}
