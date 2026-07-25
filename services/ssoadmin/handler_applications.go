package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

type applicationView struct {
	ApplicationArn         string  `json:"ApplicationArn"`
	ApplicationProviderArn string  `json:"ApplicationProviderArn"`
	Name                   string  `json:"Name"`
	Description            string  `json:"Description,omitempty"`
	InstanceArn            string  `json:"InstanceArn"`
	Status                 string  `json:"Status"`
	ApplicationAccount     string  `json:"ApplicationAccount,omitempty"`
	CreatedFrom            string  `json:"CreatedFrom,omitempty"`
	IdentityStoreArn       string  `json:"IdentityStoreArn,omitempty"`
	CreatedDate            float64 `json:"CreatedDate,omitempty"`
}

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
	var req struct {
		PortalOptions struct {
			Visibility    string `json:"Visibility"`
			SignInOptions struct {
				Origin         string `json:"Origin"`
				ApplicationURL string `json:"ApplicationUrl"`
			} `json:"SignInOptions"`
		} `json:"PortalOptions"`
		InstanceArn            string    `json:"InstanceArn"`
		ApplicationProviderArn string    `json:"ApplicationProviderArn"`
		Name                   string    `json:"Name"`
		Description            string    `json:"Description"`
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

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	portalOptions := &PortalOptions{
		Visibility: req.PortalOptions.Visibility,
		SignInOptions: SignInOptions{
			Origin:         req.PortalOptions.SignInOptions.Origin,
			ApplicationURL: req.PortalOptions.SignInOptions.ApplicationURL,
		},
	}

	app, err := h.Backend.CreateApplication(
		req.InstanceArn,
		req.ApplicationProviderArn,
		req.Name,
		req.Description,
		tags,
		portalOptions,
	)
	if err != nil {
		if errors.Is(err, ErrApplicationAlreadyExists) {
			return writeError(c, http.StatusBadRequest, "ConflictException", "application already exists: "+req.Name)
		}

		return handleBackendError(c, err, "failed to create application: "+req.Name)
	}

	// Real CreateApplicationOutput is exactly {ApplicationArn, IdentityStoreArn,
	// InstanceArn} -- flat, top-level. No nested "Application" object exists on
	// the wire (gopherstack previously invented one here); see
	// awsAwsjson11_deserializeOpDocumentCreateApplicationOutput in the real
	// SDK's deserializers.go.
	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationArn:  app.ApplicationArn,
		"IdentityStoreArn": app.IdentityStoreArn,
		keyInstanceArn:     app.InstanceArn,
	})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}

	if err := h.Backend.DeleteApplication(req.ApplicationArn); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	app, err := h.Backend.DescribeApplication(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	// Real DescribeApplicationOutput fields are flat, top-level -- no nested
	// "Application" wrapper (gopherstack previously invented one here), and it
	// has no Tags member at all (tags are fetched separately via
	// ListTagsForResource, matching every other taggable ssoadmin resource);
	// see awsAwsjson11_deserializeOpDocumentDescribeApplicationOutput in the
	// real SDK's deserializers.go.
	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAccount":      app.ApplicationAccount,
		keyApplicationArn:         app.ApplicationArn,
		keyApplicationProviderArn: app.ApplicationProviderArn,
		"CreatedDate":             float64(app.CreatedDate.Unix()),
		"CreatedFrom":             app.CreatedFrom,
		"Description":             app.Description,
		"IdentityStoreArn":        app.IdentityStoreArn,
		keyInstanceArn:            app.InstanceArn,
		keyName:                   app.Name,
		"PortalOptions":           app.PortalOptions,
		keyStatus:                 app.Status,
	})
}

func (h *Handler) handleDescribeApplicationProvider(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationProviderArn string `json:"ApplicationProviderArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	provider, err := h.Backend.DescribeApplicationProvider(req.ApplicationProviderArn)
	if err != nil {
		return handleBackendError(c, err, "application provider not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationProviderArn: provider.ApplicationProviderArn,
		"DisplayData":             provider.DisplayData,
		"FederationProtocol":      provider.FederationProtocol,
	})
}

func (h *Handler) handleListApplicationProviders(c *echo.Context, body []byte) error {
	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	providers := h.Backend.ListApplicationProviders()
	out := make([]map[string]any, 0, len(providers))

	for _, provider := range providers {
		out = append(out, map[string]any{
			keyApplicationProviderArn: provider.ApplicationProviderArn,
			"DisplayData":             provider.DisplayData,
			"FederationProtocol":      provider.FederationProtocol,
		})
	}

	page, next := paginateBy(out, req.MaxResults, req.NextToken, func(v map[string]any) string {
		arn, _ := v[keyApplicationProviderArn].(string)

		return arn
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationProviders": page,
		keyNextToken:           next,
	})
}

func (h *Handler) handleListApplications(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	apps := h.Backend.ListApplications(req.InstanceArn)
	sort.Slice(apps, func(i, j int) bool { return apps[i].ApplicationArn < apps[j].ApplicationArn })
	out := make([]applicationView, 0, len(apps))
	for _, app := range apps {
		out = append(out, applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			ApplicationAccount:     app.ApplicationAccount,
			CreatedFrom:            app.CreatedFrom,
			IdentityStoreArn:       app.IdentityStoreArn,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		})
	}

	page, next := paginateBy(out, req.MaxResults, req.NextToken, func(v applicationView) string {
		return v.ApplicationArn
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"Applications": page,
		keyNextToken:   next,
	})
}

func (h *Handler) handleUpdateApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Name           string `json:"Name"`
		Description    string `json:"Description"`
		Status         string `json:"Status"`
		PortalOptions  struct {
			Visibility    string `json:"Visibility"`
			SignInOptions struct {
				Origin         string `json:"Origin"`
				ApplicationURL string `json:"ApplicationUrl"`
			} `json:"SignInOptions"`
		} `json:"PortalOptions"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	portalOptions := &PortalOptions{
		Visibility: req.PortalOptions.Visibility,
		SignInOptions: SignInOptions{
			Origin:         req.PortalOptions.SignInOptions.Origin,
			ApplicationURL: req.PortalOptions.SignInOptions.ApplicationURL,
		},
	}

	if _, err := h.Backend.UpdateApplication(
		req.ApplicationArn, req.Name, req.Description, req.Status, portalOptions,
	); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	// Real UpdateApplicationOutput carries no members at all (gopherstack
	// previously invented a full "Application" echo here); see
	// api_op_UpdateApplication.go in the real SDK.
	return writeJSON(c, http.StatusOK, map[string]any{})
}
