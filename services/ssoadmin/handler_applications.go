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
			return writeError(c, http.StatusConflict, "ConflictException", "application already exists: "+req.Name)
		}

		return handleBackendError(c, err, "failed to create application: "+req.Name)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationArn: app.ApplicationArn,
		keyApplication: applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		},
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

	tagList := make([]tagView, 0, len(app.Tags))
	for k, v := range app.Tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplication: map[string]any{
			keyApplicationArn:         app.ApplicationArn,
			keyApplicationProviderArn: app.ApplicationProviderArn,
			keyName:                   app.Name,
			"Description":             app.Description,
			keyInstanceArn:            app.InstanceArn,
			keyStatus:                 app.Status,
			"CreatedDate":             float64(app.CreatedDate.Unix()),
			keyTags:                   tagList,
		},
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
	})
}

func (h *Handler) handleListApplicationProviders(c *echo.Context, _ []byte) error {
	providers := h.Backend.ListApplicationProviders()
	out := make([]map[string]any, 0, len(providers))
	for _, provider := range providers {
		out = append(out, map[string]any{
			keyApplicationProviderArn: provider.ApplicationProviderArn,
			"DisplayData":             provider.DisplayData,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationProviders": out,
		keyNextToken:           nil,
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

	app, err := h.Backend.UpdateApplication(req.ApplicationArn, req.Name, req.Description, req.Status, portalOptions)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplication: applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		},
	})
}
