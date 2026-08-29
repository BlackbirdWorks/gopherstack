package mq

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createConfigurationInput struct {
	Tags                   map[string]string `json:"tags"`
	Name                   string            `json:"name"`
	Description            string            `json:"description"`
	EngineType             string            `json:"engineType"`
	EngineVersion          string            `json:"engineVersion"`
	AuthenticationStrategy string            `json:"authenticationStrategy"`
}

func (h *Handler) handleCreateConfiguration(c *echo.Context, body []byte) error {
	var in createConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "invalid request body"),
		)
	}

	if in.Name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "name is required"),
		)
	}

	if in.EngineType == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "engineType is required"),
		)
	}

	cfg, err := h.Backend.CreateConfiguration(
		in.Name,
		in.Description,
		in.EngineType,
		in.EngineVersion,
		in.AuthenticationStrategy,
		in.Tags,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":                     cfg.ID,
		"arn":                    cfg.Arn,
		"name":                   cfg.Name,
		keyCreated:               cfg.Created,
		"engineType":             cfg.EngineType,
		"engineVersion":          cfg.EngineVersion,
		"authenticationStrategy": cfg.AuthenticationStrategy,
		"latestRevision":         cfg.LatestRevision,
	})
}

func (h *Handler) handleDescribeConfiguration(c *echo.Context, configID string) error {
	cfg, err := h.Backend.DescribeConfiguration(configID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, toConfigurationResponse(cfg))
}

func (h *Handler) handleListConfigurations(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults := 0

	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 100 {
			maxResults = n
		}
	}

	cfgs := h.Backend.ListConfigurations()
	if cfgs == nil {
		cfgs = []*Configuration{}
	}

	// Use opaque index-based tokens so the page boundary is stable.
	pg := page.New(cfgs, nextToken, maxResults, mqDefaultPageSize)

	list := make([]any, 0, len(pg.Data))
	for _, cfg := range pg.Data {
		list = append(list, toConfigurationResponse(cfg))
	}

	resp := map[string]any{"configurations": list}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return c.JSON(http.StatusOK, resp)
}

type updateConfigurationInput struct {
	Data        string `json:"data"`
	Description string `json:"description"`
}

func (h *Handler) handleUpdateConfiguration(c *echo.Context, configID string, body []byte) error {
	var in updateConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "invalid request body"),
		)
	}

	cfg, err := h.Backend.UpdateConfiguration(configID, in.Description, in.Data)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":             cfg.ID,
		"arn":            cfg.Arn,
		"name":           cfg.Name,
		keyCreated:       cfg.Created,
		"latestRevision": cfg.LatestRevision,
		"warnings":       []any{},
	})
}

// configurationResponse is the full configuration detail response.
type configurationResponse struct {
	Tags                   map[string]string      `json:"tags"`
	Arn                    string                 `json:"arn"`
	ID                     string                 `json:"id"`
	Name                   string                 `json:"name"`
	Description            string                 `json:"description"`
	EngineType             string                 `json:"engineType"`
	EngineVersion          string                 `json:"engineVersion"`
	AuthenticationStrategy string                 `json:"authenticationStrategy,omitempty"`
	LatestRevision         *ConfigurationRevision `json:"latestRevision"`
	Created                string                 `json:"created"`
}

func toConfigurationResponse(cfg *Configuration) configurationResponse {
	return configurationResponse{
		Arn:                    cfg.Arn,
		ID:                     cfg.ID,
		Name:                   cfg.Name,
		Description:            cfg.Description,
		EngineType:             cfg.EngineType,
		EngineVersion:          cfg.EngineVersion,
		AuthenticationStrategy: cfg.AuthenticationStrategy,
		LatestRevision:         cfg.LatestRevision,
		Created:                cfg.Created,
		Tags:                   tagsOrEmpty(cfg.Tags),
	}
}

func (h *Handler) handleDeleteConfiguration(c *echo.Context, configID string) error {
	if err := h.Backend.DeleteConfiguration(configID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
