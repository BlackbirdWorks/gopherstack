package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func parseJobTemplateRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListJobTemplates}
		case http.MethodPost:
			return mcRoute{operation: opCreateJobTemplate}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetJobTemplate, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdateJobTemplate, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeleteJobTemplate, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

// --- Job Template handlers ---

type createJobTemplateInput struct {
	AccelerationSettings *AccelerationSettings `json:"accelerationSettings,omitempty"`
	Settings             map[string]any        `json:"settings,omitempty"`
	Tags                 map[string]string     `json:"tags,omitempty"`
	Name                 string                `json:"name"`
	Description          string                `json:"description,omitempty"`
	Category             string                `json:"category,omitempty"`
	Queue                string                `json:"queue,omitempty"`
	StatusUpdateInterval string                `json:"statusUpdateInterval,omitempty"`
	HopDestinations      []HopDestination      `json:"hopDestinations,omitempty"`
	Priority             int                   `json:"priority"`
}

type jobTemplateWrapper struct {
	JobTemplate *JobTemplate `json:"jobTemplate"`
}

type jobTemplatesListOutput struct {
	JobTemplates []*JobTemplate `json:"jobTemplates"`
}

func (h *Handler) handleCreateJobTemplate(c *echo.Context, body []byte) error {
	var in createJobTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	accelMode := ""
	if in.AccelerationSettings != nil {
		accelMode = in.AccelerationSettings.Mode
	}

	jt, err := h.Backend.CreateJobTemplateFull(
		in.Name,
		in.Description,
		in.Category,
		in.Queue,
		in.Priority,
		in.Settings,
		in.Tags,
		accelMode,
		in.StatusUpdateInterval,
		in.HopDestinations,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleGetJobTemplate(c *echo.Context, name string) error {
	jt, err := h.Backend.GetJobTemplate(name)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleListJobTemplates(c *echo.Context) error {
	templates := h.Backend.ListJobTemplates()
	if templates == nil {
		templates = []*JobTemplate{}
	}

	q := c.Request().URL.Query()
	category := q.Get("category")

	if category != "" {
		filtered := templates[:0:0]

		for _, t := range templates {
			if t.Category == category {
				filtered = append(filtered, t)
			}
		}

		templates = filtered
	}

	if q.Get("order") == orderDescending {
		reverseSlice(templates)
	}

	out := jobTemplatesListOutput{JobTemplates: limitSlice(templates, parseMaxResults(q.Get("maxResults")))}

	return c.JSON(http.StatusOK, out)
}

type updateJobTemplateInput struct {
	Priority             *int                  `json:"priority,omitempty"`
	Settings             map[string]any        `json:"settings,omitempty"`
	AccelerationSettings *AccelerationSettings `json:"accelerationSettings,omitempty"`
	Description          string                `json:"description,omitempty"`
	Category             string                `json:"category,omitempty"`
	Queue                string                `json:"queue,omitempty"`
	StatusUpdateInterval string                `json:"statusUpdateInterval,omitempty"`
	HopDestinations      []HopDestination      `json:"hopDestinations,omitempty"`
}

func (h *Handler) handleUpdateJobTemplate(c *echo.Context, name string, body []byte) error {
	var in updateJobTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	jt, err := h.Backend.UpdateJobTemplateFull(
		name,
		in.Description,
		in.Category,
		in.Queue,
		in.Priority,
		in.Settings,
		in.AccelerationSettings,
		in.StatusUpdateInterval,
		in.HopDestinations,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleDeleteJobTemplate(c *echo.Context, name string) error {
	if err := h.Backend.DeleteJobTemplate(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
