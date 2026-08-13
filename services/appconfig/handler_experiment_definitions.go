package appconfig

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// experimentDefinitionCreateRequest binds CreateExperimentDefinitionInput's
// JSON body (ApplicationIdentifier is bound separately from the URI path --
// see handleCreateExperimentDefinition). Control/Treatments reuse the
// Treatment struct for binding even though real TreatmentInput has no Key
// member: any Key field a caller sends is simply ignored, since this
// backend always overwrites it with a server-generated value (see the
// Treatment doc comment in models.go).
type experimentDefinitionCreateRequest struct {
	Control                        *Treatment        `json:"Control"`
	AudienceDescription            string            `json:"AudienceDescription"`
	AudienceRule                   string            `json:"AudienceRule"`
	ConfigurationProfileIdentifier string            `json:"ConfigurationProfileIdentifier"`
	EnvironmentIdentifier          string            `json:"EnvironmentIdentifier"`
	FlagKey                        string            `json:"FlagKey"`
	Hypothesis                     string            `json:"Hypothesis"`
	LaunchCriteria                 string            `json:"LaunchCriteria"`
	Name                           string            `json:"Name"`
	Tags                           map[string]string `json:"Tags"`
	Treatments                     []Treatment       `json:"Treatments"`
}

func (h *Handler) handleCreateExperimentDefinition(c *echo.Context, applicationID string) error {
	var req experimentDefinitionCreateRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: errInvalidRequestBody})
	}

	def, err := h.Backend.CreateExperimentDefinition(
		applicationID, req.Name, req.EnvironmentIdentifier, req.ConfigurationProfileIdentifier, req.FlagKey,
		req.AudienceRule, req.AudienceDescription, req.Hypothesis, req.LaunchCriteria,
		req.Control, req.Treatments, req.Tags,
	)
	if err != nil {
		return experimentDefinitionErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, def)
}

func (h *Handler) handleGetExperimentDefinition(c *echo.Context, applicationID, defIdentifier string) error {
	def, err := h.Backend.GetExperimentDefinition(applicationID, defIdentifier)
	if err != nil {
		return experimentDefinitionErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, def)
}

func (h *Handler) handleListExperimentDefinitions(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	q := c.Request().URL.Query()

	defs, outToken := h.Backend.ListExperimentDefinitions(
		q.Get("application_identifier"),
		q.Get("configuration_profile_identifier"),
		q.Get("environment_identifier"),
		q.Get("status"),
		nextToken, maxResults,
	)

	summaries := make([]ExperimentDefinitionSummary, 0, len(defs))
	for _, d := range defs {
		summaries = append(summaries, experimentDefinitionToSummary(d))
	}

	resp := map[string]any{keyItems: summaries}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateExperimentDefinition(c *echo.Context, applicationID, defIdentifier string) error {
	var req struct {
		Control             *Treatment   `json:"Control"`
		AudienceDescription *string      `json:"AudienceDescription"`
		AudienceRule        *string      `json:"AudienceRule"`
		Hypothesis          *string      `json:"Hypothesis"`
		LaunchCriteria      *string      `json:"LaunchCriteria"`
		Treatments          *[]Treatment `json:"Treatments"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: errInvalidRequestBody})
	}

	def, err := h.Backend.UpdateExperimentDefinition(
		applicationID, defIdentifier,
		req.AudienceDescription, req.AudienceRule, req.Control, req.Hypothesis, req.LaunchCriteria, req.Treatments,
	)
	if err != nil {
		return experimentDefinitionErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, def)
}

func (h *Handler) handleDeleteExperimentDefinition(c *echo.Context, applicationID, defIdentifier string) error {
	deleteType := c.Request().URL.Query().Get("delete_type")

	if err := h.Backend.DeleteExperimentDefinition(applicationID, defIdentifier, deleteType); err != nil {
		return experimentDefinitionErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// experimentDefinitionErrorResponse maps a StorageBackend experiment-
// definition error to its AWS HTTP status/body, shared by every handler in
// this file.
func experimentDefinitionErrorResponse(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return notFoundResponse(c, err)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return conflictResponse(c, err)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return badRequestResponse(c, err)
	default:
		return internalServerErrorResponse(c, err)
	}
}
