package bedrock

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// extractEnforcedGuardrailConfigOperation mirrors
// routeEnforcedGuardrailConfig's dispatch order exactly, so ExtractOperation
// agrees with the real dispatch contract -- previously absent from
// ExtractOperation's extractor list entirely (found by gopherstack-n1mb's
// route table; Handler() itself already dispatched these correctly).
func extractEnforcedGuardrailConfigOperation(path, method string) (string, bool) {
	switch {
	case path == enforcedGuardrailsPath && method == http.MethodGet:
		return "ListEnforcedGuardrailsConfiguration", true
	case path == enforcedGuardrailsPath && method == http.MethodPut:
		return "PutEnforcedGuardrailConfiguration", true
	case strings.HasPrefix(path, enforcedGuardrailsPath+"/") && method == http.MethodDelete:
		return "DeleteEnforcedGuardrailConfiguration", true
	}

	return "", false
}

// routeEnforcedGuardrailConfig handles PutEnforcedGuardrailConfiguration,
// ListEnforcedGuardrailsConfiguration, and DeleteEnforcedGuardrailConfiguration.
//
// Real AWS: PUT/GET /enforcedGuardrailsConfiguration (put/list) and
// DELETE /enforcedGuardrailsConfiguration/{configId}. gopherstack previously
// used an invented path ("/enforced-guardrail-configuration"), an invented
// guardrailID+version-keyed resource model, and a query-param ?guardrailId=
// for delete instead of the real ConfigId-keyed model with a path-param
// delete -- all fixed here; see enforced_guardrail_config.go for the backend
// side of the redesign.
func (h *Handler) routeEnforcedGuardrailConfig(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == enforcedGuardrailsPath && method == http.MethodGet:
		return true, h.handleListEnforcedGuardrailsConfiguration(c)
	case path == enforcedGuardrailsPath && method == http.MethodPut:
		return true, h.handlePutEnforcedGuardrailConfiguration(c)
	case strings.HasPrefix(path, enforcedGuardrailsPath+"/") && method == http.MethodDelete:
		configID := decodePath(strings.TrimPrefix(path, enforcedGuardrailsPath+"/"))

		return true, h.handleDeleteEnforcedGuardrailConfiguration(c, configID)
	}

	return false, nil
}

type modelEnforcementWire struct {
	IncludedModels []string `json:"includedModels,omitempty"`
	ExcludedModels []string `json:"excludedModels,omitempty"`
}

type guardrailInferenceConfigWire struct {
	ModelEnforcement    *modelEnforcementWire `json:"modelEnforcement,omitempty"`
	GuardrailIdentifier string                `json:"guardrailIdentifier"`
	GuardrailVersion    string                `json:"guardrailVersion"`
	InputTags           string                `json:"inputTags"`
}

type putEnforcedGuardrailConfigurationInput struct {
	GuardrailInferenceConfig *guardrailInferenceConfigWire `json:"guardrailInferenceConfig"`
	ConfigID                 string                        `json:"configId,omitempty"`
}

type putEnforcedGuardrailConfigurationOutput struct {
	ConfigID  string  `json:"configId"`
	UpdatedAt isoTime `json:"updatedAt"`
	UpdatedBy string  `json:"updatedBy"`
}

func enforcedGuardrailConfigToWire(cfg *AccountEnforcedGuardrailConfig) map[string]any {
	return map[string]any{
		"configId":         cfg.ConfigID,
		"createdAt":        isoTime{cfg.CreatedAt},
		"createdBy":        cfg.CreatedBy,
		"guardrailArn":     cfg.GuardrailArn,
		"guardrailId":      cfg.GuardrailID,
		"guardrailVersion": cfg.GuardrailVersion,
		"inputTags":        cfg.InputTags,
		"modelEnforcement": modelEnforcementWire{
			IncludedModels: cfg.IncludedModels,
			ExcludedModels: cfg.ExcludedModels,
		},
		"owner":     cfg.Owner,
		"updatedAt": isoTime{cfg.UpdatedAt},
		"updatedBy": cfg.UpdatedBy,
	}
}

func (h *Handler) handleListEnforcedGuardrailsConfiguration(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")

	configs, newToken := h.Backend.ListEnforcedGuardrailsConfiguration(nextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		items = append(items, enforcedGuardrailConfigToWire(cfg))
	}

	out := map[string]any{"guardrailsConfig": items}
	if newToken != "" {
		out["nextToken"] = newToken
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handlePutEnforcedGuardrailConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[putEnforcedGuardrailConfigurationInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.GuardrailInferenceConfig == nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "guardrailInferenceConfig is required"),
		)
	}

	var includedModels, excludedModels []string
	if me := in.GuardrailInferenceConfig.ModelEnforcement; me != nil {
		includedModels = me.IncludedModels
		excludedModels = me.ExcludedModels
	}

	cfg, opErr := h.Backend.PutEnforcedGuardrailConfiguration(
		in.ConfigID,
		in.GuardrailInferenceConfig.GuardrailIdentifier,
		in.GuardrailInferenceConfig.GuardrailVersion,
		in.GuardrailInferenceConfig.InputTags,
		includedModels,
		excludedModels,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, putEnforcedGuardrailConfigurationOutput{
		ConfigID:  cfg.ConfigID,
		UpdatedAt: isoTime{cfg.UpdatedAt},
		UpdatedBy: cfg.UpdatedBy,
	})
}

func (h *Handler) handleDeleteEnforcedGuardrailConfiguration(c *echo.Context, configID string) error {
	if configID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "configId path parameter is required"),
		)
	}

	if err := h.Backend.DeleteEnforcedGuardrailConfiguration(configID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
