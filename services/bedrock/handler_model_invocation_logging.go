package bedrock

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func extractLoggingConfigOperation(path, method string) (string, bool) {
	switch {
	case path == loggingConfigPath && method == http.MethodGet:
		return "GetModelInvocationLoggingConfiguration", true
	case path == loggingConfigPath && method == http.MethodPut:
		return "PutModelInvocationLoggingConfiguration", true
	case path == loggingConfigPath && method == http.MethodDelete:
		return "DeleteModelInvocationLoggingConfiguration", true
	default:
		return "", false
	}
}

func (h *Handler) routeLoggingConfig(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if path != loggingConfigPath {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.handleGetModelInvocationLoggingConfiguration(c)
	case http.MethodPut:
		return true, h.handlePutModelInvocationLoggingConfiguration(c, body)
	case http.MethodDelete:
		return true, h.handleDeleteModelInvocationLoggingConfiguration(c)
	default:
		return false, nil
	}
}

type modelInvocationLoggingConfigOutput struct {
	LoggingConfig *ModelInvocationLoggingConfiguration `json:"loggingConfig,omitempty"`
}

func (h *Handler) handleGetModelInvocationLoggingConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetModelInvocationLoggingConfiguration()

	return c.JSON(http.StatusOK, modelInvocationLoggingConfigOutput{LoggingConfig: cfg})
}

// putModelInvocationLoggingConfigurationInput mirrors PutModelInvocationLoggingConfigurationInput
// (api_op_PutModelInvocationLoggingConfiguration.go): the real request body wraps the config
// under a required top-level "loggingConfig" key, not the flat fields this handler used to
// accept at the top level.
type putModelInvocationLoggingConfigurationInput struct {
	LoggingConfig *ModelInvocationLoggingConfiguration `json:"loggingConfig"`
}

func (h *Handler) handlePutModelInvocationLoggingConfiguration(c *echo.Context, body []byte) error {
	in, err := parseBody[putModelInvocationLoggingConfigurationInput](body)
	if err != nil || in.LoggingConfig == nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "loggingConfig is required"),
		)
	}

	h.Backend.PutModelInvocationLoggingConfiguration(in.LoggingConfig)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteModelInvocationLoggingConfiguration(c *echo.Context) error {
	h.Backend.DeleteModelInvocationLoggingConfiguration()

	return c.NoContent(http.StatusOK)
}
