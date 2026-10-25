package iot

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// handleStartCommandExecution serves the iot-jobs-data StartCommandExecution (POST /command-executions).
func (h *Handler) handleStartCommandExecution(c *echo.Context) error {
	var body struct {
		ExecutionTimeoutSeconds *int64                    `json:"executionTimeoutSeconds"`
		Parameters              map[string]map[string]any `json:"parameters"`
		CommandArn              string                    `json:"commandArn"`
		TargetArn               string                    `json:"targetArn"`
		ClientToken             string                    `json:"clientToken"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	started, err := h.Backend.StartCommandExecution(&StartCommandExecutionInput{
		CommandARN: body.CommandArn, TargetARN: body.TargetArn, ClientToken: body.ClientToken,
		TimeoutSeconds: body.ExecutionTimeoutSeconds, Parameters: body.Parameters,
	})
	if err != nil {
		return writeCommandStartError(c, err)
	}

	if h.broker != nil && started.DeviceID != "" {
		h.broker.publishCommandRequest(started)
	}

	return c.JSON(http.StatusOK, map[string]string{keyExecutionID: started.ExecutionID})
}

func writeCommandStartError(c *echo.Context, err error) error {
	if isValidationErr(err) {
		return c.JSON(http.StatusBadRequest, awsErrBody{"ValidationException", publicMessage(err)})
	}

	return writeIoTError(c, err)
}

func isValidationErr(err error) bool { return errors.Is(err, ErrValidation) }
