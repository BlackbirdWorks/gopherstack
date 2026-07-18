package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type scramSecretInput struct {
	SecretArnList []string `json:"secretArnList"`
}

type batchScramSecretOutput struct {
	UnprocessedScramSecrets []ScramSecretError `json:"unprocessedScramSecrets"`
}

func (h *Handler) handleBatchAssociateScramSecret(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	errs, err := h.Backend.BatchAssociateScramSecret(ctx, clusterArn, in.SecretArnList)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, batchScramSecretOutput{UnprocessedScramSecrets: errs})
}

func (h *Handler) handleBatchDisassociateScramSecret(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in scramSecretInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	errs, err := h.Backend.BatchDisassociateScramSecret(ctx, clusterArn, in.SecretArnList)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, batchScramSecretOutput{UnprocessedScramSecrets: errs})
}

type listScramSecretsOutput struct {
	SecretArnList []string `json:"secretArnList"`
}

func (h *Handler) handleListScramSecrets(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	secrets, err := h.Backend.ListScramSecrets(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listScramSecretsOutput{SecretArnList: secrets})
}
