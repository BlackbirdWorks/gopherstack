package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleAssumeDecoratedRoleWithSAML(_ context.Context, c *echo.Context, body []byte) error {
	var in assumeDecoratedRoleWithSAMLInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.PrincipalArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PrincipalArn is required")
	}

	if strings.TrimSpace(in.RoleArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "RoleArn is required")
	}

	if strings.TrimSpace(in.SAMLAssertion) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "SAMLAssertion is required")
	}

	out := h.Backend.AssumeDecoratedRoleWithSAML(in.PrincipalArn, in.RoleArn, in.SAMLAssertion, in.DurationSeconds)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleGetDataLakePrincipal(ctx context.Context, c *echo.Context, _ []byte) error {
	principal := h.Backend.GetDataLakePrincipal(ctx)

	return c.JSON(http.StatusOK, getDataLakePrincipalOutput{Identity: principal.DataLakePrincipalIdentifier})
}

func (h *Handler) handleGetTemporaryDataLocationCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryDataLocationCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.ResourceArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "ResourceArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryDataLocationCredentialsOutput{Credentials: creds})
}

func (h *Handler) handleGetTemporaryGluePartitionCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryGluePartitionCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.TableArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryGluePartitionCredentialsOutput{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expiration,
	})
}

func (h *Handler) handleGetTemporaryGlueTableCredentials(_ context.Context, c *echo.Context, body []byte) error {
	var in getTemporaryGlueTableCredentialsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.TableArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TableArn is required")
	}
	creds := h.Backend.GetTemporaryCredentials(in.DurationSeconds)

	return c.JSON(http.StatusOK, getTemporaryGlueTableCredentialsOutput{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expiration,
	})
}
