package kinesisanalyticsv2

import (
	"context"
	"encoding/json/v2"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type createPresignedURLInput struct {
	ApplicationName                    string `json:"ApplicationName"`
	URLType                            string `json:"URLType"`
	SessionExpirationDurationInSeconds int64  `json:"SessionExpirationDurationInSeconds,omitempty"`
}

type createPresignedURLOutput struct {
	AuthorizedURL string `json:"AuthorizedUrl,omitempty"` //nolint:tagliatelle // AWS API field is AuthorizedUrl
}

func (h *Handler) handleCreateApplicationPresignedURL(ctx context.Context, c *echo.Context, body []byte) error {
	var in createPresignedURLInput
	if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.URLType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "URLType is required")
	}

	// Verify the application exists.
	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return a synthetic presigned URL based on the application ARN.
	presignedURL := "https://flink.amazonaws.com/dashboard/" + app.ApplicationARN + "?type=" + in.URLType

	return c.JSON(http.StatusOK, createPresignedURLOutput{AuthorizedURL: presignedURL})
}
