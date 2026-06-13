package kinesis

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// handleCBORRequest handles a Kinesis request that arrived with
// Content-Type: application/x-amz-cbor-1.1.
func (h *Handler) handleCBORRequest(
	ctx context.Context,
	c *echo.Context,
	log *slog.Logger,
	action string,
) error {
	// "Data" – Record payload in PutRecord / PutRecords / GetRecords.
	binaryKeys := map[string]bool{"Data": true}

	raw, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "failed to read CBOR request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	jsonBody, err := service.CBORToJSON(raw)
	if err != nil {
		log.ErrorContext(ctx, "failed to decode CBOR body", "error", err, "action", action)

		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "SerializationException",
			Message: "invalid CBOR body: " + err.Error(),
		})
	}

	log.DebugContext(ctx, "Kinesis CBOR request", "action", action)

	jsonResponse, reqErr := h.kinesisRoute(ctx, c.Request(), action, jsonBody)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	cborPayload, err := service.JSONToCBOR(jsonResponse, binaryKeys)
	if err != nil {
		log.ErrorContext(ctx, "failed to encode CBOR response", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(http.StatusOK, service.ContentTypeCBOR, cborPayload)
}
