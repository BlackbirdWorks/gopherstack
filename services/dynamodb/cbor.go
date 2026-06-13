package dynamodb

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ddbBinaryKeys lists DynamoDB attribute-value keys whose JSON string values
// are standard base64-encoded binary data and must be re-encoded as CBOR byte
// strings on the response path.
//
// "B"  – Binary   AttributeValue
// "BS" – BinarySet AttributeValue (elements of the set are the binary values)
var ddbBinaryKeys = map[string]bool{
	"B":  true,
	"BS": true,
}

// handleCBORRequest handles a DynamoDB request that arrived with
// Content-Type: application/x-amz-cbor-1.1. It decodes the CBOR body to JSON,
// dispatches through the normal JSON handlers, then re-encodes the response as
// CBOR before writing it.
func (h *DynamoDBHandler) handleCBORRequest(
	c *echo.Context,
	ctx context.Context,
	log *slog.Logger,
	action string,
) error {
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

	log.DebugContext(ctx, "DynamoDB CBOR request", "action", action)

	response, reqErr := h.dispatch(ctx, action, jsonBody)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	jsonPayload, err := json.Marshal(response)
	if err != nil {
		log.ErrorContext(ctx, "failed to marshal DynamoDB response", "error", err)
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	cborPayload, err := service.JSONToCBOR(jsonPayload, ddbBinaryKeys)
	if err != nil {
		log.ErrorContext(ctx, "failed to encode CBOR response", "error", err)
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	checksum := crc32.ChecksumIEEE(cborPayload)
	c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))

	return c.Blob(http.StatusOK, service.ContentTypeCBOR, cborPayload)
}
