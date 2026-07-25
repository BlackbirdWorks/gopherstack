package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborGetOTelEnrichment(_ cbor.Map, c *echo.Context) error {
	status, err := h.Backend.GetOTelEnrichment()
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{"Status": cbor.String(status)})
}

func (h *Handler) cborStartOTelEnrichment(_ cbor.Map, c *echo.Context) error {
	if err := h.Backend.StartOTelEnrichment(); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborStopOTelEnrichment(_ cbor.Map, c *echo.Context) error {
	if err := h.Backend.StopOTelEnrichment(); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}
