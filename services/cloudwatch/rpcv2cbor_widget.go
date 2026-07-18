package cloudwatch

import (
	"encoding/base64"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// cborGetMetricWidgetImage renders the requested MetricWidget into a real PNG plot.
// The CBOR MetricWidgetImage member is a blob, so the raw PNG bytes are emitted
// rather than the base64 text used by the XML/form response.
func (h *Handler) cborGetMetricWidgetImage(input cbor.Map, c *echo.Context) error {
	widgetJSON := cborStr(input, "MetricWidget")
	bk, _ := h.Backend.(*InMemoryBackend)

	img, err := renderMetricWidgetPNG(bk, widgetJSON, time.Now().UTC())
	if err != nil || len(img) == 0 {
		// Fall back to the minimal placeholder so callers always get a valid PNG.
		img, _ = base64.StdEncoding.DecodeString(minimalPNG1x1)
	}

	return writeCBOR(c, cbor.Map{
		"MetricWidgetImage": cbor.Slice(img),
	})
}
