package cloudwatch

import (
	"encoding/base64"
	"encoding/xml"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// minimalPNG1x1 is a base64-encoded 1×1 white PNG. It is retained as a last-resort
// fallback for the (unreachable) case where widget rendering fails.
const minimalPNG1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVQI12NgAAAAAgAB4iG8MwAAAABJRU5ErkJggg=="

// renderWidgetImageBase64 renders the MetricWidget JSON into a base64-encoded PNG.
// On any failure it falls back to the 1×1 placeholder so the response is always a
// decodable image.
func (h *Handler) renderWidgetImageBase64(form url.Values) string {
	widgetJSON := form.Get("MetricWidget")
	bk, _ := h.Backend.(*InMemoryBackend)

	img, err := renderMetricWidgetPNG(bk, widgetJSON, time.Now().UTC())
	if err != nil || len(img) == 0 {
		return minimalPNG1x1
	}

	return base64.StdEncoding.EncodeToString(img)
}

func (h *Handler) handleGetMetricWidgetImage(form url.Values, c *echo.Context) error {
	type response struct {
		MetricWidgetImage string   `xml:"GetMetricWidgetImageResult>MetricWidgetImage"`
		XMLName           xml.Name `xml:"GetMetricWidgetImageResponse"`
		Xmlns             string   `xml:"xmlns,attr"`
		RequestID         string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:             cloudwatchNS,
		RequestID:         uuid.New().String(),
		MetricWidgetImage: h.renderWidgetImageBase64(form),
	})
}
