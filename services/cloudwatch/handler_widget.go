package cloudwatch

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// minimalPNG1x1 is a base64-encoded 1×1 white PNG. It is retained as a last-resort
// fallback for the (unreachable) case where widget rendering fails.
const minimalPNG1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVQI12NgAAAAAgAB4iG8MwAAAABJRU5ErkJggg=="

// outputFormatRawPNG is GetMetricWidgetImageInput.OutputFormat's one
// non-default value (api_op_GetMetricWidgetImage.go): "If you specify
// image/png, the HTTP response has a content-type set to image/png, and the
// body of the response is a PNG image" -- a raw binary response instead of
// the default XML-wrapped, base64-encoded one. Documented as intended only
// for custom (non-SDK) HTTP requests, which is exactly this awsquery/form
// path (the pinned aws-sdk-go-v2 client always speaks rpc-v2-cbor for this
// service and never sends OutputFormat at all).
const outputFormatRawPNG = "image/png"

// renderWidgetImagePNG renders the MetricWidget JSON into raw PNG bytes. On
// any failure it falls back to the 1x1 placeholder so the response is
// always a decodable image.
func (h *Handler) renderWidgetImagePNG(form url.Values) []byte {
	widgetJSON := form.Get("MetricWidget")
	bk, _ := h.Backend.(*InMemoryBackend)

	img, err := renderMetricWidgetPNG(bk, widgetJSON, time.Now().UTC())
	if err != nil || len(img) == 0 {
		img, _ = base64.StdEncoding.DecodeString(minimalPNG1x1)
	}

	return img
}

func (h *Handler) handleGetMetricWidgetImage(form url.Values, c *echo.Context) error {
	img := h.renderWidgetImagePNG(form)

	if form.Get("OutputFormat") == outputFormatRawPNG {
		// dispatchFormAction's caller pre-sets Content-Type: text/xml for
		// every awsquery action before dispatch; Echo's Blob only sets the
		// header when absent, so it must be overridden explicitly here.
		c.Response().Header().Set("Content-Type", "image/png")
		c.Response().WriteHeader(http.StatusOK)
		_, err := c.Response().Write(img)

		return err
	}

	type response struct {
		MetricWidgetImage string   `xml:"GetMetricWidgetImageResult>MetricWidgetImage"`
		XMLName           xml.Name `xml:"GetMetricWidgetImageResponse"`
		Xmlns             string   `xml:"xmlns,attr"`
		RequestID         string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:             cloudwatchNS,
		RequestID:         uuid.New().String(),
		MetricWidgetImage: base64.StdEncoding.EncodeToString(img),
	})
}
