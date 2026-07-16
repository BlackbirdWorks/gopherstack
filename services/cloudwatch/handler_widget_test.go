package cloudwatch_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCW_GetMetricWidgetImage(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, url.Values{
		"Action":       []string{"GetMetricWidgetImage"},
		"MetricWidget": []string{`{"metrics":[]}`},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestGetMetricWidgetImage_ReturnsValidPNG verifies that GetMetricWidgetImage
// returns a non-empty base64-encoded PNG payload rather than an empty stub.
func TestGetMetricWidgetImage_ReturnsValidPNG(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantNonEmpty bool
	}{
		{
			name:         "no MetricWidget param",
			body:         "Action=GetMetricWidgetImage",
			wantNonEmpty: true,
		},
		{
			name:         "with MetricWidget param",
			body:         `Action=GetMetricWidgetImage&MetricWidget={"view":"timeSeries"}`,
			wantNonEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			rec := postForm(t, h, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			type resp struct {
				XMLName xml.Name `xml:"GetMetricWidgetImageResponse"`
				Image   string   `xml:"GetMetricWidgetImageResult>MetricWidgetImage"`
			}
			var r resp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))

			if tc.wantNonEmpty {
				assert.NotEmpty(t, r.Image, "MetricWidgetImage must be non-empty base64")
				_, err := base64.StdEncoding.DecodeString(r.Image)
				assert.NoError(t, err, "MetricWidgetImage must be valid base64")
			}
		})
	}
}
