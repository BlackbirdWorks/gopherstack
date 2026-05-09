package cloudwatch_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCW_MetricStreams(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// StartMetricStreams
	rec := postForm(t, h, url.Values{
		"Action":         []string{"StartMetricStreams"},
		"Names.member.1": []string{"my-stream"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// StopMetricStreams
	rec = postForm(t, h, url.Values{
		"Action":         []string{"StopMetricStreams"},
		"Names.member.1": []string{"my-stream"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

func TestCW_GetMetricWidgetImage(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, url.Values{
		"Action":      []string{"GetMetricWidgetImage"},
		"MetricWidget": []string{`{"metrics":[]}`},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
