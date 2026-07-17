package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestELBv2_DescribeAccountLimits verifies the handler returns hardcoded limits.
func TestELBv2_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.Limits.Members)
}

func TestDescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Limits")
}
