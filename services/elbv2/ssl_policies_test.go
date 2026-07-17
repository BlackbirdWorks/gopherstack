package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestELBv2_DescribeSSLPolicies verifies the handler returns standard SSL policies.
func TestELBv2_DescribeSSLPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.SslPolicies.Members)

	names := make([]string, 0, len(resp.Result.SslPolicies.Members))
	for _, m := range resp.Result.SslPolicies.Members {
		names = append(names, m.Name)
	}

	assert.Contains(t, names, "ELBSecurityPolicy-2016-08")
}

// TestDescribeSSLPoliciesFiltering verifies that SSL policies can be filtered by name.
func TestDescribeSSLPoliciesFiltering(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeSSLPolicies"},
		"Version":        {"2015-12-01"},
		"Names.member.1": {"ELBSecurityPolicy-2016-08"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SslPolicies.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.SslPolicies.Members[0].Name)
}

// TestDescribeSSLPoliciesAll verifies that unfiltered DescribeSSLPolicies returns multiple policies.
func TestDescribeSSLPoliciesAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Greater(t, len(resp.Result.SslPolicies.Members), 2)

	// Verify specific policies are present.
	names := make(map[string]bool)
	for _, p := range resp.Result.SslPolicies.Members {
		names[p.Name] = true
	}
	assert.True(t, names["ELBSecurityPolicy-2016-08"])
	assert.True(t, names["ELBSecurityPolicy-TLS13-1-2-2021-06"])
	assert.True(t, names["ELBSecurityPolicy-FS-1-2-Res-2020-10"])
	assert.True(t, names["ELBSecurityPolicy-FS-2018-06"])
}

func TestDescribeSSLPolicies_All(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ELBSecurityPolicy")
}

func TestDescribeSSLPolicies_Filter(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeSSLPolicies"},
		"Version":        {"2015-12-01"},
		"Names.member.1": {"ELBSecurityPolicy-2016-08"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SSLPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SSLPolicies.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.SSLPolicies.Members[0].Name)
}
