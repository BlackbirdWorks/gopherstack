package elasticsearch_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestElasticsearchHandler_ErrValidationSentinel verifies that ErrValidation
// is exported and wraps correctly.
func TestElasticsearchHandler_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(context.Background(), elasticsearch.CreateDomainInput{})
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestElasticsearchHandler_DomainNameValidation verifies CreateDomain rejects
// domain names that don't satisfy the 3-28 lowercase alphanumeric/hyphen,
// starts-with-a-letter constraint.
func TestElasticsearchHandler_DomainNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
	}{
		{name: "too_short", domainName: "ab"},
		{name: "too_long", domainName: "abcdefghijklmnopqrstuvwxyzabc"},
		{name: "invalid_chars", domainName: "my_domain"},
		{name: "must_start_with_letter", domainName: "1bad-domain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.CreateDomain(context.Background(), elasticsearch.CreateDomainInput{Name: tt.domainName})
			require.Error(t, err)
			assert.ErrorIs(t, err, elasticsearch.ErrValidation)
		})
	}
}

// TestElasticsearchHandler_ErrValidation400Mapping verifies ErrValidation maps to HTTP 400.
func TestElasticsearchHandler_ErrValidation400Mapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// An invalid domain name (starts with digit) should return 400.
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "1invalid",
	})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestElasticsearchHandler_ElasticsearchVersionValidation verifies version
// string validation on CreateDomain.
func TestElasticsearchHandler_ElasticsearchVersionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    string
		wantStatus int
	}{
		{name: "empty-defaults", version: "", wantStatus: http.StatusOK},
		{name: "valid-710", version: "7.10", wantStatus: http.StatusOK},
		{name: "valid-68", version: "6.8", wantStatus: http.StatusOK},
		{name: "valid-23", version: "2.3", wantStatus: http.StatusOK},
		{name: "invalid-800", version: "8.0", wantStatus: http.StatusBadRequest},
		{name: "invalid-open", version: "OpenSearch_1.0", wantStatus: http.StatusBadRequest},
		{name: "invalid-str", version: "not-a-ver", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"DomainName": fmt.Sprintf("ver-domain-%02d", i),
			}
			if tt.version != "" {
				body["ElasticsearchVersion"] = tt.version
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

// TestElasticsearchHandler_ESVersionBackend verifies the backend directly
// rejects invalid ES versions.
func TestElasticsearchHandler_ESVersionBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "ver-dom", ElasticsearchVersion: "8.0"},
	)
	require.ErrorIs(t, err, elasticsearch.ErrValidation)

	_, err = b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "ver-dom2", ElasticsearchVersion: "7.10"},
	)
	require.NoError(t, err)
}

// TestElasticsearchHandler_DescribeDomainsMaxFive verifies
// DescribeElasticsearchDomains enforces max 5 names.
func TestElasticsearchHandler_DescribeDomainsMaxFive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		count      int
		wantStatus int
	}{
		{name: "five-ok", count: 5, wantStatus: http.StatusOK},
		{name: "six-rejected", count: 6, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			names := make([]string, tt.count)
			for i := range names {
				names[i] = fmt.Sprintf("desc-max-%02d", i)
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
				"DomainNames": names,
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
