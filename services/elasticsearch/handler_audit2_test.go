package elasticsearch_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestAudit2_TagKeyLength verifies tag key length constraints (1-128 chars).
func TestAudit2_TagKeyLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{name: "empty-key", key: "", wantStatus: http.StatusBadRequest},
		{name: "max-key", key: strings.Repeat("k", 128), wantStatus: http.StatusOK},
		{name: "key-too-long", key: strings.Repeat("k", 129), wantStatus: http.StatusBadRequest},
		{name: "valid-key", key: "env", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := createDomainAndGetARN(t, h, "a2-tagkey-"+tt.name[:3])

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
				"ARN":     arn,
				"TagList": []map[string]string{{"Key": tt.key, "Value": "val"}},
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

// TestAudit2_TagValueLength verifies tag value length constraint (0-256 chars).
func TestAudit2_TagValueLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      string
		wantStatus int
	}{
		{name: "empty-val", value: "", wantStatus: http.StatusOK},
		{name: "max-val", value: strings.Repeat("v", 256), wantStatus: http.StatusOK},
		{name: "val-too-long", value: strings.Repeat("v", 257), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := createDomainAndGetARN(t, h, "a2-tagval-"+tt.name[:3])

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
				"ARN":     arn,
				"TagList": []map[string]string{{"Key": "env", "Value": tt.value}},
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

// TestAudit2_TagMaxPerResource verifies max 50 tags per resource.
func TestAudit2_TagMaxPerResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	arn := createDomainAndGetARN(t, h, "a2-tagmax-dom")

	tags50 := make([]map[string]string, 50)
	for i := range tags50 {
		tags50[i] = map[string]string{"Key": fmt.Sprintf("tag-key-%02d", i), "Value": "v"}
	}

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": tags50,
	})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Adding one new tag exceeds the 50-tag limit.
	resp2 := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": []map[string]string{{"Key": "overflow-key", "Value": "x"}},
	})
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
}

// TestAudit2_TagUpdateExistingKeyNoLimit verifies updating an existing tag key does not count as new.
func TestAudit2_TagUpdateExistingKeyNoLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	arn := createDomainAndGetARN(t, h, "a2-tagupd-dom")

	tags50 := make([]map[string]string, 50)
	for i := range tags50 {
		tags50[i] = map[string]string{"Key": fmt.Sprintf("tag-key-%02d", i), "Value": "v"}
	}

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": tags50,
	})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Updating an existing key does not add a new tag — should succeed.
	resp2 := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": []map[string]string{{"Key": tags50[0]["Key"], "Value": "new-val"}},
	})
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

// TestAudit2_ElasticsearchVersionValidation verifies version string validation on CreateDomain.
func TestAudit2_ElasticsearchVersionValidation(t *testing.T) {
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
				"DomainName": fmt.Sprintf("a2-ver-%02d", i),
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

// TestAudit2_DescribeDomainsMaxFive verifies DescribeElasticsearchDomains enforces max 5 names.
func TestAudit2_DescribeDomainsMaxFive(t *testing.T) {
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
				names[i] = fmt.Sprintf("a2-desc-%02d", i)
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
				"DomainNames": names,
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

// TestAudit2_PackageTypeValidation verifies CreatePackage rejects unknown PackageType values.
func TestAudit2_PackageTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		packageType string
		wantStatus  int
	}{
		{name: "txt-dictionary", packageType: "TXT-DICTIONARY", wantStatus: http.StatusOK},
		{name: "zip-plugin", packageType: "ZIP-PLUGIN", wantStatus: http.StatusOK},
		{name: "invalid-type", packageType: "INVALID-TYPE", wantStatus: http.StatusBadRequest},
		{name: "empty-type", packageType: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages", map[string]any{
				"PackageName":        "pkg-" + tt.name,
				"PackageType":        tt.packageType,
				"PackageSource":      map[string]any{"S3BucketName": "my-bucket", "S3Key": "file.txt"},
				"PackageDescription": "test package",
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

// TestAudit2_PackageTypeBackend verifies the backend directly rejects invalid package types.
func TestAudit2_PackageTypeBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreatePackage("my-pkg", "UNKNOWN", "desc")
	require.ErrorIs(t, err, elasticsearch.ErrValidation)

	_, err = b.CreatePackage("my-pkg2", "TXT-DICTIONARY", "desc")
	require.NoError(t, err)
}

// TestAudit2_ESVersionBackend verifies the backend directly rejects invalid ES versions.
func TestAudit2_ESVersionBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain("ver-dom", "8.0", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{})
	require.ErrorIs(t, err, elasticsearch.ErrValidation)

	_, err = b.CreateDomain("ver-dom2", "7.10", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{})
	require.NoError(t, err)
}
