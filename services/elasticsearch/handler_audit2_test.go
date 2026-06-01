package elasticsearch_test

import (
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
		{name: "empty_key", key: "", wantStatus: http.StatusBadRequest},
		{name: "max_key_128", key: strings.Repeat("k", 128), wantStatus: http.StatusOK},
		{name: "key_too_long_129", key: strings.Repeat("k", 129), wantStatus: http.StatusBadRequest},
		{name: "valid_key", key: "env", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := createDomainAndGetARN(t, h, "tag-key-"+tt.name[:min(len(tt.name), 12)])

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
		{name: "empty_value", value: "", wantStatus: http.StatusOK},
		{name: "max_value_256", value: strings.Repeat("v", 256), wantStatus: http.StatusOK},
		{name: "value_too_long_257", value: strings.Repeat("v", 257), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := createDomainAndGetARN(t, h, "tag-val-"+tt.name[:min(len(tt.name), 11)])

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
	arn := createDomainAndGetARN(t, h, "tag-max-dom")

	// Add 50 tags — should succeed.
	tags50 := make([]map[string]string, 50)
	for i := range tags50 {
		tags50[i] = map[string]string{"Key": strings.Repeat("a", 1) + string(rune('A'+i%26)) + string(rune('0'+i%10)), "Value": "v"}
	}
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": tags50,
	})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Adding one more new tag exceeds limit — should fail.
	resp2 := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": []map[string]string{{"Key": "overflow-key", "Value": "x"}},
	})
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
}

// TestAudit2_TagUpdateExistingKeyNoLimit verifies that updating an existing tag key does not count as new.
func TestAudit2_TagUpdateExistingKeyNoLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	arn := createDomainAndGetARN(t, h, "tag-upd-dom")

	// Add 50 tags.
	tags50 := make([]map[string]string, 50)
	for i := range tags50 {
		tags50[i] = map[string]string{"Key": strings.Repeat("a", 1) + string(rune('A'+i%26)) + string(rune('0'+i%10)), "Value": "v"}
	}
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN":     arn,
		"TagList": tags50,
	})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Updating the same key does not add a new tag — should succeed.
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
		{name: "empty_defaults_to_710", version: "", wantStatus: http.StatusOK},
		{name: "valid_710", version: "7.10", wantStatus: http.StatusOK},
		{name: "valid_68", version: "6.8", wantStatus: http.StatusOK},
		{name: "valid_23", version: "2.3", wantStatus: http.StatusOK},
		{name: "invalid_800", version: "8.0", wantStatus: http.StatusBadRequest},
		{name: "invalid_opensearch", version: "OpenSearch_1.0", wantStatus: http.StatusBadRequest},
		{name: "invalid_garbage", version: "not-a-version", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"DomainName": "ver-test-" + string(rune('a'+i)),
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
		{name: "five_ok", count: 5, wantStatus: http.StatusOK},
		{name: "six_rejected", count: 6, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			names := make([]string, tt.count)
			for i := range names {
				names[i] = "desc-dom-" + string(rune('a'+i))
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
		{name: "txt_dictionary", packageType: "TXT-DICTIONARY", wantStatus: http.StatusOK},
		{name: "zip_plugin", packageType: "ZIP-PLUGIN", wantStatus: http.StatusOK},
		{name: "invalid_type", packageType: "INVALID-TYPE", wantStatus: http.StatusBadRequest},
		{name: "empty_type", packageType: "", wantStatus: http.StatusBadRequest},
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
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)

	_, err = b.CreatePackage("my-pkg2", "TXT-DICTIONARY", "desc")
	require.NoError(t, err)
}

// TestAudit2_ESVersionBackend verifies the backend directly rejects invalid ES versions.
func TestAudit2_ESVersionBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain("ver-dom", "8.0", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{})
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)

	_, err = b.CreateDomain("ver-dom2", "7.10", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{})
	require.NoError(t, err)
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
