package elasticsearch_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearchHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, h *elasticsearch.Handler)
		name                 string
		domainName           string
		elasticsearchVersion string
		wantContains         []string
		wantCode             int
	}{
		{
			name:                 "success",
			domainName:           "test-domain",
			elasticsearchVersion: "7.10",
			wantCode:             http.StatusOK,
			wantContains:         []string{"test-domain", "7.10", "ARN", "Endpoint", "CognitoOptions"},
		},
		{
			name:       "already_exists",
			domainName: "my-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
					map[string]any{"DomainName": "my-domain"},
				)
				resp.Body.Close()
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "no_name",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_json",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			if tt.setup != nil {
				tt.setup(t, h)
			}

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(http.MethodPost, "/2015-01-01/es/domain", strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				assert.Equal(t, tt.wantCode, rw.Code)

				return
			}

			body := map[string]any{}
			if tt.domainName != "" {
				body["DomainName"] = tt.domainName
			}

			if tt.elasticsearchVersion != "" {
				body["ElasticsearchVersion"] = tt.elasticsearchVersion
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestElasticsearchHandler_DescribeDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elasticsearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
					map[string]any{"DomainName": "my-domain"},
				)
				resp.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-domain", "CognitoOptions"},
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			if tt.setup != nil {
				tt.setup(t, h)
			}

			resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestElasticsearchHandler_DeleteDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *elasticsearch.Handler)
		domainName string
		wantCode   int
	}{
		{
			name:       "success",
			domainName: "to-delete",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				r := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
					map[string]any{"DomainName": "to-delete"},
				)
				r.Body.Close()
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			if tt.setup != nil {
				tt.setup(t, h)
			}

			resp := doRequest(t, h, http.MethodDelete, "/2015-01-01/es/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusOK {
				resp2 := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+tt.domainName, nil)
				defer resp2.Body.Close()
				assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
			}
		})
	}
}

func TestElasticsearchHandler_ListDomainNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"alpha", "beta"} {
		r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
			"DomainName": name,
		})
		r.Body.Close()
	}

	// ListDomainNames is served at the distinct "/2015-01-01/domain" resource
	// (no "es/" segment), per the real aws-sdk-go-v2 elasticsearchservice
	// serializer -- it is NOT the same path as CreateElasticsearchDomain.
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	names, ok := out["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

// TestElasticsearchHandler_DomainCollectionRouteMatrix drives the full
// create/describe/list/update/delete flow through h.ServeHTTP (the same
// dispatch path RouteMatcher-selected requests hit in production) to pin the
// method+path matrix on the "/2015-01-01/es/domain" collection resource.
// Regression coverage for a routing change that made "GET /2015-01-01/es/domain"
// (bare, no domain name segment) 404 instead of reaching ListDomainNames while
// "POST /2015-01-01/es/domain" (create) and "GET .../es/domain/{name}"
// (describe) kept working -- see buildOps and extractRootDomainOperation.
func TestElasticsearchHandler_DomainCollectionRouteMatrix(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainName := "route-matrix-domain"

	steps := []struct {
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "create_domain",
			method:   http.MethodPost,
			path:     "/2015-01-01/es/domain",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				status, ok := body["DomainStatus"].(map[string]any)
				require.True(t, ok, "expected DomainStatus in create response")
				assert.Equal(t, domainName, status["DomainName"])
			},
		},
		{
			name:     "describe_domain",
			method:   http.MethodGet,
			path:     "/2015-01-01/es/domain/" + domainName,
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				status, ok := body["DomainStatus"].(map[string]any)
				require.True(t, ok, "expected DomainStatus in describe response")
				assert.Equal(t, domainName, status["DomainName"])
			},
		},
		{
			name:     "list_domain_names_bare_collection_path",
			method:   http.MethodGet,
			path:     "/2015-01-01/es/domain",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				assert.True(t, domainListContains(body, domainName))
			},
		},
		{
			name:     "list_domain_names_real_aws_path",
			method:   http.MethodGet,
			path:     "/2015-01-01/domain",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				assert.True(t, domainListContains(body, domainName))
			},
		},
		{
			name:     "update_domain_config",
			method:   http.MethodPost,
			path:     "/2015-01-01/es/domain/" + domainName + "/config",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				assert.NotNil(t, body["DomainConfig"])
			},
		},
		{
			name:     "delete_domain",
			method:   http.MethodDelete,
			path:     "/2015-01-01/es/domain/" + domainName,
			wantCode: http.StatusOK,
		},
		{
			name:     "describe_after_delete_not_found",
			method:   http.MethodGet,
			path:     "/2015-01-01/es/domain/" + domainName,
			wantCode: http.StatusNotFound,
		},
	}

	for _, step := range steps {
		var body any
		if step.method == http.MethodPost && step.path == "/2015-01-01/es/domain" {
			body = map[string]any{
				"DomainName":           domainName,
				"ElasticsearchVersion": "7.10",
				"ElasticsearchClusterConfig": map[string]any{
					"InstanceType":  "t3.small.elasticsearch",
					"InstanceCount": 1,
				},
			}
		} else if step.method == http.MethodPost && strings.HasSuffix(step.path, "/config") {
			body = map[string]any{
				"ElasticsearchClusterConfig": map[string]any{
					"InstanceType":  "r5.large.elasticsearch",
					"InstanceCount": 2,
				},
			}
		}

		resp := doRequest(t, h, step.method, step.path, body)
		require.Equalf(t, step.wantCode, resp.StatusCode, "step %q", step.name)

		var out map[string]any
		if resp.Header.Get("Content-Type") != "" {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		}

		resp.Body.Close()

		if step.check != nil {
			step.check(t, out)
		}
	}
}

// domainListContains reports whether a ListDomainNames JSON body contains domainName.
func domainListContains(body map[string]any, domainName string) bool {
	names, ok := body["DomainNames"].([]any)
	if !ok {
		return false
	}

	for _, entry := range names {
		e, entryOK := entry.(map[string]any)
		if entryOK && e["DomainName"] == domainName {
			return true
		}
	}

	return false
}

func TestElasticsearchHandler_DescribeElasticsearchDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainNames []string
		query       []string
		wantCount   int
		wantCode    int
	}{
		{
			name:        "multiple_domains",
			domainNames: []string{"domain-a", "domain-b"},
			query:       []string{"domain-a", "domain-b"},
			wantCount:   2,
			wantCode:    http.StatusOK,
		},
		{
			name:        "nonexistent_filtered",
			domainNames: []string{"existing"},
			query:       []string{"existing", "missing"},
			wantCount:   1,
			wantCode:    http.StatusOK,
		},
		{
			name:      "empty_list",
			query:     []string{},
			wantCount: 0,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			for _, name := range tt.domainNames {
				r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
					"DomainName": name,
				})
				r.Body.Close()
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
				"DomainNames": tt.query,
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			list, ok := out["DomainStatusList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestElasticsearchHandler_CreateDomain_WithEBSOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":           "ebs-domain",
		"ElasticsearchVersion": "7.10",
		"ElasticsearchClusterConfig": map[string]any{
			"InstanceType":  "r5.large.elasticsearch",
			"InstanceCount": 3,
		},
		"EBSOptions": map[string]any{
			"EBSEnabled": true,
			"VolumeSize": 20,
			"VolumeType": "gp2",
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "r5.large.elasticsearch")
	assert.Contains(t, string(bodyBytes), "ebs-domain")
}

// mockDNSRegistrar is a test double for elasticsearch.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}

func TestElasticsearchHandler_DNSRegistrar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		domainName     string
		wantRegistered bool
		deleteAfter    bool
	}{
		{
			name:           "registers_on_create",
			domainName:     "my-domain",
			wantRegistered: true,
		},
		{
			name:           "deregisters_on_delete",
			domainName:     "del-domain",
			deleteAfter:    true,
			wantRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registrar := &mockDNSRegistrar{registered: make(map[string]bool)}
			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetDNSRegistrar(registrar)

			domain, err := b.CreateDomain(
				context.Background(), elasticsearch.CreateDomainInput{Name: tt.domainName},
			)
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteDomain(context.Background(), tt.domainName)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[domain.Endpoint])
		})
	}
}

// TestElasticsearchHandler_SortedListDomainNames verifies that ListDomainNames
// returns domains in sorted order.
func TestElasticsearchHandler_SortedListDomainNames(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	for _, name := range []string{"zoo-domain", "apple-dom", "mid-domain"} {
		_, err := b.CreateDomain(
			context.Background(), elasticsearch.CreateDomainInput{Name: name},
		)
		require.NoError(t, err)
	}

	names := b.ListDomainNames(context.Background())
	require.Len(t, names, 3)
	assert.Equal(t, "apple-dom", names[0])
	assert.Equal(t, "mid-domain", names[1])
	assert.Equal(t, "zoo-domain", names[2])
}

// TestElasticsearchHandler_AddDomainInternal verifies the seed helper adds a
// domain directly.
func TestElasticsearchHandler_AddDomainInternal(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal(context.Background(), elasticsearch.Domain{
		Name:                 "seed-domain",
		ARN:                  "arn:aws:es:us-east-1:123456789012:domain/seed-domain",
		ElasticsearchVersion: "7.10",
		Status:               "Active",
	})

	assert.Equal(t, 1, b.DomainCount())

	d, err := b.DescribeDomain(context.Background(), "seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "seed-domain", d.Name)
	assert.Equal(t, "7.10", d.ElasticsearchVersion)
}

// TestElasticsearchHandler_DescribeDomainDeepCopy verifies DescribeDomain
// returns an independent copy.
func TestElasticsearchHandler_DescribeDomainDeepCopy(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "copy-domain", ElasticsearchVersion: "7.10"},
	)
	require.NoError(t, err)

	d1, err := b.DescribeDomain(context.Background(), "copy-domain")
	require.NoError(t, err)

	d2, err := b.DescribeDomain(context.Background(), "copy-domain")
	require.NoError(t, err)

	// Both copies are independent; modifying one doesn't affect the other or the stored domain.
	d1.ElasticsearchVersion = "8.0"
	assert.Equal(t, "7.10", d2.ElasticsearchVersion)
}

// TestElasticsearchHandler_NonNilEmptyDomainList verifies ListDomainNames
// returns [] not null in JSON.
func TestElasticsearchHandler_NonNilEmptyDomainList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/domain", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	domainNames, ok := out["DomainNames"]
	require.True(t, ok)
	require.NotNil(t, domainNames)

	list, ok := domainNames.([]any)
	require.True(t, ok)
	assert.Empty(t, list)
}
