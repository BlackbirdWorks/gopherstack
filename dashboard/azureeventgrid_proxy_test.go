package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

const (
	egBase  = "/dashboard/api/v1/azure/eventgrid"
	ehBase  = "/dashboard/api/v1/azure/eventhubs"
	egStore = "/subscriptions/s/resourceGroups/r/providers/Microsoft.Storage/storageAccounts/acct"
)

func newEventGridBackend(t *testing.T) *azureeventgrid.InMemoryBackend {
	t.Helper()

	b := azureeventgrid.NewInMemoryBackend()
	_, err := b.PutTopic("topic1", "/subscriptions/s/topics/topic1", "")
	require.NoError(t, err)
	_, err = b.PutTopic("ce1", "/subscriptions/s/topics/ce1", azureeventgrid.SchemaCloudEvent)
	require.NoError(t, err)
	_, err = b.PutDomain("dom1", "/subscriptions/s/domains/dom1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.PutDomainTopic("dom1", "orders")
	require.NoError(t, err)
	require.NoError(t, b.PutNamespace("ns1"))
	_, err = b.PutNamespaceTopic("ns1", "nt1")
	require.NoError(t, err)
	_, err = b.PutPullSubscription("ns1", "nt1", "pull1", map[string]any{})
	require.NoError(t, err)

	return b
}

func eventGridHandler(t *testing.T, b *azureeventgrid.InMemoryBackend) *dashboard.DashboardHandler {
	t.Helper()

	return dashboard.NewHandler(
		dashboard.Config{AzureEventGridOps: azureeventgrid.NewHandler(b, azureeventgrid.Settings{})},
	)
}

func doProxy(t *testing.T, h *dashboard.DashboardHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	return rec
}

func TestAzureEventGridOverview(t *testing.T) {
	t.Parallel()

	b := newEventGridBackend(t)
	_, err := b.PutSubscription(t.Context(), "topic1", "sub1", map[string]any{
		"destination": map[string]any{
			"endpointType": "StorageQueue",
			"properties":   map[string]any{"resourceId": egStore, "queueName": "q"},
		},
		"filter": map[string]any{
			"subjectBeginsWith": "/a", "includedEventTypes": []any{"T1"},
			"advancedFilters": []any{
				map[string]any{"operatorType": "StringIn", "key": "subject", "values": []any{"x"}},
			},
		},
		"retryPolicy": map[string]any{"maxDeliveryAttempts": 7, "eventTimeToLiveInMinutes": 90},
		"deadLetterDestination": map[string]any{
			"endpointType": "StorageBlob",
			"properties":   map[string]any{"resourceId": egStore, "blobContainerName": "dead"},
		},
	})
	require.NoError(t, err)

	rec := doProxy(t, eventGridHandler(t, b), http.MethodGet, egBase+"/overview", "")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	for _, want := range []string{
		`"name":"topic1"`, `"name":"dom1"`, `"name":"orders"`, `"name":"ns1"`, `"name":"nt1"`, `"name":"pull1"`,
		`"endpointType":"StorageQueue"`, `"subjectBeginsWith":"/a"`, `"advancedFilterCount":1`,
		`"maxDeliveryAttempts":7`, `"eventTimeToLiveMinutes":90`, `"deadLetterContainer":"dead"`,
		`"endpointType":"Queue (pull)"`,
	} {
		assert.Contains(t, body, want)
	}

	assert.NotContains(t, body, egStore)
	assert.NotContains(t, strings.ToLower(body), "key1")
}

func TestAzureEventGridRevealKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantCode int
	}{
		{name: "topic", path: "/topics/topic1/keys/reveal", wantCode: http.StatusOK},
		{name: "domain", path: "/domains/dom1/keys/reveal", wantCode: http.StatusOK},
		{name: "namespace", path: "/namespaces/ns1/keys/reveal", wantCode: http.StatusOK},
		{name: "missing", path: "/topics/nope/keys/reveal", wantCode: http.StatusNotFound},
		{name: "unknown kind", path: "/widgets/x/keys/reveal", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doProxy(t, eventGridHandler(t, newEventGridBackend(t)), http.MethodPost, egBase+tt.path, "")
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
				assert.Contains(t, rec.Body.String(), `"key1":"`)
			}
		})
	}

	t.Run("get is not routed", func(t *testing.T) {
		t.Parallel()

		rec := doProxy(
			t,
			eventGridHandler(t, newEventGridBackend(t)),
			http.MethodGet,
			egBase+"/topics/topic1/keys/reveal",
			"",
		)
		assert.NotEqual(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "key1")
	})
}

func TestAzureEventGridPublishRebuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "topic",
			path:     "/topics/topic1/events",
			body:     `{"eventType":"T","subject":"/s","data":"{\"a\":1}"}`,
			wantCode: 200,
			wantBody: `"published":1`,
		},
		{
			name:     "cloud topic",
			path:     "/topics/ce1/events",
			body:     `{"eventType":"T","data":"plain"}`,
			wantCode: 200,
			wantBody: `"published":1`,
		},
		{
			name:     "domain",
			path:     "/domains/dom1/events",
			body:     `{"eventType":"T","domainTopic":"orders"}`,
			wantCode: 200,
			wantBody: `"published":1`,
		},
		{
			name:     "domain needs topic",
			path:     "/domains/dom1/events",
			body:     `{"eventType":"T"}`,
			wantCode: 400,
			wantBody: "domainTopic is required",
		},
		{
			name:     "namespace",
			path:     "/namespaces/ns1/topics/nt1/events",
			body:     `{"eventType":"T"}`,
			wantCode: 200,
			wantBody: `"published":1`,
		},
		{
			name:     "missing ns topic",
			path:     "/namespaces/ns1/topics/nope/events",
			body:     `{"eventType":"T"}`,
			wantCode: 404,
			wantBody: "NOT_FOUND",
		},
		{
			name:     "missing topic",
			path:     "/topics/nope/events",
			body:     `{"eventType":"T"}`,
			wantCode: 404,
			wantBody: "NOT_FOUND",
		},
		{
			name:     "no event type",
			path:     "/topics/topic1/events",
			body:     `{"subject":"/s"}`,
			wantCode: 400,
			wantBody: "eventType is required",
		},
		{
			name:     "long subject",
			path:     "/topics/topic1/events",
			body:     `{"eventType":"T","subject":"` + strings.Repeat("a", 300) + `"}`,
			wantCode: 400,
			wantBody: "subject",
		},
		{
			name:     "oversized data",
			path:     "/topics/topic1/events",
			body:     `{"eventType":"T","data":"` + strings.Repeat("a", 70<<10) + `"}`,
			wantCode: 400,
			wantBody: "data is at most",
		},
		{
			name:     "oversized body",
			path:     "/topics/topic1/events",
			body:     `{"eventType":"T","data":"` + strings.Repeat("a", 200<<10) + `"}`,
			wantCode: 400,
		},
		{name: "bad json", path: "/topics/topic1/events", body: `{`, wantCode: 400, wantBody: "INVALID_ARGUMENT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doProxy(t, eventGridHandler(t, newEventGridBackend(t)), http.MethodPost, egBase+tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}

	t.Run("client schema fields ignored", func(t *testing.T) {
		t.Parallel()

		rec := doProxy(t, eventGridHandler(t, newEventGridBackend(t)), http.MethodPost, egBase+"/topics/topic1/events",
			`{"eventType":"T","id":"evil","topic":"/other","specversion":"1.0"}`)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestAzureEventGridPublishAllowList(t *testing.T) {
	t.Parallel()

	h := eventGridHandler(t, newEventGridBackend(t))

	for _, tt := range []struct{ method, path string }{
		{http.MethodDelete, "/topics/topic1"},
		{http.MethodPut, "/topics/topic1"},
		{http.MethodGet, "/topics/topic1/events"},
	} {
		rec := doProxy(t, h, tt.method, egBase+tt.path, "")
		assert.NotEqual(t, http.StatusOK, rec.Code, tt.method+" "+tt.path)
	}
}

func TestAzureEventGridUnavailable(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, tt := range []struct{ method, path string }{
		{http.MethodGet, egBase + "/overview"},
		{http.MethodPost, egBase + "/topics/t/events"},
		{http.MethodPost, egBase + "/domains/d/events"},
		{http.MethodPost, egBase + "/namespaces/n/topics/t/events"},
		{http.MethodPost, egBase + "/topics/t/keys/reveal"},
		{http.MethodGet, ehBase + "/namespaces/n/policies"},
		{http.MethodPost, ehBase + "/namespaces/n/policies/reveal"},
	} {
		rec := doProxy(t, h, tt.method, tt.path, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, tt.path)
	}
}

func TestAzureEventHubsPolicies(t *testing.T) {
	t.Parallel()

	b := azureeventhubs.NewInMemoryBackend()
	require.NoError(t, b.EnsureNamespace("ns1"))
	_, err := b.CreateHub(azureeventhubs.HubRef{Namespace: "ns1", Hub: "hub1"},
		azureeventhubs.HubConfig{PartitionCount: 1, Retention: azureeventhubs.DefaultRetention})
	require.NoError(t, err)
	rule, err := b.PutRule("ns1", "hub1", "sender", []string{azureeventhubs.RightSend})
	require.NoError(t, err)

	h := dashboard.NewHandler(dashboard.Config{AzureEventHubsOps: azureeventhubs.NewHandler(b)})

	list := doProxy(t, h, http.MethodGet, ehBase+"/namespaces/ns1/policies", "")
	require.Equal(t, http.StatusOK, list.Code)
	assert.Contains(t, list.Body.String(), `"name":"sender"`)
	assert.Contains(t, list.Body.String(), `"hub":"hub1"`)
	assert.Contains(t, list.Body.String(), `"name":"`+azureeventhubs.DefaultKeyName+`"`)
	assert.NotContains(t, list.Body.String(), rule.PrimaryKey)
	assert.NotContains(t, list.Body.String(), azureeventhubs.DefaultKeyValue)

	tests := []struct {
		name     string
		path     string
		body     string
		wantCode int
	}{
		{
			name:     "hub rule",
			path:     "/namespaces/ns1/policies/reveal",
			body:     `{"hub":"hub1","name":"sender"}`,
			wantCode: 200,
		},
		{
			name:     "root rule",
			path:     "/namespaces/ns1/policies/reveal",
			body:     `{"name":"` + azureeventhubs.DefaultKeyName + `"}`,
			wantCode: 200,
		},
		{name: "missing rule", path: "/namespaces/ns1/policies/reveal", body: `{"name":"nope"}`, wantCode: 404},
		{name: "missing ns", path: "/namespaces/zz/policies/reveal", body: `{"name":"x"}`, wantCode: 404},
		{name: "bad json", path: "/namespaces/ns1/policies/reveal", body: `{`, wantCode: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doProxy(t, h, http.MethodPost, ehBase+tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
				assert.Contains(t, rec.Body.String(), `"primaryKey":"`)
			}
		})
	}
}
