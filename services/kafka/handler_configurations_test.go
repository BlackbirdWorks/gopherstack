package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestKafka_CreateAndDescribeConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		confName   string
		wantStatus int
	}{
		{
			name:       "success",
			confName:   "my-config",
			wantStatus: http.StatusOK,
		},
		{
			name:       "duplicate",
			confName:   "my-config",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"name":             "my-config",
				"kafkaVersions":    []string{"2.8.0"},
				"serverProperties": "auto.create.topics.enable=false",
			}

			if tt.name == "duplicate" {
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", body)
			}

			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-config", resp["name"])
				assert.NotEmpty(t, resp["arn"])
			}
		})
	}
}

func TestKafka_ListConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*kafka.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *kafka.Handler) {},
			wantCount: 0,
		},
		{
			name: "with_configurations",
			setup: func(h *kafka.Handler) {
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
					"name": "config-a", "kafkaVersions": []string{"2.8.0"}, "serverProperties": "",
				})
				doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
					"name": "config-b", "kafkaVersions": []string{"2.8.0"}, "serverProperties": "",
				})
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/configurations", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs, ok := resp["configurations"].([]any)
			require.True(t, ok)
			assert.Len(t, configs, tt.wantCount)
		})
	}
}

// ----------------------------------------
// Tag handler tests
// ----------------------------------------

func TestKafka_DescribeAndDeleteConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		useRealArn bool
		wantStatus int
	}{
		{name: "describe_success", op: "describe", useRealArn: true, wantStatus: http.StatusOK},
		{name: "describe_not_found", op: "describe", useRealArn: false, wantStatus: http.StatusNotFound},
		{name: "delete_success", op: "delete", useRealArn: true, wantStatus: http.StatusOK},
		{name: "delete_not_found", op: "delete", useRealArn: false, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
				"name":             "cfg-test",
				"kafkaVersions":    []string{"2.8.0"},
				"serverProperties": "auto.create.topics.enable=false",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			var cfgArn string
			if tt.useRealArn {
				cfgArn = createResp["arn"].(string)
			} else {
				cfgArn = "arn:aws:kafka:us-east-1:000000000000:configuration/nonexistent/bad-uuid"
			}

			encodedArn := url.PathEscape(cfgArn)

			var rec *httptest.ResponseRecorder
			switch tt.op {
			case "describe":
				rec = doKafkaRequest(t, h, http.MethodGet, "/v1/configurations/"+encodedArn, nil)
			default:
				rec = doKafkaRequest(t, h, http.MethodDelete, "/v1/configurations/"+encodedArn, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateConfigurationResponseIncludesStateAndRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantState   string
		wantRevison float64
	}{
		{
			name:        "basic_config",
			wantRevison: 1,
			wantState:   "ACTIVE",
			body: map[string]any{
				"name":             "my-cfg",
				"kafkaVersions":    []string{"2.8.0"},
				"serverProperties": "auto.create.topics.enable=false",
			},
		},
		{
			name:        "config_with_description",
			wantRevison: 1,
			wantState:   "ACTIVE",
			body: map[string]any{
				"name":             "described-cfg",
				"description":      "my description",
				"kafkaVersions":    []string{"3.5.1"},
				"serverProperties": "log.retention.hours=168",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotEmpty(t, resp["arn"], "arn must be present")
			assert.NotEmpty(t, resp["name"], "name must be present")
			assert.Equal(t, tt.wantState, resp["state"], "state must match ACTIVE")

			rev, ok := resp["latestRevision"].(map[string]any)
			require.True(t, ok, "latestRevision must be an object")
			assert.InDelta(t, tt.wantRevison, rev["revision"], 0,
				"latestRevision.revision must be 1")
		})
	}
}

// TestParity_UpdateOpsRequireCurrentVersion verifies that cluster update
// operations reject requests where currentVersion is empty or does not match
// the cluster's recorded CurrentVersion, mirroring AWS MSK's optimistic-lock
// guard. Without this, a stale client can silently overwrite concurrent changes.

func TestListConfigurationsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		totalConfs  int
		maxResults  int
		wantPage1   int
		wantHasMore bool
	}{
		{
			name:        "all_fit",
			totalConfs:  2,
			maxResults:  5,
			wantPage1:   2,
			wantHasMore: false,
		},
		{
			name:        "two_pages",
			totalConfs:  4,
			maxResults:  2,
			wantPage1:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.totalConfs {
				b.AddConfigurationInternal(fmt.Sprintf("cfg-%02d", i))
			}

			path := fmt.Sprintf("/v1/configurations?maxResults=%d", tt.maxResults)
			rec := doKafkaRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs := resp["configurations"].([]any)
			assert.Len(t, configs, tt.wantPage1)

			nextToken, _ := resp["nextToken"].(string)
			if tt.wantHasMore {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

// ----------------------------------------
// Pagination: ListReplicators
// ----------------------------------------

func TestConfigRevision_DescribeAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configArn := createTestConfig(t, h, "cfg-revision-test")
	encoded := url.PathEscape(configArn)

	// DescribeConfigurationRevision for revision 1.
	descRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/configurations/"+encoded+"/revisions/1", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.InDelta(t, float64(1), descResp["revision"], 0)

	// ListConfigurationRevisions.
	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/configurations/"+encoded+"/revisions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	revisions, _ := listResp["revisions"].([]any)
	assert.NotEmpty(t, revisions, "should have at least one revision")
}

func TestConfigRevision_UpdateConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configArn := createTestConfig(t, h, "cfg-update-test")
	encoded := url.PathEscape(configArn)

	// UpdateConfiguration — new description and server properties.
	updateRec := doKafkaRequest(t, h, http.MethodPut, "/v1/configurations/"+encoded, map[string]any{
		"description":      "updated config",
		"serverProperties": "auto.create.topics.enable=true",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, configArn, updateResp["arn"])
	assert.Equal(t, "cfg-update-test", updateResp["name"])
}

func TestConfigRevision_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, code := doKafkaRequestJSON(
		t,
		h,
		http.MethodGet,
		"/v1/configurations/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Aconfiguration%2Fmissing%2F1/revisions/1",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Replicator UpdateReplicationInfo
// ----------------------------------------

func TestConfigurationRevisions(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)
	configArn := createTestConfig(t, h, "cfg-revisions")

	_ = configArn
	// Use backend directly for revision tests
	_, be2 := newTestHandlerWithBackend(t)

	cfg, err := be2.CreateConfiguration(
		context.Background(),
		"cfg2",
		"",
		[]string{"2.8.0"},
		"auto.create.topics.enable=false",
	)
	require.NoError(t, err)

	// DescribeConfigurationRevision
	revision, err := be2.DescribeConfigurationRevision(context.Background(), cfg.Arn, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), revision.Revision)

	// ListConfigurationRevisions
	revs, err := be2.ListConfigurationRevisions(context.Background(), cfg.Arn)
	require.NoError(t, err)
	assert.NotEmpty(t, revs)

	// UpdateConfiguration (description, serverProperties)
	updated, err := be2.UpdateConfiguration(context.Background(), cfg.Arn, "v2 desc", "auto.create.topics.enable=true")
	require.NoError(t, err)
	assert.NotEmpty(t, updated.Arn)
}
