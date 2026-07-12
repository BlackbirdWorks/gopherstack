package kafka_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestParity_CreateConfigurationResponseIncludesStateAndRevision verifies that
// CreateConfiguration returns state and latestRevision in the response body —
// fields required by real AWS MSK that the emulator previously omitted.
func TestParity_CreateConfigurationResponseIncludesStateAndRevision(t *testing.T) {
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
func TestParity_UpdateOpsRequireCurrentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		suffix   string
		wantCode int
	}{
		{
			name:     "broker_count_empty_version_rejected",
			suffix:   "/nodes/count",
			body:     map[string]any{"targetNumberOfBrokerNodes": int32(6)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_count_wrong_version_rejected",
			suffix: "/nodes/count",
			body: map[string]any{
				"currentVersion":            "WRONG_VERSION",
				"targetNumberOfBrokerNodes": int32(6),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_count_correct_version_accepted",
			suffix: "/nodes/count",
			body: map[string]any{
				"currentVersion":            kafka.DefaultClusterVersion,
				"targetNumberOfBrokerNodes": int32(6),
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "broker_type_empty_version_rejected",
			suffix:   "/nodes/type",
			body:     map[string]any{"targetInstanceType": "kafka.m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_type_correct_version_accepted",
			suffix: "/nodes/type",
			body: map[string]any{
				"currentVersion":     kafka.DefaultClusterVersion,
				"targetInstanceType": "kafka.m5.xlarge",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "connectivity_empty_version_rejected",
			suffix:   "/connectivity",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "connectivity_correct_version_accepted",
			suffix:   "/connectivity",
			body:     map[string]any{"currentVersion": kafka.DefaultClusterVersion},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterArn := paCreateCluster(t, h, "parity-version-check")
			encoded := url.PathEscape(clusterArn)

			rec := doKafkaRequest(t, h, http.MethodPut,
				"/v1/clusters/"+encoded+tt.suffix, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "suffix=%s body=%v", tt.suffix, tt.body)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "BadRequestException", errResp["__type"],
					"wrong version must produce BadRequestException")
			}
		})
	}
}

func paCreateCluster(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create cluster failed: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}
