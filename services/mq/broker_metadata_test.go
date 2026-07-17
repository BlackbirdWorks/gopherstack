package mq_test

// DescribeBrokerEngineTypes and DescribeBrokerInstanceOptions broker
// metadata operations.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestDescribeBrokerEngineTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		engineType     string
		wantEngineType string
		wantMinCount   int
	}{
		{
			name:         "all_engine_types",
			engineType:   "",
			wantMinCount: 2,
		},
		{
			name:           "activemq_only",
			engineType:     "ACTIVEMQ",
			wantMinCount:   1,
			wantEngineType: "ACTIVEMQ",
		},
		{
			name:           "rabbitmq_only",
			engineType:     "RABBITMQ",
			wantMinCount:   1,
			wantEngineType: "RABBITMQ",
		},
		{
			name:         "unknown_engine_type",
			engineType:   "UNKNOWN",
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/v1/broker-engine-types"

			if tt.engineType != "" {
				path += "?engineType=" + tt.engineType
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseResponse(t, rec)
			types, ok := resp["brokerEngineTypes"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(types), tt.wantMinCount)

			if tt.wantEngineType != "" && len(types) > 0 {
				first := types[0].(map[string]any)
				assert.Equal(t, tt.wantEngineType, first["engineType"])
			}
		})
	}

	t.Run("all_types_include_activemq_and_rabbitmq", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/v1/broker-engine-types", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		types, ok := parseResponse(t, rec)["brokerEngineTypes"].([]any)
		require.True(t, ok)

		names := make([]string, 0, len(types))
		for _, et := range types {
			names = append(names, et.(map[string]any)["engineType"].(string))
		}

		assert.Contains(t, names, mq.EngineTypeActiveMQ)
		assert.Contains(t, names, mq.EngineTypeRabbitMQ)
	})

	t.Run("activemq_has_engine_versions", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/v1/broker-engine-types?engineType=ACTIVEMQ", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		types := parseResponse(t, rec)["brokerEngineTypes"].([]any)
		engineType := types[0].(map[string]any)
		versions, ok := engineType["engineVersions"].([]any)
		require.True(t, ok && len(versions) > 0, "ACTIVEMQ must have engine versions")

		for _, v := range versions {
			assert.NotEmpty(t, v.(map[string]any)["name"], "engine version name must not be empty")
		}
	})
}

func TestDescribeBrokerInstanceOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		engineType       string
		hostInstanceType string
		storageType      string
		wantMinCount     int
	}{
		{
			name:         "all_options",
			wantMinCount: 1,
		},
		{
			name:         "activemq_only",
			engineType:   "ACTIVEMQ",
			wantMinCount: 1,
		},
		{
			name:             "specific_instance_type",
			engineType:       "ACTIVEMQ",
			hostInstanceType: "mq.m5.large",
			wantMinCount:     1,
		},
		{
			name:         "ebs_storage_type",
			storageType:  "EBS",
			wantMinCount: 1,
		},
		{
			name:         "unknown_engine_type",
			engineType:   "UNKNOWN",
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/v1/broker-instance-options"

			var params []string

			if tt.engineType != "" {
				params = append(params, "engineType="+tt.engineType)
			}

			if tt.hostInstanceType != "" {
				params = append(params, "hostInstanceType="+tt.hostInstanceType)
			}

			if tt.storageType != "" {
				params = append(params, "storageType="+tt.storageType)
			}

			if len(params) > 0 {
				path += "?" + strings.Join(params, "&")
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			opts, ok := parseResponse(t, rec)["brokerInstanceOptions"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(opts), tt.wantMinCount)
		})
	}

	t.Run("activemq_options_have_required_fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/v1/broker-instance-options?engineType=ACTIVEMQ", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		opts, ok := parseResponse(t, rec)["brokerInstanceOptions"].([]any)
		require.True(t, ok && len(opts) > 0, "must return ActiveMQ instance options")

		opt := opts[0].(map[string]any)
		assert.Equal(t, mq.EngineTypeActiveMQ, opt["engineType"])
		assert.NotEmpty(t, opt["hostInstanceType"])
		assert.NotEmpty(t, opt["storageType"])
		assert.NotEmpty(t, opt["availabilityZones"])
	})

	t.Run("all_engine_types_present", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/v1/broker-instance-options", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		opts, ok := parseResponse(t, rec)["brokerInstanceOptions"].([]any)
		require.True(t, ok)
		assert.NotEmpty(t, opts, "broker instance options must not be empty")

		engineTypes := make(map[string]bool)
		for _, o := range opts {
			om := o.(map[string]any)
			engineTypes[om["engineType"].(string)] = true
		}

		assert.True(t, engineTypes[mq.EngineTypeActiveMQ], "must have ActiveMQ instance options")
		assert.True(t, engineTypes[mq.EngineTypeRabbitMQ], "must have RabbitMQ instance options")
	})

	t.Run("filter_by_host_instance_type", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/v1/broker-instance-options?hostInstanceType=mq.m5.large", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		opts, ok := parseResponse(t, rec)["brokerInstanceOptions"].([]any)
		require.True(t, ok)

		for _, o := range opts {
			assert.Equal(t, "mq.m5.large", o.(map[string]any)["hostInstanceType"])
		}
	})
}
