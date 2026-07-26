package awsconfig_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_DeleteErrors_SpecificTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		del     func(b *awsconfig.InMemoryBackend) error
		name    string
	}{
		{
			name:    "delete_delivery_channel_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteDeliveryChannel("x") },
			wantErr: awsconfig.ErrNoSuchDeliveryChannel,
		},
		{
			name:    "delete_config_rule_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConfigRule("x") },
			wantErr: awsconfig.ErrNoSuchConfigRule,
		},
		{
			name:    "delete_aggregator_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConfigurationAggregator("x") },
			wantErr: awsconfig.ErrNoSuchAggregator,
		},
		{
			name:    "delete_conformance_pack_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConformancePack("x") },
			wantErr: awsconfig.ErrNoSuchConformancePack,
		},
		{
			name:    "delete_org_config_rule_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteOrganizationConfigRule("x") },
			wantErr: awsconfig.ErrNoSuchOrganizationConfigRule,
		},
		{
			name:    "delete_org_conformance_pack_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteOrganizationConformancePack("x") },
			wantErr: awsconfig.ErrNoSuchOrganizationConformancePack,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := tt.del(b)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAWSConfigHandler_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		operation    string
		wantContains string
		wantCode     int
	}{
		{
			name:         "delete_configuration_recorder_not_found_type",
			operation:    "DeleteConfigurationRecorder",
			body:         map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigurationRecorderException",
		},
		{
			name:         "delete_delivery_channel_not_found_type",
			operation:    "DeleteDeliveryChannel",
			body:         map[string]any{"DeliveryChannelName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchDeliveryChannelException",
		},
		{
			name:         "delete_config_rule_not_found_type",
			operation:    "DeleteConfigRule",
			body:         map[string]any{"ConfigRuleName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigRuleException",
		},
		{
			name:         "delete_aggregator_not_found_type",
			operation:    "DeleteConfigurationAggregator",
			body:         map[string]any{"ConfigurationAggregatorName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigurationAggregatorException",
		},
		{
			name:         "delete_conformance_pack_not_found_type",
			operation:    "DeleteConformancePack",
			body:         map[string]any{"ConformancePackName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConformancePackException",
		},
		{
			name:         "delete_org_config_rule_not_found_type",
			operation:    "DeleteOrganizationConfigRule",
			body:         map[string]any{"OrganizationConfigRuleName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchOrganizationConfigRuleException",
		},
		{
			name:         "delete_org_conformance_pack_not_found_type",
			operation:    "DeleteOrganizationConformancePack",
			body:         map[string]any{"OrganizationConformancePackName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchOrganizationConformancePackException",
		},
		{
			name:         "start_recorder_no_delivery_channel_400",
			operation:    "StartConfigurationRecorder",
			body:         map[string]any{"ConfigurationRecorderName": "default"},
			wantCode:     http.StatusBadRequest,
			wantContains: "NoAvailableDeliveryChannelException",
		},
		{
			name:         "get_connector_not_found_type",
			operation:    "GetConnector",
			body:         map[string]any{"Arn": "arn:aws:config:us-east-1:000000000000:connector/nonexistent"},
			wantCode:     http.StatusBadRequest,
			wantContains: "ResourceNotFoundException",
		},
		{
			name:         "delete_connector_not_found_type",
			operation:    "DeleteConnector",
			body:         map[string]any{"Arn": "arn:aws:config:us-east-1:000000000000:connector/nonexistent"},
			wantCode:     http.StatusBadRequest,
			wantContains: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.operation == "StartConfigurationRecorder" {
				require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000:role/r", nil))
			}

			rec := doAWSConfigRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}
