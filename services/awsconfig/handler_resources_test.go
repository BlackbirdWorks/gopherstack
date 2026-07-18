package awsconfig_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSConfigHandler_BatchGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "returns_unprocessed_identifiers",
			body: map[string]any{
				"ConfigurationAggregatorName": "my-aggregator",
				"ResourceIdentifiers": []map[string]any{
					{
						"SourceAccountId": "000000000000",
						"SourceRegion":    "us-east-1",
						"ResourceId":      "i-1234567890abcdef0",
						"ResourceType":    "AWS::EC2::Instance",
					},
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems", "UnprocessedResourceIdentifiers"},
		},
		{
			name: "empty_identifiers",
			body: map[string]any{
				"ConfigurationAggregatorName": "my-aggregator",
				"ResourceIdentifiers":         []any{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "BatchGetAggregateResourceConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_BatchGetResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "returns_unprocessed_keys",
			body: map[string]any{
				"ResourceKeys": []map[string]any{
					{
						"resourceType": "AWS::EC2::Instance",
						"resourceId":   "i-1234567890abcdef0",
					},
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems", "UnprocessedResourceKeys"},
		},
		{
			name: "empty_resource_keys",
			body: map[string]any{
				"ResourceKeys": []any{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "BatchGetResourceConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
