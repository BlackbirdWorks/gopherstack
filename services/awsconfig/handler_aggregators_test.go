package awsconfig_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestAggregationAuthorizationARN verifies PutAggregationAuthorization generates an ARN.
func TestAggregationAuthorizationARN(t *testing.T) {
	t.Parallel()

	b := newTestAWSConfigHandler(t).Backend
	require.NoError(t, b.PutAggregationAuthorization("999999999999", "eu-west-1"))

	auths := b.DescribeAggregationAuthorizations()
	require.Len(t, auths, 1)
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "arn:aws:config:")
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "999999999999")
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "eu-west-1")
}

// TestAggregatorARNAndSources verifies PutConfigurationAggregator generates an ARN.
func TestAggregatorARNAndSources(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutConfigurationAggregator", map[string]any{
		"ConfigurationAggregatorName": "my-agg",
		"AccountAggregationSources": []map[string]any{
			{"AccountIds": []string{"111111111111"}, "AllAwsRegions": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeConfigurationAggregators", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ConfigurationAggregatorArn")
	assert.Contains(t, body, "arn:aws:config:")
}

func TestAWSConfigHandler_DeleteAggregationAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutAggregationAuthorization("123456789012", "us-east-1"))
			},
			body: map[string]any{
				"AuthorizedAccountId": "123456789012",
				"AuthorizedAwsRegion": "us-east-1",
			},
			wantCode: http.StatusOK,
		},
		{
			// Real AWS Config's DeleteAggregationAuthorization is idempotent, so
			// deleting a nonexistent authorization also returns 200.
			name: "not_found_is_idempotent",
			body: map[string]any{
				"AuthorizedAccountId": "999999999999",
				"AuthorizedAwsRegion": "us-west-2",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteAggregationAuthorization", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteConfigurationAggregator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigurationAggregator("my-aggregator", nil, nil))
			},
			body:     map[string]any{"ConfigurationAggregatorName": "my-aggregator"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationAggregatorName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConfigurationAggregator", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
