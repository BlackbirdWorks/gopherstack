package main

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespacedTargetRouting(t *testing.T) {
	t.Parallel()

	base := newWiredSDKConfig(t, CLI{})
	require.NotNil(t, base.BaseEndpoint)

	tests := []struct {
		name    string
		sigName string
		target  string
	}{
		{"cloudtrail_bare", "cloudtrail", "CloudTrail_20131101.DescribeTrails"},
		{"cloudtrail_botocore", "cloudtrail", "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails"},
		{"codeconnections_bare", "codeconnections", "CodeConnections_20231201.ListConnections"},
		{
			"codeconnections_botocore", "codeconnections",
			"com.amazonaws.codeconnections.CodeConnections_20231201.ListConnections",
		},
		{"codestar_bare", "codestar-connections", "CodeStar_connections_20191201.ListConnections"},
		{
			"codestar_botocore", "codestar-connections",
			"com.amazonaws.codestar.connections.CodeStar_connections_20191201.ListConnections",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(
				t.Context(), http.MethodPost, *base.BaseEndpoint+"/", strings.NewReader("{}"),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", tt.target)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20261001/us-east-1/"+tt.sigName+"/aws4_request, SignedHeaders=host, Signature=x",
			)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}
