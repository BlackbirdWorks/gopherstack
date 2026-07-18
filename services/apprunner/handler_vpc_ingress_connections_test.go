package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVpcIngressConnectionCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "create returns ARN",
			action: "CreateVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionName": "my-vic",
				"ServiceArn":               svcArn,
				"IngressVpcConfiguration": map[string]any{
					"VpcId":         "vpc-aaa",
					"VpcEndpointId": "vpce-bbb",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Contains(t, vic["VpcIngressConnectionArn"], "vpcingressconnection/my-vic/")
				assert.Equal(t, "AVAILABLE", vic["Status"])
				assert.Equal(t, svcArn, vic["ServiceArn"])
				ivc := vic["IngressVpcConfiguration"].(map[string]any)
				assert.Equal(t, "vpc-aaa", ivc["VpcId"])
			},
		},
		{
			name:     "create missing name returns 400",
			action:   "CreateVpcIngressConnection",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create missing service ARN returns 400",
			action:   "CreateVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionName": "x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestVpcIngressConnectionDescribeDeleteListUpdate(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "CreateVpcIngressConnection", map[string]any{
		"VpcIngressConnectionName": "vic1",
		"ServiceArn":               svcArn,
		"IngressVpcConfiguration":  map[string]any{"VpcId": "vpc-111", "VpcEndpointId": "vpce-222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	vicArn := createResp["VpcIngressConnection"].(map[string]any)["VpcIngressConnectionArn"].(string)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "describe returns VIC",
			action:   "DescribeVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionArn": vicArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Equal(t, "vic1", vic["VpcIngressConnectionName"])
			},
		},
		{
			name:     "describe missing ARN returns 400",
			action:   "DescribeVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "describe unknown ARN returns 400",
			action: "DescribeVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionArn": "arn:aws:apprunner:us-east-1:000000000000:vpcingressconnection/notexist/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list returns 1",
			action:   "ListVpcIngressConnections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["VpcIngressConnectionSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:     "list with service ARN filter",
			action:   "ListVpcIngressConnections",
			body:     map[string]any{"Filter": map[string]any{"ServiceArn": svcArn}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list := resp["VpcIngressConnectionSummaryList"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name:   "update changes VPC config",
			action: "UpdateVpcIngressConnection",
			body: map[string]any{
				"VpcIngressConnectionArn": vicArn,
				"IngressVpcConfiguration": map[string]any{
					"VpcId":         "vpc-new",
					"VpcEndpointId": "vpce-new",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				ivc := vic["IngressVpcConfiguration"].(map[string]any)
				assert.Equal(t, "vpc-new", ivc["VpcId"])
			},
		},
		{
			name:     "update missing ARN returns 400",
			action:   "UpdateVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete returns VIC",
			action:   "DeleteVpcIngressConnection",
			body:     map[string]any{"VpcIngressConnectionArn": vicArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				vic := resp["VpcIngressConnection"].(map[string]any)
				assert.Equal(t, "vic1", vic["VpcIngressConnectionName"])
			},
		},
		{
			name:     "delete missing ARN returns 400",
			action:   "DeleteVpcIngressConnection",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
