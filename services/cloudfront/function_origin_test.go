package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestTestFunction_OriginHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		runtime string
		call    string
		wantErr string
		event   string
	}{
		{
			name: "update_ok",
			call: `cf.updateRequestOrigin({domainName: 'a.example.com', originPath: '/p', customHeaders: {'x-a': 'b'},` +
				` timeouts: {readTimeout: 20, connectionTimeout: 3}, connectionAttempts: 2,` +
				` customOriginConfig: {port: 8080, protocol: 'http', sslProtocols: ['TLSv1.2'], ipAddressType: 'ipv4'},` +
				` originShield: {enabled: true, region: 'us-east-1'}, originAccessControlConfig: {enabled: false}})`,
		},
		{
			name:    "update_requires_object",
			wantErr: "params must be an object",
			call:    `cf.updateRequestOrigin('x')`,
		},
		{
			name:    "update_bad_read_timeout",
			wantErr: "readTimeout must be an integer between 1 and 60",
			call:    `cf.updateRequestOrigin({timeouts: {readTimeout: 61}})`,
		},
		{
			name:    "update_bad_conn_timeout",
			wantErr: "connectionTimeout",
			call:    `cf.updateRequestOrigin({timeouts: {connectionTimeout: 11}})`,
		},
		{
			name:    "update_bad_attempts",
			wantErr: "connectionAttempts",
			call:    `cf.updateRequestOrigin({connectionAttempts: 4})`,
		},
		{
			name:    "update_bad_domain",
			wantErr: "domainName is invalid",
			call:    `cf.updateRequestOrigin({domainName: 7})`,
		},
		{
			name:    "update_bad_protocol",
			wantErr: "protocol is invalid",
			call:    `cf.updateRequestOrigin({customOriginConfig: {protocol: 'ftp'}})`,
		},
		{
			name:    "update_bad_ssl",
			wantErr: "sslProtocols",
			call:    `cf.updateRequestOrigin({customOriginConfig: {sslProtocols: ['TLSv9']}})`,
		},
		{
			name:    "update_bad_ip_type",
			wantErr: "ipAddressType",
			call:    `cf.updateRequestOrigin({customOriginConfig: {ipAddressType: 'ipv5'}})`,
		},
		{
			name:    "update_bad_port",
			wantErr: "port",
			call:    `cf.updateRequestOrigin({customOriginConfig: {port: 0}})`,
		},
		{
			name:    "update_bad_headers",
			wantErr: "customHeaders",
			call:    `cf.updateRequestOrigin({customHeaders: {a: 1}})`,
		},
		{
			name:    "update_shield_needs_region",
			wantErr: "originShield.region",
			call:    `cf.updateRequestOrigin({originShield: {enabled: true}})`,
		},
		{
			name:    "update_bad_oac",
			wantErr: "enabled must be a boolean",
			call:    `cf.updateRequestOrigin({originAccessControlConfig: {enabled: 'yes'}})`,
		},
		{
			name: "select_ok",
			call: `cf.selectRequestOriginById('any-origin', {originPath: '/x'})`,
		},
		{
			name:    "select_needs_id",
			wantErr: "originId must be a non-empty string",
			call:    `cf.selectRequestOriginById()`,
		},
		{
			name: "group_ok",
			call: `cf.createRequestOriginGroup({originIds: ['a', {originId: 'b', originOverrides: {originPath: '/b'}}],` +
				` selectionCriteria: 'default', failoverCriteria: {statusCodes: [500, 503]}})`,
		},
		{
			name:    "group_needs_two",
			wantErr: "exactly two",
			call:    `cf.createRequestOriginGroup({originIds: ['a']})`,
		},
		{
			name:    "group_distinct",
			wantErr: "distinct",
			call:    `cf.createRequestOriginGroup({originIds: ['a', 'a']})`,
		},
		{
			name:    "group_bad_criteria",
			wantErr: "selectionCriteria",
			call:    `cf.createRequestOriginGroup({originIds: ['a', 'b'], selectionCriteria: 'random'})`,
		},
		{
			name:    "group_bad_codes",
			wantErr: "statusCodes",
			call:    `cf.createRequestOriginGroup({originIds: ['a', 'b'], failoverCriteria: {statusCodes: [200]}})`,
		},
		{
			name:  "viewer_response_rejected",
			event: respEvent, wantErr: "only available in viewer-request",
			call: `cf.updateRequestOrigin({domainName: 'a.example.com'})`,
		},
		{
			name:    "not_in_js1",
			runtime: "cloudfront-js-1.0", wantErr: "Cannot find module",
			call: `cf.updateRequestOrigin({})`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, ev := tt.runtime, tt.event
			if rt == "" {
				rt = "cloudfront-js-2.0"
			}

			if ev == "" {
				ev = reqEvent
			}

			code := `const cf = require('cloudfront'); function handler(e) { ` + tt.call + `; return e.request; }`
			res := testFunction(t, 0, rt, code, ev)

			if tt.wantErr != "" {
				assert.Contains(t, aws.ToString(res.FunctionErrorMessage), tt.wantErr)
				assert.Empty(t, aws.ToString(res.FunctionOutput))

				return
			}

			assert.Empty(t, aws.ToString(res.FunctionErrorMessage))
			assert.Contains(t, aws.ToString(res.FunctionOutput), `"uri":"/a"`)
		})
	}
}
