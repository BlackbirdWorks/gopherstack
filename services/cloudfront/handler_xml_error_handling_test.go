package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestXMLUnmarshalErrorHandled is a regression suite for gopherstack-ob1g: handlers
// that discarded xml.Unmarshal's error via `_ = xml.Unmarshal(...)` proceeded on a
// zeroed request struct instead of rejecting the malformed body, which is exactly
// what let PutResourcePolicy and CreateRealtimeLogConfig's root-mismatch bugs
// (gopherstack-nfka / e5fbae252) go unnoticed. Each case posts an unparsable body and
// asserts the handler now returns 400 MalformedXML instead of silently succeeding on
// zero values. Against the unfixed `_ = xml.Unmarshal(...)` form every case here
// fails, since the old code returns 200/201/204 regardless of body content.
func TestXMLUnmarshalErrorHandled(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"
	const malformed = "<<<not xml"

	tests := []struct {
		// setup returns the request path and any headers (e.g. If-Match) needed to
		// reach the xml.Unmarshal call itself, bypassing unrelated precondition checks.
		setup  func(t *testing.T, h *cloudfront.Handler) (path string, headers map[string]string)
		name   string
		method string
	}{
		{
			name:   "create_monitoring_subscription",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "distribution/d1/monitoring-subscription"),
		},
		{
			name:   "create_realtime_log_config",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "realtime-log-config"),
		},
		{
			name:   "get_realtime_log_config",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "get-realtime-log-config"),
		},
		{
			name:   "update_realtime_log_config",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "realtime-log-config"),
		},
		{
			name:   "delete_realtime_log_config",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "delete-realtime-log-config"),
		},
		{
			name:   "create_vpc_origin",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "vpc-origin"),
		},
		{
			name:   "create_field_level_encryption",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "field-level-encryption"),
		},
		{
			name:   "update_field_level_encryption",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "field-level-encryption/x"),
		},
		{
			name:   "create_field_level_encryption_profile",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "field-level-encryption-profile"),
		},
		{
			name:   "update_field_level_encryption_profile",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "field-level-encryption-profile/x"),
		},
		{
			name:   "create_distribution_tenant",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "distribution-tenant"),
		},
		{
			name:   "update_distribution_tenant",
			method: http.MethodPut,
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				out := cfOK(t, h, http.MethodPost, prefix+"distribution-tenant",
					`<CreateDistributionTenantRequest>`+
						`<DistributionId>dist-x</DistributionId>`+
						`<Domain>xml-error-tenant.example.com</Domain>`+
						`</CreateDistributionTenantRequest>`)
				id := extractXMLID(t, out)
				rec := cfRequest(t, h, http.MethodGet, prefix+"distribution-tenant/"+id, "")
				etag := rec.Header().Get("ETag")

				return prefix + "distribution-tenant/" + id, map[string]string{"If-Match": etag}
			},
		},
		{
			name:   "verify_dns_configuration",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "verify-dns-configuration"),
		},
		{
			name:   "create_invalidation_for_tenant",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "distribution-tenant/t1/invalidation"),
		},
		{
			name:   "list_domain_conflicts",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "domain-conflicts"),
		},
		{
			name:   "create_key_value_store",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "key-value-store"),
		},
		{
			name:   "update_key_value_store",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "key-value-store/name1"),
		},
		{
			name:   "create_public_key",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "public-key"),
		},
		{
			// NOTE: real UpdatePublicKey PUTs to /public-key/{Id}/config
			// (cloudfront@v1.67.4 serializers.go), but gopherstack's route table
			// currently binds UpdatePublicKey to the bare /public-key/{Id} path
			// instead and leaves the /config-suffixed PUT unmatched
			// (handler_paths.go parseCFPublicKeyRealtimePath passes
			// updateConfigOp=""). That routing gap is a separate, pre-existing
			// bug (gopherstack-ob1g follow-up) -- this case exercises the path
			// gopherstack actually serves today.
			name:   "update_public_key",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "public-key/x"),
		},
		{
			name:   "create_key_group",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "key-group"),
		},
		{
			name:   "update_key_group",
			method: http.MethodPut,
			setup:  staticCFPath(prefix + "key-group/x"),
		},
		{
			name:   "test_connection_function",
			method: http.MethodPost,
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				out := cfOK(t, h, http.MethodPost, prefix+"connection-function",
					`<CreateConnectionFunctionRequest><Name>fn-xml-error</Name></CreateConnectionFunctionRequest>`)
				id := extractXMLID(t, out)

				return prefix + "connection-function/" + id + "/test", nil
			},
		},
		{
			name:   "get_resource_policy",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "get-resource-policy"),
		},
		{
			name:   "put_resource_policy",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "put-resource-policy"),
		},
		{
			name:   "delete_resource_policy",
			method: http.MethodPost,
			setup:  staticCFPath(prefix + "delete-resource-policy"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			path, headers := tt.setup(t, h)

			rec := doXMLWithHeaders(t, h, tt.method, path, []byte(malformed), headers)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "MalformedXML")
		})
	}
}

// staticCFPath adapts a fixed path string to the table's per-case setup func signature.
func staticCFPath(p string) func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
	return func(t *testing.T, _ *cloudfront.Handler) (string, map[string]string) {
		t.Helper()

		return p, nil
	}
}
