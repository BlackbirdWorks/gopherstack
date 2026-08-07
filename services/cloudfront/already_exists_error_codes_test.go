package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_AlreadyExists_ResourceSpecificErrorCodes proves that a name/identifier collision
// on each of these resource types returns the AWS-accurate *AlreadyExists error code for
// that specific resource type (e.g. FunctionAlreadyExists, PublicKeyAlreadyExists), not a
// single generic code borrowed from an unrelated resource. Before this fix, every one of
// these collisions returned the literal string "DistributionAlreadyExists" (CloudFront's
// distribution-specific error code, reused as a blanket fallback for every resource type),
// which would break any SDK caller that switches on the error code / typed error.
func Test_AlreadyExists_ResourceSpecificErrorCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		path     string
		wantCode string
		body     []byte
	}{
		{
			name: "CloudFront Function",
			path: "/2020-05-31/function",
			body: []byte(`<CreateFunctionRequest>` +
				`<Name>dup-fn</Name>` +
				`<FunctionConfig><Comment>c</Comment><Runtime>cloudfront-js-2.0</Runtime></FunctionConfig>` +
				`<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge3JldHVybiBldmVudC5yZXF1ZXN0O30=</FunctionCode>` +
				`</CreateFunctionRequest>`),
			wantCode: "FunctionAlreadyExists",
		},
		{
			name: "PublicKey",
			path: "/2020-05-31/public-key",
			body: []byte(`<PublicKeyConfig>` +
				`<CallerReference>pk-ref-dup</CallerReference>` +
				`<Name>dup-public-key</Name>` +
				`<EncodedKey>` + testRSA2048PublicKeyPEM + `</EncodedKey>` +
				`</PublicKeyConfig>`),
			wantCode: "PublicKeyAlreadyExists",
		},
		{
			name:     "KeyGroup",
			path:     "/2020-05-31/key-group",
			body:     []byte(`<KeyGroupConfig><Name>dup-key-group</Name><Items></Items></KeyGroupConfig>`),
			wantCode: "KeyGroupAlreadyExists",
		},
		{
			name: "OriginRequestPolicy",
			path: "/2020-05-31/origin-request-policy",
			body: []byte(`<OriginRequestPolicyConfig>` +
				`<Name>dup-orp</Name><Comment>c</Comment>` +
				`</OriginRequestPolicyConfig>`),
			wantCode: "OriginRequestPolicyAlreadyExists",
		},
		{
			name:     "ResponseHeadersPolicy",
			path:     "/2020-05-31/response-headers-policy",
			body:     []byte(`<ResponseHeadersPolicyConfig><Name>dup-rhp</Name></ResponseHeadersPolicyConfig>`),
			wantCode: "ResponseHeadersPolicyAlreadyExists",
		},
		{
			name: "OriginAccessControl",
			path: "/2020-05-31/origin-access-control",
			body: []byte(`<OriginAccessControlConfig>` +
				`<Name>dup-oac</Name>` +
				`<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>` +
				`<SigningBehavior>always</SigningBehavior><SigningProtocol>sigv4</SigningProtocol>` +
				`</OriginAccessControlConfig>`),
			wantCode: "OriginAccessControlAlreadyExists",
		},
		{
			name: "FieldLevelEncryption (keyed by CallerReference)",
			path: "/2020-05-31/field-level-encryption",
			body: []byte(`<FieldLevelEncryptionConfig>` +
				`<CallerReference>fle-ref-dup</CallerReference>` +
				`<Comment>c</Comment>` +
				`</FieldLevelEncryptionConfig>`),
			wantCode: "FieldLevelEncryptionConfigAlreadyExists",
		},
		{
			name: "RealtimeLogConfig",
			path: "/2020-05-31/realtime-log-config",
			body: []byte(`<RealtimeLogConfig>` +
				`<Name>dup-rt-log</Name>` +
				`<SamplingRate>50</SamplingRate>` +
				`<Fields><Field>timestamp</Field></Fields>` +
				`</RealtimeLogConfig>`),
			wantCode: "RealtimeLogConfigAlreadyExists",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec1 := doXML(t, h, http.MethodPost, tc.path, tc.body)
			require.Equal(t, http.StatusCreated, rec1.Code, "first create; body=%s", rec1.Body.String())

			rec2 := doXML(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, http.StatusConflict, rec2.Code, "duplicate create status; body=%s", rec2.Body.String())
			assert.Contains(t, rec2.Body.String(), tc.wantCode)
			assert.NotContains(t, rec2.Body.String(), "DistributionAlreadyExists",
				"must not fall back to the unrelated Distribution error code")
		})
	}
}
