package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// Test_ResourceInUse_BlocksDelete proves that CachePolicy, OriginRequestPolicy,
// ResponseHeadersPolicy, OAI, OriginAccessControl, and KeyGroup deletes are rejected
// with AWS's resource-specific *InUse error while a distribution's stored config
// still references the resource, and that the delete succeeds once the reference is
// gone. Before this fix, none of these Delete operations checked for in-use
// references at all: a resource could be deleted out from under a live distribution,
// which real CloudFront forbids.
//
// The embedded reference value differs per resource to match its real wire shape:
// CachePolicyId/OriginRequestPolicyId/ResponseHeadersPolicyId/KeyGroup are bare IDs,
// but S3OriginConfig.OriginAccessIdentity uses the literal path
// "origin-access-identity/cloudfront/{id}" (see oaiReferencePath), and
// OriginAccessControlId is a bare ID too. The distribution's search index tokenizes
// the whole raw config, so embedding the reference value in any tag (not necessarily
// the real DistributionConfig field name/nesting) is sufficient to prove the InUse
// guard triggers on the correct token.
func Test_ResourceInUse_BlocksDelete(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	cases := []struct {
		deletePathFor      func(resourceID string) string
		refValue           func(resourceID string) string
		createResourcePath string
		createResourceBody string
		configField        string
		wantInUseCode      string
	}{
		{
			createResourcePath: prefix + "cache-policy",
			createResourceBody: `<CachePolicyConfig><Name>iu-cp</Name>` +
				`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
				`</CachePolicyConfig>`,
			configField:   "CachePolicyId",
			deletePathFor: func(id string) string { return prefix + "cache-policy/" + id },
			wantInUseCode: "CachePolicyInUse",
		},
		{
			createResourcePath: prefix + "origin-request-policy",
			createResourceBody: `<OriginRequestPolicyConfig><Name>iu-orp</Name></OriginRequestPolicyConfig>`,
			configField:        "OriginRequestPolicyId",
			deletePathFor:      func(id string) string { return prefix + "origin-request-policy/" + id },
			wantInUseCode:      "OriginRequestPolicyInUse",
		},
		{
			createResourcePath: prefix + "response-headers-policy",
			createResourceBody: `<ResponseHeadersPolicyConfig><Name>iu-rhp</Name></ResponseHeadersPolicyConfig>`,
			configField:        "ResponseHeadersPolicyId",
			deletePathFor:      func(id string) string { return prefix + "response-headers-policy/" + id },
			wantInUseCode:      "ResponseHeadersPolicyInUse",
		},
		{
			createResourcePath: prefix + "origin-access-identity/cloudfront",
			createResourceBody: `<CloudFrontOriginAccessIdentityConfig>` +
				`<CallerReference>cr-iu-oai</CallerReference><Comment>iu-oai</Comment>` +
				`</CloudFrontOriginAccessIdentityConfig>`,
			configField: "OriginAccessIdentity",
			refValue:    func(id string) string { return "origin-access-identity/cloudfront/" + id },
			deletePathFor: func(id string) string {
				return prefix + "origin-access-identity/cloudfront/" + id
			},
			wantInUseCode: "CloudFrontOriginAccessIdentityInUse",
		},
		{
			createResourcePath: prefix + "origin-access-control",
			createResourceBody: `<OriginAccessControlConfig><Name>iu-oac</Name>` +
				`<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>` +
				`<SigningBehavior>always</SigningBehavior><SigningProtocol>sigv4</SigningProtocol>` +
				`</OriginAccessControlConfig>`,
			configField:   "OriginAccessControlId",
			deletePathFor: func(id string) string { return prefix + "origin-access-control/" + id },
			wantInUseCode: "OriginAccessControlInUse",
		},
		{
			createResourcePath: prefix + "key-group",
			createResourceBody: `<KeyGroupConfig><Name>iu-kg</Name></KeyGroupConfig>`,
			configField:        "KeyGroup",
			deletePathFor:      func(id string) string { return prefix + "key-group/" + id },
			wantInUseCode:      "ResourceInUse",
		},
	}

	for _, tc := range cases {
		t.Run(tc.wantInUseCode, func(t *testing.T) {
			t.Parallel()
			h := newCFHandler(t)

			createRR := cfRequest(t, h, http.MethodPost, tc.createResourcePath, tc.createResourceBody)
			if createRR.Code != http.StatusCreated {
				t.Fatalf("expected 201 on create, got %d: %s", createRR.Code, createRR.Body.String())
			}

			resourceID := extractXMLID(t, createRR.Body.String())
			if resourceID == "" {
				t.Fatalf("expected non-empty resource ID from create, got: %s", createRR.Body.String())
			}

			etag := createRR.Header().Get("ETag")
			if etag == "" {
				t.Fatalf("expected non-empty ETag from create")
			}

			refValue := resourceID
			if tc.refValue != nil {
				refValue = tc.refValue(resourceID)
			}

			distBody := `<DistributionConfig>` +
				`<CallerReference>cr-iu-` + tc.wantInUseCode + `</CallerReference>` +
				`<Enabled>true</Enabled>` +
				`<` + tc.configField + `>` + refValue + `</` + tc.configField + `>` +
				`</DistributionConfig>`
			distResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
			distID := extractXMLID(t, distResp)
			if distID == "" {
				t.Fatalf("expected non-empty distribution ID, got: %s", distResp)
			}

			// Still referenced: delete must fail with the resource-specific InUse code.
			blockedRR := cfRequestWithHeader(
				t, h, http.MethodDelete, tc.deletePathFor(resourceID), map[string]string{"If-Match": etag},
			)
			if blockedRR.Code != http.StatusConflict {
				t.Fatalf("expected 409 while in use, got %d: %s", blockedRR.Code, blockedRR.Body.String())
			}
			if !strings.Contains(blockedRR.Body.String(), tc.wantInUseCode) {
				t.Fatalf("expected %s, got: %s", tc.wantInUseCode, blockedRR.Body.String())
			}

			disableAndDeleteDistribution(t, h, distID)

			// No longer referenced: delete must now succeed.
			freeRR := cfRequestWithHeader(
				t, h, http.MethodDelete, tc.deletePathFor(resourceID), map[string]string{"If-Match": etag},
			)
			if freeRR.Code != http.StatusNoContent {
				t.Fatalf("expected 204 once unreferenced, got %d: %s", freeRR.Code, freeRR.Body.String())
			}
		})
	}
}

// Test_FunctionInUse_BlocksDelete proves DeleteFunction rejects a function still
// associated with a distribution's cache behavior via FunctionAssociations.
func Test_FunctionInUse_BlocksDelete(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"
	h := newCFHandler(t)

	createRR := cfRequest(t, h, http.MethodPost, prefix+"function", `<CreateFunctionRequest>`+
		`<Name>iu-fn</Name>`+
		`<FunctionConfig><Comment>c</Comment><Runtime>cloudfront-js-2.0</Runtime></FunctionConfig>`+
		`<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge3JldHVybiBldmVudC5yZXF1ZXN0O30=</FunctionCode>`+
		`</CreateFunctionRequest>`)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected 201 on create function, got %d: %s", createRR.Code, createRR.Body.String())
	}

	fnARN := extractBetween(createRR.Body.String(), "<FunctionARN>", "</FunctionARN>")
	if fnARN == "" {
		t.Fatalf("expected non-empty FunctionARN from create, got: %s", createRR.Body.String())
	}

	fnETag := createRR.Header().Get("ETag")
	if fnETag == "" {
		t.Fatalf("expected non-empty ETag from create function")
	}

	distBody := `<DistributionConfig>` +
		`<CallerReference>cr-iu-fn</CallerReference>` +
		`<Enabled>true</Enabled>` +
		`<DefaultCacheBehavior><FunctionAssociations><Quantity>1</Quantity><Items>` +
		`<FunctionAssociation><FunctionARN>` + fnARN + `</FunctionARN><EventType>viewer-request</EventType>` +
		`</FunctionAssociation></Items></FunctionAssociations></DefaultCacheBehavior>` +
		`</DistributionConfig>`
	distResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
	distID := extractXMLID(t, distResp)
	if distID == "" {
		t.Fatalf("expected non-empty distribution ID, got: %s", distResp)
	}

	blockedRR := cfRequestWithHeader(
		t, h, http.MethodDelete, prefix+"function/iu-fn", map[string]string{"If-Match": fnETag},
	)
	if blockedRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 while function in use, got %d: %s", blockedRR.Code, blockedRR.Body.String())
	}
	if !strings.Contains(blockedRR.Body.String(), "FunctionInUse") {
		t.Fatalf("expected FunctionInUse, got: %s", blockedRR.Body.String())
	}

	disableAndDeleteDistribution(t, h, distID)

	freeRR := cfRequestWithHeader(
		t, h, http.MethodDelete, prefix+"function/iu-fn", map[string]string{"If-Match": fnETag},
	)
	if freeRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 once unreferenced, got %d: %s", freeRR.Code, freeRR.Body.String())
	}
}

// disableAndDeleteDistribution disables distID (Enabled=false, satisfying If-Match on
// its current ETag) and then deletes it, mirroring the real AWS two-step teardown
// (CloudFront refuses to delete an enabled distribution).
func disableAndDeleteDistribution(t *testing.T, h *cloudfront.Handler, distID string) {
	t.Helper()

	const prefix = "/2020-05-31/"

	getRR := cfRequestWithHeader(t, h, http.MethodGet, prefix+"distribution/"+distID, nil)
	if getRR.Code != http.StatusOK {
		t.Fatalf("expected 200 on get distribution, got %d: %s", getRR.Code, getRR.Body.String())
	}

	etag := getRR.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("expected non-empty ETag on get distribution response")
	}

	updateBody := `<DistributionConfig>` +
		`<CallerReference>cr-disable-` + distID + `</CallerReference>` +
		`<Enabled>false</Enabled>` +
		`</DistributionConfig>`
	updateRR := cfRequestWithBodyHeaders(
		t, h, http.MethodPut, prefix+"distribution/"+distID+"/config", updateBody,
		map[string]string{"If-Match": etag},
	)
	if updateRR.Code != http.StatusOK {
		t.Fatalf("expected 200 on disable, got %d: %s", updateRR.Code, updateRR.Body.String())
	}

	newETag := updateRR.Header().Get("ETag")
	if newETag == "" {
		t.Fatalf("expected non-empty ETag on update distribution response")
	}

	deleteRR := cfRequestWithHeader(
		t, h, http.MethodDelete, prefix+"distribution/"+distID,
		map[string]string{"If-Match": newETag},
	)
	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete distribution, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}
}
