package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestListDistributionsByPolicyID_RoundTrip covers the ListDistributionsBy* operations that
// filter the real stored distributions by an ID referenced somewhere in the distribution's
// config (cache policy, origin request policy, response headers policy, realtime log config).
func TestListDistributionsByPolicyID_RoundTrip(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		listPath    func(id string) string
		name        string
		configField string
		configValue string
	}{
		{
			name:        "CachePolicyId",
			configField: "CachePolicyId",
			configValue: "cache-policy-abc123",
			listPath: func(id string) string {
				return prefix + "distributions/by-cache-policy-id/" + id
			},
		},
		{
			name:        "OriginRequestPolicyId",
			configField: "OriginRequestPolicyId",
			configValue: "orp-def456",
			listPath: func(id string) string {
				return prefix + "distributions/by-origin-request-policy-id/" + id
			},
		},
		{
			name:        "ResponseHeadersPolicyId",
			configField: "ResponseHeadersPolicyId",
			configValue: "rhp-ghi789",
			listPath: func(id string) string {
				return prefix + "distributions/by-response-headers-policy-id/" + id
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCFHandler(t)

			distBody := `<DistributionConfig>
				<CallerReference>cr-` + tc.name + `</CallerReference>
				<Enabled>true</Enabled>
				<` + tc.configField + `>` + tc.configValue + `</` + tc.configField + `>
			</DistributionConfig>`
			createResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
			distID := extractXMLID(t, createResp)
			if distID == "" {
				t.Fatal("expected non-empty distribution ID from create")
			}

			// Found: the distribution referencing the ID must appear in the list.
			foundResp := cfOK(t, h, http.MethodGet, tc.listPath(tc.configValue), "")
			if !strings.Contains(foundResp, "DistributionList") {
				t.Fatalf("expected DistributionList, got: %s", foundResp)
			}
			if strings.Contains(foundResp, "<Quantity>0</Quantity>") {
				t.Fatalf("expected non-empty list for matching id, got: %s", foundResp)
			}
			if !strings.Contains(foundResp, distID) {
				t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
			}

			// Not found: an unrelated ID must return an empty list, not an error.
			notFoundResp := cfOK(t, h, http.MethodGet, tc.listPath("no-such-id-xyz"), "")
			if !strings.Contains(notFoundResp, "DistributionList") {
				t.Fatalf("expected DistributionList for empty result, got: %s", notFoundResp)
			}
			if !strings.Contains(notFoundResp, "<Quantity>0</Quantity>") {
				t.Fatalf("expected empty list for non-matching id, got: %s", notFoundResp)
			}
			if strings.Contains(notFoundResp, distID) {
				t.Fatalf("distribution %s should not match unrelated id, got: %s", distID, notFoundResp)
			}
		})
	}
}

// TestListDistributionsByRealtimeLogConfig_RoundTrip verifies the realtime log config ARN is
// read from the RealtimeLogConfigArn query parameter and used to filter real distributions.
func TestListDistributionsByRealtimeLogConfig_RoundTrip(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	arn := "arn:aws:cloudfront::123456789012:realtime-log-config/rlc-xyz"
	distBody := `<DistributionConfig>
		<CallerReference>cr-rlc</CallerReference>
		<Enabled>true</Enabled>
		<RealtimeLogConfigArn>` + arn + `</RealtimeLogConfigArn>
	</DistributionConfig>`
	createResp := cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)
	distID := extractXMLID(t, createResp)
	if distID == "" {
		t.Fatal("expected non-empty distribution ID from create")
	}

	foundResp := cfOK(t, h, http.MethodGet,
		prefix+"distributions/by-realtime-log-config?RealtimeLogConfigArn="+arn, "")
	if !strings.Contains(foundResp, "DistributionList") {
		t.Fatalf("expected DistributionList, got: %s", foundResp)
	}
	if !strings.Contains(foundResp, distID) {
		t.Fatalf("expected distribution %s in list, got: %s", distID, foundResp)
	}

	otherARN := "arn:aws:cloudfront::123456789012:realtime-log-config/other"
	notFoundResp := cfOK(t, h, http.MethodGet,
		prefix+"distributions/by-realtime-log-config?RealtimeLogConfigArn="+otherARN, "")
	if !strings.Contains(notFoundResp, "<Quantity>0</Quantity>") {
		t.Fatalf("expected empty list for non-matching arn, got: %s", notFoundResp)
	}
}
