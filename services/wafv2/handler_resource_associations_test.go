package wafv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func TestHandler_AssociateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) (string, string) {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return webACLARN, "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_web_acl_arn",
			setup: func(_ *wafv2.Handler) (string, string) {
				return "", "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			setup: func(h *wafv2.Handler) (string, string) {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return webACLARN, ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "web_acl_not_found",
			setup: func(_ *wafv2.Handler) (string, string) {
				webACLARN := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
				resourceARN := "arn:aws:ec2:us-east-1:000000000000:instance/i-abc"

				return webACLARN, resourceARN
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			webACLARN, resourceARN := tt.setup(h)

			body := map[string]any{}
			if webACLARN != "" {
				body["WebACLArn"] = webACLARN
			}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "AssociateWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DisassociateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
				require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

				return resourceARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS DisassociateWebACL is idempotent — calling it when no
			// association exists succeeds (no-op), matching real AWS behaviour.
			name: "not_found_idempotent",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:ec2:us-east-1:000000000000:instance/i-nonexistent"
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "DisassociateWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetWebACLForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
				require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

				return resourceARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:ec2:us-east-1:000000000000:instance/i-nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "GetWebACLForResource", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				webACL, ok := result["WebACL"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, webACL["Id"])
				assert.Equal(t, "my-acl", webACL["Name"])
			}
		})
	}
}

func TestHandler_GetWebACLForResource_WithVisibilityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	w, err := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "BLOCK", nil)
	require.NoError(t, err)

	webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/xyz"
	require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

	rec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	webACL, ok := result["WebACL"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-acl", webACL["Name"])
	defaultAction, ok := webACL["DefaultAction"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, defaultAction, "Block")
}

func TestGetWebACLForResourceReturnsRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createWebACLWithRules(t, h, "acl-for-resource", "REGIONAL")

	// Get the ARN.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getRespW map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getRespW))
	webACLARN := getRespW["WebACL"].(map[string]any)["ARN"].(string)

	// Associate with a resource.
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
	assocRec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLARN,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// GetWebACLForResource should return the full WebACL including Rules.
	forResourceRec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, forResourceRec.Code)

	var forResourceResp map[string]any
	require.NoError(t, json.Unmarshal(forResourceRec.Body.Bytes(), &forResourceResp))

	webACL := forResourceResp["WebACL"].(map[string]any)
	rules, ok := webACL["Rules"].([]any)
	require.True(t, ok, "Rules should be present")
	assert.Len(t, rules, 1, "should return 1 rule")
}

// ---- Gap 19: DeleteRuleGroup referenced by WebACL -------------------------

func TestListResourcesForWebACL_MultipleResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "list-resources-acl", "REGIONAL")

	resources := []string{
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-a/abc",
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-b/def",
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-c/ghi",
	}

	for _, r := range resources {
		rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
			"WebACLArn":   webACLARN,
			"ResourceArn": r,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doWafv2Request(t, h, "ListResourcesForWebACL", map[string]any{
		"WebACLArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	arns, _ := resp["ResourceArns"].([]any)
	assert.Len(t, arns, 3, "should list 3 associated resources")

	// Verify all expected ARNs are present.
	arnSet := make(map[string]bool)
	for _, a := range arns {
		arnSet[a.(string)] = true
	}

	for _, expected := range resources {
		assert.True(t, arnSet[expected], "resource %q should be in list", expected)
	}
}

// ---- GetRateBasedStatementManagedKeys ---------------------------------------

func TestHandler_ListResourcesForWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		webACLArn     string
		associations  []string
		wantStatus    int
		wantResources int
	}{
		{
			name:       "missing_webacl_arn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "webacl_not_found",
			webACLArn:  "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/notexist/xxx",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "no_associations",
			wantStatus:    http.StatusOK,
			wantResources: 0,
		},
		{
			name:          "with_association",
			associations:  []string{"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/1234"},
			wantStatus:    http.StatusOK,
			wantResources: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			webACLArn := tt.webACLArn
			if tt.wantStatus == http.StatusOK || len(tt.associations) > 0 {
				_, webACLArn = createWebACLHelper(t, h, "test-acl", "REGIONAL")

				for _, resourceArn := range tt.associations {
					rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
						"WebACLArn":   webACLArn,
						"ResourceArn": resourceArn,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			var body any
			if webACLArn != "" {
				body = map[string]any{"WebACLArn": webACLArn}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "ListResourcesForWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resources, ok := resp["ResourceArns"].([]any)
				require.True(t, ok)
				assert.Len(t, resources, tt.wantResources)
			}
		})
	}
}

func TestDisassociateWebACL_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Call DisassociateWebACL on a resource that was never associated.
	// AWS returns success (no-op).
	rec := doWafv2Request(t, h, "DisassociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code, "DisassociateWebACL should be idempotent: %s", rec.Body.String())

	// Call again to confirm repeated calls also succeed.
	rec = doWafv2Request(t, h, "DisassociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DisassociateWebACL removes the association ------------------------------

func TestDisassociateWebACL_RemovesAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL and associate it with a resource.
	_, webACLARN := createWebACLHelper(t, h, "assoc-acl", "REGIONAL")
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"

	rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLARN,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify association exists.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	// Disassociate.
	rec = doWafv2Request(t, h, "DisassociateWebACL", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now GetWebACLForResource should return not found.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "after disassociation, resource should have no WebACL")
}

// ---- DeleteWebACL fails when associated to a resource ------------------------

func TestAssociateWebACL_ReplaceExisting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, aclARN1 := createWebACLHelper(t, h, "acl-one", "REGIONAL")
	_, aclARN2 := createWebACLHelper(t, h, "acl-two", "REGIONAL")
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"

	// Associate first ACL.
	rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   aclARN1,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Associate second ACL — should replace, not duplicate.
	rec = doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   aclARN2,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Only one association should remain, pointing to acl-two.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	webACL := resp["WebACL"].(map[string]any)
	assert.Equal(t, "acl-two", webACL["Name"])
}

// ---- CLOUDFRONT scope ARN uses empty region ---------------------------------

func TestGetWebACLForResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/no-acl/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

// ---- CheckCapacity handles nil / empty rules gracefully ---------------------
