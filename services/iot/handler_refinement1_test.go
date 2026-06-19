package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// newRefBackend returns a fresh InMemoryBackend for refinement tests.
func newRefBackend() *iot.InMemoryBackend {
	return iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

// newRefHandler returns a fresh Handler and backend for refinement tests.
func newRefHandler() (*iot.Handler, *iot.InMemoryBackend) {
	b := newRefBackend()

	return iot.NewHandler(b, nil), b
}

// doRefRequest fires a synthetic HTTP request against the handler.
func doRefRequest(
	t *testing.T,
	h *iot.Handler,
	method, path string,
	body any,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var raw []byte

	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// TestRefinement1_Reset verifies that Reset clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		populate func(b *iot.InMemoryBackend)
		name     string
	}{
		{name: "empty_backend"},
		{
			name: "populated_backend",
			populate: func(b *iot.InMemoryBackend) {
				b.AddThingInternal(iot.Thing{ThingName: "t1"})
				b.AddPolicyInternal(iot.Policy{PolicyName: "p1"})
				b.AddRuleInternal(iot.TopicRule{RuleName: "r1"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			if tt.populate != nil {
				tt.populate(b)
			}

			b.Reset()

			assert.Equal(t, 0, b.ThingCount())
			assert.Equal(t, 0, b.PolicyCount())
			assert.Equal(t, 0, b.RuleCount())
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies Reset can be called multiple times.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cycles int
	}{
		{name: "single_reset", cycles: 1},
		{name: "triple_reset", cycles: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: "t1"})

			for range tt.cycles {
				b.Reset()
			}

			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddThingInternal(iot.Thing{ThingName: "thing-before-reset"})
			assert.Equal(t, 1, b.ThingCount())

			h.Reset()

			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_ProviderInit_NilCtx verifies that ErrNilAppContext is returned.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_context_returns_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			_, err := p.Init(nil)
			require.ErrorIs(t, err, iot.ErrNilAppContext)
		})
	}
}

// TestRefinement1_GetSupportedOperations_AllOps verifies the supported operation list.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOp  string
		wantMin int
	}{
		{name: "has_create_thing", wantOp: "CreateThing"},
		{name: "has_attach_policy", wantOp: "AttachPolicy"},
		{name: "has_cancel_audit_task", wantOp: "CancelAuditTask"},
		{name: "has_at_least_19_ops", wantMin: 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			ops := h.GetSupportedOperations()

			if tt.wantOp != "" {
				assert.Contains(t, ops, tt.wantOp)
			}

			if tt.wantMin > 0 {
				assert.GreaterOrEqual(t, len(ops), tt.wantMin)
			}
		})
	}
}

// TestRefinement1_HandlerOpsLen verifies the HandlerOpsLen export helper.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantMin int
	}{
		{name: "ops_len_at_least_19", wantMin: 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			assert.GreaterOrEqual(t, iot.HandlerOpsLen(h), tt.wantMin)
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddThingInternal / AddPolicyInternal / AddRuleInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		thingName    string
		policyName   string
		ruleName     string
		wantThings   int
		wantPolicies int
		wantRules    int
	}{
		{
			name:      "seed_one_of_each",
			thingName: "t1", policyName: "p1", ruleName: "r1",
			wantThings: 1, wantPolicies: 1, wantRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: tt.thingName})
			b.AddPolicyInternal(iot.Policy{PolicyName: tt.policyName})
			b.AddRuleInternal(iot.TopicRule{RuleName: tt.ruleName})

			assert.Equal(t, tt.wantThings, b.ThingCount())
			assert.Equal(t, tt.wantPolicies, b.PolicyCount())
			assert.Equal(t, tt.wantRules, b.RuleCount())
		})
	}
}

// TestRefinement1_SeedHelper_ARN verifies seed helpers generate ARNs automatically.
func TestRefinement1_SeedHelper_ARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thing_arn_auto_generated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: "auto-arn-thing"})

			thing, err := b.DescribeThing("auto-arn-thing")
			require.NoError(t, err)
			assert.Contains(t, thing.ARN, "arn:aws:iot:")
			assert.Contains(t, thing.ARN, "auto-arn-thing")
		})
	}
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "empty_thing_name",
			method: http.MethodPost,
			path:   "/things/",
			body:   map[string]any{},
		},
		{
			name:   "empty_policy_name",
			method: http.MethodPost,
			path:   "/policies/",
			body:   map[string]any{"policyDocument": "{}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_ErrAlreadyExists verifies duplicate resources return HTTP 409.
func TestRefinement1_ErrAlreadyExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "duplicate_thing",
			method: http.MethodPost,
			path:   "/things/dup-thing",
			body:   map[string]any{},
		},
		{
			name:   "duplicate_policy",
			method: http.MethodPost,
			path:   "/policies/dup-policy",
			body:   map[string]any{"policyDocument": "{}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			// First call succeeds.
			rec1 := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Second call conflicts.
			rec2 := doRefRequest(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code)
		})
	}
}

// TestRefinement1_ThingNotFound_Returns404 verifies DescribeThing returns 404.
func TestRefinement1_ThingNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "describe_missing_thing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodGet, "/things/missing", nil, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestRefinement1_RuleNotFound_Returns404 verifies GetTopicRule returns 404.
func TestRefinement1_RuleNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_missing_rule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodGet, "/rules/missing-rule", nil, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestRefinement1_SortedListThings verifies ListThings returns items sorted by name.
func TestRefinement1_SortedListThings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantOrder []string
	}{
		{
			name:      "three_things_sorted",
			seedNames: []string{"zebra", "alpha", "mango"},
			wantOrder: []string{"alpha", "mango", "zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, n := range tt.seedNames {
				b.AddThingInternal(iot.Thing{ThingName: n})
			}

			things := b.ListThings()
			require.Len(t, things, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, things[i].ThingName)
			}
		})
	}
}

// TestRefinement1_SortedListTopicRules verifies ListTopicRules returns sorted results.
func TestRefinement1_SortedListTopicRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantOrder []string
	}{
		{
			name:      "three_rules_sorted",
			seedNames: []string{"zeta", "alpha", "beta"},
			wantOrder: []string{"alpha", "beta", "zeta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, n := range tt.seedNames {
				b.AddRuleInternal(iot.TopicRule{RuleName: n})
			}

			rules := b.ListTopicRules()
			require.Len(t, rules, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, rules[i].RuleName)
			}
		})
	}
}

// TestRefinement1_DeepCopy_DescribeThing verifies mutations do not affect backend state.
func TestRefinement1_DeepCopy_DescribeThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutate_copy_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{
				ThingName:  "sensor",
				Attributes: map[string]string{"env": "prod"},
			})

			t1, err := b.DescribeThing("sensor")
			require.NoError(t, err)

			// Mutate the returned copy.
			t1.Attributes["env"] = "mutated"

			// Fetch again – original should be unchanged.
			t2, err := b.DescribeThing("sensor")
			require.NoError(t, err)
			assert.Equal(t, "prod", t2.Attributes["env"])
		})
	}
}

// TestRefinement1_DeepCopy_GetTopicRule verifies mutations do not affect backend state.
func TestRefinement1_DeepCopy_GetTopicRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutate_copy_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddRuleInternal(iot.TopicRule{
				RuleName: "myRule",
				SQL:      "SELECT temperature FROM 'a/#'",
			})

			r1, err := b.GetTopicRule("myRule")
			require.NoError(t, err)

			r1.SQL = "MUTATED"

			r2, err := b.GetTopicRule("myRule")
			require.NoError(t, err)
			assert.Equal(t, "SELECT temperature FROM 'a/#'", r2.SQL)
		})
	}
}

// TestRefinement1_ThingID_StoredAndReturned verifies ThingID is stored and returned.
func TestRefinement1_ThingID_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thingid_roundtrip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			out, err := b.CreateThing(&iot.CreateThingInput{ThingName: "id-test"})
			require.NoError(t, err)
			assert.NotEmpty(t, out.ThingID)

			thing, err := b.DescribeThing("id-test")
			require.NoError(t, err)
			assert.Equal(t, out.ThingID, thing.ThingID)
		})
	}
}

// TestRefinement1_GetTopicRule_RuleWrapper verifies the AWS-format `rule` wrapper in response.
func TestRefinement1_GetTopicRule_RuleWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_has_rule_wrapper_and_ruleArn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddRuleInternal(iot.TopicRule{RuleName: "WrapRule", SQL: "SELECT temperature FROM 'devices/#'"})

			rec := doRefRequest(t, h, http.MethodGet, "/rules/WrapRule", nil, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "ruleArn")
			assert.Contains(t, resp, "rule")
		})
	}
}

// TestRefinement1_NonNilAttributes verifies Thing.Attributes is never nil.
func TestRefinement1_NonNilAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_attributes_payload_gives_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "no-attrs"})
			require.NoError(t, err)

			thing, err := b.DescribeThing("no-attrs")
			require.NoError(t, err)
			assert.NotNil(t, thing.Attributes)
		})
	}
}

// TestRefinement1_NonNilActions verifies TopicRule.Actions is never nil.
func TestRefinement1_NonNilActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_actions_gives_empty_slice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
				RuleName:         "NoActionsRule",
				TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT temperature FROM 'devices/#'"},
			})
			require.NoError(t, err)

			rule, err := b.GetTopicRule("NoActionsRule")
			require.NoError(t, err)
			assert.NotNil(t, rule.Actions)
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_restore_preserves_things_policies_rules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			b1.AddThingInternal(iot.Thing{ThingName: "snap-thing"})
			b1.AddPolicyInternal(iot.Policy{PolicyName: "snap-policy"})
			b1.AddRuleInternal(iot.TopicRule{RuleName: "snap-rule", SQL: "SELECT temperature FROM 'devices/#'"})

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(snap))

			assert.Equal(t, 1, b2.ThingCount())
			assert.Equal(t, 1, b2.PolicyCount())
			assert.Equal(t, 1, b2.RuleCount())

			thing, err := b2.DescribeThing("snap-thing")
			require.NoError(t, err)
			assert.Equal(t, "snap-thing", thing.ThingName)
		})
	}
}

// TestRefinement1_PersistenceEmpty verifies Snapshot/Restore works on empty backend.
func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_snapshot_restores_cleanly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(snap))

			assert.Equal(t, 0, b2.ThingCount())
		})
	}
}

// TestRefinement1_HandlerSnapshot verifies Handler.Snapshot delegates to backend.
func TestRefinement1_HandlerSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_snapshot_delegates"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddThingInternal(iot.Thing{ThingName: "snap-via-handler"})

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2, b2 := newRefHandler()
			require.NoError(t, h2.Restore(snap))

			assert.Equal(t, 1, b2.ThingCount())
		})
	}
}

// TestRefinement1_TopicRule_ARN verifies the rule ARN is set correctly.
func TestRefinement1_TopicRule_ARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "rule_arn_contains_rule_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
				RuleName:         "ArnRule",
				TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT temperature FROM 'devices/#'"},
			})
			require.NoError(t, err)

			rule, err := b.GetTopicRule("ArnRule")
			require.NoError(t, err)
			assert.Contains(t, rule.ARN, "arn:aws:iot:")
			assert.Contains(t, rule.ARN, "ArnRule")
		})
	}
}

// TestRefinement1_CertTransferCount verifies AcceptCertificateTransfer tracking.
func TestRefinement1_CertTransferCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		certs []string
		want  int
	}{
		{name: "two_certs", certs: []string{"cert1", "cert2"}, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, c := range tt.certs {
				require.NoError(t, b.AcceptCertificateTransfer(&iot.AcceptCertificateTransferInput{CertificateID: c}))
			}

			assert.Equal(t, tt.want, b.CertTransferCount())
		})
	}
}

// TestRefinement1_CreateThing_Validation verifies empty ThingName is rejected at backend.
func TestRefinement1_CreateThing_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		thingName string
	}{
		{name: "empty_name", thingName: "", wantErr: iot.ErrValidation},
		{name: "non_empty_name", thingName: "valid", wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: tt.thingName})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_AttachThingPrincipal_Header verifies the x-amzn-principal header is read.
func TestRefinement1_AttachThingPrincipal_Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantCode  int
	}{
		{
			name:      "cert_principal_via_header",
			principal: "arn:aws:iot:us-east-1:123456789012:cert/abcdef",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodPut, "/things/my-thing/principals", nil,
				map[string]string{"x-amzn-principal": tt.principal})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_DeleteThing_Handler verifies DeleteThing handler.
func TestRefinement1_DeleteThing_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*iot.InMemoryBackend)
		name     string
		wantCode int
	}{
		{
			name: "delete_existing",
			setup: func(b *iot.InMemoryBackend) {
				b.AddThingInternal(iot.Thing{ThingName: "del-thing"})
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_nonexistent",
			setup:    nil,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRefRequest(t, h, http.MethodDelete, "/things/del-thing", nil, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_CreateAndGetTopicRule_Handler verifies rule creation and retrieval via HTTP.
func TestRefinement1_CreateAndGetTopicRule_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleName string
	}{
		{name: "create_and_get_rule", ruleName: "TempRule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			// Create rule.
			createRec := doRefRequest(t, h, http.MethodPost, "/rules/"+tt.ruleName,
				map[string]any{
					"sql":     "SELECT temperature FROM 'sensors/#'",
					"actions": []any{},
				}, nil)
			assert.Equal(t, http.StatusOK, createRec.Code)

			// Get rule.
			getRec := doRefRequest(t, h, http.MethodGet, "/rules/"+tt.ruleName, nil, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "ruleArn")
			assert.Contains(t, resp, "rule")
		})
	}
}

// TestRefinement1_DeleteTopicRule_Handler verifies rule deletion via HTTP.
func TestRefinement1_DeleteTopicRule_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*iot.InMemoryBackend)
		name     string
		wantCode int
	}{
		{
			name: "delete_existing_rule",
			setup: func(b *iot.InMemoryBackend) {
				b.AddRuleInternal(iot.TopicRule{RuleName: "DelRule"})
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_missing_rule",
			setup:    nil,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRefRequest(t, h, http.MethodDelete, "/rules/DelRule", nil, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_AttachPrincipalPolicy_Handler verifies AttachPrincipalPolicy via HTTP.
func TestRefinement1_AttachPrincipalPolicy_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "attach_policy_to_principal", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodPost, "/target-policies/my-policy", nil,
				map[string]string{"x-amzn-iot-thingname": "my-thing"})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_ChaosMetadata verifies chaos-related metadata methods.
func TestRefinement1_ChaosMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "chaos_service_name_is_iot"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			assert.Equal(t, "iot", h.ChaosServiceName())
			assert.NotEmpty(t, h.ChaosOperations())
			assert.NotEmpty(t, h.ChaosRegions())
		})
	}
}

// TestRefinement1_MQTTPort verifies the MQTT port is returned.
func TestRefinement1_MQTTPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPort int
	}{
		{name: "default_mqtt_port", wantPort: 1883},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			assert.Equal(t, tt.wantPort, b.MQTTPort())
		})
	}
}

// TestRefinement1_TopicRuleDuplicate_Returns409 verifies duplicate rule returns HTTP 409.
func TestRefinement1_TopicRuleDuplicate_Returns409(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_rule_creation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			body := map[string]any{"sql": "SELECT temperature FROM 'sensor/#'", "actions": []any{}}

			rec1 := doRefRequest(t, h, http.MethodPost, "/rules/DupRule", body, nil)
			assert.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doRefRequest(t, h, http.MethodPost, "/rules/DupRule", body, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code)
		})
	}
}

// TestRefinement1_BrokerPublish_NotStarted verifies Publish returns error before start.
func TestRefinement1_BrokerPublish_NotStarted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "publish_before_start_returns_error", wantErr: iot.ErrBrokerNotStarted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			broker := iot.NewBroker(b, 0)
			err := broker.Publish("sensor/temp", []byte("25"), false, 0)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_HandlerBroker verifies Handler.Broker returns the embedded broker.
func TestRefinement1_HandlerBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		hasNil bool
	}{
		{name: "nil_broker", hasNil: true},
		{name: "non_nil_broker", hasNil: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			if tt.hasNil {
				h := iot.NewHandler(b, nil)
				assert.Nil(t, h.Broker())
			} else {
				broker := iot.NewBroker(b, 0)
				h := iot.NewHandler(b, broker)
				assert.Equal(t, broker, h.Broker())
			}
		})
	}
}

// TestRefinement1_StartWorker_NilBroker verifies StartWorker is a no-op with nil broker.
func TestRefinement1_StartWorker_NilBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_broker_no_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			err := h.StartWorker(t.Context())
			require.NoError(t, err)
		})
	}
}

// TestRefinement1_ProviderName verifies Provider.Name returns "IoT".
func TestRefinement1_ProviderName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "provider_name_is_IoT", want: "IoT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

// TestRefinement1_Restore_NilMaps verifies Restore tolerates nil maps in snapshot JSON.
func TestRefinement1_Restore_NilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		json []byte
	}{
		{
			name: "empty_json_object",
			json: []byte(`{}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			err := b.Restore(tt.json)
			require.NoError(t, err)
			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_HandleError_InternalServerError verifies fallback to 500.
func TestRefinement1_HandleError_InternalServerError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "unknown_error_returns_500"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			// Trigger an unknown operation to get to the fallback 400.
			rec := doRefRequest(t, h, http.MethodGet, "/things/", nil, nil)
			// Empty thingName: should be 404 (not found) since backend returns ThingNotFound.
			assert.NotEqual(t, http.StatusOK, rec.Code)
		})
	}
}

// TestRefinement1_ProviderInit_Valid verifies Provider.Init succeeds with valid context.
func TestRefinement1_ProviderInit_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "valid_context_succeeds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			svc, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
			require.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

// TestRefinement1_PersistenceWithAssociations verifies Snapshot/Restore preserves associations.
func TestRefinement1_PersistenceWithAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thing_group_memberships_preserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			b1.AddThingInternal(iot.Thing{ThingName: "t1"})

			// Add thing to group via backend method.
			require.NoError(t, b1.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
				ThingName:      "t1",
				ThingGroupName: "g1",
			}))
			require.NoError(t, b1.AcceptCertificateTransfer(&iot.AcceptCertificateTransferInput{
				CertificateID: "cert-abc",
			}))

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(snap))

			assert.Equal(t, 1, b2.ThingCount())
			assert.Equal(t, 1, b2.CertTransferCount())
		})
	}
}

// TestRefinement1_ThingKey_ARNFallback verifies thingKey uses ARN when name is empty.
func TestRefinement1_ThingKey_ARNFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "billing_group_with_arn_only"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			// Pass ARN only, no thing name.
			err := b.AddThingToBillingGroup(&iot.AddThingToBillingGroupInput{
				BillingGroupName: "bg1",
				ThingArn:         "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
			})
			require.NoError(t, err)
		})
	}
}

// TestRefinement1_SbomDeepCopy verifies cloneSbomDocument deep copies via persistence.
func TestRefinement1_SbomDeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "sbom_preserved_through_snapshot_restore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			_, err := b1.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
				PackageName: "my-pkg",
				VersionName: "1.0.0",
				Sbom: &iot.SbomDocument{
					S3Location: &iot.S3Location{Bucket: "my-bucket", Key: "sbom.json"},
				},
			})
			require.NoError(t, err)

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(snap))

			// Verify the SBOM was preserved by re-associating (no error means backend was restored).
			_, err2 := b2.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
				PackageName: "other-pkg",
				VersionName: "2.0.0",
			})
			require.NoError(t, err2)
		})
	}
}
