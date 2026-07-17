package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestHandler_CreateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "creates rule",
			body: map[string]any{
				"SamplingRule": map[string]any{"RuleName": "my-rule", "FixedRate": 0.05, "Priority": 1},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing RuleName returns 400",
			body:       map[string]any{"SamplingRule": map[string]any{}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSamplingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ruleNames     []string
		wantUserRules int // number of user-created rules; Default is always included
	}{
		{
			// A fresh backend includes the built-in Default rule.
			name:          "returns default rule only",
			wantUserRules: 0,
		},
		{
			name:          "returns seeded rules plus Default",
			ruleNames:     []string{"rule-a", "rule-b"},
			wantUserRules: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, ruleName := range tt.ruleNames {
				rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
					"SamplingRule": map[string]any{"RuleName": ruleName, "FixedRate": 0.05, "Priority": 1},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			records, ok := resp["SamplingRuleRecords"].([]any)
			require.True(t, ok)
			// +1 for the always-present Default rule.
			assert.Len(t, records, tt.wantUserRules+1)
		})
	}
}

func TestHandler_UpdateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates existing rule",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateSamplingRule(
					xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1},
				)
			},
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{"RuleName": "my-rule", "ServiceName": "updated-svc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing RuleName returns 400",
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found returns 400",
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{"RuleName": "missing-rule"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/UpdateSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing rule",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateSamplingRule(
					xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1},
				)
			},
			body:       map[string]any{"RuleName": "my-rule"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing RuleName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"RuleName": "missing-rule"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/DeleteSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestUpdateSamplingRule_ZeroFixedRate(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "zero-rate-rule", FixedRate: 0.5, Priority: 1})

	rec := doXrayRequest(t, h, "/UpdateSamplingRule", map[string]any{
		"SamplingRuleUpdate": map[string]any{
			"RuleName":  "zero-rate-rule",
			"FixedRate": 0.0,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	record, ok := resp["SamplingRuleRecord"].(map[string]any)
	require.True(t, ok)

	rule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)

	fixedRate, ok := rule["FixedRate"].(float64)
	require.True(t, ok)
	assert.InDelta(t, 0.0, fixedRate, 1e-9, "FixedRate should have been updated to 0.0")
}

// TestSamplingRuleAttributes verifies Attributes field roundtrip.
func TestSamplingRuleAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SamplingRule": map[string]any{
			"RuleName":      "attrs-rule",
			"FixedRate":     0.05,
			"Priority":      1,
			"ReservoirSize": 5,
			"ResourceARN":   "*",
			"ServiceName":   "*",
			"ServiceType":   "*",
			"Host":          "*",
			"HTTPMethod":    "*",
			"URLPath":       "*",
			"Attributes":    map[string]any{"env": "prod", "team": "platform"},
		},
	}

	rec := doXrayRequest(t, h, "/CreateSamplingRule", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	record, ok := resp["SamplingRuleRecord"].(map[string]any)
	require.True(t, ok)

	rule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)

	attrs, ok := rule["Attributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", attrs["env"])
}

// TestSamplingRuleModifiedAtInRecord verifies toSamplingRuleRecord uses separate times.
func TestSamplingRuleModifiedAtInRecord(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: "time-rule", FixedRate: 0.1, Priority: 1})
	require.NoError(t, err)

	// Small sleep so Modified and Created timestamps will differ after update.
	time.Sleep(time.Millisecond * 2)

	_, err = b.UpdateSamplingRule("time-rule", xray.SamplingRule{ServiceName: "updated"})
	require.NoError(t, err)

	h := xray.NewHandler(b)
	rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	records, ok := resp["SamplingRuleRecords"].([]any)
	require.True(t, ok)
	// 2 rules: time-rule + Default.
	require.Len(t, records, 2)

	// The first record (sorted by priority=1) should be time-rule.
	record, ok := records[0].(map[string]any)
	require.True(t, ok)

	samplingRule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "time-rule", samplingRule["RuleName"])

	createdAt, ok1 := record["CreatedAt"].(float64)
	modifiedAt, ok2 := record["ModifiedAt"].(float64)

	require.True(t, ok1)
	require.True(t, ok2)
	assert.GreaterOrEqual(t, modifiedAt, createdAt)
}

// TestDefaultSamplingRuleUndeletable verifies the Default rule cannot be deleted via handler.
func TestDefaultSamplingRuleUndeletable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/DeleteSamplingRule", map[string]any{
		"RuleName": "Default",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "deleting Default rule must return 400")
}

// TestResetPreservesDefaultRule verifies Reset() re-seeds the Default rule.
func TestResetPreservesDefaultRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Reset()

	rules := h.Backend.GetSamplingRules()
	var found bool

	for _, r := range rules {
		if r.RuleName == "Default" {
			found = true
		}
	}

	assert.True(t, found, "Default rule must be present after Reset()")
}

func TestHandler_GetSamplingRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nextToken   string
		extraRules  int
		wantMin     int
		wantHasNext bool
	}{
		{
			name:        "returns default rule with no extra rules",
			extraRules:  0,
			wantMin:     1,
			wantHasNext: false,
		},
		{
			name:        "returns all rules when under page limit",
			extraRules:  3,
			wantMin:     4,
			wantHasNext: false,
		},
		{
			name:        "empty body is accepted",
			extraRules:  0,
			wantMin:     1,
			wantHasNext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.extraRules {
				b.AddSamplingRuleInternal(xray.SamplingRule{
					RuleName:      fmt.Sprintf("rule-%d", i),
					ResourceARN:   "*",
					ServiceName:   "*",
					ServiceType:   "*",
					Host:          "*",
					HTTPMethod:    "*",
					URLPath:       "*",
					FixedRate:     0.05,
					Priority:      int32(i + 1),
					ReservoirSize: 1,
				})
			}

			var body map[string]any
			if tt.nextToken != "" {
				body = map[string]any{"NextToken": tt.nextToken}
			}

			rec := doXrayRequest(t, h, "/GetSamplingRules", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			records, ok := resp["SamplingRuleRecords"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(records), tt.wantMin)

			nextToken, _ := resp["NextToken"].(string)
			if tt.wantHasNext {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestSamplingRule_PriorityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		priority   int
		wantStatus int
	}{
		{name: "priority 0 rejected", priority: 0, wantStatus: http.StatusBadRequest},
		{name: "priority 1 accepted", priority: 1, wantStatus: http.StatusOK},
		{name: "priority 100 accepted", priority: 100, wantStatus: http.StatusOK},
		{name: "priority 9999 accepted", priority: 9999, wantStatus: http.StatusOK},
		{name: "priority 10000 rejected", priority: 10000, wantStatus: http.StatusBadRequest},
		{name: "priority negative rejected", priority: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("rule-prio-%d", tt.priority+10000),
					"Priority":  tt.priority,
					"FixedRate": 0.05,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestSamplingRule_ReservoirSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		reservoirSize int
		wantStatus    int
	}{
		{name: "0 accepted", reservoirSize: 0, wantStatus: http.StatusOK},
		{name: "100 accepted", reservoirSize: 100, wantStatus: http.StatusOK},
		{name: "-1 rejected", reservoirSize: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":      fmt.Sprintf("rule-res-%d", tt.reservoirSize+1000),
					"Priority":      100,
					"FixedRate":     0.05,
					"ReservoirSize": tt.reservoirSize,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestSamplingRule_RuleNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ruleName   string
		wantStatus int
	}{
		{name: "1 char accepted", ruleName: "x", wantStatus: http.StatusOK},
		{name: "32 chars accepted", ruleName: "a23456789012345678901234567890ab", wantStatus: http.StatusOK},
		{name: "33 chars rejected", ruleName: "a234567890123456789012345678901bc", wantStatus: http.StatusBadRequest},
		{name: "empty rejected", ruleName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  tt.ruleName,
					"Priority":  100,
					"FixedRate": 0.05,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestSamplingRule_DefaultRulePresentAtStart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	records, ok := resp["SamplingRuleRecords"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, records)

	var foundDefault bool
	for _, r := range records {
		rec, _ := r.(map[string]any)
		rule, _ := rec["SamplingRule"].(map[string]any)
		if rule["RuleName"] == "Default" {
			foundDefault = true

			break
		}
	}

	assert.True(t, foundDefault, "Default sampling rule must always be present")
}

func TestSamplingRule_DefaultRuleUndeletable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/DeleteSamplingRule", map[string]any{"RuleName": "Default"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSamplingRule_DuplicateRuleNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SamplingRule": map[string]any{
			"RuleName":  "dup-rule",
			"Priority":  100,
			"FixedRate": 0.05,
		},
	}

	rec1 := doXrayRequest(t, h, "/CreateSamplingRule", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doXrayRequest(t, h, "/CreateSamplingRule", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestUpdateSamplingRule_ZeroValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update        map[string]any
		name          string
		wantFixRate   float64
		wantReservoir float64
	}{
		{
			name:          "set FixedRate to 0.0",
			update:        map[string]any{"RuleName": "zero-test", "FixedRate": 0.0},
			wantFixRate:   0.0,
			wantReservoir: 10, // unchanged
		},
		{
			name:          "set ReservoirSize to 0",
			update:        map[string]any{"RuleName": "zero-test", "ReservoirSize": 0},
			wantFixRate:   0.5, // unchanged
			wantReservoir: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			b.AddSamplingRuleInternal(
				xray.SamplingRule{RuleName: "zero-test", FixedRate: 0.5, ReservoirSize: 10, Priority: 100},
			)

			rec := doXrayRequest(t, h, "/UpdateSamplingRule", map[string]any{"SamplingRuleUpdate": tt.update})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			record, ok := resp["SamplingRuleRecord"].(map[string]any)
			require.True(t, ok)

			rule, ok := record["SamplingRule"].(map[string]any)
			require.True(t, ok)

			if _, hasFixedRate := tt.update["FixedRate"]; hasFixedRate {
				assert.InDelta(t, tt.wantFixRate, rule["FixedRate"], 1e-9)
			}
			if _, hasReservoir := tt.update["ReservoirSize"]; hasReservoir {
				assert.InDelta(t, tt.wantReservoir, rule["ReservoirSize"], 0.001)
			}
		})
	}
}

func TestSamplingRule_FixedRateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fixedRate  float64
		wantStatus int
	}{
		{name: "0.0 accepted", fixedRate: 0.0, wantStatus: http.StatusOK},
		{name: "0.5 accepted", fixedRate: 0.5, wantStatus: http.StatusOK},
		{name: "1.0 accepted", fixedRate: 1.0, wantStatus: http.StatusOK},
		{name: "1.001 rejected", fixedRate: 1.001, wantStatus: http.StatusBadRequest},
		{name: "1.1 rejected", fixedRate: 1.1, wantStatus: http.StatusBadRequest},
		{name: "-0.001 rejected", fixedRate: -0.001, wantStatus: http.StatusBadRequest},
		{name: "-0.1 rejected", fixedRate: -0.1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("rule-rate-%.3f", tt.fixedRate+10),
					"Priority":  100,
					"FixedRate": tt.fixedRate,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
