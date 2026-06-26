package securityhub_test

// parity_c_test.go — parity fixes: disable status, import limits, action target validation,
// control status validation, association updates, filter operators, automation rule fields.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_BatchDisableStandards_StatusIsDeleting verifies the subscription
// status is "DELETING" (not "INCOMPLETE") when BatchDisableStandards is called.
func TestParity_BatchDisableStandards_StatusIsDeleting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Enable a standard first
	enRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []any{
			map[string]any{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})
	require.Equal(t, http.StatusOK, enRec.Code)

	var enResp map[string]any
	require.NoError(t, json.Unmarshal(enRec.Body.Bytes(), &enResp))

	subs, _ := enResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, subs)
	subArn, _ := subs[0].(map[string]any)["StandardsSubscriptionArn"].(string)
	require.NotEmpty(t, subArn)

	// Disable it
	disRec := doRequest(t, h, http.MethodPost, "/standards/deregister", map[string]any{
		"StandardsSubscriptionArns": []string{subArn},
	})
	require.Equal(t, http.StatusOK, disRec.Code)

	var disResp map[string]any
	require.NoError(t, json.Unmarshal(disRec.Body.Bytes(), &disResp))

	disabledSubs, _ := disResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, disabledSubs)

	status, _ := disabledSubs[0].(map[string]any)["StandardsStatus"].(string)
	assert.Equal(t, "DELETING", status, "BatchDisableStandards must return DELETING status, not INCOMPLETE")
}

// TestParity_BatchImportFindings_MaxLimit verifies that BatchImportFindings
// rejects requests with more than 100 findings.
func TestParity_BatchImportFindings_MaxLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantCode int
	}{
		{name: "100_findings_accepted", count: 100, wantCode: http.StatusOK},
		{name: "101_findings_rejected", count: 101, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			enableHub(t, h)

			findings := make([]any, tt.count)
			for i := range findings {
				findings[i] = validFinding(nil)
			}

			rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
				"Findings": findings,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_CreateActionTarget_RequiredFields verifies that CreateActionTarget
// rejects requests with missing Name, Description, or Id.
func TestParity_CreateActionTarget_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "all_fields_present",
			body:     map[string]any{"Name": "MyTarget", "Description": "Test action target", "Id": "MyTargetId"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"Description": "desc", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_description",
			body:     map[string]any{"Name": "name1", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_id",
			body:     map[string]any{"Name": "name1", "Description": "desc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_name",
			body:     map[string]any{"Name": "", "Description": "desc", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			enableHub(t, h)
			rec := doRequest(t, h, http.MethodPost, "/actionTargets", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_UpdateStandardsControl_StatusValidation verifies that
// UpdateStandardsControl enforces valid enum values and requires DisabledReason
// when disabling.
func TestParity_UpdateStandardsControl_StatusValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Enable a standard to get a real control ARN
	enRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []any{
			map[string]any{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})
	require.Equal(t, http.StatusOK, enRec.Code)

	var enResp map[string]any
	require.NoError(t, json.Unmarshal(enRec.Body.Bytes(), &enResp))

	subs, _ := enResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, subs)
	subArn, _ := subs[0].(map[string]any)["StandardsSubscriptionArn"].(string)
	controlArn := subArn + "/control/1"

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "enable_accepted",
			body:     map[string]any{"ControlStatus": "ENABLED"},
			wantCode: http.StatusOK,
		},
		{
			name:     "disable_with_reason_accepted",
			body:     map[string]any{"ControlStatus": "DISABLED", "DisabledReason": "Not needed"},
			wantCode: http.StatusOK,
		},
		{
			name:     "disable_without_reason_rejected",
			body:     map[string]any{"ControlStatus": "DISABLED"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_status_rejected",
			body:     map[string]any{"ControlStatus": "ACTIVE"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPatch, "/standards/control/"+controlArn, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_BatchUpdateStdCtlAssociations_Persisted verifies that
// BatchUpdateStandardsControlAssociations actually persists updates which
// are then reflected in BatchGetStandardsControlAssociations.
func TestParity_BatchUpdateStdCtlAssociations_Persisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Enable a standard
	doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []any{
			map[string]any{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})

	stdArn := "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0"
	secCtlID := "S3.1"

	// Update association to DISABLED
	updateRec := doRequest(t, h, http.MethodPatch, "/associations", map[string]any{
		"StandardsControlAssociationUpdates": []any{
			map[string]any{
				"SecurityControlId": secCtlID,
				"StandardsArn":      stdArn,
				"AssociationStatus": "DISABLED",
				"UpdatedReason":     "Not applicable",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	unprocessed, _ := updateResp["UnprocessedAssociationUpdates"].([]any)
	assert.Empty(t, unprocessed, "update must succeed without unprocessed items")

	// Verify it's reflected in get
	getRec := doRequest(t, h, http.MethodPost, "/associations/batchGet", map[string]any{
		"StandardsControlAssociationIds": []any{
			map[string]any{
				"SecurityControlId": secCtlID,
				"StandardsArn":      stdArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	details, _ := getResp["StandardsControlAssociationDetails"].([]any)
	require.Len(t, details, 1)

	detail := details[0].(map[string]any)
	assert.Equal(t, "DISABLED", detail["AssociationStatus"])
}

// TestParity_GetFindings_FiltersApplied verifies that GetFindings applies
// multiple filter fields and comparison operators correctly.
func TestParity_GetFindings_FiltersApplied(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Import two findings with different properties
	f1 := validFinding(map[string]any{
		"Id":           "finding-1",
		"Title":        "Finding Alpha",
		"RecordState":  "ACTIVE",
		"AwsAccountId": "111111111111",
	})
	f2 := validFinding(map[string]any{
		"Id":           "finding-2",
		"Title":        "Finding Beta",
		"RecordState":  "ARCHIVED",
		"AwsAccountId": "222222222222",
	})

	importRec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{f1, f2},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	tests := []struct {
		filters   map[string]any
		name      string
		wantCount int
	}{
		{
			name: "filter_by_record_state_equals",
			filters: map[string]any{
				"RecordState": []any{map[string]any{"Value": "ACTIVE", "Comparison": "EQUALS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_account_id_not_equals",
			filters: map[string]any{
				"AwsAccountId": []any{map[string]any{"Value": "222222222222", "Comparison": "NOT_EQUALS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_title_prefix",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Finding", "Comparison": "PREFIX"}},
			},
			wantCount: 2,
		},
		{
			name: "filter_by_title_contains",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Alpha", "Comparison": "CONTAINS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_title_not_contains",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Alpha", "Comparison": "NOT_CONTAINS"}},
			},
			wantCount: 1,
		},
		{
			name:      "no_filter_returns_all",
			filters:   map[string]any{},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
				"Filters": tt.filters,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			findings, _ := resp["Findings"].([]any)
			assert.Len(t, findings, tt.wantCount)
		})
	}
}

// TestParity_CreateAutomationRule_ResponseFields verifies that CreateAutomationRule
// returns RuleStatus, RuleOrder, and RuleName in addition to RuleArn.
func TestParity_CreateAutomationRule_ResponseFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	rec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":    "my-rule",
		"RuleOrder":   float64(5),
		"RuleStatus":  "ENABLED",
		"IsTerminal":  false,
		"Criteria":    map[string]any{},
		"Actions":     []any{},
		"Description": "test rule",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["RuleArn"], "RuleArn must be present")
	assert.NotEmpty(t, resp["CreatedAt"], "CreatedAt must be present")
	assert.Equal(t, "my-rule", resp["RuleName"])
	assert.Equal(t, "ENABLED", resp["RuleStatus"])
	assert.InDelta(t, float64(5), resp["RuleOrder"], 0)
}

// TestParity_BatchUpdateAutomationRules_CriteriaAndActions verifies that
// BatchUpdateAutomationRules persists updates to Criteria and Actions fields.
func TestParity_BatchUpdateAutomationRules_CriteriaAndActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Create a rule
	createRec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":  "rule-to-update",
		"RuleOrder": float64(1),
		"Criteria":  map[string]any{},
		"Actions":   []any{},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ruleArn := createResp["RuleArn"].(string)

	// Update its Criteria and Actions
	newCriteria := map[string]any{"SeverityLabel": []any{map[string]any{"Value": "CRITICAL", "Comparison": "EQUALS"}}}
	newActions := []any{map[string]any{"Type": "FINDING_FIELDS_UPDATE"}}

	updateRec := doRequest(t, h, http.MethodPatch, "/automationrules/update", map[string]any{
		"UpdateAutomationRulesRequestItems": []any{
			map[string]any{
				"RuleArn":  ruleArn,
				"Criteria": newCriteria,
				"Actions":  newActions,
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Get the rule to verify criteria/actions were persisted
	getRec := doRequest(t, h, http.MethodPost, "/automationrules/get", map[string]any{
		"AutomationRulesArns": []string{ruleArn},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules, _ := getResp["Rules"].([]any)
	require.Len(t, rules, 1)

	rule := rules[0].(map[string]any)
	criteria, ok := rule["Criteria"].(map[string]any)
	require.True(t, ok, "Criteria must be present after update")
	assert.Contains(t, criteria, "SeverityLabel", "updated Criteria must be stored")

	actions, ok := rule["Actions"].([]any)
	require.True(t, ok, "Actions must be present after update")
	assert.Len(t, actions, 1, "updated Actions must be stored")
}

// TestParity_EnableImportFindingsForProduct_DuplicateReturns409 verifies that
// enabling the same product integration twice returns a conflict error.
func TestParity_EnableImportFindingsForProduct_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	body := map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/productSubscriptions", body)
	assert.Equal(t, http.StatusOK, rec1.Code, "first enable must succeed")

	rec2 := doRequest(t, h, http.MethodPost, "/productSubscriptions", body)
	assert.Equal(t, http.StatusConflict, rec2.Code, "second enable must return 409 Conflict")
}

// TestParity_StandardsSubscriptions_IncludesStatusReason verifies that
// BatchEnableStandards and GetEnabledStandards responses include StatusReason.
func TestParity_StandardsSubscriptions_IncludesStatusReason(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	enRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []any{
			map[string]any{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})
	require.Equal(t, http.StatusOK, enRec.Code)

	var enResp map[string]any
	require.NoError(t, json.Unmarshal(enRec.Body.Bytes(), &enResp))

	subs, _ := enResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, subs)
	// StatusReason must be present (even if nil/empty)
	_, hasStatusReason := subs[0].(map[string]any)["StatusReason"]
	assert.True(t, hasStatusReason, "BatchEnableStandards response must include StatusReason field")

	// GetEnabledStandards
	getRec := doRequest(t, h, http.MethodPost, "/standards/get", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	getSubs, _ := getResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, getSubs)
	_, hasStatusReasonGet := getSubs[0].(map[string]any)["StatusReason"]
	assert.True(t, hasStatusReasonGet, "GetEnabledStandards response must include StatusReason field")
}

// TestParity_BatchGetStdCtlAssociations_MissingFields verifies that
// BatchGetStandardsControlAssociations includes RelatedRequirements and other
// fields that were previously omitted.
func TestParity_BatchGetStdCtlAssociations_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	stdArn := "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0"
	getRec := doRequest(t, h, http.MethodPost, "/associations/batchGet", map[string]any{
		"StandardsControlAssociationIds": []any{
			map[string]any{
				"SecurityControlId": "S3.1",
				"StandardsArn":      stdArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	details, _ := resp["StandardsControlAssociationDetails"].([]any)
	require.Len(t, details, 1)

	detail := details[0].(map[string]any)
	_, hasRelated := detail["RelatedRequirements"]
	_, hasTitle := detail["StandardsControlTitle"]
	_, hasDesc := detail["StandardsControlDescription"]
	_, hasArns := detail["StandardsControlArns"]
	_, hasReason := detail["UpdatedReason"]

	assert.True(t, hasRelated, "RelatedRequirements must be present")
	assert.True(t, hasTitle, "StandardsControlTitle must be present")
	assert.True(t, hasDesc, "StandardsControlDescription must be present")
	assert.True(t, hasArns, "StandardsControlArns must be present")
	assert.True(t, hasReason, "UpdatedReason must be present")
}

// TestParity_GetFindings_MultipleFilterCombinations verifies that compound
// filters (multiple fields) are applied with AND semantics.
func TestParity_GetFindings_MultipleFilterCombinations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	f1 := validFinding(map[string]any{
		"Id":          "f-compound-1",
		"Title":       "Critical Alert",
		"RecordState": "ACTIVE",
	})
	f2 := validFinding(map[string]any{
		"Id":          "f-compound-2",
		"Title":       "Low Alert",
		"RecordState": "ACTIVE",
	})
	f3 := validFinding(map[string]any{
		"Id":          "f-compound-3",
		"Title":       "Critical Alert",
		"RecordState": "ARCHIVED",
	})

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{f1, f2, f3},
	})

	// Filter: Title contains "Critical" AND RecordState = ACTIVE → should match only f1
	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
		"Filters": map[string]any{
			"Title":       []any{map[string]any{"Value": "Critical", "Comparison": "CONTAINS"}},
			"RecordState": []any{map[string]any{"Value": "ACTIVE", "Comparison": "EQUALS"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	findings, _ := resp["Findings"].([]any)
	assert.Len(t, findings, 1)

	if len(findings) > 0 {
		id, _ := findings[0].(map[string]any)["Id"].(string)
		assert.True(t, strings.HasSuffix(id, "f-compound-1"), "only Critical+ACTIVE finding should match")
	}
}
