package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// --- Backend direct tests ---

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "standard", accountID: "123456789012", region: "us-east-1"},
		{name: "other region", accountID: "000000000000", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend(tc.accountID, tc.region)
			assert.Equal(t, tc.accountID, b.AccountID())
			assert.Equal(t, tc.region, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears hub and findings"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(map[string]any{"Id": "f1", "ProductArn": "arn:aws:securityhub:::product/x/y"}),
			})
			assert.True(t, securityhub.IsHubEnabled(b))
			assert.Equal(t, 1, securityhub.FindingCount(b))

			b.Reset()

			assert.False(t, securityhub.IsHubEnabled(b))
			assert.Equal(t, 0, securityhub.FindingCount(b))
		})
	}
}

func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot and restore preserves state"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(
					map[string]any{"Id": "snap-1", "ProductArn": "arn:aws:securityhub:::product/x/y"},
				),
			})
			snap := b.Snapshot(t.Context())
			assert.NotEmpty(t, snap)

			b2 := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))
			assert.True(t, securityhub.IsHubEnabled(b2))
			assert.Equal(t, 1, securityhub.FindingCount(b2))
		})
	}
}

func TestBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "bad json", data: []byte(`{bad`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tc.data)
			assert.Error(t, err)
		})
	}
}

func TestBackend_UpdateActionTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updateName string
		updateDesc string
		wantErrMsg string
	}{
		{
			name:       "update name only",
			updateName: "NewName",
			updateDesc: "",
		},
		{
			name:       "update description only",
			updateName: "",
			updateDesc: "New desc",
		},
		{
			name:       "not found",
			updateName: "x",
			updateDesc: "",
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var targetArn string
			if tc.wantErrMsg == "" {
				var err error
				targetArn, err = b.CreateActionTarget("Original", "Orig desc", "my-action")
				require.NoError(t, err)
			} else {
				targetArn = "arn:aws:securityhub:us-east-1:000000000000:action/custom/nonexistent"
			}

			err := b.UpdateActionTarget(targetArn, tc.updateName, tc.updateDesc)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_DisableImportFindingsForProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preEnable  bool
	}{
		{
			name:      "disable after enable succeeds",
			preEnable: true,
		},
		{
			name:       "disable non-existent returns error",
			preEnable:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var subArn string
			if tc.preEnable {
				var err error
				subArn, err = b.EnableImportFindingsForProduct(
					"arn:aws:securityhub:us-east-1::product/aws/guardduty",
				)
				require.NoError(t, err)
			} else {
				subArn = "arn:aws:securityhub:us-east-1:000000000000:product-subscription/nonexistent"
			}

			err := b.DisableImportFindingsForProduct(subArn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_UpdateSecurityControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params    map[string]any
		name      string
		controlID string
	}{
		{
			name:      "update known control",
			controlID: "CloudTrail.1",
			params:    map[string]any{"key": "value"},
		},
		{
			name:      "update unknown control stores params",
			controlID: "Unknown.99",
			params:    map[string]any{"threshold": 10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.UpdateSecurityControl(tc.controlID, tc.params, "")
			require.NoError(t, err)

			// verify via BatchGetSecurityControls
			if tc.controlID == "CloudTrail.1" {
				controls, unprocessed := b.BatchGetSecurityControls([]string{tc.controlID})
				require.Len(t, controls, 1)
				assert.Empty(t, unprocessed)
				assert.Equal(t, "value", controls[0].Parameters["key"])
			}
		})
	}
}

func TestBackend_BatchUpdateAutomationRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantProcessed int
		wantUnproc    int
	}{
		{
			name:          "update existing rule",
			wantProcessed: 1,
			wantUnproc:    0,
		},
		{
			name:          "update non-existent rule",
			wantProcessed: 0,
			wantUnproc:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			ruleArn, _ := b.CreateAutomationRule(map[string]any{
				"RuleName":   "MyRule",
				"RuleStatus": "ENABLED",
				"RuleOrder":  float64(1),
			})

			var updates []map[string]any
			if tc.wantProcessed > 0 {
				updates = []map[string]any{{
					"RuleArn":     ruleArn,
					"RuleName":    "UpdatedName",
					"Description": "Updated desc",
					"RuleStatus":  "DISABLED",
					"RuleOrder":   float64(2),
					"IsTerminal":  true,
				}}
			} else {
				updates = []map[string]any{{
					"RuleArn":  "arn:aws:securityhub:us-east-1:000000000000:automation-rule/99999",
					"RuleName": "Ghost",
				}}
			}

			processed, unprocessed := b.BatchUpdateAutomationRules(updates)
			assert.Len(t, processed, tc.wantProcessed)
			assert.Len(t, unprocessed, tc.wantUnproc)
		})
	}
}

func TestBackend_DeclineInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		preInvite    bool
		wantDeclined int
		wantUnproc   int
	}{
		{
			name:         "decline existing invitation",
			preInvite:    true,
			wantDeclined: 1,
			wantUnproc:   0,
		},
		{
			name:         "decline non-existent invitation",
			preInvite:    false,
			wantDeclined: 0,
			wantUnproc:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.preInvite {
				_, _ = b.CreateMembers([]map[string]any{
					{"AccountId": "111111111111", "Email": "a@test.com"},
				})
				b.InviteMembers([]string{"111111111111"})
			}

			declined, unprocessed := b.DeclineInvitations([]string{"111111111111"})
			assert.Len(t, declined, tc.wantDeclined)
			assert.Len(t, unprocessed, tc.wantUnproc)
		})
	}
}

func TestBackend_DeleteInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		preInvite  bool
		wantDel    int
		wantUnproc int
	}{
		{
			name:       "delete existing invitation",
			preInvite:  true,
			wantDel:    1,
			wantUnproc: 0,
		},
		{
			name:       "delete non-existent invitation",
			preInvite:  false,
			wantDel:    0,
			wantUnproc: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.preInvite {
				_, _ = b.CreateMembers([]map[string]any{
					{"AccountId": "222222222222", "Email": "b@test.com"},
				})
				b.InviteMembers([]string{"222222222222"})
			}

			deleted, unprocessed := b.DeleteInvitations([]string{"222222222222"})
			assert.Len(t, deleted, tc.wantDel)
			assert.Len(t, unprocessed, tc.wantUnproc)
		})
	}
}

func TestBackend_GetFindingAggregator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preCreate  bool
	}{
		{
			name:      "get existing aggregator",
			preCreate: true,
		},
		{
			name:       "get non-existent aggregator",
			preCreate:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var arn string
			if tc.preCreate {
				agg, err := b.CreateFindingAggregator("ALL_REGIONS", []string{})
				require.NoError(t, err)
				arn = agg.FindingAggregatorArn
			} else {
				arn = "arn:aws:securityhub:us-east-1:000000000000:finding-aggregator/missing"
			}

			result, err := b.GetFindingAggregator(arn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, arn, result.FindingAggregatorArn)
			}
		})
	}
}

func TestBackend_GetConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		useByARN   bool
		useByName  bool
		preCreate  bool
	}{
		{
			name:      "get by ID",
			preCreate: true,
		},
		{
			name:      "get by ARN",
			preCreate: true,
			useByARN:  true,
		},
		{
			name:      "get by name",
			preCreate: true,
			useByName: true,
		},
		{
			name:       "not found",
			preCreate:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.preCreate {
				cp, err := b.CreateConfigurationPolicy("TestPolicy", "desc", map[string]any{}, nil)
				require.NoError(t, err)
				if tc.useByARN { //nolint:gocritic // existing issue.
					identifier = cp.Arn
				} else if tc.useByName {
					identifier = cp.Name
				} else {
					identifier = cp.Id
				}
			} else {
				identifier = "nonexistent-id"
			}

			result, err := b.GetConfigurationPolicy(identifier)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, "TestPolicy", result.Name)
			}
		})
	}
}

func TestBackend_UpdateConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updateField string
		wantErrMsg  string
	}{
		{name: "update name", updateField: "name"},
		{name: "update description", updateField: "desc"},
		{name: "update policy", updateField: "policy"},
		{name: "not found", wantErrMsg: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.wantErrMsg == "" {
				cp, err := b.CreateConfigurationPolicy(
					"Original",
					"orig desc",
					map[string]any{},
					nil,
				)
				require.NoError(t, err)
				identifier = cp.Id
			} else {
				identifier = "nonexistent"
			}

			var newName, newDesc string
			var newPolicy map[string]any

			switch tc.updateField {
			case "name":
				newName = "UpdatedName"
			case "desc":
				newDesc = "Updated desc"
			case "policy":
				newPolicy = map[string]any{"SecurityHub": map[string]any{"ServiceEnabled": false}}
			}

			result, err := b.UpdateConfigurationPolicy(identifier, newName, newDesc, newPolicy)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

func TestBackend_DeleteConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		useByARN   bool
		preCreate  bool
	}{
		{name: "delete by ID", preCreate: true},
		{name: "delete by ARN", preCreate: true, useByARN: true},
		{name: "not found", preCreate: false, wantErrMsg: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.preCreate {
				cp, err := b.CreateConfigurationPolicy("ToDelete", "desc", map[string]any{}, nil)
				require.NoError(t, err)
				if tc.useByARN {
					identifier = cp.Arn
				} else {
					identifier = cp.Id
				}
			} else {
				identifier = "nonexistent"
			}

			err := b.DeleteConfigurationPolicy(identifier)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_UpdateFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		hubEnabled bool
	}{
		{name: "hub enabled, updates findings", hubEnabled: true},
		{name: "hub not enabled returns error", hubEnabled: false, wantErrMsg: "not enabled"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.hubEnabled {
				require.NoError(t, b.EnableHub(false, nil))
				_, _, _ = b.ImportFindings([]map[string]any{
					securityhub.ValidFinding(
						map[string]any{
							"Id":         "f1",
							"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
						},
					),
				})
			}

			err := b.UpdateFindings(map[string]any{}, map[string]any{"Text": "note"}, "ACTIVE")
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_GetInsights_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{name: "get all insights", maxResults: 100, wantLen: 2, wantToken: false},
		{name: "paginate with max 1", maxResults: 1, wantLen: 1, wantToken: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			_, err := b.CreateInsight("Insight1", "SeverityLabel", map[string]any{})
			require.NoError(t, err)
			_, err = b.CreateInsight("Insight2", "Type", map[string]any{})
			require.NoError(t, err)

			results, nextToken, err := b.GetInsights(nil, "", tc.maxResults)
			require.NoError(t, err)
			assert.Len(t, results, tc.wantLen)
			if tc.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestBackend_GetInsights_ByArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "get specific insight by ARN", wantLen: 1},
		{name: "get non-existent ARN returns empty", wantLen: 0},
	}

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.EnableHub(false, nil))
	arn, err := b.CreateInsight("SomeInsight", "SeverityLabel", map[string]any{})
	require.NoError(t, err)

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var arns []string
			if i == 0 {
				arns = []string{arn}
			} else {
				arns = []string{"arn:nonexistent"}
			}
			results, _, err2 := b.GetInsights(arns, "", 100)
			require.NoError(t, err2)
			assert.Len(t, results, tc.wantLen)
		})
	}
}

func TestBackend_UpdateInsight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newFilters map[string]any
		name       string
		newName    string
		newGroupBy string
		wantErrMsg string
		notFound   bool
	}{
		{
			name:    "update name",
			newName: "NewName",
		},
		{
			name:       "update groupByAttribute",
			newGroupBy: "Type",
		},
		{
			name:       "update filters",
			newFilters: map[string]any{"SeverityLabel": []any{"HIGH"}},
		},
		{
			name:       "not found returns error",
			notFound:   true,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var insightArn string
			if !tc.notFound {
				var err error
				insightArn, err = b.CreateInsight("Original", "SeverityLabel", map[string]any{})
				require.NoError(t, err)
			} else {
				insightArn = "arn:aws:securityhub:us-east-1:000000000000:insight/x/99"
			}

			err := b.UpdateInsight(insightArn, tc.newName, tc.newGroupBy, tc.newFilters)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_GetInsightResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preCreate  bool
	}{
		{name: "get results for existing insight", preCreate: true},
		{name: "get results for missing insight", preCreate: false, wantErrMsg: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var insightArn string
			if tc.preCreate {
				var err error
				insightArn, err = b.CreateInsight("TestInsight", "SeverityLabel", map[string]any{})
				require.NoError(t, err)
			} else {
				insightArn = "arn:aws:securityhub:us-east-1:000000000000:insight/x/999"
			}

			result, err := b.GetInsightResults(insightArn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, insightArn, result.InsightArn)
				assert.Equal(t, "SeverityLabel", result.GroupByAttribute)
			}
		})
	}
}

func TestBackend_DeleteInsight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preCreate  bool
	}{
		{name: "delete existing insight", preCreate: true},
		{
			name:       "delete non-existent insight returns error",
			preCreate:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var insightArn string
			if tc.preCreate {
				var err error
				insightArn, err = b.CreateInsight("ToDelete", "SeverityLabel", map[string]any{})
				require.NoError(t, err)
			} else {
				insightArn = "arn:aws:securityhub:us-east-1:000000000000:insight/x/999"
			}

			returnedArn, err := b.DeleteInsight(insightArn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, insightArn, returnedArn)
				assert.Equal(t, 0, securityhub.InsightCount(b))
			}
		})
	}
}

func TestBackend_MatchesStringFilter(t *testing.T) {
	t.Parallel()

	// Tested indirectly through GetFindings with filters
	tests := []struct {
		filter    map[string]any
		name      string
		wantFound bool
	}{
		{
			name:      "no filter returns all",
			filter:    map[string]any{},
			wantFound: true,
		},
		{
			name: "filter by ID equals match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "filter-finding-1", "Comparison": "EQUALS"},
				},
			},
			wantFound: true,
		},
		{
			name: "filter by ID equals no match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "wrong-id", "Comparison": "EQUALS"},
				},
			},
			wantFound: false,
		},
		{
			name: "filter by ID NOT_EQUALS includes others",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "other-id", "Comparison": "NOT_EQUALS"},
				},
			},
			wantFound: true,
		},
		{
			name: "filter by ID NOT_EQUALS excludes match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "filter-finding-1", "Comparison": "NOT_EQUALS"},
				},
			},
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(
					map[string]any{
						"Id":         "filter-finding-1",
						"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
					},
				),
			})

			results, _ := b.GetFindings(tc.filter, nil, "", 100)
			if tc.wantFound {
				assert.NotEmpty(t, results)
			} else {
				assert.Empty(t, results)
			}
		})
	}
}

// --- Handler endpoint tests ---

func TestHandler_UpdateActionTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update existing action target", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(
				t,
				h,
				http.MethodPost,
				"/accounts",
				map[string]any{"EnableDefaultStandards": false},
			)
			doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
				"Name":        "MyTarget",
				"Description": "desc",
				"Id":          "my-target",
			})

			rec := doRequest(
				t,
				h,
				http.MethodPatch,
				"/actionTargets/arn:aws:securityhub:us-east-1:000000000000:action/custom/my-target",
				map[string]any{
					"Name":        "Updated",
					"Description": "New desc",
				},
			)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_DisableImportFindingsForProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "disable product import", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(
				t,
				h,
				http.MethodPost,
				"/accounts",
				map[string]any{"EnableDefaultStandards": false},
			)

			// Enable first
			enableRec := doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
				"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
			})
			require.Equal(t, http.StatusOK, enableRec.Code)

			var enableResp map[string]any
			require.NoError(t, json.Unmarshal(enableRec.Body.Bytes(), &enableResp))
			subArn, _ := enableResp["ProductSubscriptionArn"].(string)
			require.NotEmpty(t, subArn)

			// Now disable
			rec := doRequest(t, h, http.MethodDelete, "/productSubscriptions/"+subArn, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_UpdateSecurityControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update security control parameters", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPatch, "/securityControl/update", map[string]any{
				"SecurityControlId": "CloudTrail.1",
				"Parameters":        map[string]any{"key": "value"},
				"LastUpdateReason":  "Testing",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_BatchUpdateAutomationRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "batch update automation rules", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a rule first
			createRec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
				"RuleName":   "TestRule",
				"RuleStatus": "ENABLED",
				"RuleOrder":  float64(1),
				"Criteria":   map[string]any{},
				"Actions":    []any{},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			ruleArn, _ := createResp["RuleArn"].(string)
			require.NotEmpty(t, ruleArn)

			rec := doRequest(t, h, http.MethodPatch, "/automationrules/update", map[string]any{
				"UpdateAutomationRulesRequestItems": []any{
					map[string]any{
						"RuleArn":     ruleArn,
						"RuleName":    "UpdatedRule",
						"Description": "Updated",
					},
				},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeclineDeleteInvitations(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "decline invitations",
			steps: []step{
				{
					name:   "create member",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "999999000001", "Email": "inv@test.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite member",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"999999000001"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "decline invitation",
					method: http.MethodPost,
					path:   "/invitations/decline",
					body:   map[string]any{"AccountIds": []any{"999999000001"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						unproc, _ := resp["UnprocessedAccounts"].([]any)
						assert.Empty(t, unproc)
					},
				},
			},
		},
		{
			name: "delete invitations",
			steps: []step{
				{
					name:   "create member",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "999999000002", "Email": "del@test.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite member",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"999999000002"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "delete invitation",
					method: http.MethodPost,
					path:   "/invitations/delete",
					body:   map[string]any{"AccountIds": []any{"999999000002"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						unproc, _ := resp["UnprocessedAccounts"].([]any)
						assert.Empty(t, unproc)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

func TestHandler_UpdateAggregatorV2(t *testing.T) {
	t.Parallel()

	t.Run("create then update AggregatorV2", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, "/aggregatorv2/create", map[string]any{
			"RegionLinkingMode": "ALL_REGIONS",
			"Regions":           []any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		arn := resp["AggregatorV2Arn"].(string)

		rec2 := doRequest(t, h, http.MethodPatch, "/aggregatorv2/update/"+arn, map[string]any{
			"RegionLinkingMode": "SPECIFIED_REGIONS",
			"Regions":           []any{"us-west-2"},
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var resp2 map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
		require.Equal(t, "SPECIFIED_REGIONS", resp2["RegionLinkingMode"])
	})

	t.Run("delete AggregatorV2", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, "/aggregatorv2/create", map[string]any{
			"RegionLinkingMode": "ALL_REGIONS",
			"Regions":           []any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		arn := resp["AggregatorV2Arn"].(string)

		rec2 := doRequest(t, h, http.MethodDelete, "/aggregatorv2/delete/"+arn, nil)
		require.Equal(t, http.StatusOK, rec2.Code)

		rec3 := doRequest(t, h, http.MethodGet, "/aggregatorv2/get/"+arn, nil)
		require.Equal(t, http.StatusNotFound, rec3.Code)
	})
}

func TestHandler_DisableSecurityHubV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preEnable bool
		wantCode  int
	}{
		{
			name:      "disable after enable succeeds",
			preEnable: true,
			wantCode:  http.StatusOK,
		},
		{
			name:      "disable without enable returns 400",
			preEnable: false,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.preEnable {
				doRequest(t, h, http.MethodPost, "/hubv2", map[string]any{})
			}

			rec := doRequest(t, h, http.MethodDelete, "/hubv2", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_ConnectorV2_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "update non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPatch, "/connectorsv2/nonexistent-id", map[string]any{
				"Name": "Updated",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_ConnectorV2_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "delete non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/connectorsv2/nonexistent-id", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_RegisterConnectorV2_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "register non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/connectorsv2/register", map[string]any{
				"ConnectorId": "nonexistent-connector",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetFindingsV2_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCode   int
	}{
		{name: "default max results", maxResults: 0, wantCode: http.StatusOK},
		{name: "explicit max results", maxResults: 10, wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := map[string]any{}
			if tc.maxResults > 0 {
				body["MaxResults"] = tc.maxResults
			}

			rec := doRequest(t, h, http.MethodPost, "/findingsv2", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetFindingsV2_InvalidNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantCode  int
	}{
		{name: "valid next token", nextToken: "", wantCode: http.StatusOK},
		{
			name:      "non-numeric next token falls back",
			nextToken: "notanumber",
			wantCode:  http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := map[string]any{}
			if tc.nextToken != "" {
				body["NextToken"] = tc.nextToken
			}

			rec := doRequest(t, h, http.MethodPost, "/findingsv2", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_AutomationRuleV2_GetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "get non-existent automation rule V2", wantCode: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/automationrulesv2/nonexistent-rule", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_AutomationRuleV2_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update non-existent automation rule V2", wantCode: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(
				t,
				h,
				http.MethodPatch,
				"/automationrulesv2/nonexistent-rule",
				map[string]any{
					"RuleName": "Updated",
				},
			)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_AutomationRuleV2_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete non-existent automation rule V2", wantCode: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/automationrulesv2/nonexistent-rule", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeProductsV2_WithFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		wantCode int
	}{
		{name: "list all products V2", query: "", wantCode: http.StatusOK},
		{
			name:     "filter by product ARN",
			query:    "?ProductArn=arn:aws:securityhub:us-east-1::product/aws/guardduty",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/productsV2"+tc.query, nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			products, _ := resp["Products"].([]any)
			assert.NotNil(t, products)
		})
	}
}

func TestHandler_HubV2_EnableWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "enable with tags",
			body:     map[string]any{"Tags": map[string]any{"env": "test"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "enable without tags",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/hubv2", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
