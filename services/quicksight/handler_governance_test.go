package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

func approvalPolicyPath(id string) string {
	if id == "" {
		return "/governance/approvalworkflows/policies"
	}

	return "/governance/approvalworkflows/policies/" + id
}

func limitsProfilePath(id string) string {
	base := fmt.Sprintf("/governance/limits/accounts/%s/profiles", testAccountID)
	if id == "" {
		return base
	}

	return base + "/" + id
}

func dlpSettingPath(id string) string {
	if id == "" {
		return accountPath("/data-loss-prevention/settings")
	}

	return accountPath("/data-loss-prevention/settings/" + id)
}

func createApprovalPolicyBody(id string) map[string]any {
	return map[string]any{
		"PolicyId":       id,
		"Name":           "Policy " + id,
		"Description":    "a governed-action approval policy",
		"Actions":        []any{"SHARE"},
		"AssetTypes":     []any{"AGENT"},
		"ApprovalGroups": []any{"arn:aws:quicksight:us-east-1:000000000000:group/default/approvers"},
		"ApplicableTo": map[string]any{
			"Type":      "GROUP",
			"GroupArns": []any{"arn:aws:quicksight:us-east-1:000000000000:group/default/eng"},
		},
	}
}

func createDlpSettingBody() map[string]any {
	return map[string]any{
		"Enabled":              true,
		"Name":                 "My DLP Setting",
		"ProviderType":         "MICROSOFT_PURVIEW",
		"ProviderOutageAction": "BLOCK",
		"ProviderConfig": map[string]any{
			"MicrosoftPurview": map[string]any{
				"Credentials": map[string]any{
					"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:purview-xyz",
				},
				"LabelActionMappings": []any{
					map[string]any{"Action": "BLOCK", "LabelId": "lbl1", "LabelName": "Confidential"},
				},
				"UnmappedAction": "WARN",
			},
		},
	}
}

func createLimitsProfileBody(clientToken string) map[string]any {
	return map[string]any{
		"clientToken": clientToken,
		"profileName": "Profile " + clientToken,
		"description": "a limits profile",
		"resourceLimits": map[string]any{
			"INDEX_STORAGE": map[string]any{"maxValue": float64(100), "unit": "GB"},
		},
	}
}

// ---- Approval Policy CRUD ----

func TestQuickSight_ApprovalPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateApprovalPolicy returns 200 with Policy",
			method:   http.MethodPost,
			path:     approvalPolicyPath(""),
			body:     createApprovalPolicyBody("pol1"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				p, ok := body["Policy"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "pol1", p["PolicyId"])
				assert.Equal(t, "Policy pol1", p["Name"])
				assert.Contains(t, p["PolicyArn"], "approval-policy/pol1")
				at, ok := p["ApplicableTo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "GROUP", at["Type"])
			},
		},
		{
			name:   "CreateApprovalPolicy duplicate returns 409 ConflictException",
			method: http.MethodPost,
			path:   approvalPolicyPath(""),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody("dup"))
			},
			body:     createApprovalPolicyBody("dup"),
			wantCode: http.StatusConflict,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ConflictException", body["Code"])
			},
		},
		{
			name:     "CreateApprovalPolicy missing Name returns 400",
			method:   http.MethodPost,
			path:     approvalPolicyPath(""),
			body:     map[string]any{"PolicyId": "noname"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "InvalidParameterValueException", body["Code"])
			},
		},
		{
			name:     "DescribeApprovalPolicy unknown returns 404",
			method:   http.MethodGet,
			path:     approvalPolicyPath("no-such-policy"),
			wantCode: http.StatusNotFound,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ResourceNotFoundException", body["Code"])
			},
		},
		{
			name:   "DescribeApprovalPolicy returns policy",
			method: http.MethodGet,
			path:   approvalPolicyPath("pol2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody("pol2"))
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				p, ok := body["Policy"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "pol2", p["PolicyId"])
				actions, ok := p["Actions"].([]any)
				require.True(t, ok)
				assert.Equal(t, []any{"SHARE"}, actions)
			},
		},
		{
			name:   "UpdateApprovalPolicy replaces Name and Description",
			method: http.MethodPatch,
			path:   approvalPolicyPath("pol3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody("pol3"))
			},
			body:     map[string]any{"Name": "Renamed", "Description": "new description"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				p, ok := body["Policy"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Renamed", p["Name"])
				assert.Equal(t, "new description", p["Description"])
				// Actions must survive an update that doesn't mention them.
				actions, ok := p["Actions"].([]any)
				require.True(t, ok)
				assert.Equal(t, []any{"SHARE"}, actions)
			},
		},
		{
			name:   "DeleteApprovalPolicy returns 200",
			method: http.MethodDelete,
			path:   approvalPolicyPath("pol4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody("pol4"))
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			require.Equal(t, tc.wantCode, rec.Code)
			body := parseBody(t, rec)
			if tc.check != nil {
				tc.check(t, body)
			}
		})
	}
}

func TestQuickSight_DeleteApprovalPolicy_ThenDescribeNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody("gone"))

	recDelete := doRequest(t, h, http.MethodDelete, approvalPolicyPath("gone"), nil)
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe := doRequest(t, h, http.MethodGet, approvalPolicyPath("gone"), nil)
	require.Equal(t, http.StatusNotFound, recDescribe.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, recDescribe)["Code"])
}

func TestQuickSight_ApprovalPoliciesPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 3 {
		id := fmt.Sprintf("pag%d", i)
		rec := doRequest(t, h, http.MethodPost, approvalPolicyPath(""), createApprovalPolicyBody(id))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	first := doRequest(t, h, http.MethodGet, approvalPolicyPath("")+"?max-results=2", nil)
	require.Equal(t, http.StatusOK, first.Code)
	firstBody := parseBody(t, first)
	firstItems, ok := firstBody["Policies"].([]any)
	require.True(t, ok)
	require.Len(t, firstItems, 2)
	nextToken, hasNext := firstBody["NextToken"].(string)
	require.True(t, hasNext)
	require.NotEmpty(t, nextToken)

	second := doRequest(t, h, http.MethodGet, approvalPolicyPath("")+"?max-results=2&next-token="+nextToken, nil)
	require.Equal(t, http.StatusOK, second.Code)
	secondBody := parseBody(t, second)
	secondItems, ok := secondBody["Policies"].([]any)
	require.True(t, ok)
	require.Len(t, secondItems, 1)
	_, hasNext = secondBody["NextToken"]
	assert.False(t, hasNext, "final page must carry no NextToken")
}

// ---- DLP Setting CRUD ----

func TestQuickSight_DlpSettingCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateDlpSetting returns 200 with Arn and DlpSettingId",
			method:   http.MethodPost,
			path:     dlpSettingPath("dlp1"),
			body:     createDlpSettingBody(),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "dlp1", body["DlpSettingId"])
				assert.Contains(t, body["Arn"], "dlp-setting/dlp1")
			},
		},
		{
			name:   "CreateDlpSetting duplicate returns 409 ResourceExistsException",
			method: http.MethodPost,
			path:   dlpSettingPath("dupdlp"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, dlpSettingPath("dupdlp"), createDlpSettingBody())
			},
			body:     createDlpSettingBody(),
			wantCode: http.StatusConflict,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ResourceExistsException", body["Code"])
			},
		},
		{
			name:     "DescribeDlpSetting unknown returns 404",
			method:   http.MethodGet,
			path:     dlpSettingPath("no-such-setting"),
			wantCode: http.StatusNotFound,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ResourceNotFoundException", body["Code"])
			},
		},
		{
			name:   "DescribeDlpSetting returns setting with ACTIVE status",
			method: http.MethodGet,
			path:   dlpSettingPath("dlp2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, dlpSettingPath("dlp2"), createDlpSettingBody())
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				d, ok := body["DlpSetting"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "dlp2", d["DlpSettingId"])
				assert.Equal(t, "ACTIVE", d["Status"])
				assert.Equal(t, "MICROSOFT_PURVIEW", d["ProviderType"])
				pc, ok := d["ProviderConfig"].(map[string]any)
				require.True(t, ok)
				mp, ok := pc["MicrosoftPurview"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "WARN", mp["UnmappedAction"])
			},
		},
		{
			name:   "UpdateDlpSetting disables the setting",
			method: http.MethodPut,
			path:   dlpSettingPath("dlp3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, dlpSettingPath("dlp3"), createDlpSettingBody())
			},
			body:     map[string]any{"Enabled": false},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "dlp3", body["DlpSettingId"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			require.Equal(t, tc.wantCode, rec.Code)
			body := parseBody(t, rec)
			if tc.check != nil {
				tc.check(t, body)
			}
		})
	}
}

func TestQuickSight_UpdateDlpSetting_DisabledReflectsInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, dlpSettingPath("dlp4"), createDlpSettingBody())

	recUpdate := doRequest(t, h, http.MethodPut, dlpSettingPath("dlp4"), map[string]any{"Enabled": false})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doRequest(t, h, http.MethodGet, dlpSettingPath("dlp4"), nil)
	require.Equal(t, http.StatusOK, recDescribe.Code)
	d, ok := parseBody(t, recDescribe)["DlpSetting"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INACTIVE", d["Status"])
}

func TestQuickSight_DeleteDlpSetting_ThenDescribeNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, dlpSettingPath("dlp5"), createDlpSettingBody())

	recDelete := doRequest(t, h, http.MethodDelete, dlpSettingPath("dlp5"), nil)
	require.Equal(t, http.StatusOK, recDelete.Code)
	deleteBody := parseBody(t, recDelete)
	assert.Equal(t, "dlp5", deleteBody["DlpSettingId"])
	assert.Contains(t, deleteBody["Arn"], "dlp-setting/dlp5")

	recDescribe := doRequest(t, h, http.MethodGet, dlpSettingPath("dlp5"), nil)
	require.Equal(t, http.StatusNotFound, recDescribe.Code)
}

func TestQuickSight_DlpSettingsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 3 {
		id := fmt.Sprintf("pagdlp%d", i)
		rec := doRequest(t, h, http.MethodPost, dlpSettingPath(id), createDlpSettingBody())
		require.Equal(t, http.StatusOK, rec.Code)
	}

	first := doRequest(t, h, http.MethodGet, dlpSettingPath("")+"?max-results=2", nil)
	require.Equal(t, http.StatusOK, first.Code)
	firstBody := parseBody(t, first)
	firstItems, ok := firstBody["DlpSettingSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, firstItems, 2)
	nextToken, hasNext := firstBody["NextToken"].(string)
	require.True(t, hasNext)

	second := doRequest(t, h, http.MethodGet, dlpSettingPath("")+"?max-results=2&next-token="+nextToken, nil)
	require.Equal(t, http.StatusOK, second.Code)
	secondItems, ok := parseBody(t, second)["DlpSettingSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, secondItems, 1)
}

// ---- Limits Profile CRUD ----

func TestQuickSight_LimitsProfileCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recCreate := doRequest(
		t, h, http.MethodPost, limitsProfilePath(""), createLimitsProfileBody("tok-a"),
	)
	require.Equal(t, http.StatusOK, recCreate.Code)
	createBody := parseBody(t, recCreate)
	profileID, ok := createBody["profileId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, profileID)
	assert.Contains(t, createBody["arn"], "limits-profile/"+profileID)

	recDescribe := doRequest(t, h, http.MethodGet, limitsProfilePath(profileID), nil)
	require.Equal(t, http.StatusOK, recDescribe.Code)
	p, ok := parseBody(t, recDescribe)["profile"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, profileID, p["profileId"])
	assert.Equal(t, "Profile tok-a", p["profileName"])
	assert.Equal(t, testAccountID, p["accountId"])
	limits, ok := p["resourceLimits"].(map[string]any)
	require.True(t, ok)
	indexStorage, ok := limits["INDEX_STORAGE"].(map[string]any)
	require.True(t, ok)
	assert.InEpsilon(t, float64(100), indexStorage["maxValue"], 0)
	assert.Equal(t, "GB", indexStorage["unit"])

	recUpdate := doRequest(t, h, http.MethodPut, limitsProfilePath(profileID), map[string]any{
		"profileName": "Renamed Profile",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)
	updateBody := parseBody(t, recUpdate)
	assert.Equal(t, createBody["arn"], updateBody["arn"])

	recDescribe2 := doRequest(t, h, http.MethodGet, limitsProfilePath(profileID), nil)
	require.Equal(t, http.StatusOK, recDescribe2.Code)
	p2, ok := parseBody(t, recDescribe2)["profile"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Renamed Profile", p2["profileName"])

	recDelete := doRequest(t, h, http.MethodDelete, limitsProfilePath(profileID), nil)
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe3 := doRequest(t, h, http.MethodGet, limitsProfilePath(profileID), nil)
	require.Equal(t, http.StatusNotFound, recDescribe3.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, recDescribe3)["Code"])
}

func TestQuickSight_CreateLimitsProfile_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	first := doRequest(
		t, h, http.MethodPost, limitsProfilePath(""), createLimitsProfileBody("same-token"),
	)
	require.Equal(t, http.StatusOK, first.Code)
	firstID := parseBody(t, first)["profileId"]

	second := doRequest(
		t, h, http.MethodPost, limitsProfilePath(""), createLimitsProfileBody("same-token"),
	)
	require.Equal(t, http.StatusOK, second.Code)
	secondID := parseBody(t, second)["profileId"]

	assert.Equal(t, firstID, secondID, "repeating ClientToken must return the same profile, not mint a new one")

	list := doRequest(t, h, http.MethodGet, limitsProfilePath(""), nil)
	require.Equal(t, http.StatusOK, list.Code)
	items, ok := parseBody(t, list)["profiles"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1, "idempotent create must not have created a second profile")
}

func TestQuickSight_ListLimitsProfiles_ResourceTypeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, limitsProfilePath(""), map[string]any{
		"clientToken": "storage-tok",
		"profileName": "Storage Profile",
		"resourceLimits": map[string]any{
			"INDEX_STORAGE": map[string]any{"maxValue": float64(50), "unit": "GB"},
		},
	})
	doRequest(t, h, http.MethodPost, limitsProfilePath(""), map[string]any{
		"clientToken": "hours-tok",
		"profileName": "Hours Profile",
		"resourceLimits": map[string]any{
			"AGENT_HOURS": map[string]any{"maxValue": float64(10), "unit": "HOURS"},
		},
	})

	rec := doRequest(t, h, http.MethodGet, limitsProfilePath("")+"?resourceType=AGENT_HOURS", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items, ok := parseBody(t, rec)["profiles"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	p, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Hours Profile", p["profileName"])
}
