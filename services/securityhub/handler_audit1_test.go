package securityhub_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

func newTestHandler(t *testing.T) *securityhub.Handler {
	t.Helper()
	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	return securityhub.NewHandler(b)
}

func doRequest(t *testing.T, h *securityhub.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/securityhub/aws4_request")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- Hub accuracy tests ---

// Batch-1 accuracy gap: EnableSecurityHub is POST /accounts (not POST /hub or POST /securityhub).
func TestBatch1_EnableSecurityHubPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/accounts", map[string]any{
		"EnableDefaultStandards": true,
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// EnableSecurityHub returns an empty object, not a hub object
	assert.NotContains(t, resp, "HubArn", "EnableSecurityHub must return empty body")
}

// Batch-1 accuracy gap: DescribeHub is GET /accounts (not GET /hub).
func TestBatch1_DescribeHubIsGETAccounts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// First enable
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodGet, "/accounts", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "HubArn")
	assert.Contains(t, resp, "SubscribedAt")
	assert.Contains(t, resp, "AutoEnableControls")
	assert.Contains(t, resp, "AutoEnableStandards")
	assert.Contains(t, resp, "ControlFindingGenerator")

	hubArn, _ := resp["HubArn"].(string)
	assert.Contains(t, hubArn, "arn:aws:securityhub:")
	assert.Contains(t, hubArn, ":hub/default")
}

// Batch-1 accuracy gap: DescribeHub when not enabled returns 400 (not 404).
func TestBatch1_DescribeHubNotEnabledReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/accounts", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Batch-1 accuracy gap: DisableSecurityHub is DELETE /accounts.
func TestBatch1_DisableSecurityHubPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodDelete, "/accounts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// After disable, DescribeHub should return 400
	rec2 := doRequest(t, h, http.MethodGet, "/accounts", nil)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// Batch-1 accuracy gap: UpdateSecurityHubConfiguration is PATCH /accounts.
func TestBatch1_UpdateSecurityHubConfigurationIsPATCHAccounts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPatch, "/accounts", map[string]any{
		"AutoEnableControls":      false,
		"AutoEnableStandards":     "NONE",
		"ControlFindingGenerator": "STANDARD_CONTROL",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the change
	rec2 := doRequest(t, h, http.MethodGet, "/accounts", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	assert.Equal(t, false, resp["AutoEnableControls"])
	assert.Equal(t, "NONE", resp["AutoEnableStandards"])
	assert.Equal(t, "STANDARD_CONTROL", resp["ControlFindingGenerator"])
}

// Batch-1 accuracy gap: HubArn format is arn:aws:securityhub:{region}:{account}:hub/default.
func TestBatch1_HubArnFormat(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("123456789012", "eu-west-1")
	h := securityhub.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodGet, "/accounts", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	hubArn, _ := resp["HubArn"].(string)
	assert.Equal(t, "arn:aws:securityhub:eu-west-1:123456789012:hub/default", hubArn)
}

// --- Findings accuracy tests ---

// Batch-1 accuracy gap: GetFindings is POST /findings (body with Filters).
func TestBatch1_GetFindingsIsPOSTFindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
		"Filters": map[string]any{},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasFindings := resp["Findings"]
	assert.True(t, hasFindings, "GetFindings must return 'Findings' key")
}

// Batch-1 accuracy gap: BatchImportFindings is POST /findings/import (not POST /findings).
func TestBatch1_BatchImportFindingsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []map[string]any{
			{
				"SchemaVersion": "2018-10-08",
				"Id":            "finding-1",
				"ProductArn":    "arn:aws:securityhub:us-east-1::product/aws/guardduty",
				"GeneratorId":   "test-generator",
				"AwsAccountId":  "000000000000",
				"Types":         []string{"Software and Configuration Checks"},
				"CreatedAt":     "2024-01-01T00:00:00Z",
				"UpdatedAt":     "2024-01-01T00:00:00Z",
				"Severity":      map[string]any{"Label": "HIGH"},
				"Title":         "Test Finding",
				"Description":   "Test finding description",
				"Resources":     []map[string]any{{"Type": "AwsEc2Instance", "Id": "i-1234567890abcdef0"}},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "SuccessCount")
	assert.Contains(t, resp, "FailedCount")
	assert.Contains(t, resp, "FailedFindings")

	assert.InDelta(t, float64(1), resp["SuccessCount"], 0.01)
	assert.InDelta(t, float64(0), resp["FailedCount"], 0.01)
}

// Batch-1 accuracy gap: BatchUpdateFindings is PATCH /findings/batchupdate (not PATCH /findings).
func TestBatch1_BatchUpdateFindingsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Import a finding first
	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []map[string]any{
			{
				"SchemaVersion": "2018-10-08",
				"Id":            "finding-batch-update",
				"ProductArn":    "arn:aws:securityhub:us-east-1::product/aws/guardduty",
				"GeneratorId":   "gen",
				"AwsAccountId":  "000000000000",
				"Types":         []string{"Software and Configuration Checks"},
				"CreatedAt":     "2024-01-01T00:00:00Z",
				"UpdatedAt":     "2024-01-01T00:00:00Z",
				"Severity":      map[string]any{"Label": "LOW"},
				"Title":         "Batch Update Test",
				"Description":   "desc",
				"Resources":     []map[string]any{{"Type": "AwsS3Bucket", "Id": "my-bucket"}},
			},
		},
	})

	rec := doRequest(t, h, http.MethodPatch, "/findings/batchupdate", map[string]any{
		"FindingIdentifiers": []map[string]any{
			{
				"Id":         "finding-batch-update",
				"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
			},
		},
		"Workflow": map[string]any{"Status": "NOTIFIED"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "ProcessedFindings")
	assert.Contains(t, resp, "UnprocessedFindings")
}

// Batch-1 accuracy gap: UpdateFindings is PATCH /findings (with Filters/Note/RecordState).
func TestBatch1_UpdateFindingsIsPATCHFindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPatch, "/findings", map[string]any{
		"Filters":     map[string]any{},
		"RecordState": "ARCHIVED",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: GetFindingHistory is POST /findingHistory/get.
func TestBatch1_GetFindingHistoryPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findingHistory/get", map[string]any{
		"FindingIdentifier": map[string]any{
			"Id":         "finding-1",
			"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "Records")
}

// --- Insights accuracy tests ---

// Batch-1 accuracy gap: CreateInsight is POST /insights, returns InsightArn.
func TestBatch1_CreateInsightPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/insights", map[string]any{
		"Name":             "test-insight",
		"GroupByAttribute": "ResourceType",
		"Filters":          map[string]any{},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, _ := resp["InsightArn"].(string)
	assert.NotEmpty(t, arn)
	assert.Contains(t, arn, "arn:aws:securityhub:")
	assert.Contains(t, arn, ":insight/")
}

// Batch-1 accuracy gap: GetInsights is POST /insights/get (not GET /insights).
func TestBatch1_GetInsightsIsPOSTInsightsGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/insights/get", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "Insights")
}

// Batch-1 accuracy gap: GetInsightResults is GET /insights/results/{InsightArn}.
func TestBatch1_GetInsightResultsIsGETInsightsResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	// Create an insight
	createRec := doRequest(t, h, http.MethodPost, "/insights", map[string]any{
		"Name":             "result-test",
		"GroupByAttribute": "SeverityLabel",
		"Filters":          map[string]any{},
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	insightArn, _ := createResp["InsightArn"].(string)
	require.NotEmpty(t, insightArn)

	rec := doRequest(t, h, http.MethodGet, "/insights/results/"+insightArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Response is wrapped in InsightResults key
	insightResults, hasWrapper := resp["InsightResults"]
	assert.True(t, hasWrapper, "GetInsightResults must return 'InsightResults' wrapper")

	if resultsMap, ok := insightResults.(map[string]any); ok {
		assert.Contains(t, resultsMap, "InsightArn")
		assert.Contains(t, resultsMap, "GroupByAttribute")
		assert.Contains(t, resultsMap, "ResultValues")
	}
}

// Batch-1 accuracy gap: UpdateInsight is PATCH /insights/{InsightArn}.
func TestBatch1_UpdateInsightIsPATCHInsightsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	createRec := doRequest(t, h, http.MethodPost, "/insights", map[string]any{
		"Name":             "to-update",
		"GroupByAttribute": "ResourceType",
		"Filters":          map[string]any{},
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	insightArn, _ := createResp["InsightArn"].(string)
	require.NotEmpty(t, insightArn)

	rec := doRequest(t, h, http.MethodPatch, "/insights/"+insightArn, map[string]any{
		"Name": "updated-name",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: DeleteInsight is DELETE /insights/{InsightArn}, returns InsightArn.
func TestBatch1_DeleteInsightIsDELETEInsightsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	createRec := doRequest(t, h, http.MethodPost, "/insights", map[string]any{
		"Name":             "to-delete",
		"GroupByAttribute": "ResourceType",
		"Filters":          map[string]any{},
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	insightArn, _ := createResp["InsightArn"].(string)
	require.NotEmpty(t, insightArn)

	rec := doRequest(t, h, http.MethodDelete, "/insights/"+insightArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deletedArn, _ := resp["InsightArn"].(string)
	assert.Equal(t, insightArn, deletedArn, "DeleteInsight must return the deleted InsightArn")
}

// --- Standards accuracy tests ---

// Batch-1 accuracy gap: BatchEnableStandards is POST /standards/register.
func TestBatch1_BatchEnableStandardsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []map[string]any{
			{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	subs, _ := resp["StandardsSubscriptions"].([]any)
	assert.Len(t, subs, 1)

	sub := subs[0].(map[string]any)
	assert.NotEmpty(t, sub["StandardsSubscriptionArn"])
	assert.Equal(
		t,
		"arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
		sub["StandardsArn"],
	)
	assert.Equal(t, "PENDING", sub["StandardsStatus"])
}

// Batch-1 accuracy gap: BatchDisableStandards is POST /standards/deregister.
func TestBatch1_BatchDisableStandardsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Enable first
	enableRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []map[string]any{
			{"StandardsArn": "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0"},
		},
	})

	var enableResp map[string]any
	require.NoError(t, json.Unmarshal(enableRec.Body.Bytes(), &enableResp))

	subs, _ := enableResp["StandardsSubscriptions"].([]any)
	require.Len(t, subs, 1)
	subArn, _ := subs[0].(map[string]any)["StandardsSubscriptionArn"].(string)

	rec := doRequest(t, h, http.MethodPost, "/standards/deregister", map[string]any{
		"StandardsSubscriptionArns": []string{subArn},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	disabledSubs, _ := resp["StandardsSubscriptions"].([]any)
	assert.Len(t, disabledSubs, 1)

	sub := disabledSubs[0].(map[string]any)
	assert.Equal(t, "INCOMPLETE", sub["StandardsStatus"])
}

// Batch-1 accuracy gap: GetEnabledStandards is POST /standards/get.
func TestBatch1_GetEnabledStandardsIsPOSTStandardsGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []map[string]any{
			{"StandardsArn": "arn:aws:securityhub:us-east-1::standards/pci-dss/v/3.2.1"},
		},
	})

	rec := doRequest(t, h, http.MethodPost, "/standards/get", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "StandardsSubscriptions")
}

// Batch-1 accuracy gap: DescribeStandards is GET /standards.
func TestBatch1_DescribeStandardsIsGETStandards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/standards", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	standards, _ := resp["Standards"].([]any)
	assert.NotEmpty(t, standards, "DescribeStandards must return built-in standards")

	for _, s := range standards {
		sm := s.(map[string]any)
		assert.NotEmpty(t, sm["StandardsArn"])
		assert.NotEmpty(t, sm["Name"])
		_, hasEnabledByDefault := sm["EnabledByDefault"]
		assert.True(t, hasEnabledByDefault)
	}
}

// Batch-1 accuracy gap: DescribeStandardsControls is GET /standards/controls/{StandardsSubscriptionArn}.
func TestBatch1_DescribeStandardsControlsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	enableRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []map[string]any{
			{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})

	var enableResp map[string]any
	require.NoError(t, json.Unmarshal(enableRec.Body.Bytes(), &enableResp))

	subs, _ := enableResp["StandardsSubscriptions"].([]any)
	subArn, _ := subs[0].(map[string]any)["StandardsSubscriptionArn"].(string)

	rec := doRequest(t, h, http.MethodGet, "/standards/controls/"+subArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	controls, _ := resp["Controls"].([]any)
	assert.NotEmpty(t, controls)

	ctrl := controls[0].(map[string]any)
	assert.NotEmpty(t, ctrl["StandardsControlArn"])
	assert.NotEmpty(t, ctrl["ControlStatus"])
	assert.NotEmpty(t, ctrl["Title"])
}

// Batch-1 accuracy gap: UpdateStandardsControl is PATCH /standards/control/{StandardsControlArn}.
func TestBatch1_UpdateStandardsControlPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	enableRec := doRequest(t, h, http.MethodPost, "/standards/register", map[string]any{
		"StandardsSubscriptionRequests": []map[string]any{
			{
				"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})

	var enableResp map[string]any
	require.NoError(t, json.Unmarshal(enableRec.Body.Bytes(), &enableResp))

	subs, _ := enableResp["StandardsSubscriptions"].([]any)
	subArn, _ := subs[0].(map[string]any)["StandardsSubscriptionArn"].(string)
	controlArn := subArn + "/control/1"

	rec := doRequest(t, h, http.MethodPatch, "/standards/control/"+controlArn, map[string]any{
		"ControlStatus":  "DISABLED",
		"DisabledReason": "Not applicable to our environment",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Action Target accuracy tests ---

// Batch-1 accuracy gap: CreateActionTarget is POST /actionTargets (not POST /action-targets).
func TestBatch1_CreateActionTargetPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "Send to Slack",
		"Description": "Sends finding to Slack channel",
		"Id":          "send-to-slack",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, _ := resp["ActionTargetArn"].(string)
	assert.NotEmpty(t, arn)
	assert.Contains(t, arn, "arn:aws:securityhub:")
	assert.Contains(t, arn, ":action/custom/")
}

// Batch-1 accuracy gap: DescribeActionTargets is POST /actionTargets/get.
func TestBatch1_DescribeActionTargetsIsPOSTActionTargetsGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)
	doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "My Action",
		"Description": "desc",
		"Id":          "my-action",
	})

	rec := doRequest(t, h, http.MethodPost, "/actionTargets/get", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	targets, _ := resp["ActionTargets"].([]any)
	assert.Len(t, targets, 1)

	t0 := targets[0].(map[string]any)
	assert.Equal(t, "My Action", t0["Name"])
}

// Batch-1 accuracy gap: DeleteActionTarget is DELETE /actionTargets/{ActionTargetArn}.
func TestBatch1_DeleteActionTargetIsDELETEActionTargetsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	createRec := doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "To Delete",
		"Description": "desc",
		"Id":          "to-delete",
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	arn, _ := createResp["ActionTargetArn"].(string)

	rec := doRequest(t, h, http.MethodDelete, "/actionTargets/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deletedArn, _ := resp["ActionTargetArn"].(string)
	assert.Equal(t, arn, deletedArn, "DeleteActionTarget must return the deleted ActionTargetArn")
}

// --- Products accuracy tests ---

// Batch-1 accuracy gap: DescribeProducts is GET /products.
func TestBatch1_DescribeProductsIsGETProducts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/products", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	products, _ := resp["Products"].([]any)
	assert.NotEmpty(t, products)

	p0 := products[0].(map[string]any)
	assert.NotEmpty(t, p0["ProductArn"])
	assert.NotEmpty(t, p0["ProductName"])
	assert.NotEmpty(t, p0["CompanyName"])
}

// Batch-1 accuracy gap: EnableImportFindingsForProduct is POST /productSubscriptions.
func TestBatch1_EnableImportFindingsForProductPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	subArn, _ := resp["ProductSubscriptionArn"].(string)
	assert.NotEmpty(t, subArn)
}

// Batch-1 accuracy gap: ListEnabledProductsForImport is GET /productSubscriptions.
func TestBatch1_ListEnabledProductsForImportIsGETProductSubscriptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)
	doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/inspector",
	})

	rec := doRequest(t, h, http.MethodGet, "/productSubscriptions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	subs, _ := resp["ProductSubscriptions"].([]any)
	assert.Len(t, subs, 1)
}

// --- Security Controls accuracy tests ---

// Batch-1 accuracy gap: ListSecurityControlDefinitions is GET /securityControls/definitions.
func TestBatch1_ListSecurityControlDefinitionsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/securityControls/definitions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	defs, _ := resp["SecurityControlDefinitions"].([]any)
	assert.NotEmpty(t, defs)

	d0 := defs[0].(map[string]any)
	assert.NotEmpty(t, d0["SecurityControlId"])
	assert.NotEmpty(t, d0["Title"])
	assert.NotEmpty(t, d0["SeverityRating"])
}

// Batch-1 accuracy gap: GetSecurityControlDefinition is GET /securityControl/definition?SecurityControlId=...
func TestBatch1_GetSecurityControlDefinitionPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/securityControl/definition?SecurityControlId=CloudTrail.1", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	def, _ := resp["SecurityControlDefinition"].(map[string]any)
	assert.Equal(t, "CloudTrail.1", def["SecurityControlId"])
}

// Batch-1 accuracy gap: BatchGetSecurityControls is POST /securityControls/batchGet.
func TestBatch1_BatchGetSecurityControlsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/securityControls/batchGet", map[string]any{
		"SecurityControlIds": []string{"IAM.1", "S3.1"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	controls, _ := resp["SecurityControls"].([]any)
	assert.Len(t, controls, 2)

	for _, ctrl := range controls {
		cm := ctrl.(map[string]any)
		assert.NotEmpty(t, cm["SecurityControlId"])
		assert.NotEmpty(t, cm["SecurityControlArn"])
		assert.Contains(t, cm["SecurityControlArn"].(string), "arn:aws:securityhub:")
		assert.Contains(t, cm["SecurityControlArn"].(string), ":security-control/")
		assert.Equal(t, "ENABLED", cm["SecurityControlStatus"])
	}
}

// --- Automation Rules accuracy tests ---

// Batch-1 accuracy gap: CreateAutomationRule is POST /automationrules/create, returns RuleArn.
func TestBatch1_CreateAutomationRulePath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":    "auto-archive-low",
		"Description": "Archive LOW severity findings",
		"RuleOrder":   1,
		"RuleStatus":  "ENABLED",
		"Criteria":    map[string]any{},
		"Actions":     []map[string]any{{"Type": "FINDING_FIELDS_UPDATE"}},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	ruleArn, _ := resp["RuleArn"].(string)
	assert.NotEmpty(t, ruleArn)
	assert.Contains(t, ruleArn, "arn:aws:securityhub:")
	assert.Contains(t, ruleArn, ":automation-rule/")
	assert.Contains(t, resp, "CreatedAt")
}

// Batch-1 accuracy gap: ListAutomationRules is GET /automationrules/list.
func TestBatch1_ListAutomationRulesIsGETAutomationrulesList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":   "rule-1",
		"RuleOrder":  1,
		"RuleStatus": "ENABLED",
		"Criteria":   map[string]any{},
		"Actions":    []map[string]any{},
	})

	rec := doRequest(t, h, http.MethodGet, "/automationrules/list", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rules, _ := resp["AutomationRulesMetadata"].([]any)
	assert.Len(t, rules, 1)

	r0 := rules[0].(map[string]any)
	assert.NotEmpty(t, r0["RuleArn"])
	assert.Equal(t, "rule-1", r0["RuleName"])
	assert.Equal(t, "ENABLED", r0["RuleStatus"])
}

// Batch-1 accuracy gap: BatchGetAutomationRules is POST /automationrules/get.
func TestBatch1_BatchGetAutomationRulesPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":   "batch-get-test",
		"RuleOrder":  1,
		"RuleStatus": "ENABLED",
		"Criteria":   map[string]any{"SeverityLabel": []any{map[string]any{"Value": "HIGH", "Comparison": "EQUALS"}}},
		"Actions":    []map[string]any{},
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ruleArn, _ := createResp["RuleArn"].(string)

	rec := doRequest(t, h, http.MethodPost, "/automationrules/get", map[string]any{
		"AutomationRulesArns": []string{ruleArn},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rules, _ := resp["Rules"].([]any)
	assert.Len(t, rules, 1)

	r0 := rules[0].(map[string]any)
	assert.Equal(t, ruleArn, r0["RuleArn"])
	assert.Contains(t, r0, "Criteria")
	assert.Contains(t, r0, "Actions")
}

// Batch-1 accuracy gap: BatchDeleteAutomationRules is POST /automationrules/delete.
func TestBatch1_BatchDeleteAutomationRulesPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/automationrules/create", map[string]any{
		"RuleName":   "to-delete-rule",
		"RuleOrder":  1,
		"RuleStatus": "ENABLED",
		"Criteria":   map[string]any{},
		"Actions":    []map[string]any{},
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ruleArn, _ := createResp["RuleArn"].(string)

	rec := doRequest(t, h, http.MethodPost, "/automationrules/delete", map[string]any{
		"AutomationRulesArns": []string{ruleArn},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "ProcessedAutomationRules")
	assert.Contains(t, resp, "UnprocessedAutomationRules")

	processed, _ := resp["ProcessedAutomationRules"].([]any)
	assert.Len(t, processed, 1)
}

// --- Tags accuracy tests ---

// Batch-1 accuracy gap: TagResource is POST /tags/{ResourceArn}.
func TestBatch1_TagResourcePath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	rec := doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "test", "team": "security"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: ListTagsForResource is GET /tags/{ResourceArn}.
func TestBatch1_ListTagsForResourceIsGETTagsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "prod", "cost-center": "security"},
	})

	rec := doRequest(t, h, http.MethodGet, "/tags/"+testArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "security", tags["cost-center"])
}

// Batch-1 accuracy gap: UntagResource is DELETE /tags/{ResourceArn}?tagKeys=k1&tagKeys=k2.
func TestBatch1_UntagResourceIsDELETETagsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "test", "team": "security"},
	})

	req := httptest.NewRequest(http.MethodDelete, "/tags/"+testArn+"?tagKeys=env", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag removed
	getRec := doRequest(t, h, http.MethodGet, "/tags/"+testArn, nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].(map[string]any)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "env tag must be removed")
	assert.Equal(t, "security", tags["team"], "team tag must remain")
}

// --- Route matcher accuracy tests ---

// Batch-1 accuracy gap: RouteMatcher must match SecurityHub-specific paths.
func TestBatch1_RouteMatcherMatchesSecurityHubPaths(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	securityHubPaths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/accounts"},
		{http.MethodGet, "/accounts"},
		{http.MethodDelete, "/accounts"},
		{http.MethodPatch, "/accounts"},
		{http.MethodPost, "/insights"},
		{http.MethodPost, "/insights/get"},
		{http.MethodGet, "/insights/results/arn:aws:securityhub:us-east-1:000000000000:insight/000000000000/1"},
		{http.MethodPost, "/actionTargets"},
		{http.MethodPost, "/actionTargets/get"},
		{http.MethodGet, "/standards"},
		{http.MethodPost, "/standards/register"},
		{http.MethodPost, "/standards/deregister"},
		{http.MethodPost, "/standards/get"},
		{http.MethodGet, "/associations"},
		{http.MethodGet, "/products"},
		{http.MethodGet, "/productSubscriptions"},
		{http.MethodPost, "/productSubscriptions"},
		{http.MethodGet, "/securityControls/definitions"},
		{http.MethodGet, "/automationrules/list"},
		{http.MethodPost, "/automationrules/create"},
		{http.MethodPost, "/automationrules/get"},
		{http.MethodPost, "/automationrules/delete"},
	}

	for _, tt := range securityHubPaths {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.True(t, matcher(c), "must match %s %s", tt.method, tt.path)
		})
	}
}

// Batch-1 accuracy gap: RouteMatcher for /tags/ only matches SecurityHub ARNs.
func TestBatch1_RouteMatcherTagsOnlyMatchesSecurityHubARNs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	tests := []struct {
		path string
		want bool
	}{
		{"/tags/arn:aws:securityhub:us-east-1:000000000000:hub/default", true},
		{"/tags/arn:aws:securityhub:us-east-1:000000000000:insight/000000000000/1", true},
		{"/tags/arn:aws:macie2:us-east-1:000000000000:allow-list/id", false},
		{"/tags/arn:aws:guardduty:us-east-1:000000000000:detector/id", false},
		{"/tags/arn:aws:s3:::my-bucket", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// Batch-1 accuracy gap: EnableDefaultStandards=true auto-enables default standards.
func TestBatch1_EnableDefaultStandardsAutoEnablesDefaults(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/accounts", map[string]any{
		"EnableDefaultStandards": true,
	})

	count := securityhub.StandardsSubscriptionCount(b)
	assert.Positive(t, count, "enabling with EnableDefaultStandards=true must auto-enable default standards")
}

// Batch-1 accuracy gap: EnableDefaultStandards=false must NOT auto-enable standards.
func TestBatch1_EnableDefaultStandardsFalseNoAutoEnable(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/accounts", map[string]any{
		"EnableDefaultStandards": false,
	})

	count := securityhub.StandardsSubscriptionCount(b)
	assert.Equal(t, 0, count, "enabling with EnableDefaultStandards=false must NOT auto-enable standards")
}

// Batch-1 accuracy gap: ListStandardsControlAssociations is GET /associations.
func TestBatch1_ListStandardsControlAssociationsIsGETAssociations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/associations?SecurityControlId=IAM.1", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assocs, _ := resp["StandardsControlAssociationSummaries"].([]any)
	assert.NotEmpty(t, assocs)

	a0 := assocs[0].(map[string]any)
	assert.Equal(t, "IAM.1", a0["SecurityControlId"])
	assert.NotEmpty(t, a0["StandardsArn"])
	assert.Equal(t, "ENABLED", a0["AssociationStatus"])
}

// Batch-1 accuracy gap: BatchGetStandardsControlAssociations is POST /associations/batchGet.
func TestBatch1_BatchGetStdCtlAssociationsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/associations/batchGet", map[string]any{
		"StandardsControlAssociationIds": []map[string]any{
			{
				"SecurityControlId": "CloudTrail.1",
				"StandardsArn":      "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "StandardsControlAssociationDetails")
	assert.Contains(t, resp, "UnprocessedAssociations")
}

// Batch-1 accuracy gap: BatchUpdateStandardsControlAssociations is PATCH /associations.
func TestBatch1_BatchUpdateStdCtlAssociationsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPatch, "/associations", map[string]any{
		"StandardsControlAssociationUpdates": []map[string]any{
			{
				"SecurityControlId": "S3.1",
				"StandardsArn":      "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
				"AssociationStatus": "DISABLED",
				"UpdatedReason":     "Not applicable",
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "UnprocessedAssociationUpdates")
}
