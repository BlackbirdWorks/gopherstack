package securityhub_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: BatchEnableStandards is POST /standards/register.
func TestBatchEnableStandardsPath(t *testing.T) {
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
func TestBatchDisableStandardsPath(t *testing.T) {
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
	assert.Equal(t, "DELETING", sub["StandardsStatus"])
}

// Batch-1 accuracy gap: GetEnabledStandards is POST /standards/get.
func TestGetEnabledStandardsIsPOSTStandardsGet(t *testing.T) {
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
func TestDescribeStandardsIsGETStandards(t *testing.T) {
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
func TestDescribeStandardsControlsPath(t *testing.T) {
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
func TestUpdateStandardsControlPath(t *testing.T) {
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

// Batch-1 accuracy gap: EnableDefaultStandards=true auto-enables default standards.
func TestEnableDefaultStandardsAutoEnablesDefaults(t *testing.T) {
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
func TestEnableDefaultStandardsFalseNoAutoEnable(t *testing.T) {
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
func TestListStandardsControlAssociationsIsGETAssociations(t *testing.T) {
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
func TestBatchGetStdCtlAssociationsPath(t *testing.T) {
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
func TestBatchUpdateStdCtlAssociationsPath(t *testing.T) {
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

// TestParity_BatchDisableStandards_StatusIsDeleting verifies the subscription
// status is "DELETING" (not "INCOMPLETE") when BatchDisableStandards is called.
func TestBatchDisableStandards_StatusIsDeleting(t *testing.T) {
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

// TestParity_UpdateStandardsControl_StatusValidation verifies that
// UpdateStandardsControl enforces valid enum values and requires DisabledReason
// when disabling.
func TestUpdateStandardsControl_StatusValidation(t *testing.T) {
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
func TestBatchUpdateStdCtlAssociations_Persisted(t *testing.T) {
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

// TestStandardsSubscriptions_IncludesStandardsStatusReason verifies that
// BatchEnableStandards and GetEnabledStandards responses include
// StandardsStatusReason under its real key (securityhub@v1.75.4
// deserializers.go's StandardsSubscription case list) -- this test
// previously asserted the wrong key ("StatusReason") as correct, which
// passed because the pre-fix handler emitted that same wrong key. This
// backend never sets a subscription's status-reason value (no
// INCOMPLETE/FAILED lifecycle), so the value is always nil regardless of
// the fix; this only guards the key name, not the value.
func TestStandardsSubscriptions_IncludesStandardsStatusReason(t *testing.T) {
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
	// StandardsStatusReason must be present (even if nil/empty)
	_, hasStatusReason := subs[0].(map[string]any)["StandardsStatusReason"]
	assert.True(t, hasStatusReason, "BatchEnableStandards response must include StandardsStatusReason field")

	// GetEnabledStandards
	getRec := doRequest(t, h, http.MethodPost, "/standards/get", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	getSubs, _ := getResp["StandardsSubscriptions"].([]any)
	require.NotEmpty(t, getSubs)
	_, hasStatusReasonGet := getSubs[0].(map[string]any)["StandardsStatusReason"]
	assert.True(t, hasStatusReasonGet, "GetEnabledStandards response must include StandardsStatusReason field")
}

// TestParity_BatchGetStdCtlAssociations_MissingFields verifies that
// BatchGetStandardsControlAssociations includes RelatedRequirements and other
// fields that were previously omitted.
func TestBatchGetStdCtlAssociations_MissingFields(t *testing.T) {
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
