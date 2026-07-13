package support_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/support"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, support.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ support.StorageBackend = (*support.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)
	assert.Equal(t, 16, support.HandlerOpsLen(h))
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_Reset verifies handler and backend Reset.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)

	rec := doSupportRequest(t, h, "CreateCase", map[string]any{"subject": "Test Reset", "communicationBody": "Initial"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, support.CaseCount(b))

	h.Reset()
	assert.Equal(t, 0, support.CaseCount(b))
}

// TestRefinement1_ErrValidationSentinel verifies ErrValidation is exported.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	assert.Error(t, support.ErrValidation)
}

// TestRefinement1_ExportCounts verifies export helpers work correctly.
func TestRefinement1_ExportCounts(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	assert.Equal(t, 0, support.CaseCount(b))
	assert.Equal(t, 0, support.AttachmentCount(b))
	assert.Equal(t, 0, support.CheckRefreshStatusCount(b))
	assert.Equal(t, 0, support.CommunicationCount(b))
}

// TestRefinement1_AddCaseInternal verifies seed helper.
func TestRefinement1_AddCaseInternal(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	b.AddCaseInternal(&support.Case{
		CaseID:      "case-seed001",
		Subject:     "Seeded Case",
		Status:      "opened",
		CreatedTime: time.Now(),
	})

	assert.Equal(t, 1, support.CaseCount(b))
	cases := b.DescribeCases([]string{"case-seed001"}, false)
	require.Len(t, cases, 1)
	assert.Equal(t, "Seeded Case", cases[0].Subject)
}

// TestRefinement1_DescribeCases_IncludeResolved verifies includeResolvedCases filter.
func TestRefinement1_DescribeCases_IncludeResolved(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	// Create and resolve a case.
	rec := doSupportRequest(t, h, "CreateCase", map[string]any{"subject": "Resolve me", "communicationBody": "Initial"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	rec2 := doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Without includeResolvedCases, resolved case should not appear.
	rec3 := doSupportRequest(t, h, "DescribeCases", map[string]any{
		"includeResolvedCases": false,
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	cases3 := resp3["cases"].([]any)
	assert.Empty(t, cases3)

	// With includeResolvedCases, resolved case should appear.
	rec4 := doSupportRequest(t, h, "DescribeCases", map[string]any{
		"includeResolvedCases": true,
	})
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &resp4))
	cases4 := resp4["cases"].([]any)
	assert.Len(t, cases4, 1)
}

// TestRefinement1_DescribeCases_ReturnsTimestamps verifies timeCreated is populated.
func TestRefinement1_DescribeCases_ReturnsTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Timestamp test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSupportRequest(t, h, "DescribeCases", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	cases := resp["cases"].([]any)
	require.Len(t, cases, 1)
	cs := cases[0].(map[string]any)
	assert.NotEmpty(t, cs["timeCreated"])
}

// TestRefinement1_DescribeCases_ResolvedStatus verifies resolved cases expose AWS status without internal timestamp.
func TestRefinement1_DescribeCases_ResolvedStatus(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Resolve timestamp", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})

	rec2 := doSupportRequest(t, h, "DescribeCases", map[string]any{
		"includeResolvedCases": true,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	cases := resp["cases"].([]any)
	require.Len(t, cases, 1)
	cs := cases[0].(map[string]any)
	assert.Equal(t, "resolved", cs["status"])
	assert.NotContains(t, cs, "timeResolved")
}

// TestRefinement1_AddCommunicationToCase_EmptyBody verifies empty body is rejected.
func TestRefinement1_AddCommunicationToCase_EmptyBody(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Comm body test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	rec2 := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestRefinement1_AddCommunicationToCase_AttachmentSetId verifies attachmentSetId is stored.
func TestRefinement1_AddCommunicationToCase_AttachmentSetId(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Attach test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	setRec := doSupportRequest(t, h, "AddAttachmentsToSet", map[string]any{
		"attachments": []map[string]any{{"fileName": "detail.txt", "data": []byte("details")}},
	})
	require.Equal(t, http.StatusOK, setRec.Code)
	var setResp map[string]any
	require.NoError(t, json.Unmarshal(setRec.Body.Bytes(), &setResp))

	rec2 := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "See attached",
		"attachmentSetId":   setResp["attachmentSetId"],
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Verify through DescribeCommunications.
	rec3 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))
	comms := resp["communications"].([]any)
	require.Len(t, comms, 2)
	comm := comms[0].(map[string]any)
	assert.NotEmpty(t, comm["attachmentSet"])
}

// TestRefinement1_CommunicationView_TimeCreated verifies timeCreated in communication view.
func TestRefinement1_CommunicationView_TimeCreated(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "TimeCreated test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "Test body",
	})

	rec2 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	comms := resp["communications"].([]any)
	require.Len(t, comms, 2)
	comm := comms[0].(map[string]any)
	assert.NotEmpty(t, comm["timeCreated"])
}

// TestRefinement1_Snapshot_IncludesCommunications verifies communications survive Snapshot/Restore.
func TestRefinement1_Snapshot_IncludesCommunications(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	c, err := b.CreateCase("Snap comm test", "", "", "", "")
	require.NoError(t, err)

	require.NoError(t, b.AddCommunicationToCase(c.CaseID, "Hello", ""))
	assert.Equal(t, 1, support.CommunicationCount(b))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := support.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, support.CommunicationCount(b2))
}

// TestRefinement1_Snapshot_IncludesAttachments verifies attachments survive Snapshot/Restore.
func TestRefinement1_Snapshot_IncludesAttachments(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	b.AddAttachmentInternal(&support.Attachment{
		AttachmentID: "att-snap-001",
		FileName:     "snap.log",
		Data:         []byte("log data"),
	})

	assert.Equal(t, 1, support.AttachmentCount(b))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := support.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, support.AttachmentCount(b2))

	a, err := b2.DescribeAttachment("att-snap-001")
	require.NoError(t, err)
	assert.Equal(t, "snap.log", a.FileName)
	assert.Equal(t, []byte("log data"), a.Data)
}

// TestRefinement1_Snapshot_IncludesCheckRefreshStatuses verifies refresh statuses survive Snapshot/Restore.
func TestRefinement1_Snapshot_IncludesCheckRefreshStatuses(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	_, err := b.RefreshTrustedAdvisorCheck("Pfx0RwqBli")
	require.NoError(t, err)

	assert.Equal(t, 1, support.CheckRefreshStatusCount(b))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := support.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, support.CheckRefreshStatusCount(b2))

	statuses := b2.DescribeTrustedAdvisorCheckRefreshStatuses([]string{"Pfx0RwqBli"})
	require.Len(t, statuses, 1)
	assert.Equal(t, "enqueued", statuses[0].Status)
}

// TestRefinement1_TrustedAdvisorChecks_MoreChecks verifies at least 8 checks returned.
func TestRefinement1_TrustedAdvisorChecks_MoreChecks(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	checks := b.DescribeTrustedAdvisorChecks()
	assert.GreaterOrEqual(t, len(checks), 8, "expected at least 8 Trusted Advisor checks")
}

// TestRefinement1_TrustedAdvisorChecks_Categories verifies multiple categories present.
func TestRefinement1_TrustedAdvisorChecks_Categories(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	checks := b.DescribeTrustedAdvisorChecks()

	cats := make(map[string]bool)
	for _, c := range checks {
		cats[c.Category] = true
	}

	assert.True(t, cats["security"], "expected security category")
	assert.True(t, cats["cost_optimizing"], "expected cost_optimizing category")
	assert.True(t, cats["fault_tolerance"], "expected fault_tolerance category")
}

// TestRefinement1_CreateCase_ValidationError verifies subject validation uses ErrValidation.
func TestRefinement1_CreateCase_ValidationError(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "CreateCase", map[string]any{"serviceCode": "amazon-s3"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_AlreadyResolved verifies double-resolve is idempotent: real
// AWS Support's ResolveCase has no "already resolved" error (its modeled
// exceptions are only CaseIdNotFound and InternalServerError), so resolving
// an already-resolved case succeeds and reports "resolved" for both the
// initial and final status.
func TestRefinement1_AlreadyResolved(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Double resolve", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	rec2 := doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec2.Code)

	rec3 := doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	assert.Equal(t, "resolved", resp3["initialCaseStatus"])
	assert.Equal(t, "resolved", resp3["finalCaseStatus"])
}

// TestRefinement1_DescribeCommunications_MissingCaseId verifies validation.
func TestRefinement1_DescribeCommunications_MissingCaseId(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "DescribeCommunications", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_AttachmentInternal_DeepCopyData verifies data bytes are deep-copied.
func TestRefinement1_AttachmentInternal_DeepCopyData(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	data := []byte("original data")
	b.AddAttachmentInternal(&support.Attachment{
		AttachmentID: "att-deep-001",
		FileName:     "test.log",
		Data:         data,
	})

	// Mutate original data after adding.
	data[0] = 'X'

	a, err := b.DescribeAttachment("att-deep-001")
	require.NoError(t, err)
	assert.Equal(t, byte('o'), a.Data[0], "AddAttachmentInternal should deep-copy data bytes")
}

// TestRefinement1_DescribeCases_Pagination verifies nextToken/maxResults fields accepted.
func TestRefinement1_DescribeCases_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	doSupportRequest(t, h, "CreateCase", map[string]any{"subject": "Page1", "communicationBody": "Initial"})
	doSupportRequest(t, h, "CreateCase", map[string]any{"subject": "Page2", "communicationBody": "Initial"})

	rec := doSupportRequest(t, h, "DescribeCases", map[string]any{
		"maxResults": 10,
		"nextToken":  "",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HandleError_ErrValidation verifies ErrValidation → 400.
func TestRefinement1_HandleError_ErrValidation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	// Missing attachmentId triggers ErrValidation.
	rec := doSupportRequest(t, h, "DescribeAttachment", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_HandleError_ErrUnknown verifies unknown actions → 400.
func TestRefinement1_HandleError_ErrUnknown(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CheckRefreshStatus_Count verifies counter increments.
func TestRefinement1_CheckRefreshStatus_Count(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	_, err := b.RefreshTrustedAdvisorCheck("check-A")
	require.NoError(t, err)

	_, err = b.RefreshTrustedAdvisorCheck("check-B")
	require.NoError(t, err)

	assert.Equal(t, 2, support.CheckRefreshStatusCount(b))
}

// TestRefinement1_DescribeCommunications_Pagination verifies nextToken/maxResults accepted.
func TestRefinement1_DescribeCommunications_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Comm pagination", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "First message",
	})

	rec2 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{
		"caseId":     caseID,
		"maxResults": 10,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}
