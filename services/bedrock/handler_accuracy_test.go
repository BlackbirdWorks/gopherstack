package bedrock_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ─────────────────────────────────────────────────────────────
// Gap 7: CreateModelCustomizationJob — basic validation
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CreateModelCustomizationJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		map[string]any{"baseModelId": "amazon.titan-text-express-v1"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateModelCustomizationJob_StatusInProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		map[string]any{
			"jobName":     "my-finetune-job",
			"baseModelId": "amazon.titan-text-express-v1",
		})

	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobARN := out["jobArn"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/model-customization-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var jobOut map[string]any
	mustUnmarshal(t, recGet, &jobOut)
	assert.Equal(t, "InProgress", jobOut["status"])
	assert.NotEmpty(t, jobOut["creationTime"])
	assert.NotEmpty(t, jobOut["lastModifiedTime"])
}

func TestAccuracy_AdvanceCustomizationJobStatus(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob("advance-test-job", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	n := b.AdvanceCustomizationJobStatuses(0)
	assert.Equal(t, 1, n)

	advanced, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", advanced.Status)
}

// ─────────────────────────────────────────────────────────────
// Gap 8: ProvisionedModelThroughput starts as "Creating"
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ProvisionedModelThroughput_InitialStatusCreating(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("my-pmt", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", pmt.Status)
}

// TestAccuracy_CreateProvisionedModelThroughput_ModelUnitsBounds verifies that
// modelUnits is validated against both the lower (>0) and upper bound AWS enforces.
func TestAccuracy_CreateProvisionedModelThroughput_ModelUnitsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modelUnits int32
		wantErr    bool
	}{
		{name: "valid", modelUnits: 1, wantErr: false},
		{name: "zero_rejected", modelUnits: 0, wantErr: true},
		{name: "negative_rejected", modelUnits: -1, wantErr: true},
		{name: "above_upper_bound_rejected", modelUnits: 100000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateProvisionedModelThroughput(
				"pmt-"+tt.name, "amazon.titan-text-express-v1", tt.modelUnits, "", nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAccuracy_AdvanceProvisionedModelThroughput_CreatingToInService(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("pmt-advance", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", pmt.Status)

	n := b.AdvanceProvisionedModelThroughputStatuses()
	assert.Equal(t, 1, n)

	advanced, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
	require.NoError(t, err)
	assert.Equal(t, "InService", advanced.Status)
}

func TestAccuracy_AdvanceProvisionedModelThroughput_AlreadyInService(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("pmt-inservice", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)

	// Advance once to InService.
	b.AdvanceProvisionedModelThroughputStatuses()

	// Advance again — should not count any new advancements.
	n := b.AdvanceProvisionedModelThroughputStatuses()
	assert.Equal(t, 0, n)

	got, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
	require.NoError(t, err)
	assert.Equal(t, "InService", got.Status)
}

// ─────────────────────────────────────────────────────────────
// Gap 9: Guardrail versioning — per-guardrail counter
// ─────────────────────────────────────────────────────────────

func TestAccuracy_GuardrailVersion_PerGuardrailCounter(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	g1, err := b.CreateGuardrail("guardrail-alpha", "", "", "", nil)
	require.NoError(t, err)

	g2, err := b.CreateGuardrail("guardrail-beta", "", "", "", nil)
	require.NoError(t, err)

	v1a, err := b.CreateGuardrailVersion(g1.GuardrailID, "v1 of alpha")
	require.NoError(t, err)
	assert.Equal(t, "1", v1a.Version)

	v1b, err := b.CreateGuardrailVersion(g2.GuardrailID, "v1 of beta")
	require.NoError(t, err)
	assert.Equal(t, "1", v1b.Version, "each guardrail should start at version 1")

	v2a, err := b.CreateGuardrailVersion(g1.GuardrailID, "v2 of alpha")
	require.NoError(t, err)
	assert.Equal(t, "2", v2a.Version)
}

// TestAccuracy_GuardrailVersion_UpdateAfterPublishAllowed verifies AWS's actual
// behavior: UpdateGuardrail always edits the mutable DRAFT and succeeds regardless of
// how many numbered versions have been published from it. Published (numbered) versions
// are immutable snapshots and are unaffected by later DRAFT edits.
func TestAccuracy_GuardrailVersion_UpdateAfterPublishAllowed(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	g, err := b.CreateGuardrail("versioned-guard", "", "", "", nil)
	require.NoError(t, err)

	v1, err := b.CreateGuardrailVersion(g.GuardrailID, "first version")
	require.NoError(t, err)

	// Updating DRAFT after a version is published must succeed.
	updated, err := b.UpdateGuardrail(g.GuardrailID, "new-name", "new-description", "", "")
	require.NoError(t, err, "update of DRAFT after publishing a version should succeed")
	assert.Equal(t, "new-name", updated.Name)

	// The already-published version stays a frozen snapshot of the pre-update name.
	snapshot, err := b.GetGuardrailVersion(g.GuardrailID, v1.Version)
	require.NoError(t, err)
	assert.Equal(t, "versioned-guard", snapshot.Name, "published version must not reflect later DRAFT edits")
}

// ─────────────────────────────────────────────────────────────
// Gap 11: ModelCopyJob and ModelImportJob — real backend state
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ModelCopyJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs",
		map[string]any{"sourceModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"})

	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)
	assert.Equal(t, "InProgress", createOut["status"])
	assert.NotEmpty(t, createOut["creationTime"])
	assert.NotEmpty(t, createOut["lastModifiedTime"])

	// List.
	recList := doRequest(t, h, http.MethodGet, "/model-copy-jobs", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	summaries, ok := listOut["modelCopyJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	// Get.
	recGet := doRequest(t, h, http.MethodGet, "/model-copy-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
	assert.Equal(t, "InProgress", getOut["status"])
}

func TestAccuracy_ModelCopyJob_MissingSourceModelArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_ModelImportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "my-import-job"})

	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)
	assert.Equal(t, "InProgress", createOut["status"])
	assert.NotEmpty(t, createOut["creationTime"])
	assert.NotEmpty(t, createOut["lastModifiedTime"])
	assert.NotEmpty(t, createOut["importedModelArn"])

	// List.
	recList := doRequest(t, h, http.MethodGet, "/model-import-jobs", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	summaries, ok := listOut["modelImportJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	// Get.
	recGet := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
}

func TestAccuracy_ModelImportJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_AdvanceCopyImportJobStatuses(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	copyJob, err := b.CreateModelCopyJob(
		"arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1", nil,
	)
	require.NoError(t, err)

	importJob, err := b.CreateModelImportJob("test-import", nil)
	require.NoError(t, err)

	assert.Equal(t, "InProgress", copyJob.Status)
	assert.Equal(t, "InProgress", importJob.Status)

	n := b.AdvanceCopyImportJobStatuses(0)
	assert.Equal(t, 2, n)

	advCopy, err := b.GetModelCopyJob(copyJob.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", advCopy.Status)

	advImport, err := b.GetModelImportJob(importJob.JobArn)
	require.NoError(t, err)
	assert.NotEqual(t, "InProgress", advImport.Status)
}

// ─────────────────────────────────────────────────────────────
// Gap: Bedrock janitor wiring
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Bedrock_Handler_StartWorker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Verify that StartWorker / Shutdown can be called without panic.
	err := h.StartWorker(t.Context())
	require.NoError(t, err)

	// Allow the janitor a short window to run.
	time.Sleep(20 * time.Millisecond)

	shutdownCtx := t.Context()
	h.Shutdown(shutdownCtx)
}
