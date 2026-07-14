package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ─────────────────────────────────────────────────────────────
// InferenceProfile — full lifecycle accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InferenceProfile_CreateResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input             map[string]any
		name              string
		wantProfileStatus string
		wantHTTPStatus    int
		wantARN           bool
	}{
		{
			name:              "valid create returns arn and status",
			input:             map[string]any{"inferenceProfileName": "my-profile"},
			wantHTTPStatus:    http.StatusCreated,
			wantARN:           true,
			wantProfileStatus: "ACTIVE",
		},
		{
			name:           "missing name returns 400",
			input:          map[string]any{"inferenceProfileName": ""},
			wantHTTPStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/inference-profiles", tt.input)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if tt.wantARN {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["inferenceProfileArn"])
				assert.Equal(t, tt.wantProfileStatus, out["status"])
			}
		})
	}
}

func TestAccuracy_InferenceProfile_GetReturnsAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		description string
	}{
		{
			name:        "profile with description",
			profileName: "profile-with-desc",
			description: "a test profile",
		},
		{
			name:        "profile without description",
			profileName: "profile-no-desc",
			description: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			p, err := b.CreateInferenceProfile(tt.profileName, tt.description, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/inference-profiles/"+url.PathEscape(p.InferenceProfileArn), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Equal(t, p.InferenceProfileArn, out["inferenceProfileArn"])
			assert.Equal(t, p.InferenceProfileID, out["inferenceProfileId"])
			assert.Equal(t, tt.profileName, out["inferenceProfileName"])
			assert.Equal(t, "ACTIVE", out["status"])
			assert.Equal(t, "APPLICATION", out["type"])
			assert.NotEmpty(t, out["createdAt"])
			assert.NotEmpty(t, out["updatedAt"])
			if tt.description != "" {
				assert.Equal(t, tt.description, out["description"])
			}
		})
	}
}

func TestAccuracy_InferenceProfile_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/inference-profiles/nonexistent-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_InferenceProfile_ListContainsCreated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		profileNames []string
	}{
		{
			name:         "single profile",
			profileNames: []string{"profile-alpha"},
		},
		{
			name:         "multiple profiles",
			profileNames: []string{"profile-a", "profile-b", "profile-c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.profileNames {
				_, err := b.CreateInferenceProfile(name, "", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/inference-profiles", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			summaries := out["inferenceProfileSummaries"].([]any)
			assert.Len(t, summaries, len(tt.profileNames))

			for _, raw := range summaries {
				s := raw.(map[string]any)
				assert.NotEmpty(t, s["inferenceProfileArn"])
				assert.NotEmpty(t, s["inferenceProfileName"])
				assert.Equal(t, "ACTIVE", s["status"])
				assert.Equal(t, "APPLICATION", s["type"])
			}
		})
	}
}

func TestAccuracy_InferenceProfile_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	p, err := b.CreateInferenceProfile("to-delete", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/inference-profiles/"+url.PathEscape(p.InferenceProfileArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/inference-profiles/"+url.PathEscape(p.InferenceProfileArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)

	recList := doRequest(t, h, http.MethodGet, "/inference-profiles", nil)
	require.Equal(t, http.StatusOK, recList.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["inferenceProfileSummaries"].([]any))
}

func TestAccuracy_InferenceProfile_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/inference-profiles/no-such-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_InferenceProfile_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	rec1 := doRequest(t, h, http.MethodPost, "/inference-profiles",
		map[string]any{"inferenceProfileName": "unique-profile"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/inference-profiles",
		map[string]any{"inferenceProfileName": "unique-profile"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestAccuracy_InferenceProfile_TagsPreserved(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "env", Value: "test"}, {Key: "owner", Value: "team"}}
	p, err := b.CreateInferenceProfile("tagged-profile", "", tags)
	require.NoError(t, err)
	assert.Len(t, p.Tags, 2)

	got, err := b.GetInferenceProfile(p.InferenceProfileArn)
	require.NoError(t, err)
	assert.Len(t, got.Tags, 2)
	assert.Equal(t, "env", got.Tags[0].Key)
	assert.Equal(t, "test", got.Tags[0].Value)
}

func TestAccuracy_InferenceProfile_LookupByName(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateInferenceProfile("name-lookup-profile", "desc", nil)
	require.NoError(t, err)

	got, err := b.GetInferenceProfile("name-lookup-profile")
	require.NoError(t, err)
	assert.Equal(t, "name-lookup-profile", got.InferenceProfileName)
}

// ─────────────────────────────────────────────────────────────
// MarketplaceModelEndpoint — lifecycle and status transitions
// ─────────────────────────────────────────────────────────────

func TestAccuracy_MarketplaceEndpoint_CreateStartsAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		endpointName  string
		modelSourceID string
		wantStatus    int
	}{
		{
			name:          "valid endpoint starts Creating",
			endpointName:  "my-endpoint",
			modelSourceID: "arn:aws:sagemaker:us-east-1:000000000000:endpoint/my-ep",
			wantStatus:    http.StatusCreated,
		},
		{
			name:         "missing endpoint name returns 400",
			endpointName: "",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/marketplace-model/endpoints",
				map[string]any{
					"endpointName":          tt.endpointName,
					"modelSourceIdentifier": tt.modelSourceID,
				},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				ep := out["marketplaceModelEndpoint"].(map[string]any)
				assert.Equal(t, "Creating", ep["status"])
				assert.NotEmpty(t, ep["endpointArn"])
				assert.Equal(t, tt.endpointName, ep["endpointName"])
				assert.Equal(t, tt.modelSourceID, ep["modelSourceIdentifier"])
				assert.NotEmpty(t, ep["createdAt"])
				assert.NotEmpty(t, ep["updatedAt"])
			}
		})
	}
}

func TestAccuracy_MarketplaceEndpoint_RegisterTransitionsToActive(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("activate-ep", "src-id", nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", ep.Status)

	rec := doRequest(t, h, http.MethodPost,
		"/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn)+"/registration", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	got, err := b.GetMarketplaceModelEndpoint(ep.EndpointArn)
	require.NoError(t, err)
	assert.Equal(t, "Active", got.Status)
}

func TestAccuracy_MarketplaceEndpoint_DeregisterTransitionsToDeregistered(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("deactivate-ep", "src-id", nil)
	require.NoError(t, err)

	// Register first.
	require.NoError(t, b.RegisterMarketplaceModelEndpoint(ep.EndpointArn))

	rec := doRequest(t, h, http.MethodDelete,
		"/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn)+"/registration", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	got, err := b.GetMarketplaceModelEndpoint(ep.EndpointArn)
	require.NoError(t, err)
	assert.Equal(t, "Deregistered", got.Status)
}

func TestAccuracy_MarketplaceEndpoint_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_MarketplaceEndpoint_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_MarketplaceEndpoint_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	rec1 := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints",
		map[string]any{"endpointName": "dup-ep", "modelSourceIdentifier": "src"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints",
		map[string]any{"endpointName": "dup-ep", "modelSourceIdentifier": "src"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestAccuracy_MarketplaceEndpoint_ListResponseShape(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	names := []string{"ep-one", "ep-two", "ep-three"}
	for _, n := range names {
		_, err := b.CreateMarketplaceModelEndpoint(n, "src", nil)
		require.NoError(t, err)
	}

	rec := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	endpoints := out["marketplaceModelEndpoints"].([]any)
	assert.Len(t, endpoints, 3)

	for _, raw := range endpoints {
		ep := raw.(map[string]any)
		assert.NotEmpty(t, ep["endpointArn"])
		assert.NotEmpty(t, ep["endpointName"])
		assert.Equal(t, "Creating", ep["status"])
		assert.NotEmpty(t, ep["createdAt"])
		assert.NotEmpty(t, ep["updatedAt"])
	}
}

func TestAccuracy_MarketplaceEndpoint_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("del-ep", "src", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recList := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	assert.Empty(t, out["marketplaceModelEndpoints"].([]any))
}

func TestAccuracy_MarketplaceEndpoint_UpdateReturnsEndpoint(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("update-ep", "src", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPatch, "/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, ep.EndpointArn, out["endpointArn"])
	assert.NotEmpty(t, out["updatedAt"])
}

// ─────────────────────────────────────────────────────────────
// ModelInvocationLoggingConfiguration — put/get/delete accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_LoggingConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		enabled bool
	}{
		{
			name:    "logging enabled with bucket",
			enabled: true,
			bucket:  "my-log-bucket",
		},
		{
			name:    "logging disabled no bucket",
			enabled: false,
			bucket:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(
				t, h, http.MethodPut, "/logging/modelinvocations",
				map[string]any{
					"loggingEnabled": tt.enabled,
					"s3BucketName":   tt.bucket,
				},
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			cfg := out["loggingConfig"].(map[string]any)
			assert.Equal(t, tt.enabled, cfg["loggingEnabled"])
			if tt.bucket != "" {
				assert.Equal(t, tt.bucket, cfg["s3BucketName"])
			}
		})
	}
}

func TestAccuracy_LoggingConfig_DeleteClearsConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Put a config.
	recPut := doRequest(t, h, http.MethodPut, "/logging/modelinvocations",
		map[string]any{"loggingEnabled": true, "s3BucketName": "bucket"})
	require.Equal(t, http.StatusOK, recPut.Code)

	// Delete it.
	recDel := doRequest(t, h, http.MethodDelete, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, recDel.Code)

	// Get should return an empty config.
	recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	cfg := out["loggingConfig"].(map[string]any)
	enabled, _ := cfg["loggingEnabled"].(bool)
	assert.False(t, enabled)
}

func TestAccuracy_LoggingConfig_GetBeforePutReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["loggingConfig"])
}

// ─────────────────────────────────────────────────────────────
// ModelCustomizationJob — stop, list, HTTP accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CustomizationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop running job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateModelCustomizationJob("stop-job", "amazon.titan-text-express-v1", "", nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			rec := doRequest(t, h, http.MethodPost,
				"/model-customization-jobs/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/model-customization-jobs/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_CustomizationJob_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty list", jobNames: nil, wantLen: 0},
		{name: "single job", jobNames: []string{"job-a"}, wantLen: 1},
		{name: "multiple jobs", jobNames: []string{"job-x", "job-y", "job-z"}, wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateModelCustomizationJob(name, "amazon.titan-text-express-v1", "", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/model-customization-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["modelCustomizationJobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)

			for _, raw := range summaries {
				s := raw.(map[string]any)
				assert.NotEmpty(t, s["jobArn"])
				assert.NotEmpty(t, s["jobName"])
				assert.Equal(t, "InProgress", s["status"])
				assert.NotEmpty(t, s["creationTime"])
				assert.NotEmpty(t, s["lastModifiedTime"])
			}
		})
	}
}

func TestAccuracy_CustomizationJob_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateModelCustomizationJob("dup-job", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)

	_, err2 := b.CreateModelCustomizationJob("dup-job", "amazon.titan-text-express-v1", "", nil)
	require.Error(t, err2)
}

func TestAccuracy_CustomizationJob_GetByNameOrARN(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob("lookup-job", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)

	// Get by ARN.
	byARN, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, job.JobArn, byARN.JobArn)

	// Get by name.
	byName, err := b.GetModelCustomizationJob("lookup-job")
	require.NoError(t, err)
	assert.Equal(t, job.JobArn, byName.JobArn)
}

func TestAccuracy_CustomizationJob_AdvanceCompletesJob(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob("advance-job", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	n := b.AdvanceCustomizationJobStatuses(0)
	assert.Equal(t, 1, n)

	got, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", got.Status)
}

func TestAccuracy_CustomizationJob_CustomizationTypePreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		customizationType string
	}{
		{name: "fine tuning", customizationType: "FINE_TUNING"},
		{name: "continued pretraining", customizationType: "CONTINUED_PRE_TRAINING"},
		{name: "distillation", customizationType: "DISTILLATION"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			rec := doRequest(
				t, h, http.MethodPost, "/model-customization-jobs",
				map[string]any{
					"jobName":           fmt.Sprintf("job-%s", tt.name),
					"baseModelId":       "amazon.titan-text-express-v1",
					"customizationType": tt.customizationType,
				},
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
			jobARN := createOut["jobArn"].(string)

			recGet := doRequest(t, h, http.MethodGet, "/model-customization-jobs/"+url.PathEscape(jobARN), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.customizationType, out["customizationType"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// EvaluationJob — stop status, list HTTP
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EvaluationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop in-progress job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateEvaluationJob("stop-eval-job", nil)
			require.NoError(t, err)

			recStop := doRequest(t, h, http.MethodPost,
				"/evaluation-job/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusOK, recStop.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/evaluation-jobs/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_EvaluationJob_ListResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty", jobNames: nil, wantLen: 0},
		{name: "two jobs", jobNames: []string{"eval-a", "eval-b"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateEvaluationJob(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["jobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)

			for _, raw := range summaries {
				s := raw.(map[string]any)
				assert.NotEmpty(t, s["jobArn"])
				assert.NotEmpty(t, s["jobName"])
			}
		})
	}
}

func TestAccuracy_EvaluationJob_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/nonexistent-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// CustomModel — CRUD HTTP accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CustomModel_GetViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		modelName string
	}{
		{name: "get existing model", modelName: "my-custom-model"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			m, err := b.CreateCustomModel(tt.modelName, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, m.ModelArn, out["modelArn"])
			assert.Equal(t, tt.modelName, out["modelName"])
			assert.NotEmpty(t, out["creationTime"])
		})
	}
}

func TestAccuracy_CustomModel_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/custom-models/nonexistent-model-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_CustomModel_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modelNames []string
		wantLen    int
	}{
		{name: "empty list", modelNames: nil, wantLen: 0},
		{name: "single model", modelNames: []string{"model-1"}, wantLen: 1},
		{name: "three models", modelNames: []string{"m-a", "m-b", "m-c"}, wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.modelNames {
				_, err := b.CreateCustomModel(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/custom-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			models := out["modelSummaries"].([]any)
			assert.Len(t, models, tt.wantLen)
		})
	}
}

func TestAccuracy_CustomModel_DeleteViaHTTP(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	m, err := b.CreateCustomModel("to-delete-model", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)
}

func TestAccuracy_CustomModel_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete,
		"/custom-models/arn:aws:bedrock:us-east-1:000000000000:custom-model/no-such-model", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_CustomModel_TagsOnCreate(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "project", Value: "prod"}}
	m, err := b.CreateCustomModel("tagged-model", tags)
	require.NoError(t, err)
	assert.Len(t, m.Tags, 1)
	assert.Equal(t, "project", m.Tags[0].Key)
	assert.Equal(t, "prod", m.Tags[0].Value)

	got, err := b.GetCustomModel(m.ModelArn)
	require.NoError(t, err)
	assert.Len(t, got.Tags, 1)
	assert.Equal(t, "project", got.Tags[0].Key)
}

// ─────────────────────────────────────────────────────────────
// FoundationModelAgreement — listing offers and delete
// ─────────────────────────────────────────────────────────────

func TestAccuracy_FoundationModelAgreement_ListOffersEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	offers, _ := out["modelAgreementOffers"].([]any)
	// Initially no agreements — list should be empty or nil.
	assert.Empty(t, offers)
}

func TestAccuracy_FoundationModelAgreement_CreateAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "titan model agreement", modelID: "amazon.titan-text-express-v1"},
		{name: "claude model agreement", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			// Create agreement.
			rec := doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement",
				map[string]any{"modelId": tt.modelID})
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete agreement.
			recDel := doRequest(t, h, http.MethodDelete,
				"/delete-foundation-model-agreement/"+url.PathEscape(tt.modelID), nil)
			assert.Equal(t, http.StatusNoContent, recDel.Code)

			// Delete again — not found.
			recDel2 := doRequest(t, h, http.MethodDelete,
				"/delete-foundation-model-agreement/"+url.PathEscape(tt.modelID), nil)
			assert.Equal(t, http.StatusNotFound, recDel2.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// AutomatedReasoningPolicy version numbering accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ARPVersion_PerPolicyNumberingIsolated(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	p1, err := b.CreateAutomatedReasoningPolicy("policy-alpha", "", nil)
	require.NoError(t, err)

	p2, err := b.CreateAutomatedReasoningPolicy("policy-beta", "", nil)
	require.NoError(t, err)

	// Each policy starts its own counter.
	v1a, err := b.CreateAutomatedReasoningPolicyVersion(p1.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "1", v1a.Version)

	v1b, err := b.CreateAutomatedReasoningPolicyVersion(p2.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "1", v1b.Version, "per-policy counter — beta starts at 1")

	v2a, err := b.CreateAutomatedReasoningPolicyVersion(p1.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "2", v2a.Version)

	v2b, err := b.CreateAutomatedReasoningPolicyVersion(p2.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "2", v2b.Version)
}

func TestAccuracy_ARPVersion_ViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantLastVersion string
		versionCount    int
	}{
		{name: "single version", versionCount: 1, wantLastVersion: "1"},
		{name: "three versions", versionCount: 3, wantLastVersion: "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			policy, err := b.CreateAutomatedReasoningPolicy("versioned-policy-"+tt.name, "", nil)
			require.NoError(t, err)

			var lastVersion map[string]any
			for i := range tt.versionCount {
				rec := doRequest(t, h, http.MethodPost,
					"/automated-reasoning-policies/"+url.PathEscape(policy.PolicyArn)+"/versions",
					map[string]any{"notes": fmt.Sprintf("version %d", i+1)})
				require.Equal(t, http.StatusCreated, rec.Code)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lastVersion))
			}

			assert.Equal(t, tt.wantLastVersion, lastVersion["version"])
			assert.NotEmpty(t, lastVersion["policyArn"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// GuardrailVersion — create via HTTP returns correct version number
// ─────────────────────────────────────────────────────────────

func TestAccuracy_GuardrailVersion_HTTPCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantVersion  string
		versionCount int
	}{
		{name: "first version", versionCount: 1, wantVersion: "1"},
		{name: "second version", versionCount: 2, wantVersion: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("guard-"+tt.name, "", "", "", nil)
			require.NoError(t, err)

			var lastOut map[string]any
			for i := range tt.versionCount {
				rec := doRequest(t, h, http.MethodPost,
					"/guardrails/"+g.GuardrailID,
					map[string]any{"description": fmt.Sprintf("v%d", i+1)})
				require.Equal(t, http.StatusOK, rec.Code)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lastOut))
			}

			assert.Equal(t, tt.wantVersion, lastOut["version"])
			assert.Equal(t, g.GuardrailID, lastOut["guardrailId"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// ModelInvocationJob — stop transitions status correctly
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ModelInvocationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop Submitted job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			job, err := b.CreateModelInvocationJob("stop-invoc-job", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost,
				"/model-invocation-job/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/model-invocation-job/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_ModelInvocationJob_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty", jobNames: nil, wantLen: 0},
		{name: "two jobs", jobNames: []string{"invoc-a", "invoc-b"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateModelInvocationJob(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["invocationJobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// ImportedModel — list and get after model import job
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ImportedModel_VisibleAfterImportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
	}{
		{name: "single imported model", jobName: "import-job-1"},
		{name: "another imported model", jobName: "import-job-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateModelImportJob(tt.jobName, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, job.ImportedModelArn)

			// Imported model should be listable.
			rec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			models := out["modelSummaries"].([]any)
			assert.Len(t, models, 1)

			// Get by ARN.
			recGet := doRequest(t, h, http.MethodGet,
				"/imported-models/"+url.PathEscape(job.ImportedModelArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var modelOut map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &modelOut))
			assert.Equal(t, job.ImportedModelArn, modelOut["modelArn"])
		})
	}
}

func TestAccuracy_ImportedModel_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	job, err := b.CreateModelImportJob("del-import-job", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete,
		"/imported-models/"+url.PathEscape(job.ImportedModelArn), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	recList := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	assert.Empty(t, out["modelSummaries"].([]any))
}

// ─────────────────────────────────────────────────────────────
// PromptRouter — full lifecycle accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_PromptRouter_CreateResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		routerName   string
		wantStatus   int
		wantRouterID bool
	}{
		{
			name:         "valid create",
			routerName:   "my-router",
			wantStatus:   http.StatusCreated,
			wantRouterID: true,
		},
		{
			name:       "missing name returns 400",
			routerName: "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers",
				map[string]any{"promptRouterName": tt.routerName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRouterID {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["promptRouterArn"])
			}
		})
	}
}

func TestAccuracy_PromptRouter_GetAndListAccuracy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		routerNames []string
		wantLen     int
	}{
		{name: "single router", routerNames: []string{"router-a"}, wantLen: 1},
		{name: "two routers", routerNames: []string{"router-x", "router-y"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			var lastARN string
			for _, name := range tt.routerNames {
				r, err := b.CreatePromptRouter(name, nil)
				require.NoError(t, err)
				lastARN = r.PromptRouterArn
			}

			// List.
			recList := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
			require.Equal(t, http.StatusOK, recList.Code)

			var listOut map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
			routers := listOut["promptRouters"].([]any)
			assert.Len(t, routers, tt.wantLen)

			// Get last.
			recGet := doRequest(t, h, http.MethodGet,
				"/prompt-routers/"+url.PathEscape(lastARN), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getOut))
			assert.Equal(t, lastARN, getOut["promptRouterArn"])
			assert.Equal(t, "AVAILABLE", getOut["status"])
			assert.NotEmpty(t, getOut["createdAt"])
			assert.NotEmpty(t, getOut["updatedAt"])
		})
	}
}

func TestAccuracy_PromptRouter_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	r, err := b.CreatePromptRouter("del-router", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)
}

func TestAccuracy_PromptRouter_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreatePromptRouter("dup-router", nil)
	require.NoError(t, err)

	_, err2 := b.CreatePromptRouter("dup-router", nil)
	require.Error(t, err2)
}

func TestAccuracy_PromptRouter_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prompt-routers/nonexistent-router", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// Guardrail — update and delete accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Guardrail_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/guardrails/nonexistent-guardrail", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_Guardrail_UpdatePreservesAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialName string
		newName     string
		newDesc     string
	}{
		{
			name:        "rename guardrail",
			initialName: "original-name",
			newName:     "updated-name",
			newDesc:     "updated description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail(tt.initialName, "", "", "", nil)
			require.NoError(t, err)

			rec := doRequest(
				t, h, http.MethodPut, "/guardrails/"+g.GuardrailID,
				map[string]any{
					"name":        tt.newName,
					"description": tt.newDesc,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+g.GuardrailID, nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.newName, out["name"])
			assert.Equal(t, tt.newDesc, out["description"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// ProvisionedModelThroughput — update and tag accuracy
// ─────────────────────────────────────────────────────────────

// TestAccuracy_PMT_UpdateDesiredModelAndName verifies UpdateProvisionedModelThroughput's
// real (and only) two mutable fields: desiredModelId and desiredProvisionedModelName.
// AWS has no way to change modelUnits after creation — that's fixed at CreateProvisionedModelThroughput.
func TestAccuracy_PMT_UpdateDesiredModelAndName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		desiredModelID string
		desiredNewName string
	}{
		{name: "swap desired model", desiredModelID: "anthropic.claude-v2"},
		{name: "rename", desiredNewName: "renamed-pmt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			pmt, err := b.CreateProvisionedModelThroughput(
				"update-pmt-"+tt.name, "amazon.titan-text-express-v1", 1, "", nil,
			)
			require.NoError(t, err)

			body := map[string]any{}
			if tt.desiredModelID != "" {
				body["desiredModelId"] = tt.desiredModelID
			}

			if tt.desiredNewName != "" {
				body["desiredProvisionedModelName"] = tt.desiredNewName
			}

			rec := doRequest(
				t, h, http.MethodPatch,
				"/provisioned-model-throughput/"+url.PathEscape(pmt.ProvisionedModelArn),
				body,
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			updated, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
			require.NoError(t, err)

			if tt.desiredModelID != "" {
				assert.Contains(t, updated.DesiredModelArn, tt.desiredModelID)
			}

			if tt.desiredNewName != "" {
				assert.Equal(t, tt.desiredNewName, updated.ProvisionedModelName)
			}
		})
	}
}

func TestAccuracy_PMT_TagsOnCreate(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "cost-center", Value: "ml-team"}}
	pmt, err := b.CreateProvisionedModelThroughput("tagged-pmt", "amazon.titan-text-express-v1", int32(1), "", tags)
	require.NoError(t, err)
	assert.Len(t, pmt.Tags, 1)
	assert.Equal(t, "cost-center", pmt.Tags[0].Key)
	assert.Equal(t, "ml-team", pmt.Tags[0].Value)
}

// ─────────────────────────────────────────────────────────────
// EnforcedGuardrailConfiguration — put/list/delete accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EnforcedGuardrail_PutAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		guardrailID      string
		guardrailVersion string
	}{
		{name: "version 1", guardrailID: "guard-1", guardrailVersion: "1"},
		{name: "draft version", guardrailID: "guard-2", guardrailVersion: "DRAFT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			recPut := doRequest(
				t, h, http.MethodPut, "/enforced-guardrail-configuration",
				map[string]any{
					"guardrailId":      tt.guardrailID,
					"guardrailVersion": tt.guardrailVersion,
				},
			)
			assert.Equal(t, http.StatusNoContent, recPut.Code)

			recList := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
			require.Equal(t, http.StatusOK, recList.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
			configs := out["enforcedGuardrailConfigurations"].([]any)
			assert.Len(t, configs, 1)

			cfg := configs[0].(map[string]any)
			assert.Equal(t, tt.guardrailID, cfg["guardrailId"])
			assert.Equal(t, tt.guardrailVersion, cfg["guardrailVersion"])
		})
	}
}

func TestAccuracy_EnforcedGuardrail_DeleteRemovesConfig(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Put a config.
	recPut := doRequest(t, h, http.MethodPut, "/enforced-guardrail-configuration",
		map[string]any{"guardrailId": "g-del", "guardrailVersion": "1"})
	require.Equal(t, http.StatusNoContent, recPut.Code)

	// Delete it using query parameter.
	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/enforced-guardrail-configuration?guardrailId=g-del", nil)
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c2))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// List — should be empty.
	recList := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	configs := out["enforcedGuardrailConfigurations"].([]any)
	assert.Empty(t, configs)
}

// ─────────────────────────────────────────────────────────────
// Tags — on Guardrail and EvaluationJob via ListTagsForResource
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Tags_OnGuardrailViaListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []bedrock.Tag
		tagCount int
	}{
		{
			name:     "two tags on guardrail",
			tagCount: 2,
			tags:     []bedrock.Tag{{Key: "team", Value: "ml"}, {Key: "env", Value: "staging"}},
		},
		{
			name:     "single tag on guardrail",
			tagCount: 1,
			tags:     []bedrock.Tag{{Key: "owner", Value: "alice"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("tagged-guard-"+tt.name, "", "", "", tt.tags)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/listTagsForResource",
				map[string]any{"resourceARN": g.GuardrailArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			returnedTags := out["tags"].([]any)
			assert.Len(t, returnedTags, tt.tagCount)
		})
	}
}

func TestAccuracy_Tags_TagGuardrailAfterCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{name: "add single tag", key: "added", val: "yes"},
		{name: "add env tag", key: "env", val: "prod"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("tag-later-"+tt.name, "", "", "", nil)
			require.NoError(t, err)

			recTag := doRequest(
				t, h, http.MethodPost, "/tagResource",
				map[string]any{
					"resourceARN": g.GuardrailArn,
					"tags":        []map[string]any{{"key": tt.key, "value": tt.val}},
				},
			)
			assert.Equal(t, http.StatusOK, recTag.Code)

			recList := doRequest(t, h, http.MethodPost, "/listTagsForResource",
				map[string]any{"resourceARN": g.GuardrailArn})
			require.Equal(t, http.StatusOK, recList.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
			returnedTags := out["tags"].([]any)
			assert.Len(t, returnedTags, 1)
			assert.Equal(t, tt.key, returnedTags[0].(map[string]any)["key"])
			assert.Equal(t, tt.val, returnedTags[0].(map[string]any)["value"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// ModelCopyJob / ModelImportJob — advance status accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ModelImportJob_EndTimeAbsentWhileInProgress(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	job, err := b.CreateModelImportJob("no-endtime-job", nil)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	rec := doRequest(t, h, http.MethodGet,
		"/model-import-jobs/"+url.PathEscape(job.JobArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "InProgress", out["status"])
	_, hasEndTime := out["endTime"]
	assert.False(t, hasEndTime, "endTime must not be present while InProgress")
}

func TestAccuracy_ModelCopyJob_AdvanceStatusToCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceARN string
	}{
		{
			name:      "copy titan model",
			sourceARN: "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			job, err := b.CreateModelCopyJob(tt.sourceARN, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			n := b.AdvanceCopyImportJobStatuses(0)
			assert.GreaterOrEqual(t, n, 1)

			got, err := b.GetModelCopyJob(job.JobArn)
			require.NoError(t, err)
			assert.Equal(t, "Completed", got.Status)
		})
	}
}

func TestAccuracy_ModelImportJob_AdvanceStatusToCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
	}{
		{name: "import job advances", jobName: "advance-import-job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			job, err := b.CreateModelImportJob(tt.jobName, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			n := b.AdvanceCopyImportJobStatuses(0)
			assert.GreaterOrEqual(t, n, 1)

			got, err := b.GetModelImportJob(job.JobArn)
			require.NoError(t, err)
			assert.NotEqual(t, "InProgress", got.Status)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// UseCaseForModelAccess — put/get accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_UseCaseForModelAccess_PutAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		useCaseType string
		description string
	}{
		{
			name:        "business use case",
			useCaseType: "BUSINESS",
			description: "Using models for customer support",
		},
		{
			name:        "research use case",
			useCaseType: "RESEARCH",
			description: "Academic research on LLMs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			recPut := doRequest(
				t, h, http.MethodPut, "/usecase-for-model-access",
				map[string]any{
					"useCaseType":        tt.useCaseType,
					"useCaseDescription": tt.description,
				},
			)
			assert.Equal(t, http.StatusNoContent, recPut.Code)

			recGet := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.useCaseType, out["useCaseType"])
			assert.Equal(t, tt.description, out["useCaseDescription"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// FoundationModel — edge case accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_FoundationModel_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/nonexistent.model-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_FoundationModel_ListIsNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "foundation models seeded on init"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["modelSummaries"].([]any)
			assert.NotEmpty(t, summaries, "foundation models must be seeded on init")
		})
	}
}

func TestAccuracy_FoundationModel_FieldsPresentOnList(t *testing.T) {
	t.Parallel()

	requiredFields := []string{
		"modelId",
		"modelName",
		"providerName",
		"inferenceTypesSupported",
		"inputModalities",
		"outputModalities",
	}

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries := out["modelSummaries"].([]any)
	require.NotEmpty(t, summaries)

	for _, field := range requiredFields {
		for i, raw := range summaries {
			m := raw.(map[string]any)
			_, present := m[field]
			assert.True(t, present, "model[%d] missing field %q", i, field)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Batch: stub routes respond 200 not 404
// ─────────────────────────────────────────────────────────────

func TestAccuracy_StubRoutes_Return200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{
			name:   "get logging config",
			method: http.MethodGet,
			path:   "/logging/modelinvocations",
		},
		{
			name:   "list inference profiles empty",
			method: http.MethodGet,
			path:   "/inference-profiles",
		},
		{
			name:   "list marketplace endpoints empty",
			method: http.MethodGet,
			path:   "/marketplace-model/endpoints",
		},
		{
			name:   "list model invocation jobs empty",
			method: http.MethodGet,
			path:   "/model-invocation-jobs",
		},
		{
			name:   "list prompt routers empty",
			method: http.MethodGet,
			path:   "/prompt-routers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code, "path %s returned %d: %s", tt.path, rec.Code, rec.Body.String())
		})
	}
}
