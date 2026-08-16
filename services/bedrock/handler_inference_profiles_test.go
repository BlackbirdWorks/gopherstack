package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

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
			name: "valid create returns arn and status",
			input: map[string]any{
				"inferenceProfileName": "my-profile",
				"modelSource":          map[string]any{"copyFrom": testModelSource},
			},
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

// TestAccuracy_CreateInferenceProfile_MissingModelSourceRejected proves
// ModelSource is enforced as a required member
// (api_op_CreateInferenceProfile.go:48). Against unfixed code (which never
// reads ModelSource) this gets 201, not 400.
func TestAccuracy_CreateInferenceProfile_MissingModelSourceRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/inference-profiles",
		map[string]any{"inferenceProfileName": "no-model-source-profile"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAccuracy_CreateInferenceProfile_ModelSourceRoundTrip proves
// ModelSource's CopyFrom ARN survives from Create through Get as the
// required Models list (api_op_GetInferenceProfile.go:62), not just that
// Create returns 201.
func TestAccuracy_CreateInferenceProfile_ModelSourceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/inference-profiles", map[string]any{
		"inferenceProfileName": "model-source-roundtrip",
		"modelSource": map[string]any{
			"copyFrom": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	profileARN, ok := createOut["inferenceProfileArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, profileARN)

	recGet := doRequest(t, h, http.MethodGet, "/inference-profiles/"+url.PathEscape(profileARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	mustUnmarshal(t, recGet, &out)

	models, ok := out["models"].([]any)
	require.True(t, ok)
	require.Len(t, models, 1)

	model, ok := models[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2", model["modelArn"])
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
			p, err := b.CreateInferenceProfile(tt.profileName, tt.description, testModelSource, nil)
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
				_, err := b.CreateInferenceProfile(name, "", testModelSource, nil)
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

// TestAccuracy_InferenceProfile_ListTypeFilter locks in that ListInferenceProfiles'
// real query param is "type" (not "typeEquals" -- aws-sdk-go-v2
// serializers.go:6752-6754) and that it's actually parsed and applied.
func TestAccuracy_InferenceProfile_ListTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantLen int
	}{
		{name: "type application matches all", query: "?type=APPLICATION", wantLen: 2},
		{name: "type system_defined matches none", query: "?type=SYSTEM_DEFINED", wantLen: 0},
		{name: "no type filter matches all", query: "", wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			_, err := b.CreateInferenceProfile("profile-a", "", testModelSource, nil)
			require.NoError(t, err)
			_, err = b.CreateInferenceProfile("profile-b", "", testModelSource, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/inference-profiles"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["inferenceProfileSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

func TestAccuracy_InferenceProfile_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	p, err := b.CreateInferenceProfile("to-delete", "", testModelSource, nil)
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
		map[string]any{
			"inferenceProfileName": "unique-profile",
			"modelSource":          map[string]any{"copyFrom": testModelSource},
		})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/inference-profiles",
		map[string]any{
			"inferenceProfileName": "unique-profile",
			"modelSource":          map[string]any{"copyFrom": testModelSource},
		})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestAccuracy_InferenceProfile_TagsPreserved(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "env", Value: "test"}, {Key: "owner", Value: "team"}}
	p, err := b.CreateInferenceProfile("tagged-profile", "", testModelSource, tags)
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
	_, err := b.CreateInferenceProfile("name-lookup-profile", "desc", testModelSource, nil)
	require.NoError(t, err)

	got, err := b.GetInferenceProfile("name-lookup-profile")
	require.NoError(t, err)
	assert.Equal(t, "name-lookup-profile", got.InferenceProfileName)
}

func TestHandler_InferenceProfileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create inference profile.
	rec := doRequest(t, h, http.MethodPost, "/inference-profiles", map[string]any{
		"inferenceProfileName": "my-profile",
		"modelSource":          map[string]any{"copyFrom": "anthropic.claude-v2"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	profileARN := createOut["inferenceProfileArn"].(string)

	// List profiles.
	rec2 := doRequest(t, h, http.MethodGet, "/inference-profiles", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["inferenceProfileSummaries"])

	// Get profile.
	rec3 := doRequest(t, h, http.MethodGet, "/inference-profiles/"+url.PathEscape(profileARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete profile.
	rec4 := doRequest(t, h, http.MethodDelete, "/inference-profiles/"+url.PathEscape(profileARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
