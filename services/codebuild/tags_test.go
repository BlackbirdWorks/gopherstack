package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{
			name:       "success",
			exists:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			exists:     false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := "arn:aws:codebuild:us-east-1:000000000000:project/tag-test-project"

			if tt.exists {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      "tag-test-project",
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
					"tags": map[string]string{"env": "test"},
				})
			} else {
				arn = "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent"
			}

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "tag-resource-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			})

			createRec := doRequest(t, h, "BatchGetProjects", map[string]any{
				"names": []string{"tag-resource-project"},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var batchOut struct {
				Projects []struct {
					Arn string `json:"arn"`
				} `json:"projects"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&batchOut))
			require.NotEmpty(t, batchOut.Projects)
			projectARN := batchOut.Projects[0].Arn

			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": projectARN,
				"tags":        map[string]string{"team": "backend"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": projectARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Tags map[string]string `json:"tags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Equal(t, "backend", listOut.Tags["team"])
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "untag-resource-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"tags": map[string]string{"remove-me": "yes"},
			})

			batchRec := doRequest(t, h, "BatchGetProjects", map[string]any{
				"names": []string{"untag-resource-project"},
			})
			require.Equal(t, http.StatusOK, batchRec.Code)

			var batchOut struct {
				Projects []struct {
					Arn string `json:"arn"`
				} `json:"projects"`
			}
			require.NoError(t, json.NewDecoder(batchRec.Body).Decode(&batchOut))
			require.NotEmpty(t, batchOut.Projects)
			projectARN := batchOut.Projects[0].Arn

			rec := doRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": projectARN,
				"tagKeys":     []string{"remove-me"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": projectARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Tags map[string]string `json:"tags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			_, hasKey := listOut.Tags["remove-me"]
			assert.False(t, hasKey)
		})
	}
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "nonexistent project",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid arn format",
			resourceArn: "not-a-valid-arn",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tags":        []map[string]string{{"key": "k", "value": "v"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "nonexistent project",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid arn format",
			resourceArn: "not-a-valid-arn",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tagKeys":     []string{"key"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagsOnFleet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create fleet with tags.
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":         "tagged-fleet",
		"baseCapacity": 1,
		"tags":         map[string]any{"env": "test", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	fleet, _ := createOut["fleet"].(map[string]any)
	fleetARN := fleet["arn"].(string)

	// ListTagsForResource should include fleet tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": fleetARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags, _ := tagsOut["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// TagResource should add new tags.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fleetARN,
		"tags":        map[string]any{"newkey": "newval"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// UntagResource should remove tags.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": fleetARN,
		"tagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Verify final tags.
	finalRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": fleetARN})
	require.Equal(t, http.StatusOK, finalRec.Code)
	var finalOut map[string]any
	require.NoError(t, json.NewDecoder(finalRec.Body).Decode(&finalOut))
	finalTags, _ := finalOut["tags"].(map[string]any)
	assert.NotContains(t, finalTags, "env")
	assert.Equal(t, "platform", finalTags["team"])
	assert.Equal(t, "newval", finalTags["newkey"])
}

func TestHandler_TagsOnReportGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create report group.
	rec := doRequest(t, h, "CreateReportGroup", map[string]any{
		"name": "tagged-rg",
		"type": "TEST",
		"tags": map[string]any{"owner": "teamA"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	rg, _ := createOut["reportGroup"].(map[string]any)
	rgARN := rg["arn"].(string)

	// TagResource on report group.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": rgARN,
		"tags":        map[string]any{"extra": "val"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// ListTagsForResource should return all tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": rgARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags, _ := tagsOut["tags"].(map[string]any)
	assert.Equal(t, "teamA", tags["owner"])
	assert.Equal(t, "val", tags["extra"])
}

func TestHandler_TagsDeepCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create fleet.
	createRec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":         "deepcopy-fleet",
		"baseCapacity": 1,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	fleet, _ := createOut["fleet"].(map[string]any)
	fleetARN := fleet["arn"].(string)

	// Tag with a map we'll mutate after the call.
	inputTags := map[string]string{"key": "original"}
	err := h.Backend.TagResource(fleetARN, inputTags)
	require.NoError(t, err)

	// Mutate the original tags map.
	inputTags["key"] = "mutated"

	// Fleet should still have "original", not "mutated".
	storedTags, err := h.Backend.ListTagsForResource(fleetARN)
	require.NoError(t, err)
	assert.Equal(t, "original", storedTags["key"])
}
