package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Hub CRUD
// ---------------------------------------------------------------------------

func TestHandler_CreateHub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"HubName":        "my-hub",
				"HubDescription": "a test hub",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing hub name",
			body:     map[string]any{"HubDescription": "no name"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing hub description",
			body:     map[string]any{"HubName": "no-desc-hub"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateHub", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["HubArn"], "hub/my-hub")
			}
		})
	}
}

func TestHandler_CreateHub_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"HubName": "dup-hub", "HubDescription": "d"}

	rec := doSageMakerRequest(t, h, "CreateHub", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateHub", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_HubLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	recCreate := doSageMakerRequest(t, h, "CreateHub", map[string]any{
		"HubName":           "lifecycle-hub",
		"HubDescription":    "initial description",
		"HubDisplayName":    "Lifecycle Hub",
		"HubSearchKeywords": []string{"nlp", "vision"},
		"S3StorageConfig":   map[string]any{"S3OutputPath": "s3://bucket/prefix"},
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeHub", map[string]any{"HubName": "lifecycle-hub"})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &desc))
	assert.Equal(t, "lifecycle-hub", desc["HubName"])
	assert.Equal(t, "InService", desc["HubStatus"])
	assert.Equal(t, "initial description", desc["HubDescription"])
	assert.NotEmpty(t, desc["CreationTime"])
	assert.NotEmpty(t, desc["LastModifiedTime"])

	s3cfg, ok := desc["S3StorageConfig"].(map[string]any)
	require.True(t, ok, "S3StorageConfig should be present")
	assert.Equal(t, "s3://bucket/prefix", s3cfg["S3OutputPath"])

	// Update.
	recUpdate := doSageMakerRequest(t, h, "UpdateHub", map[string]any{
		"HubName":        "lifecycle-hub",
		"HubDescription": "updated description",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDesc2 := doSageMakerRequest(t, h, "DescribeHub", map[string]any{"HubName": "lifecycle-hub"})
	require.Equal(t, http.StatusOK, recDesc2.Code)

	var desc2 map[string]any
	require.NoError(t, json.Unmarshal(recDesc2.Body.Bytes(), &desc2))
	assert.Equal(t, "updated description", desc2["HubDescription"])
	// Untouched field should be preserved.
	assert.Equal(t, "Lifecycle Hub", desc2["HubDisplayName"])

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteHub", map[string]any{"HubName": "lifecycle-hub"})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	// Gone.
	recDesc3 := doSageMakerRequest(t, h, "DescribeHub", map[string]any{"HubName": "lifecycle-hub"})
	assert.Equal(t, http.StatusBadRequest, recDesc3.Code)
}

func TestHandler_Hub_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeHub", "UpdateHub", "DeleteHub"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"HubName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_ListHubs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha-hub", "beta-hub", "gamma-hub"} {
		rec := doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": name, "HubDescription": "d",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doSageMakerRequest(t, h, "ListHubs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		HubSummaries []map[string]any `json:"HubSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.HubSummaries, 3)

	// NameContains filter.
	recFiltered := doSageMakerRequest(t, h, "ListHubs", map[string]any{"NameContains": "beta"})
	require.Equal(t, http.StatusOK, recFiltered.Code)

	var filtered struct {
		HubSummaries []map[string]any `json:"HubSummaries"`
	}
	require.NoError(t, json.Unmarshal(recFiltered.Body.Bytes(), &filtered))
	require.Len(t, filtered.HubSummaries, 1)
	assert.Equal(t, "beta-hub", filtered.HubSummaries[0]["HubName"])
}

func TestHandler_DeleteHub_NotEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "occupied-hub", "HubDescription": "d",
		}).Code,
	)

	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "occupied-hub",
			"HubContentName":        "my-model",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDocument":    `{"Foo":"Bar"}`,
		}).Code,
	)

	rec := doSageMakerRequest(t, h, "DeleteHub", map[string]any{"HubName": "occupied-hub"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// HubContent CRUD
// ---------------------------------------------------------------------------

func TestHandler_ImportHubContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "content-hub", "HubDescription": "d",
		}).Code,
	)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"HubName":               "content-hub",
				"HubContentName":        "my-model",
				"HubContentType":        "Model",
				"DocumentSchemaVersion": "1.0.0",
				"HubContentDocument":    `{"Foo":"Bar"}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "hub not found",
			body: map[string]any{
				"HubName":               "no-such-hub",
				"HubContentName":        "my-model",
				"HubContentType":        "Model",
				"DocumentSchemaVersion": "1.0.0",
				"HubContentDocument":    `{"Foo":"Bar"}`,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing document",
			body: map[string]any{
				"HubName":               "content-hub",
				"HubContentName":        "my-model-2",
				"HubContentType":        "Model",
				"DocumentSchemaVersion": "1.0.0",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ImportHubContent", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["HubArn"])
				assert.Contains(t, resp["HubContentArn"], "hub-content/content-hub/Model/my-model")
			}
		})
	}
}

func TestHandler_ImportHubContent_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "dup-content-hub", "HubDescription": "d",
		}).Code,
	)

	body := map[string]any{
		"HubName":               "dup-content-hub",
		"HubContentName":        "my-model",
		"HubContentVersion":     "1.0.0",
		"HubContentType":        "Model",
		"DocumentSchemaVersion": "1.0.0",
		"HubContentDocument":    `{}`,
	}

	rec := doSageMakerRequest(t, h, "ImportHubContent", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "ImportHubContent", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_DescribeHubContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "describe-hub", "HubDescription": "d",
		}).Code,
	)

	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "describe-hub",
			"HubContentName":        "my-model",
			"HubContentVersion":     "2.0.0",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDisplayName": "My Model",
			"HubContentDocument":    `{"Foo":"Bar"}`,
		}).Code,
	)

	// Describe without version resolves to the (only, latest) version.
	rec := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName":        "describe-hub",
		"HubContentType": "Model",
		"HubContentName": "my-model",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-model", resp["HubContentName"])
	assert.Equal(t, "2.0.0", resp["HubContentVersion"])
	assert.Equal(t, "Model", resp["HubContentType"])
	assert.Equal(t, "Available", resp["HubContentStatus"])
	assert.Equal(t, "My Model", resp["HubContentDisplayName"])
	assert.JSONEq(t, `{"Foo":"Bar"}`, resp["HubContentDocument"].(string))

	// Not found: wrong version.
	recNF := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "describe-hub", "HubContentType": "Model",
		"HubContentName": "my-model", "HubContentVersion": "9.9.9",
	})
	assert.Equal(t, http.StatusBadRequest, recNF.Code)
}

func TestHandler_HubContent_ListsAndVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "versions-hub", "HubDescription": "d",
		}).Code,
	)

	for _, v := range []string{"1.0.0", "1.1.0", "2.0.0"} {
		require.Equal(t, http.StatusOK,
			doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
				"HubName":               "versions-hub",
				"HubContentName":        "my-model",
				"HubContentVersion":     v,
				"HubContentType":        "Model",
				"DocumentSchemaVersion": "1.0.0",
				"HubContentDocument":    `{}`,
			}).Code,
		)
	}

	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "versions-hub",
			"HubContentName":        "other-model",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDocument":    `{}`,
		}).Code,
	)

	// ListHubContents returns one entry per distinct name.
	recList := doSageMakerRequest(t, h, "ListHubContents", map[string]any{
		"HubName": "versions-hub", "HubContentType": "Model",
	})
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp struct {
		HubContentSummaries []map[string]any `json:"HubContentSummaries"`
	}
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	assert.Len(t, listResp.HubContentSummaries, 2)

	// ListHubContentVersions returns every version of one named content.
	recVersions := doSageMakerRequest(t, h, "ListHubContentVersions", map[string]any{
		"HubName": "versions-hub", "HubContentType": "Model", "HubContentName": "my-model",
	})
	require.Equal(t, http.StatusOK, recVersions.Code)

	var versionsResp struct {
		HubContentSummaries []map[string]any `json:"HubContentSummaries"`
	}
	require.NoError(t, json.Unmarshal(recVersions.Body.Bytes(), &versionsResp))
	require.Len(t, versionsResp.HubContentSummaries, 3)
}

func TestHandler_DeleteHubContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "delete-content-hub", "HubDescription": "d",
		}).Code,
	)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "delete-content-hub",
			"HubContentName":        "my-model",
			"HubContentVersion":     "1.0.0",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDocument":    `{}`,
		}).Code,
	)

	// Missing version is rejected (AWS requires it).
	recMissing := doSageMakerRequest(t, h, "DeleteHubContent", map[string]any{
		"HubName": "delete-content-hub", "HubContentType": "Model", "HubContentName": "my-model",
	})
	assert.Equal(t, http.StatusBadRequest, recMissing.Code)

	recDelete := doSageMakerRequest(t, h, "DeleteHubContent", map[string]any{
		"HubName": "delete-content-hub", "HubContentType": "Model",
		"HubContentName": "my-model", "HubContentVersion": "1.0.0",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "delete-content-hub", "HubContentType": "Model", "HubContentName": "my-model",
	})
	assert.Equal(t, http.StatusBadRequest, recDescribe.Code)

	// Deleting again is a not-found.
	recDeleteAgain := doSageMakerRequest(t, h, "DeleteHubContent", map[string]any{
		"HubName": "delete-content-hub", "HubContentType": "Model",
		"HubContentName": "my-model", "HubContentVersion": "1.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, recDeleteAgain.Code)
}

// ---------------------------------------------------------------------------
// HubContentReference
// ---------------------------------------------------------------------------

func TestHandler_HubContentReference_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "ref-hub", "HubDescription": "d",
		}).Code,
	)

	publicArn := "arn:aws:sagemaker:us-east-1:123456789012:hub-content/SageMakerPublicHub/Model/huggingface-model/1.2.0"

	recCreate := doSageMakerRequest(t, h, "CreateHubContentReference", map[string]any{
		"HubName":                      "ref-hub",
		"SageMakerPublicHubContentArn": publicArn,
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createResp))
	assert.NotEmpty(t, createResp["HubArn"])
	assert.Contains(t, createResp["HubContentArn"], "hub-content/ref-hub/ModelReference/huggingface-model")

	// Describe the derived reference (name inferred from the public ARN).
	recDesc := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "ref-hub", "HubContentType": "ModelReference", "HubContentName": "huggingface-model",
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &desc))
	assert.Equal(t, publicArn, desc["SageMakerPublicHubContentArn"])

	// Duplicate reference is rejected.
	recDup := doSageMakerRequest(t, h, "CreateHubContentReference", map[string]any{
		"HubName":                      "ref-hub",
		"SageMakerPublicHubContentArn": publicArn,
	})
	assert.Equal(t, http.StatusBadRequest, recDup.Code)

	// Delete the reference.
	recDelete := doSageMakerRequest(t, h, "DeleteHubContentReference", map[string]any{
		"HubName": "ref-hub", "HubContentType": "ModelReference", "HubContentName": "huggingface-model",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	recDescGone := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "ref-hub", "HubContentType": "ModelReference", "HubContentName": "huggingface-model",
	})
	assert.Equal(t, http.StatusBadRequest, recDescGone.Code)

	// Deleting a reference that doesn't exist is a not-found.
	recDeleteAgain := doSageMakerRequest(t, h, "DeleteHubContentReference", map[string]any{
		"HubName": "ref-hub", "HubContentType": "ModelReference", "HubContentName": "huggingface-model",
	})
	assert.Equal(t, http.StatusBadRequest, recDeleteAgain.Code)
}

// ---------------------------------------------------------------------------
// Presigned URLs
// ---------------------------------------------------------------------------

func TestHandler_CreateHubContentPresignedUrls(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "presign-hub", "HubDescription": "d",
		}).Code,
	)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "presign-hub",
			"HubContentName":        "my-model",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDocument":    `{}`,
		}).Code,
	)

	rec := doSageMakerRequest(t, h, "CreateHubContentPresignedUrls", map[string]any{
		"HubName": "presign-hub", "HubContentType": "Model", "HubContentName": "my-model",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		AuthorizedURLConfigs []map[string]string `json:"AuthorizedUrlConfigs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.AuthorizedURLConfigs, 1)
	assert.Contains(t, resp.AuthorizedURLConfigs[0]["Url"], "https://")
	assert.NotEmpty(t, resp.AuthorizedURLConfigs[0]["LocalPath"])

	// Not found: unknown content.
	recNF := doSageMakerRequest(t, h, "CreateHubContentPresignedUrls", map[string]any{
		"HubName": "presign-hub", "HubContentType": "Model", "HubContentName": "no-such-model",
	})
	assert.Equal(t, http.StatusBadRequest, recNF.Code)
}

// ---------------------------------------------------------------------------
// Persistence round-trip
// ---------------------------------------------------------------------------

func TestHub_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "CreateHub", map[string]any{
			"HubName": "persist-hub", "HubDescription": "d",
		}).Code,
	)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
			"HubName":               "persist-hub",
			"HubContentName":        "my-model",
			"HubContentType":        "Model",
			"DocumentSchemaVersion": "1.0.0",
			"HubContentDocument":    `{}`,
		}).Code,
	)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	recDesc := doSageMakerRequest(t, h2, "DescribeHub", map[string]any{"HubName": "persist-hub"})
	require.Equal(t, http.StatusOK, recDesc.Code)

	recDescContent := doSageMakerRequest(t, h2, "DescribeHubContent", map[string]any{
		"HubName": "persist-hub", "HubContentType": "Model", "HubContentName": "my-model",
	})
	require.Equal(t, http.StatusOK, recDescContent.Code)
}

func TestHandler_UpdateHubContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHub", map[string]any{"HubName": "my-hub2", "HubDescription": "test hub"})
	doSageMakerRequest(t, h, "ImportHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
		"HubContentVersion": "1.0.0", "DocumentSchemaVersion": "1.0.0", "HubContentDocument": "{}",
	})

	rec := doSageMakerRequest(t, h, "UpdateHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
		"HubContentVersion": "1.0.0", "HubContentDescription": "new description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeHubContent", map[string]any{
		"HubName": "my-hub2", "HubContentName": "model-a", "HubContentType": "Model",
	})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "new description", out["HubContentDescription"])
}

func TestHandler_UpdateHubContentReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHub", map[string]any{"HubName": "my-hub3", "HubDescription": "test hub"})
	doSageMakerRequest(t, h, "CreateHubContentReference", map[string]any{
		"HubName": "my-hub3",
		"SageMakerPublicHubContentArn": "arn:aws:sagemaker:us-east-1:aws:hub-content/" +
			"SageMakerPublicHub/Model/public-model/1.0.0",
	})

	rec := doSageMakerRequest(t, h, "UpdateHubContentReference", map[string]any{
		"HubName": "my-hub3", "HubContentName": "public-model", "HubContentType": "ModelReference",
		"MinVersion": "2.0.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["HubContentArn"])
}

func TestHandler_UpdateHubContent_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateHubContent", map[string]any{
		"HubName": "no-such-hub", "HubContentName": "x", "HubContentType": "Model", "HubContentVersion": "1.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// CreatePresignedDomainUrl
// ---------------------------------------------------------------------------
