package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateProject", map[string]any{
		"ProjectName": "my-project",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ProjectArn"], "my-project")
	assert.NotEmpty(t, resp["ProjectId"])
}

func TestHandler_DescribeProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-1"})

	rec := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "proj-1", resp["ProjectName"])
	assert.Equal(t, "CreateCompleted", resp["ProjectStatus"])
}

func TestHandler_DeleteProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-del"})
	rec := doSageMakerRequest(t, h, "DeleteProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListProjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})
	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-b"})

	rec := doSageMakerRequest(t, h, "ListProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ProjectSummaryList"].([]any)
	assert.Len(t, items, 2)
}

func TestHandler_ListProjects_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "alpha-project"})
	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "beta-project"})

	tests := []struct {
		body      map[string]any
		name      string
		wantNames []string
	}{
		{name: "name contains", body: map[string]any{"NameContains": "alpha"}, wantNames: []string{"alpha-project"}},
		{
			name:      "sort by name descending",
			body:      map[string]any{"SortBy": "Name", "SortOrder": "Descending"},
			wantNames: []string{"beta-project", "alpha-project"},
		},
		{name: "max results caps page", body: map[string]any{"MaxResults": 1}, wantNames: []string{"alpha-project"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ListProjects", tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items := resp["ProjectSummaryList"].([]any)
			require.Len(t, items, len(tc.wantNames))

			for i, want := range tc.wantNames {
				assert.Equal(t, want, items[i].(map[string]any)["ProjectName"])
			}
		})
	}
}

// TestHandler_DescribeProject_NoTagsLeak asserts DescribeProjectOutput never
// includes Tags -- previously the handler marshaled the internal storage
// struct directly, which does carry Tags, so every real client's Describe
// call received a field the SDK deserializer never expects (the same
// fabricated-response-field bug class as DescribeInferenceComponent's Tags,
// found in the prior pass).
func TestHandler_DescribeProject_NoTagsLeak(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{
		"ProjectName": "proj-tags",
		"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
	})

	rec := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-tags"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotContains(t, resp, "Tags")
	assert.NotZero(t, resp["LastModifiedTime"])
}

// TestHandler_CreateProject_ServiceCatalogProvisioningDetails_RealClient
// asserts CreateProjectInput.ServiceCatalogProvisioningDetails -- previously
// entirely absent -- now round-trips through DescribeProject.
func TestHandler_CreateProject_ServiceCatalogProvisioningDetails_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateProject(t.Context(), &sagemakersdk.CreateProjectInput{
		ProjectName: aws.String("proj-service-catalog"),
		ServiceCatalogProvisioningDetails: &smtypes.ServiceCatalogProvisioningDetails{
			ProductId: aws.String("prod-abc123"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeProject(t.Context(), &sagemakersdk.DescribeProjectInput{
		ProjectName: aws.String("proj-service-catalog"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ServiceCatalogProvisioningDetails)
	assert.Equal(t, "prod-abc123", aws.ToString(out.ServiceCatalogProvisioningDetails.ProductId))
}

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

func TestHandler_UpdateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{
		"ProjectName": "proj-a", "ProjectDescription": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-a"})
	require.Equal(t, http.StatusOK, recDescribe.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "updated description", out["ProjectDescription"])
}

func TestHandler_UpdateProject_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{"ProjectName": "no-such-project"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Pipeline versions and execution-definition extras
// ---------------------------------------------------------------------------
