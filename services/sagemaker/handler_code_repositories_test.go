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

func TestHandler_CreateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "my-repo",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "my-repo")
}

// TestHandler_CreateCodeRepository_RequiresGitConfig asserts GitConfig
// (required, api_op_CreateCodeRepository.go:44) and its RepositoryUrl
// (required within it, types.go:9216-9221) are rejected when absent --
// previously neither was validated, so a request omitting GitConfig
// entirely still succeeded.
func TestHandler_CreateCodeRepository_RequiresGitConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing git config entirely", body: map[string]any{"CodeRepositoryName": "repo-no-config"}},
		{
			name: "git config missing repository url",
			body: map[string]any{"CodeRepositoryName": "repo-no-url", "GitConfig": map[string]string{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doSageMakerRequest(t, h, "CreateCodeRepository", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DescribeCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-1",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})

	rec := doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "repo-1", resp["CodeRepositoryName"])

	// The awsjson1.1 protocol encodes timestamps as epoch-seconds numbers, not
	// RFC3339 strings — assert the wire type directly rather than trusting Go's
	// default time.Time JSON marshaling (which would silently emit a string).
	_, isNumber := resp["CreationTime"].(float64)
	assert.True(t, isNumber, "CreationTime must be a JSON number (epoch seconds), got %T", resp["CreationTime"])
	_, isNumber = resp["LastModifiedTime"].(float64)
	assert.True(t, isNumber, "LastModifiedTime must be a JSON number (epoch seconds), got %T", resp["LastModifiedTime"])
}

// TestHandler_UpdateCodeRepository asserts UpdateCodeRepository applies
// GitConfig.SecretArn (the only field types.GitConfigForUpdate declares --
// api_op_UpdateCodeRepository.go:35-38, types.go:9239-9248) WITHOUT wiping
// RepositoryUrl set at Create. Previously Update replaced the entire stored
// GitConfig map wholesale, so any Update call would silently drop
// RepositoryUrl/Branch.
func TestHandler_UpdateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-upd",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	secretArn := "arn:aws:secretsmanager:us-east-1:000000000000:secret:cred"
	rec := doSageMakerRequest(t, h, "UpdateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-upd",
		"GitConfig":          map[string]string{"SecretArn": secretArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "repo-upd")

	descRec := doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-upd"})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	gitConfig, ok := descResp["GitConfig"].(map[string]any)
	require.True(t, ok, "GitConfig must be present")
	assert.Equal(t, "https://github.com/test/repo", gitConfig["RepositoryUrl"],
		"Update must not wipe RepositoryUrl, which GitConfigForUpdate cannot even express")
	assert.Equal(t, secretArn, gitConfig["SecretArn"])
}

func TestHandler_DeleteCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-del",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	rec := doSageMakerRequest(t, h, "DeleteCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCodeRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-x",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-y",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})

	rec := doSageMakerRequest(t, h, "ListCodeRepositories", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["CodeRepositorySummaryList"].([]any)
	assert.Len(t, items, 2)
}

// TestHandler_ListCodeRepositories_FilterSort_RealClient asserts NameContains
// and the op's own doc default (api_op_ListCodeRepositories.go:60,63: SortBy
// Name, SortOrder Ascending) -- previously the handler decoded only
// NextToken and dropped every filter and sort control entirely.
func TestHandler_ListCodeRepositories_FilterSort_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"repo-zeta", "repo-alpha"} {
		_, err := client.CreateCodeRepository(t.Context(), &sagemakersdk.CreateCodeRepositoryInput{
			CodeRepositoryName: aws.String(name),
			GitConfig:          &smtypes.GitConfig{RepositoryUrl: aws.String("https://github.com/test/" + name)},
		})
		require.NoError(t, err)
	}

	out, err := client.ListCodeRepositories(t.Context(), &sagemakersdk.ListCodeRepositoriesInput{})
	require.NoError(t, err)
	require.Len(t, out.CodeRepositorySummaryList, 2)
	assert.Equal(t, "repo-alpha", aws.ToString(out.CodeRepositorySummaryList[0].CodeRepositoryName))
	assert.Equal(t, "repo-zeta", aws.ToString(out.CodeRepositorySummaryList[1].CodeRepositoryName))

	out, err = client.ListCodeRepositories(t.Context(), &sagemakersdk.ListCodeRepositoriesInput{
		NameContains: aws.String("zeta"),
	})
	require.NoError(t, err)
	require.Len(t, out.CodeRepositorySummaryList, 1)
	assert.Equal(t, "repo-zeta", aws.ToString(out.CodeRepositorySummaryList[0].CodeRepositoryName))
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------
