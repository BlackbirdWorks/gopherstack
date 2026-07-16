package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- CodeSigningConfig tests ---

func TestNewOps_CodeSigningConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody string
		wantDesc   string
		wantStatus int
	}{
		{
			name:       "with_description",
			createBody: `{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]},"Description":"my csc"}`,
			wantStatus: http.StatusCreated,
			wantDesc:   "my csc",
		},
		{
			name:       "empty_body",
			createBody: `{}`,
			wantStatus: http.StatusCreated,
			wantDesc:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2020-04-22/code-signing-configs", tt.createBody)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.CreateCodeSigningConfigOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.CodeSigningConfig)
				assert.NotEmpty(t, out.CodeSigningConfig.CodeSigningConfigArn)
				assert.Equal(t, tt.wantDesc, out.CodeSigningConfig.Description)
			}
		})
	}
}

func TestNewOps_CodeSigningConfig_GetDeleteUpdate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Create
	rec := callInMemoryHandler(
		t,
		h,
		http.MethodPost,
		"/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]}}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	cscARN := createOut.CodeSigningConfig.CodeSigningConfigArn

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Get not found
	getNotFound := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/nonexistent", "")
	assert.Equal(t, http.StatusNotFound, getNotFound.Code)

	// Update
	updateRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/"+cscARN,
		`{"Description":"updated"}`)
	assert.Equal(t, http.StatusOK, updateRec.Code)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.CodeSigningConfigs, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscARN, "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestNewOps_FunctionCodeSigningConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "csc-test-fn")

	// Create a code signing config first
	rec := callInMemoryHandler(
		t,
		h,
		http.MethodPost,
		"/2020-04-22/code-signing-configs",
		`{"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:::p"]}}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	cscARN := createOut.CodeSigningConfig.CodeSigningConfigArn

	// Put function code signing config
	putBody := fmt.Sprintf(`{"CodeSigningConfigArn":%q}`, cscARN)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut lambda.PutFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&putOut))
	assert.Equal(t, cscARN, putOut.CodeSigningConfigArn)
	assert.Equal(t, "csc-test-fn", putOut.FunctionName)

	// Get function code signing config
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
	assert.Equal(t, cscARN, getOut.CodeSigningConfigArn)

	// List functions by code signing config
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		fmt.Sprintf("/2020-04-22/code-signing-configs/%s/functions", cscARN), "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionsByCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.NotEmpty(t, listOut.FunctionArns)

	// Delete function code signing config
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 200 with empty ARN (matches real AWS behavior)
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-test-fn/code-signing-config", "")
	assert.Equal(t, http.StatusOK, getRec2.Code)

	var getOut2 lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&getOut2))
	assert.Empty(t, getOut2.CodeSigningConfigArn)
}

func TestNewOps_FunctionCodeSigningConfig_MissingArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-csc")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/fn-no-csc/code-signing-config",
		`{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestNewOps_CodeSigningConfig_UpdateNotFound tests updating a nonexistent code signing config.
func TestNewOps_CodeSigningConfig_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/nonexistent-arn", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestNewOps_CodeSigningRoute_MethodNotAllowed tests method not allowed on code signing routes.
func TestNewOps_CodeSigningRoute_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPatch, "/2020-04-22/code-signing-configs", "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestNewOps_2020FunctionRoute_UnknownPath tests unknown 2020 function route.
func TestNewOps_2020FunctionRoute_UnknownPath(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2020-06-30/functions/fn/unknown-suffix", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestComprehensive_CodeSigning verifies full code signing config lifecycle and function association.
func TestComprehensive_CodeSigning(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// CreateCodeSigningConfig.
	createBody := `{
		"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:us-east-1:000000000000:/signing-profiles/p/v1"]},
		"Description":"test-csc"
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2020-04-22/code-signing-configs", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cscOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cscOut))
	require.NotNil(t, cscOut.CodeSigningConfig)
	cscArn := cscOut.CodeSigningConfig.CodeSigningConfigArn
	assert.NotEmpty(t, cscArn)
	assert.Equal(t, "test-csc", cscOut.CodeSigningConfig.Description)

	// GetCodeSigningConfig — key is the ARN.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscArn, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListCodeSigningConfigs.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.CodeSigningConfigs, 1)

	// UpdateCodeSigningConfig.
	updateBody := `{"Description":"updated"}`
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/"+cscArn, updateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a function to associate.
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "csc-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// PutFunctionCodeSigningConfig.
	putBody := fmt.Sprintf(`{"CodeSigningConfigArn":%q}`, cscArn)
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/csc-fn/code-signing-config", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetFunctionCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-fn/code-signing-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var getCSCOut lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCSCOut))
	assert.Equal(t, cscArn, getCSCOut.CodeSigningConfigArn)
	assert.Equal(t, "csc-fn", getCSCOut.FunctionName)

	// DeleteFunctionCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-06-30/functions/csc-fn/code-signing-config", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// DeleteCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-04-22/code-signing-configs/"+cscArn, "")
	require.Equal(t, http.StatusNoContent, rec.Code)
}
