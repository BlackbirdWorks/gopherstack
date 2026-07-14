package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Alias HTTP tests ---

func TestBatch1_Alias_CreateGetListUpdateDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "alias-test-fn"
	createFunctionForTest(t, h, fnName)

	// Publish a version to alias
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/versions", "{}")
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create alias
	rec = callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/functions/"+fnName+"/aliases",
		`{"Name":"live","FunctionVersion":"1","Description":"production alias"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "live")

	// Get alias
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production alias")

	// List aliases
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "live")

	// Update alias
	rec = callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/aliases/live",
		`{"FunctionVersion":"$LATEST","Description":"updated"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete alias
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete → 404
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/aliases/live", "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ============================================================
// FunctionVersion lifecycle: $LATEST → numbered
// ============================================================

func TestBatch2_PublishVersion_Basic(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-fn/versions", `{}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	assert.Equal(t, "1", v.Version)
	assert.Equal(t, "ver-fn", v.FunctionName)
	assert.NotEmpty(t, v.FunctionArn)
}

func TestBatch2_PublishVersion_Increments(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-inc-fn")

	for i := 1; i <= 3; i++ {
		rec := callInMemoryHandler(t, h, http.MethodPost,
			"/2015-03-31/functions/ver-inc-fn/versions", `{}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var v lambda.FunctionVersion
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
		assert.Equal(t, strconv.Itoa(i), v.Version)
	}
}

func TestBatch2_PublishVersion_WithDescription(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-desc-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-desc-fn/versions", `{"Description":"my-snapshot"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	assert.Equal(t, "my-snapshot", v.Description)
	assert.Equal(t, "1", v.Version)
}

func TestBatch2_PublishVersion_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/nonexistent/versions", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_ListVersionsByFunction_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-list-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/ver-list-fn/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListVersionsByFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// $LATEST is always present
	require.NotEmpty(t, out.Versions)
	assert.Equal(t, "$LATEST", out.Versions[0].Version)
}

func TestBatch2_ListVersionsByFunction_AfterPublish(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-after-fn")

	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/ver-after-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/ver-after-fn/versions", `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/ver-after-fn/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListVersionsByFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// $LATEST + 2 numbered versions
	assert.GreaterOrEqual(t, len(out.Versions), 2)

	versions := make(map[string]bool)
	for _, v := range out.Versions {
		versions[v.Version] = true
	}
	assert.True(t, versions["1"])
	assert.True(t, versions["2"])
}

func TestBatch2_ListVersionsByFunction_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/nonexistent/versions", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_PublishVersion_Publish_CreateFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"pub-create-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Publish":true}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, "1", fn.Version)
}

func TestBatch2_FunctionVersion_ARNFormat(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-arn-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-arn-fn/versions", `{}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	// versioned ARN should end with :1
	assert.True(t, strings.HasSuffix(v.FunctionArn, ":1"), "expected ARN to end with :1, got %s", v.FunctionArn)
}

// ============================================================
// Alias routing + weights
// ============================================================

func TestBatch2_Alias_CreateGetDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-fn")
	// publish a version first
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-fn/versions", `{}`)

	// Create alias
	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-fn/aliases",
		`{"Name":"live","FunctionVersion":"1","Description":"prod alias"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&alias))
	assert.Equal(t, "live", alias.Name)
	assert.Equal(t, "1", alias.FunctionVersion)
	assert.Equal(t, "prod alias", alias.Description)
	assert.NotEmpty(t, alias.AliasArn)

	// Get alias
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getAlias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getAlias))
	assert.Equal(t, "live", getAlias.Name)

	// List aliases
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListAliasesOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.Aliases, 1)

	// Delete alias
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestBatch2_Alias_RoutingConfig_WeightedTraffic(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-weighted-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-weighted-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-weighted-fn/versions", `{}`)

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-weighted-fn/aliases",
		`{"Name":"canary","FunctionVersion":"1","RoutingConfig":{"AdditionalVersionWeights":{"2":0.1}}}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&alias))
	require.NotNil(t, alias.RoutingConfig)
	assert.InDelta(t, 0.1, alias.RoutingConfig.AdditionalVersionWeights["2"], 0.001)
}

func TestBatch2_Alias_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-upd-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-upd-fn/aliases",
		`{"Name":"stable","FunctionVersion":"1"}`)

	// Update to point to version 2
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-upd-fn/aliases/stable",
		`{"FunctionVersion":"2","Description":"now v2"}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	assert.Equal(t, "2", updated.FunctionVersion)
	assert.Equal(t, "now v2", updated.Description)
}

func TestBatch2_Alias_UpdateRoutingConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-upd-rc-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-rc-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-rc-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-upd-rc-fn/aliases",
		`{"Name":"rc","FunctionVersion":"1"}`)

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-upd-rc-fn/aliases/rc",
		`{"RoutingConfig":{"AdditionalVersionWeights":{"2":0.2}}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	require.NotNil(t, updated.RoutingConfig)
	assert.InDelta(t, 0.2, updated.RoutingConfig.AdditionalVersionWeights["2"], 0.001)
}

func TestBatch2_Alias_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-dup-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-dup-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-dup-fn/aliases",
		`{"Name":"dup","FunctionVersion":"1"}`)

	dupRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-dup-fn/aliases",
		`{"Name":"dup","FunctionVersion":"1"}`)
	assert.Equal(t, http.StatusConflict, dupRec.Code)
}

func TestBatch2_Alias_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-404-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-404-fn/aliases/nope", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_Alias_FunctionNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/nonexistent-fn/aliases",
		`{"Name":"x","FunctionVersion":"1"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_Alias_RevisionID_Changes(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-rev-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-rev-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-rev-fn/versions", `{}`)

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-rev-fn/aliases",
		`{"Name":"rev","FunctionVersion":"1"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)
	var a1 lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&a1))

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-rev-fn/aliases/rev",
		`{"FunctionVersion":"2"}`)
	require.Equal(t, http.StatusOK, updRec.Code)
	var a2 lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&a2))

	assert.NotEqual(t, a1.RevisionID, a2.RevisionID)
}

// TestAudit2_Version_AlwaysLatestInCreateFunction verifies that CreateFunction always
// returns "Version": "$LATEST" in the response, matching AWS Lambda behaviour.
// Previously Version was absent when Publish was not set.
func TestAudit2_Version_AlwaysLatestInCreateFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantVersion string
	}{
		{
			name:        "no_publish_returns_latest",
			body:        `{"FunctionName":"audit2-create-fn","PackageType":"Image","Code":{"ImageUri":"x"},"Role":"arn"}`,
			wantVersion: "$LATEST",
		},
		{
			name: "with_publish_returns_numbered_version",
			body: `{"FunctionName":"audit2-create-pub-fn","PackageType":"Image",` +
				`"Code":{"ImageUri":"x"},"Role":"arn","Publish":true}`,
			wantVersion: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, tt.wantVersion, fn.Version,
				"CreateFunction response must include Version=%q", tt.wantVersion)
		})
	}
}

// TestAudit2_Version_AlwaysLatestInGetFunctionConfiguration verifies that
// GetFunctionConfiguration always returns "Version": "$LATEST" for the live code,
// matching AWS Lambda behaviour. Previously Version was absent from the response.
func TestAudit2_Version_AlwaysLatestInGetFunctionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "fresh_function"},
		{name: "after_config_update"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "audit2-getcfg-fn")

			if tt.name == "after_config_update" {
				rec := callInMemoryHandler(t, h, http.MethodPut,
					"/2015-03-31/functions/audit2-getcfg-fn/configuration",
					`{"Description":"updated"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/audit2-getcfg-fn/configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, "$LATEST", fn.Version,
				"GetFunctionConfiguration must always return Version=$LATEST for live code")
		})
	}
}

// TestAudit2_Version_LiveFunctionStaysLatestAfterPublish verifies that after publishing
// a numbered version (via PublishVersion or UpdateFunctionCode with Publish=true), the
// live function's GetFunctionConfiguration still returns "Version": "$LATEST".
// Previously maybePublishVersion mutated the stored live fn.Version to the numbered
// version, causing subsequent GetFunctionConfiguration to return "1" instead of "$LATEST".
func TestAudit2_Version_LiveFunctionStaysLatestAfterPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		publish func(t *testing.T, h *lambda.Handler)
		name    string
	}{
		{
			name: "after_explicit_publish_version",
			publish: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callInMemoryHandler(t, h, http.MethodPost,
					"/2015-03-31/functions/audit2-live-fn/versions", `{"Description":"v1"}`)
				require.Equal(t, http.StatusCreated, rec.Code)
			},
		},
		{
			name: "after_update_function_code_publish_true",
			publish: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callInMemoryHandler(t, h, http.MethodPut,
					"/2015-03-31/functions/audit2-live-fn/code",
					`{"ImageUri":"x:v2","Publish":true}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var upd lambda.FunctionConfiguration
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&upd))
				assert.Equal(t, "1", upd.Version,
					"UpdateFunctionCode Publish=true response must show numbered version")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "audit2-live-fn")

			tt.publish(t, h)

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/audit2-live-fn/configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, "$LATEST", fn.Version,
				"GetFunctionConfiguration after publish must still return Version=$LATEST")
		})
	}
}

// TestComprehensive_AliasRouting verifies that alias CRUD and weighted routing work end-to-end.
func TestComprehensive_AliasRouting(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Create a function.
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "alias-routing-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// Publish two versions.
	v1, err := bk.PublishVersion("alias-routing-fn", "v1 description")
	require.NoError(t, err)
	require.Equal(t, "1", v1.Version)

	v2, err := bk.PublishVersion("alias-routing-fn", "v2 description")
	require.NoError(t, err)
	require.Equal(t, "2", v2.Version)

	// CreateAlias pointing to v1.
	body := fmt.Sprintf(`{"Name":"prod","FunctionVersion":%q}`, v1.Version)
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-routing-fn/aliases", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
	assert.Equal(t, "prod", alias.Name)
	assert.Equal(t, v1.Version, alias.FunctionVersion)

	// GetAlias.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListAliases.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListAliasesOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Aliases, 1)

	// UpdateAlias — point to v2 with weighted routing 10% to v1.
	updateBody := fmt.Sprintf(
		`{"FunctionVersion":%q,"RoutingConfig":{"AdditionalVersionWeights":{%q:0.1}}}`,
		v2.Version, v1.Version,
	)
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", updateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, v2.Version, updated.FunctionVersion)
	require.NotNil(t, updated.RoutingConfig)
	assert.InDelta(t, 0.1, updated.RoutingConfig.AdditionalVersionWeights[v1.Version], 0.001)

	// DeleteAlias.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Confirm it's gone.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
