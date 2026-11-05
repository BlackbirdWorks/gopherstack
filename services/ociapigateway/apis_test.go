package ociapigateway_test

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const petsYAML = `openapi: 3.0.0
info: {title: pets, version: "1"}
paths:
  /pets:
    get: {}
    post: {}
  /pets/{petId}:
    get: {}
    delete: {}
`

func (e *env) mkAPI(t *testing.T, name, content string) apigateway.CreateApiResponse {
	t.Helper()

	resp, err := e.api.CreateApi(t.Context(), apigateway.CreateApiRequest{
		CreateApiDetails: apigateway.CreateApiDetails{
			CompartmentId: new(testCompartment), DisplayName: new(name), Content: new(content),
		},
	})
	require.NoError(t, err)
	e.settle()

	return resp
}

func TestAPIResource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.mkAPI(t, "pets", petsYAML)
	require.NotNil(t, created.OpcWorkRequestId)
	assert.Equal(t, "OpenAPI 3.0.0", *created.SpecificationType)

	got, err := e.api.GetApi(t.Context(), apigateway.GetApiRequest{ApiId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, apigateway.ApiLifecycleStateActive, got.LifecycleState)

	content, err := e.api.GetApiContent(t.Context(), apigateway.GetApiContentRequest{ApiId: created.Id})
	require.NoError(t, err)

	defer content.Content.Close()

	raw, err := io.ReadAll(content.Content)
	require.NoError(t, err)
	assert.Equal(t, petsYAML, string(raw))

	sum := sha256.Sum256(raw)
	assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), *content.XContentSha256)

	spec, err := e.api.GetApiDeploymentSpecification(t.Context(), apigateway.GetApiDeploymentSpecificationRequest{ApiId: created.Id})
	require.NoError(t, err)
	require.Len(t, spec.Routes, 2)
	assert.Equal(t, "/pets", *spec.Routes[0].Path)
	assert.Equal(t, []apigateway.ApiSpecificationRouteMethodsEnum{"GET", "POST"}, spec.Routes[0].Methods)
	assert.Equal(t, "/pets/{petId}", *spec.Routes[1].Path)

	vals, err := e.api.GetApiValidations(t.Context(), apigateway.GetApiValidationsRequest{ApiId: created.Id})
	require.NoError(t, err)
	require.NotEmpty(t, vals.Validations)

	for _, v := range vals.Validations {
		assert.Equal(t, apigateway.ApiValidationDetailsResultOk, v.Result, *v.Name)
	}

	list, err := e.api.ListApis(t.Context(), apigateway.ListApisRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.api.UpdateApi(t.Context(), apigateway.UpdateApiRequest{
		ApiId: created.Id, UpdateApiDetails: apigateway.UpdateApiDetails{Content: new("openapi: 3.0.1\npaths: {}\n")},
	})
	require.NoError(t, err)
	e.settle()

	vals, err = e.api.GetApiValidations(t.Context(), apigateway.GetApiValidationsRequest{ApiId: created.Id})
	require.NoError(t, err)

	results := map[string]apigateway.ApiValidationDetailsResultEnum{}
	for _, v := range vals.Validations {
		results[*v.Name] = v.Result
	}

	assert.Equal(t, apigateway.ApiValidationDetailsResultWarning, results["Paths"], "no paths is a warning")

	_, err = e.api.DeleteApi(t.Context(), apigateway.DeleteApiRequest{ApiId: created.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.api.GetApiContent(t.Context(), apigateway.GetApiContentRequest{ApiId: created.Id})
	require.NoError(t, err, "DELETED resources stay readable")
}

func TestAPIValidationFindsBadPaths(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.mkAPI(t, "bad", "openapi: 3.0.0\npaths:\n  /a/{x}/{y*}/z:\n    get: {}\n")

	vals, err := e.api.GetApiValidations(t.Context(), apigateway.GetApiValidationsRequest{ApiId: created.Id})
	require.NoError(t, err)

	var failed []string

	for _, v := range vals.Validations {
		if v.Result == apigateway.ApiValidationDetailsResultError {
			failed = append(failed, *v.Name)
			require.NotEmpty(t, v.Details)
		}
	}

	assert.Equal(t, []string{"Deployment specification"}, failed)
}

func TestAPIRejectsUnparseableContent(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	_, err := e.api.CreateApi(t.Context(), apigateway.CreateApiRequest{
		CreateApiDetails: apigateway.CreateApiDetails{CompartmentId: new(testCompartment), Content: new("a: [unclosed")},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestSDKResource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAPI(t, "sdk-api", petsYAML)

	langs, err := e.api.ListSdkLanguageTypes(t.Context(), apigateway.ListSdkLanguageTypesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.NotEmpty(t, langs.Items)

	created, err := e.api.CreateSdk(t.Context(), apigateway.CreateSdkRequest{
		CreateSdkDetails: apigateway.CreateSdkDetails{
			ApiId: a.Id, TargetLanguage: langs.Items[0].Name, DisplayName: new("s1"),
			Parameters: map[string]string{"k": "v"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, testCompartment, *created.CompartmentId)
	assert.Nil(t, created.ArtifactUrl, "no client library is generated, so no artifact url is invented")
	e.settle()

	updated, err := e.api.UpdateSdk(t.Context(), apigateway.UpdateSdkRequest{
		SdkId: created.Id, UpdateSdkDetails: apigateway.UpdateSdkDetails{DisplayName: new("s2")},
	})
	require.NoError(t, err)
	assert.Equal(t, "s2", *updated.DisplayName)
	assert.Equal(t, apigateway.SdkLifecycleStateActive, updated.LifecycleState)

	list, err := e.api.ListSdks(t.Context(), apigateway.ListSdksRequest{ApiId: a.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.api.CreateSdk(t.Context(), apigateway.CreateSdkRequest{
		CreateSdkDetails: apigateway.CreateSdkDetails{ApiId: new("ocid1.none"), TargetLanguage: new("JAVA")},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.api.DeleteSdk(t.Context(), apigateway.DeleteSdkRequest{SdkId: created.Id})
	require.NoError(t, err)
}
