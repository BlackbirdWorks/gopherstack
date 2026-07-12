package apigateway_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// restCall sends a raw REST-style request to h and returns the recorder,
// mirroring the shape a real aws-sdk-go-v2 apigateway client request takes
// (no X-Amz-Target header).
func restCall(
	t *testing.T, h *apigateway.Handler, method, path, contentType, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// ----- VPC Links (REST routing) -----

func TestAPIGateway_VpcLink_RESTLifecycle(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	createRec := restCall(
		t,
		h,
		http.MethodPost,
		"/vpclinks",
		"application/json",
		`{"name":"my-link","targetArns":["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc"]}`,
	)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	assert.Equal(t, "my-link", created["name"])

	getRec := restCall(t, h, http.MethodGet, "/vpclinks/"+id, "", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	listRec := restCall(t, h, http.MethodGet, "/vpclinks", "", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var list map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &list))
	items, _ := list["item"].([]any)
	assert.Len(t, items, 1)

	patchRec := restCall(t, h, http.MethodPatch, "/vpclinks/"+id, "application/json",
		`[{"op":"replace","path":"/name","value":"renamed-link"}]`)
	require.Equal(t, http.StatusOK, patchRec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(patchRec.Body.Bytes(), &updated))
	assert.Equal(t, "renamed-link", updated["name"])

	deleteRec := restCall(t, h, http.MethodDelete, "/vpclinks/"+id, "", "")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getAfterDeleteRec := restCall(t, h, http.MethodGet, "/vpclinks/"+id, "", "")
	assert.Equal(t, http.StatusNotFound, getAfterDeleteRec.Code)
}

// ----- Domain name access associations (REST routing + real state) -----

func TestAPIGateway_DomainNameAccessAssociation_RESTLifecycle(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	createRec := restCall(t, h, http.MethodPost, "/domainnameaccessassociations", "application/json",
		`{"domainNameArn":"arn:aws:apigateway:us-east-1::/domainnames/example.com",`+
			`"accessAssociationSource":"vpce-1234","accessAssociationSourceType":"VPCE"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	arn, _ := created["domainNameAccessAssociationArn"].(string)
	require.NotEmpty(t, arn, "create must return a real association ARN")

	listRec := restCall(t, h, http.MethodGet, "/domainnameaccessassociations", "", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var list map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &list))
	items, _ := list["item"].([]any)
	require.Len(t, items, 1, "list must reflect the created association, not a hardcoded empty list")

	deleteRec := restCall(t, h, http.MethodDelete, "/domainnameaccessassociations/"+arn, "", "")
	require.Equal(t, http.StatusAccepted, deleteRec.Code)

	listAfterDeleteRec := restCall(t, h, http.MethodGet, "/domainnameaccessassociations", "", "")
	var listAfter map[string]any
	require.NoError(t, json.Unmarshal(listAfterDeleteRec.Body.Bytes(), &listAfter))
	itemsAfter, _ := listAfter["item"].([]any)
	assert.Empty(t, itemsAfter)

	deleteAgainRec := restCall(t, h, http.MethodDelete, "/domainnameaccessassociations/"+arn, "", "")
	assert.Equal(
		t,
		http.StatusNotFound,
		deleteAgainRec.Code,
		"deleting a removed association must 404, not silently succeed",
	)
}

func TestAPIGateway_RejectDomainNameAccessAssociation(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	assoc, err := backend.CreateDomainNameAccessAssociation(apigateway.CreateDomainNameAccessAssociationInput{
		DomainNameARN:               "arn:aws:apigateway:us-east-1::/domainnames/example.com",
		AccessAssociationSource:     "vpce-1234",
		AccessAssociationSourceType: "VPCE",
	})
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodPost,
		"/rejectdomainnameaccessassociations?domainNameAccessAssociationArn="+
			assoc.DomainNameAccessAssociationARN+"&domainNameArn="+assoc.DomainNameARN,
		"", "")
	require.Equal(t, http.StatusAccepted, rec.Code)

	_, err = backend.GetDomainNameAccessAssociations("")
	require.NoError(t, err)

	list, err := backend.GetDomainNameAccessAssociations("")
	require.NoError(t, err)
	assert.Empty(t, list, "reject must remove the association")
}

// ----- SDK types (fixed catalog) -----

func TestAPIGateway_GetSdkTypes(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["item"].([]any)
	require.NotEmpty(t, items)

	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.(map[string]any)["id"].(string))
	}

	assert.Contains(t, ids, "java")
	assert.Contains(t, ids, "javascript")
	assert.Contains(t, ids, "android")
}

func TestAPIGateway_GetSdkType(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes/java", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "java", resp["id"])
	assert.Equal(t, "Java", resp["friendlyName"])

	notFoundRec := restCall(t, h, http.MethodGet, "/sdktypes/cobol", "", "")
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)
}

// ----- GetSdk (real archive containing the actual API's OpenAPI export) -----

func TestAPIGateway_GetSdk(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sdk-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/sdks/java", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "application/octet-stream", resp["contentType"])

	bodyB64, _ := resp["body"].(string)
	require.NotEmpty(t, bodyB64)

	raw, err := base64.StdEncoding.DecodeString(bodyB64)
	require.NoError(t, err)

	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err, "GetSdk must return a real ZIP archive, not a fabricated blob")

	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
	}

	assert.Contains(t, names, "swagger.json")
	assert.Contains(t, names, "README.txt")

	// Unknown sdkType must be rejected, not silently accepted.
	badRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/sdks/cobol", "", "")
	assert.Equal(t, http.StatusBadRequest, badRec.Code)

	// Unknown stage must 404.
	missingStageRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/nope/sdks/java", "", "")
	assert.Equal(t, http.StatusNotFound, missingStageRec.Code)
}

func TestAPIGateway_GetExport_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "export-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/exports/oas30", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	assert.Equal(t, "3.0.1", doc["openapi"])
}

// ----- UpdateClientCertificate / UpdateGatewayResponse (REST routing) -----

func TestAPIGateway_UpdateClientCertificate_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	cert, err := backend.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{})
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodPatch, "/clientcertificates/"+cert.ClientCertificateID, "application/json",
		`[{"op":"replace","path":"/description","value":"updated desc"}]`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "updated desc", resp["description"])
}

func TestAPIGateway_UpdateGatewayResponse_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "gwr-api"})
	require.NoError(t, err)

	_, err = backend.PutGatewayResponse(apigateway.PutGatewayResponseInput{
		RestAPIID: api.ID, ResponseType: "DEFAULT_4XX", StatusCode: "400",
	})
	require.NoError(t, err)

	rec := restCall(
		t, h, http.MethodPatch, "/restapis/"+api.ID+"/gatewayresponses/DEFAULT_4XX", "application/json",
		`[{"op":"replace","path":"/statusCode","value":"404"}]`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "404", resp["statusCode"])
}

// ----- ImportApiKeys / ImportDocumentationParts (raw-body REST routing) -----

func TestAPIGateway_ImportApiKeys_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	// AWS's API key CSV file format requires a header row naming columns
	// (e.g. "name,key"), followed by one data row per key; it is not a bare
	// "name,value" pair list.
	rec := restCall(t, h, http.MethodPost, "/apikeys?mode=import&format=csv", "application/octet-stream",
		"name,key\nfirst-key,abc123\nsecond-key,def456\n")
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ids, _ := resp["ids"].([]any)
	require.Len(t, ids, 2)

	keys, err := backend.GetAPIKeys()
	require.NoError(t, err)
	require.Len(t, keys, 2)

	names := []string{keys[0].Name, keys[1].Name}
	assert.Contains(t, names, "first-key")
	assert.Contains(t, names, "second-key")

	// A plain POST /apikeys (no mode=import) must still route to CreateAPIKey.
	createRec := restCall(t, h, http.MethodPost, "/apikeys", "application/json", `{"name":"direct-key"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)
}

func TestAPIGateway_ImportApiKeys_DuplicateWarns(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	_, err := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "dup-key"})
	require.NoError(t, err)

	// AWS's API key CSV file format requires a header row naming columns
	// (e.g. "name,key"), followed by one data row per key; it is not a bare
	// "name,value" pair list.
	rec := restCall(t, h, http.MethodPost, "/apikeys?mode=import&format=csv", "application/octet-stream",
		"name,key\ndup-key,abc123\nfresh-key,def456\n")
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ids, _ := resp["ids"].([]any)
	warnings, _ := resp["warnings"].([]any)
	assert.Len(t, ids, 1, "only the non-duplicate row is imported")
	assert.Len(t, warnings, 1, "the duplicate row is reported as a warning")
}

func TestAPIGateway_ImportDocumentationParts_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "doc-api"})
	require.NoError(t, err)

	_, err = backend.CreateDocumentationPart(apigateway.CreateDocumentationPartInput{
		RestAPIID:  api.ID,
		Location:   apigateway.DocumentationLocation{Type: "API"},
		Properties: `{"description":"old"}`,
	})
	require.NoError(t, err)

	payload := `{"documentationParts":[` +
		`{"location":{"type":"API"},"properties":{"description":"new root doc"}},` +
		`{"location":{"type":"METHOD","method":"GET","path":"/pets"},"properties":{"description":"list pets"}}` +
		`]}`

	rec := restCall(
		t, h, http.MethodPut, "/restapis/"+api.ID+"/documentation/parts?mode=overwrite",
		"application/octet-stream", payload,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ids, _ := resp["ids"].([]any)
	require.Len(t, ids, 2)

	parts, err := backend.GetDocumentationParts(api.ID)
	require.NoError(t, err)
	require.Len(t, parts, 2, "overwrite mode replaces the previously-existing part")
}

// ----- UpdateUsage (REST routing + real linked state via GetUsage) -----

func TestAPIGateway_UpdateUsage_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	plan, err := backend.CreateUsagePlan(apigateway.CreateUsagePlanInput{Name: "plan"})
	require.NoError(t, err)

	key, err := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "usage-key"})
	require.NoError(t, err)

	_, err = backend.CreateUsagePlanKey(apigateway.CreateUsagePlanKeyInput{
		UsagePlanID: plan.ID, KeyID: key.ID, KeyType: "API_KEY",
	})
	require.NoError(t, err)

	// "/remaining" is the real (and only) AWS-documented UpdateUsage patch
	// path (patch-operations.html's UpdateUsage table lists just this single
	// scalar field — there is no per-date path segment).
	rec := restCall(
		t, h, http.MethodPatch, "/usageplans/"+plan.ID+"/keys/"+key.ID+"/usage", "application/json",
		`[{"op":"replace","path":"/remaining","value":"42"}]`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["items"].(map[string]any)
	require.Contains(t, items, key.ID)

	// GetUsage must reflect the same override afterward.
	usage, err := backend.GetUsage(apigateway.GetUsageInput{UsagePlanID: plan.ID})
	require.NoError(t, err)
	require.Contains(t, usage.Items, key.ID)

	pair, ok := usage.Items[key.ID][0].([]int)
	require.True(t, ok, "usage.Items[keyID][0] must be a [used, remaining] pair")
	require.Equal(t, 42, pair[1], "PATCH /remaining must set the overridden remaining quota")
}

func TestAPIGateway_UpdateUsage_UnknownPlan(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(
		t, h, http.MethodPatch, "/usageplans/nope/keys/also-nope/usage", "application/json",
		`[{"op":"replace","path":"/remaining","value":"42"}]`,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
