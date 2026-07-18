package appstream_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func newTestHandler(t *testing.T) *appstream.Handler {
	t.Helper()
	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")

	return appstream.NewHandler(backend)
}

func doRequest(t *testing.T, h *appstream.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "PhotonAdminProxyService."+action)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createStack(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateStack", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createFleet(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createAppBlock(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateAppBlock", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createAppBlockBuilder(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateAppBlockBuilder", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createApplication(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       name,
		"LaunchPath": "/app/" + name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createImage(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateImportedImage", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createImageBuilder(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateImageBuilder", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createUser(t *testing.T, h *appstream.Handler, userName string) {
	t.Helper()
	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           userName,
		"Email":              userName + "@example.com",
		"AuthenticationType": "USERPOOL",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestAppStream_DescribeByARN covers Describe operations whose real AWS
// request identifies resources by ARN rather than Name (DescribeApplications,
// DescribeAppBlocks, and the Arns branch of DescribeImages). Filtering these
// through a Name-keyed lookup -- as opposed to matching the stored Arn field
// -- would make every real SDK client's Describe-after-Create call fail with
// ResourceNotFoundException, since real clients pass back the Arn a prior
// Create/Describe call returned to them, never the bare Name.
func TestAppStream_DescribeByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name": "arn-app", "LaunchPath": "/app/arn-app",
	})
	require.Equal(t, http.StatusOK, appRec.Code)

	var appResp map[string]any
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
	appArn := appResp["Application"].(map[string]any)["Arn"].(string)
	require.NotEmpty(t, appArn)

	abRec := doRequest(t, h, "CreateAppBlock", map[string]any{"Name": "arn-appblock"})
	require.Equal(t, http.StatusOK, abRec.Code)

	var abResp map[string]any
	require.NoError(t, json.Unmarshal(abRec.Body.Bytes(), &abResp))
	appBlockArn := abResp["AppBlock"].(map[string]any)["Arn"].(string)
	require.NotEmpty(t, appBlockArn)

	imgRec := doRequest(t, h, "CreateImportedImage", map[string]any{"Name": "arn-image"})
	require.Equal(t, http.StatusOK, imgRec.Code)

	var imgResp map[string]any
	require.NoError(t, json.Unmarshal(imgRec.Body.Bytes(), &imgResp))
	imageArn := imgResp["Image"].(map[string]any)["Arn"].(string)
	require.NotEmpty(t, imageArn)

	t.Run("DescribeApplications filters by Arn", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "DescribeApplications", map[string]any{"Arns": []string{appArn}})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		apps := resp["Applications"].([]any)
		require.Len(t, apps, 1)
		assert.Equal(t, "arn-app", apps[0].(map[string]any)["Name"])
	})

	t.Run("DescribeAppBlocks filters by Arn", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "DescribeAppBlocks", map[string]any{"Arns": []string{appBlockArn}})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		blocks := resp["AppBlocks"].([]any)
		require.Len(t, blocks, 1)
		assert.Equal(t, "arn-appblock", blocks[0].(map[string]any)["Name"])
	})

	t.Run("DescribeImages filters by Arn", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "DescribeImages", map[string]any{"Arns": []string{imageArn}})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		images := resp["Images"].([]any)
		require.Len(t, images, 1)
		assert.Equal(t, "arn-image", images[0].(map[string]any)["Name"])
	})
}

// TestAppStream_AssociationsAcceptARNIdentifiers covers association
// operations whose real AWS request identifies one side of the link by ARN
// (AssociateApplicationFleet's ApplicationArn, AssociateAppBlockBuilderAppBlock's
// AppBlockArn). A real SDK client always supplies the Arn from a prior
// Create/Describe response, so the backend must resolve it to the resource's
// canonical Name rather than trying to index its Name-keyed table with it.
func TestAppStream_AssociationsAcceptARNIdentifiers(t *testing.T) {
	t.Parallel()

	t.Run("AssociateApplicationFleet accepts ApplicationArn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createFleet(t, h, "assoc-arn-fleet")

		appRec := doRequest(t, h, "CreateApplication", map[string]any{
			"Name": "assoc-arn-app", "LaunchPath": "/app/assoc-arn-app",
		})
		require.Equal(t, http.StatusOK, appRec.Code)

		var appResp map[string]any
		require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appResp))
		appArn := appResp["Application"].(map[string]any)["Arn"].(string)
		require.NotEmpty(t, appArn)

		assocRec := doRequest(t, h, "AssociateApplicationFleet", map[string]any{
			"ApplicationArn": appArn, "FleetName": "assoc-arn-fleet",
		})
		require.Equal(t, http.StatusOK, assocRec.Code)

		descRec := doRequest(t, h, "DescribeApplicationFleetAssociations", map[string]any{
			"ApplicationArn": appArn,
		})
		require.Equal(t, http.StatusOK, descRec.Code)

		var descResp map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
		assocs := descResp["ApplicationFleetAssociations"].([]any)
		require.Len(t, assocs, 1)
		assert.Equal(t, "assoc-arn-fleet", assocs[0].(map[string]any)["FleetName"])
	})

	t.Run("AssociateAppBlockBuilderAppBlock accepts AppBlockArn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createAppBlockBuilder(t, h, "assoc-arn-builder")

		abRec := doRequest(t, h, "CreateAppBlock", map[string]any{"Name": "assoc-arn-appblock"})
		require.Equal(t, http.StatusOK, abRec.Code)

		var abResp map[string]any
		require.NoError(t, json.Unmarshal(abRec.Body.Bytes(), &abResp))
		appBlockArn := abResp["AppBlock"].(map[string]any)["Arn"].(string)
		require.NotEmpty(t, appBlockArn)

		assocRec := doRequest(t, h, "AssociateAppBlockBuilderAppBlock", map[string]any{
			"AppBlockBuilderName": "assoc-arn-builder", "AppBlockArn": appBlockArn,
		})
		require.Equal(t, http.StatusOK, assocRec.Code)

		descRec := doRequest(t, h, "DescribeAppBlockBuilderAppBlockAssociations", map[string]any{
			"AppBlockBuilderName": "assoc-arn-builder",
		})
		require.Equal(t, http.StatusOK, descRec.Code)

		var descResp map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
		assocs := descResp["AppBlockBuilderAppBlockAssociations"].([]any)
		require.Len(t, assocs, 1)
		assert.Equal(t, appBlockArn, assocs[0].(map[string]any)["AppBlockArn"])
	})
}
