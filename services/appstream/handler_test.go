package appstream_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// cborTestServicePath mirrors the rpc-v2-cbor path prefix real
// aws-sdk-go-v2/service/appstream@v1.64.5+ clients send requests to (see
// cborServicePath in rpcv2cbor.go).
const cborTestServicePath = "/service/PhotonAdminProxyService/operation/"

// postCBOR sends an rpc-v2-cbor POST to the AppStream handler.
func postCBOR(t *testing.T, h *appstream.Handler, op string, body cbor.Map) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, cborTestServicePath+op, bytes.NewReader(cbor.Encode(body)))
	req.Header.Set("Content-Type", "application/cbor")
	req.Header.Set("Smithy-Protocol", "rpc-v2-cbor")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// decodeCBORResponse decodes a CBOR response body into a cbor.Map.
func decodeCBORResponse(t *testing.T, rec *httptest.ResponseRecorder) cbor.Map {
	t.Helper()

	v, err := cbor.Decode(rec.Body.Bytes())
	require.NoError(t, err)

	m, ok := v.(cbor.Map)
	require.True(t, ok, "expected CBOR map response")

	return m
}

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
		"IconS3Location": map[string]any{
			"S3Bucket": "icon-bucket",
			"S3Key":    "icons/" + name + ".png",
		},
		"InstanceFamilies": []string{"GENERAL_PURPOSE"},
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
		"IconS3Location":   map[string]any{"S3Bucket": "icon-bucket", "S3Key": "icons/arn-app.png"},
		"InstanceFamilies": []string{"GENERAL_PURPOSE"},
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
			"IconS3Location":   map[string]any{"S3Bucket": "icon-bucket", "S3Key": "icons/assoc-arn-app.png"},
			"InstanceFamilies": []string{"GENERAL_PURPOSE"},
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

// TestAppStream_RPCv2CBOR covers the rpc-v2-cbor protocol added in
// aws-sdk-go-v2/service/appstream@v1.64.5 (see rpcv2cbor.go), which
// replaced the awsjson1.1/X-Amz-Target wire format the rest of this file's
// helpers (doRequest et al.) still exercise. Both protocols must keep
// working: real SDK clients now speak CBOR, but older pinned SDKs, the
// Terraform provider, and this file's own JSON-based helpers still speak
// the legacy protocol.
func TestAppStream_RPCv2CBOR(t *testing.T) {
	t.Parallel()

	t.Run("RouteMatcher", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			path string
			want bool
		}{
			{name: "matches known CBOR operation", path: cborTestServicePath + "CreateStack", want: true},
			{name: "rejects unknown CBOR operation", path: cborTestServicePath + "NotAnOperation", want: false},
			{name: "rejects non-CBOR path", path: "/", want: false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, tt.path, nil)
				req.Header.Set("Content-Type", "application/cbor")
				rec := httptest.NewRecorder()
				assert.Equal(t, tt.want, h.RouteMatcher()(e.NewContext(req, rec)))
			})
		}
	})

	t.Run("ExtractOperation reads the CBOR path", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, cborTestServicePath+"DescribeFleets", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.Equal(t, "DescribeFleets", h.ExtractOperation(c))
	})

	t.Run("CreateStack response encodes CreatedTime as a CBOR timestamp tag", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := postCBOR(t, h, "CreateStack", cbor.Map{"Name": cbor.String("cbor-stack")})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))

		stack, ok := decodeCBORResponse(t, rec)["Stack"].(cbor.Map)
		require.True(t, ok)
		assert.Equal(t, cbor.String("cbor-stack"), stack["Name"])

		createdTag, ok := stack["CreatedTime"].(*cbor.Tag)
		require.True(t, ok, "CreatedTime must be a CBOR Tag(1, epoch-seconds), got %T", stack["CreatedTime"])
		assert.Equal(t, uint64(1), createdTag.ID)
	})

	t.Run("CreateFleet then DescribeFleets round-trips over CBOR", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := postCBOR(t, h, "CreateFleet", cbor.Map{
			"Name":         cbor.String("cbor-fleet"),
			"InstanceType": cbor.String("stream.standard.medium"),
			"ComputeCapacity": cbor.Map{
				"DesiredInstances": cbor.Uint(2),
			},
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		fleet, ok := decodeCBORResponse(t, createRec)["Fleet"].(cbor.Map)
		require.True(t, ok)
		assert.Equal(t, cbor.String("cbor-fleet"), fleet["Name"])

		capacity, ok := fleet["ComputeCapacityStatus"].(cbor.Map)
		require.True(t, ok)
		assert.Equal(t, cbor.Uint(2), capacity["Desired"])

		descRec := postCBOR(t, h, "DescribeFleets", cbor.Map{
			"Names": cbor.List{cbor.String("cbor-fleet")},
		})
		require.Equal(t, http.StatusOK, descRec.Code)

		fleets, ok := decodeCBORResponse(t, descRec)["Fleets"].(cbor.List)
		require.True(t, ok)
		require.Len(t, fleets, 1)
	})

	t.Run("unknown resource returns a CBOR error with __type set", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := postCBOR(t, h, "DeleteStack", cbor.Map{"Name": cbor.String("does-not-exist")})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))

		body := decodeCBORResponse(t, rec)
		assert.Equal(t, cbor.String("ResourceNotFoundException"), body["__type"])
		assert.Equal(t, "ResourceNotFoundException", rec.Header().Get("X-Amzn-Errortype"))
	})

	t.Run("invalid CBOR body is rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(
			http.MethodPost,
			cborTestServicePath+"CreateStack",
			bytes.NewReader([]byte{0x00, 0xFF, 0xAA}),
		)
		req.Header.Set("Content-Type", "application/cbor")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		body := decodeCBORResponse(t, rec)
		assert.Equal(t, cbor.String("SerializationException"), body["__type"])
	})

	t.Run("legacy X-Amz-Target JSON protocol still works alongside CBOR", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "CreateStack", map[string]any{"Name": "json-stack"})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	// Every operation the handler supports must be reachable over rpc-v2-cbor,
	// not just the ones exercised above -- this is the actual regression
	// under test (CI failures on AppStream fleet/stack lifecycle ops after
	// the SDK's protocol switch). A response need not succeed (an empty
	// input body will fail validation for most operations), but it must
	// always come back as well-formed, decodable CBOR with the
	// Smithy-Protocol header set; a route miss or an encoding bug would
	// instead surface as an undecodable body, a panic, or a missing header.
	t.Run("every supported operation is reachable over CBOR", func(t *testing.T) {
		t.Parallel()

		for _, op := range newTestHandler(t).GetSupportedOperations() {
			t.Run(op, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				rec := postCBOR(t, h, op, cbor.Map{})

				_, decErr := cbor.Decode(rec.Body.Bytes())
				require.NoError(t, decErr, "operation %s produced undecodable CBOR response", op)
				assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))
			})
		}
	})
}
