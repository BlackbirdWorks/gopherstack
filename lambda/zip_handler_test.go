package lambda_test

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/lambda"
)

// makeTestZip creates a minimal in-memory zip archive containing index.py with the given content.
func makeTestZip(t *testing.T, content string) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	f, err := w.Create("index.py")
	require.NoError(t, err)

	_, err = f.Write([]byte(content))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestCreateFunction_ZipPackageType(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	zipBytes := makeTestZip(t, `def handler(event, context): return "hello"`)
	input := map[string]any{
		"FunctionName": "zip-func",
		"PackageType":  "Zip",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Code": map[string]any{
			"ZipFile": zipBytes,
		},
		"Role": "arn:aws:iam::123456789012:role/test",
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, "zip-func", fn.FunctionName)
	assert.Equal(t, lambda.PackageTypeZip, fn.PackageType)
	assert.Equal(t, "python3.12", fn.Runtime)
	assert.Equal(t, "index.handler", fn.Handler)
	assert.Positive(t, fn.CodeSize)
}

func TestCreateFunction_ZipMissingRuntime(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	zipBytes := makeTestZip(t, `def handler(event, context): return "hello"`)
	input := map[string]any{
		"FunctionName": "zip-no-runtime",
		"PackageType":  "Zip",
		"Code": map[string]any{
			"ZipFile": zipBytes,
		},
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateFunction_ZipMissingCode(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	input := map[string]any{
		"FunctionName": "zip-no-code",
		"PackageType":  "Zip",
		"Runtime":      "python3.12",
		"Code":         map[string]any{},
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateFunction_ZipWithS3Code(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	input := map[string]any{
		"FunctionName": "zip-s3-func",
		"PackageType":  "Zip",
		"Runtime":      "nodejs20.x",
		"Handler":      "index.handler",
		"Code": map[string]any{
			"S3Bucket": "my-bucket",
			"S3Key":    "functions/handler.zip",
		},
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, lambda.PackageTypeZip, fn.PackageType)
	assert.Equal(t, "nodejs20.x", fn.Runtime)
}

func TestUpdateFunctionCode_Zip(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["zip-update"] = &lambda.FunctionConfiguration{
		FunctionName: "zip-update",
		PackageType:  lambda.PackageTypeZip,
		Runtime:      "python3.12",
	}

	zipBytes := makeTestZip(t, `def handler(event, context): return "updated"`)
	input := map[string]any{
		"ZipFile": zipBytes,
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/zip-update/code", string(body), nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.NotEmpty(t, fn.RevisionID)
}

func TestUpdateFunctionCode_ZipMissingCode(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["zip-update2"] = &lambda.FunctionConfiguration{
		FunctionName: "zip-update2",
		PackageType:  lambda.PackageTypeZip,
	}

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/zip-update2/code", `{}`, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateFunction_UnsupportedPackageType(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	input := map[string]any{
		"FunctionName": "bad-type",
		"PackageType":  "S3",
		"Code":         map[string]any{"ImageUri": "my-image:v1"},
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateFunctionConfiguration_RuntimeAndHandler(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["cfg-func"] = &lambda.FunctionConfiguration{
		FunctionName: "cfg-func",
		PackageType:  lambda.PackageTypeZip,
		Runtime:      "python3.11",
		Handler:      "old.handler",
	}

	body := `{"Runtime":"python3.12","Handler":"new.handler"}`
	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/cfg-func/configuration", body, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, "python3.12", fn.Runtime)
	assert.Equal(t, "new.handler", fn.Handler)
}

func TestBuildCodeLocation_Image(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["img-fn"] = &lambda.FunctionConfiguration{
		FunctionName: "img-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "my-registry/my-image:latest",
	}

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/img-fn", "", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Code)
	assert.Equal(t, "my-registry/my-image:latest", out.Code.ImageURI)
	assert.Equal(t, "ECR", out.Code.RepositoryType)
}

func TestBuildCodeLocation_ZipWithS3(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["zip-s3-loc"] = &lambda.FunctionConfiguration{
		FunctionName: "zip-s3-loc",
		PackageType:  lambda.PackageTypeZip,
		Runtime:      "python3.12",
		// S3BucketCode and S3KeyCode are internal fields, not in JSON
	}

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/zip-s3-loc", "", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Code)
	assert.Equal(t, "S3", out.Code.RepositoryType)
}

func TestCreateFunction_DefaultsToImage(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	// When PackageType is not specified, it should default to Image
	input := map[string]any{
		"FunctionName": "default-type",
		"Code":         map[string]any{"ImageUri": "my-image:v1"},
		"Role":         "arn:aws:iam::123456789012:role/test",
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, lambda.PackageTypeImage, fn.PackageType)
}

func TestCreateFunction_NilCode(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	input := map[string]any{
		"FunctionName": "nil-code",
		"PackageType":  "Image",
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
