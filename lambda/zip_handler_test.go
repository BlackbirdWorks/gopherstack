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

// makeTestZip creates a minimal in-memory zip archive containing a single file.
func makeTestZip(t *testing.T, filename, content string) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	f, err := w.Create(filename)
	require.NoError(t, err)

	_, err = f.Write([]byte(content))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestCreateFunction_ZipPackageType(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	zipBytes := makeTestZip(t, "index.py", `def handler(event, context): return "hello"`)
	input := map[string]interface{}{
		"FunctionName": "zip-func",
		"PackageType":  "Zip",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Code": map[string]interface{}{
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
	assert.Greater(t, fn.CodeSize, int64(0))
}

func TestCreateFunction_ZipMissingRuntime(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	zipBytes := makeTestZip(t, "index.py", `def handler(event, context): return "hello"`)
	input := map[string]interface{}{
		"FunctionName": "zip-no-runtime",
		"PackageType":  "Zip",
		"Code": map[string]interface{}{
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

	input := map[string]interface{}{
		"FunctionName": "zip-no-code",
		"PackageType":  "Zip",
		"Runtime":      "python3.12",
		"Code":         map[string]interface{}{},
	}

	body, err := json.Marshal(input)
	require.NoError(t, err)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateFunction_ZipWithS3Code(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	input := map[string]interface{}{
		"FunctionName": "zip-s3-func",
		"PackageType":  "Zip",
		"Runtime":      "nodejs20.x",
		"Handler":      "index.handler",
		"Code": map[string]interface{}{
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

	zipBytes := makeTestZip(t, "index.py", `def handler(event, context): return "updated"`)
	input := map[string]interface{}{
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

	input := map[string]interface{}{
		"FunctionName": "bad-type",
		"PackageType":  "S3",
		"Code":         map[string]interface{}{"ImageUri": "my-image:v1"},
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
