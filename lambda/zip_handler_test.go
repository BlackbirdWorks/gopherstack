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

func TestZipHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, h *lambda.Handler, bk *mockBackend)
	}{
		{
			name: "CreateFunction_ZipPackageType",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
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
			},
		},
		{
			name: "CreateFunction_ZipMissingRuntime",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
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
			},
		},
		{
			name: "CreateFunction_ZipMissingCode",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
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
			},
		},
		{
			name: "CreateFunction_ZipWithS3Code",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
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
			},
		},
		{
			name: "UpdateFunctionCode_Zip",
			run: func(t *testing.T, h *lambda.Handler, bk *mockBackend) {
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
			},
		},
		{
			name: "UpdateFunctionCode_ZipMissingCode",
			run: func(t *testing.T, h *lambda.Handler, bk *mockBackend) {
				bk.functions["zip-update2"] = &lambda.FunctionConfiguration{
					FunctionName: "zip-update2",
					PackageType:  lambda.PackageTypeZip,
				}

				rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/zip-update2/code", `{}`, nil)

				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "CreateFunction_UnsupportedPackageType",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
				input := map[string]any{
					"FunctionName": "bad-type",
					"PackageType":  "S3",
					"Code":         map[string]any{"ImageUri": "my-image:v1"},
				}

				body, err := json.Marshal(input)
				require.NoError(t, err)

				rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "UpdateFunctionConfiguration_RuntimeAndHandler",
			run: func(t *testing.T, h *lambda.Handler, bk *mockBackend) {
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
			},
		},
		{
			name: "BuildCodeLocation_Image",
			run: func(t *testing.T, h *lambda.Handler, bk *mockBackend) {
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
			},
		},
		{
			name: "BuildCodeLocation_ZipWithS3",
			run: func(t *testing.T, h *lambda.Handler, bk *mockBackend) {
				bk.functions["zip-s3-loc"] = &lambda.FunctionConfiguration{
					FunctionName: "zip-s3-loc",
					PackageType:  lambda.PackageTypeZip,
					Runtime:      "python3.12",
				}

				rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/zip-s3-loc", "", nil)

				require.Equal(t, http.StatusOK, rec.Code)

				var out lambda.GetFunctionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Code)
				assert.Equal(t, "S3", out.Code.RepositoryType)
			},
		},
		{
			name: "CreateFunction_DefaultsToImage",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
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
			},
		},
		{
			name: "CreateFunction_NilCode",
			run: func(t *testing.T, h *lambda.Handler, _ *mockBackend) {
				input := map[string]any{
					"FunctionName": "nil-code",
					"PackageType":  "Image",
				}

				body, err := json.Marshal(input)
				require.NoError(t, err)

				rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, bk := newHandler(t)
			tt.run(t, h, bk)
		})
	}
}
