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

func TestZipHandler_CreateFunction(t *testing.T) {
	t.Parallel()

	zipBytes := makeTestZip(t, `def handler(event, context): return "hello"`)

	tests := []struct {
		input                map[string]any
		name                 string
		wantFnName           string
		wantPackageType      string
		wantRuntime          string
		wantHandler          string
		wantStatus           int
		wantPositiveCodeSize bool
	}{
		{
			name: "ZipPackageType",
			input: map[string]any{
				"FunctionName": "zip-func",
				"PackageType":  "Zip",
				"Runtime":      "python3.12",
				"Handler":      "index.handler",
				"Code": map[string]any{
					"ZipFile": zipBytes,
				},
				"Role": "arn:aws:iam::123456789012:role/test",
			},
			wantStatus:           http.StatusCreated,
			wantFnName:           "zip-func",
			wantPackageType:      lambda.PackageTypeZip,
			wantRuntime:          "python3.12",
			wantHandler:          "index.handler",
			wantPositiveCodeSize: true,
		},
		{
			name: "ZipMissingRuntime",
			input: map[string]any{
				"FunctionName": "zip-no-runtime",
				"PackageType":  "Zip",
				"Code": map[string]any{
					"ZipFile": zipBytes,
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ZipMissingCode",
			input: map[string]any{
				"FunctionName": "zip-no-code",
				"PackageType":  "Zip",
				"Runtime":      "python3.12",
				"Code":         map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ZipWithS3Code",
			input: map[string]any{
				"FunctionName": "zip-s3-func",
				"PackageType":  "Zip",
				"Runtime":      "nodejs20.x",
				"Handler":      "index.handler",
				"Code": map[string]any{
					"S3Bucket": "my-bucket",
					"S3Key":    "functions/handler.zip",
				},
			},
			wantStatus:      http.StatusCreated,
			wantPackageType: lambda.PackageTypeZip,
			wantRuntime:     "nodejs20.x",
		},
		{
			name: "UnsupportedPackageType",
			input: map[string]any{
				"FunctionName": "bad-type",
				"PackageType":  "S3",
				"Code":         map[string]any{"ImageUri": "my-image:v1"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "DefaultsToImage",
			input: map[string]any{
				"FunctionName": "default-type",
				"Code":         map[string]any{"ImageUri": "my-image:v1"},
				"Role":         "arn:aws:iam::123456789012:role/test",
			},
			wantStatus:      http.StatusCreated,
			wantPackageType: lambda.PackageTypeImage,
		},
		{
			name: "NilCode",
			input: map[string]any{
				"FunctionName": "nil-code",
				"PackageType":  "Image",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)

			body, err := json.Marshal(tt.input)
			require.NoError(t, err)

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", string(body), nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusCreated {
				return
			}

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))

			if tt.wantFnName != "" {
				assert.Equal(t, tt.wantFnName, fn.FunctionName)
			}

			if tt.wantPackageType != "" {
				assert.Equal(t, tt.wantPackageType, fn.PackageType)
			}

			if tt.wantRuntime != "" {
				assert.Equal(t, tt.wantRuntime, fn.Runtime)
			}

			if tt.wantHandler != "" {
				assert.Equal(t, tt.wantHandler, fn.Handler)
			}

			if tt.wantPositiveCodeSize {
				assert.Positive(t, fn.CodeSize)
			}
		})
	}
}

func TestZipHandler_UpdateFunctionCode(t *testing.T) {
	t.Parallel()

	zipBody := func() string {
		zipBytes := makeTestZip(t, `def handler(event, context): return "updated"`)
		b, err := json.Marshal(map[string]any{"ZipFile": zipBytes})
		require.NoError(t, err)

		return string(b)
	}()

	tests := []struct {
		name           string
		fnName         string
		body           string
		seedFn         lambda.FunctionConfiguration
		wantStatus     int
		wantRevisionID bool
	}{
		{
			name:   "Zip",
			fnName: "zip-update",
			seedFn: lambda.FunctionConfiguration{
				FunctionName: "zip-update",
				PackageType:  lambda.PackageTypeZip,
				Runtime:      "python3.12",
			},
			body:           zipBody,
			wantStatus:     http.StatusOK,
			wantRevisionID: true,
		},
		{
			name:   "ZipMissingCode",
			fnName: "zip-update2",
			seedFn: lambda.FunctionConfiguration{
				FunctionName: "zip-update2",
				PackageType:  lambda.PackageTypeZip,
			},
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			seedFn := tt.seedFn
			bk.functions[tt.fnName] = &seedFn

			rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/"+tt.fnName+"/code", tt.body, nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRevisionID {
				var fn lambda.FunctionConfiguration
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
				assert.NotEmpty(t, fn.RevisionID)
			}
		})
	}
}

func TestZipHandler_UpdateFunctionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fnName      string
		body        string
		wantRuntime string
		wantHandler string
		seedFn      lambda.FunctionConfiguration
		wantStatus  int
	}{
		{
			name:   "RuntimeAndHandler",
			fnName: "cfg-func",
			seedFn: lambda.FunctionConfiguration{
				FunctionName: "cfg-func",
				PackageType:  lambda.PackageTypeZip,
				Runtime:      "python3.11",
				Handler:      "old.handler",
			},
			body:        `{"Runtime":"python3.12","Handler":"new.handler"}`,
			wantStatus:  http.StatusOK,
			wantRuntime: "python3.12",
			wantHandler: "new.handler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			seedFn := tt.seedFn
			bk.functions[tt.fnName] = &seedFn

			rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/"+tt.fnName+"/configuration", tt.body, nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
			assert.Equal(t, tt.wantRuntime, fn.Runtime)
			assert.Equal(t, tt.wantHandler, fn.Handler)
		})
	}
}

func TestZipHandler_BuildCodeLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		fnName             string
		wantImageURI       string
		wantRepositoryType string
		seedFn             lambda.FunctionConfiguration
		wantStatus         int
	}{
		{
			name:   "Image",
			fnName: "img-fn",
			seedFn: lambda.FunctionConfiguration{
				FunctionName: "img-fn",
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "my-registry/my-image:latest",
			},
			wantStatus:         http.StatusOK,
			wantImageURI:       "my-registry/my-image:latest",
			wantRepositoryType: "ECR",
		},
		{
			name:   "ZipWithS3",
			fnName: "zip-s3-loc",
			seedFn: lambda.FunctionConfiguration{
				FunctionName: "zip-s3-loc",
				PackageType:  lambda.PackageTypeZip,
				Runtime:      "python3.12",
			},
			wantStatus:         http.StatusOK,
			wantRepositoryType: "S3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			seedFn := tt.seedFn
			bk.functions[tt.fnName] = &seedFn

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.fnName, "", nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out lambda.GetFunctionOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.NotNil(t, out.Code)

			if tt.wantImageURI != "" {
				assert.Equal(t, tt.wantImageURI, out.Code.ImageURI)
			}

			assert.Equal(t, tt.wantRepositoryType, out.Code.RepositoryType)
		})
	}
}
