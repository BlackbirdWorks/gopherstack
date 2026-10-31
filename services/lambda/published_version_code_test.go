package lambda_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestInvokePublishedVersionKeepsCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		qualifier string
	}{
		{name: "numbered_version", qualifier: "1"},
		{name: "latest", qualifier: "$LATEST"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(19700+i*20, 19710+i*20)
			require.NoError(t, err)

			bk := lambda.NewInMemoryBackend(
				newMockDockerClient(),
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)
			closeBackend(t, bk)

			require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "zipfn", PackageType: lambda.PackageTypeZip, Runtime: "python3.12",
				Handler: "index.handler", ZipData: tinyZip(t), Timeout: 3,
			}))

			_, err = bk.PublishVersion("zipfn", "")
			require.NoError(t, err)

			_, _, _, status, invokeErr := bk.InvokeFunctionWithQualifier(
				t.Context(), "zipfn", tt.qualifier, "", "", lambda.InvocationTypeEvent, []byte(`{}`),
			)
			if invokeErr != nil {
				assert.NotContains(t, invokeErr.Error(), "no zip data available")
			}

			assert.NotEqual(t, http.StatusInternalServerError, status, "%v", invokeErr)
		})
	}
}

func tinyZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.py")
	require.NoError(t, err)
	_, err = w.Write([]byte("def handler(e, c):\n    return {}\n"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}
