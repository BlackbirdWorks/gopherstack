package stepfunctions_test

import (
	"encoding/base64"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

func TestSDKIntegration_EKSCall(t *testing.T) {
	t.Parallel()

	svc := stepfunctions.NewSDKIntegration(http.NotFoundHandler(), "us-east-1")

	tests := []struct {
		params    map[string]any
		wantOut   map[string]any
		name      string
		wantErr   string
		wantPath  string
		wantQuery string
		wantBody  string
	}{
		{
			name: "get_with_query",
			params: map[string]any{
				"Method": "GET", "Path": "/api/v1/namespaces",
				"QueryParameters": map[string]any{"labelSelector": []any{"app=x"}},
			},
			wantPath: "/api/v1/namespaces", wantQuery: "labelSelector=app%3Dx",
			wantOut: map[string]any{"ResponseBody": map[string]any{"kind": "NamespaceList", "items": []any{}}},
		},
		{
			name: "post_body",
			params: map[string]any{
				"Method": "POST", "Path": "/api/v1/namespaces", "RequestBody": map[string]any{"kind": "Namespace"},
			},
			wantPath: "/api/v1/namespaces", wantBody: `{"kind":"Namespace"}`,
			wantOut: map[string]any{"ResponseBody": map[string]any{"kind": "NamespaceList", "items": []any{}}},
		},
		{
			name:     "not_found_fails_task",
			params:   map[string]any{"Method": "GET", "Path": "/api/v1/namespaces/missing"},
			wantPath: "/api/v1/namespaces/missing", wantErr: "States.TaskFailed",
		},
		{
			name:    "missing_path",
			params:  map[string]any{"Method": "GET"},
			wantErr: "States.Runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotAuth, gotPath, gotQuery, gotMethod, gotBody string

			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				gotAuth, gotPath, gotQuery = r.Header.Get("Authorization"), r.URL.Path, r.URL.RawQuery
				gotMethod, gotBody = r.Method, string(b)

				if r.URL.Path == "/api/v1/namespaces/missing" {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"kind":"Status","reason":"NotFound"}`))

					return
				}

				_, _ = w.Write([]byte(`{"kind":"NamespaceList","items":[]}`))
			}))
			t.Cleanup(srv.Close)

			ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(
				&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))

			tt.params["ClusterName"] = "c1"
			tt.params["Endpoint"] = srv.URL
			tt.params["CertificateAuthority"] = ca

			out, err := svc.SFNCallSDK(t.Context(), asl.SDKCall{
				Service: "eks", Action: "call", Optimized: true, Params: tt.params, Region: "us-east-1",
			})

			if tt.wantErr != "" {
				var fe *asl.FailError

				require.ErrorAs(t, err, &fe)
				assert.Equal(t, tt.wantErr, fe.ErrCode)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOut, out)
			assert.Equal(t, tt.wantPath, gotPath)
			assert.Equal(t, tt.wantQuery, gotQuery)
			assert.Equal(t, tt.wantBody, gotBody)
			assert.Equal(t, tt.params["Method"], gotMethod)
			assert.True(t, strings.HasPrefix(gotAuth, "Bearer k8s-aws-v1."), gotAuth)
		})
	}
}

func TestSDKIntegration_EKSCall_SyntheticEndpointNeedsEngine(t *testing.T) {
	t.Parallel()

	svc := stepfunctions.NewSDKIntegration(http.NotFoundHandler(), "us-east-1")

	_, err := svc.SFNCallSDK(t.Context(), asl.SDKCall{
		Service: "eks", Action: "call", Optimized: true,
		Params: map[string]any{
			"ClusterName": "c", "Method": "GET", "Path": "/version",
			"Endpoint": "https://ABCDEF.gr7.us-east-1.eks.amazonaws.com",
		},
	})

	var fe *asl.FailError

	require.ErrorAs(t, err, &fe)
	assert.Equal(t, "States.TaskFailed", fe.ErrCode)
	assert.Contains(t, fe.Cause, "docker engine")
}
