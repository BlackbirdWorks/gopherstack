package apigateway_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

type recordingAWSInvoker struct {
	got  apigateway.AWSServiceRequest
	resp apigateway.AWSServiceResponse
}

func (r *recordingAWSInvoker) Supports(service string) bool { return service != "unknown" }

func (r *recordingAWSInvoker) InvokeAWSService(
	_ context.Context, req apigateway.AWSServiceRequest,
) (apigateway.AWSServiceResponse, error) {
	r.got = req

	return r.resp, nil
}

func TestAWSServiceInvoker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		wantSvc    string
		wantSpec   string
		resp       apigateway.AWSServiceResponse
		wantStatus int
		wantCalled bool
	}{
		{
			name: "action", uri: "arn:aws:apigateway:eu-west-1:states:action/StartExecution",
			resp: apigateway.AWSServiceResponse{Status: http.StatusOK, Body: []byte(`{}`)}, wantStatus: http.StatusOK,
			wantCalled: true, wantSvc: "states", wantSpec: "StartExecution",
		},
		{
			name: "backend_error_status", uri: "arn:aws:apigateway:us-east-1:dynamodb:action/GetItem",
			resp:       apigateway.AWSServiceResponse{Status: http.StatusBadRequest, Body: []byte(`{"__type":"x"}`)},
			wantStatus: http.StatusBadRequest, wantCalled: true, wantSvc: "dynamodb", wantSpec: "GetItem",
		},
		{
			name: "unsupported_falls_back", uri: "arn:aws:apigateway:us-east-1:unknown:action/Foo",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIViaHandler(t, "AWS", tt.uri)
			inv := &recordingAWSInvoker{resp: tt.resp}
			h.SetAWSServiceInvoker(inv)

			rec := proxyReq(t, h, e, apiID, "/items", `{"a":1}`)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCalled {
				require.Equal(t, tt.wantSvc, inv.got.Service)
				assert.Equal(t, tt.wantSpec, inv.got.Spec)
				assert.JSONEq(t, `{"a":1}`, string(inv.got.Body))
			}
		})
	}
}
