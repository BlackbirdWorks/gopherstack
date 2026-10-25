package sagemakerruntime_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_RequestHeaderValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers  map[string]string
		name     string
		path     string
		wantCode int
	}{
		{
			name: "custom attributes too long", path: "/endpoints/e/invocations",
			headers:  map[string]string{"X-Amzn-Sagemaker-Custom-Attributes": strings.Repeat("x", 1025)},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "inference id too long", path: "/endpoints/e/async-invocations",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inference-Id":  strings.Repeat("x", 65),
				"X-Amzn-Sagemaker-Inputlocation": "s3://b/k",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "input location not s3", path: "/endpoints/e/async-invocations",
			headers:  map[string]string{"X-Amzn-Sagemaker-Inputlocation": "notas3"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "ttl below minimum", path: "/endpoints/e/async-invocations",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation":     "s3://b/k",
				"X-Amzn-Sagemaker-Requestttlseconds": "10",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "timeout above maximum", path: "/endpoints/e/async-invocations",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation":            "s3://b/k",
				"X-Amzn-Sagemaker-Invocationtimeoutseconds": "3601",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "timeout overflowing int32", path: "/endpoints/e/async-invocations",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation":            "s3://b/k",
				"X-Amzn-Sagemaker-Invocationtimeoutseconds": "4294967297",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bad session id", path: "/endpoints/e/invocations",
			headers:  map[string]string{"X-Amzn-Sagemaker-Session-Id": "bad id!"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid async", path: "/endpoints/e/async-invocations",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation":     "s3://b/k",
				"X-Amzn-Sagemaker-Requestttlseconds": "60",
			},
			wantCode: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(t, h, http.MethodPost, tt.path, nil, tt.headers)

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}
