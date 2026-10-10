package mq_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBroker_InputRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(m map[string]any)
		name     string
		wantCode int
	}{
		{name: "bad_instance_type", mutate: func(m map[string]any) { m["hostInstanceType"] = "foo" }, wantCode: 400},
		{name: "unsupported_version", mutate: func(m map[string]any) { m["engineVersion"] = "9.9.9" }, wantCode: 400},
		{
			name: "short_password",
			mutate: func(m map[string]any) {
				m["users"] = []map[string]any{{"username": "admin", "password": "short"}}
			},
			wantCode: 400,
		},
		{name: "valid", mutate: func(map[string]any) {}, wantCode: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"brokerName": "realism", "engineType": "ACTIVEMQ", "engineVersion": "5.17.6",
				"hostInstanceType": "mq.t3.micro", "deploymentMode": "SINGLE_INSTANCE",
				"users": []map[string]any{{"username": "admin", "password": "password123456"}},
			}
			tt.mutate(body)

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				assert.NotContains(t, rec.Body.String(), "BadRequestException:")
			}
		})
	}
}

func TestListBrokers_RejectsBadToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/brokers?nextToken=garbage", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
