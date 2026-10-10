package servicediscovery_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_NameAndAddressValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{name: "http_ns_space", op: "CreateHttpNamespace", body: map[string]any{"Name": "bad name"}, wantStatus: 400},
		{name: "http_ns_arn_prefix", op: "CreateHttpNamespace", body: map[string]any{"Name": "arn:x"}, wantStatus: 400},
		{name: "http_ns_ok", op: "CreateHttpNamespace", body: map[string]any{"Name": "ok-ns"}, wantStatus: 200},
		{
			name:       "public_ns_underscore",
			op:         "CreatePublicDnsNamespace",
			body:       map[string]any{"Name": "a_b.example.com"},
			wantStatus: 400,
		},
		{
			name: "private_ns_too_long", op: "CreatePrivateDnsNamespace",
			body: map[string]any{"Name": strings.Repeat("a", 254), "Vpc": "vpc-1"}, wantStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSDRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ServiceAndInstanceValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "val-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSDRequest(t, h, "ListNamespaces", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	nsID := extractFirstID(t, rec.Body.String())

	rec = doSDRequest(t, h, "CreateService", map[string]any{"Name": "bad name!", "NamespaceId": nsID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc", "NamespaceId": nsID})
	require.Equal(t, http.StatusOK, svcRec.Code)

	svcID := extractServiceID(t, svcRec.Body.String())

	tests := []struct {
		attrs      map[string]string
		name       string
		instanceID string
		wantStatus int
	}{
		{
			name:       "bad_ipv4",
			instanceID: "i-1",
			attrs:      map[string]string{"AWS_INSTANCE_IPV4": "999.1.1.1"},
			wantStatus: 400,
		},
		{
			name:       "bad_ipv6",
			instanceID: "i-2",
			attrs:      map[string]string{"AWS_INSTANCE_IPV6": "1.2.3.4"},
			wantStatus: 400,
		},
		{name: "bad_id", instanceID: "i 3", attrs: map[string]string{"A": "b"}, wantStatus: 400},
		{name: "ok", instanceID: "i-4", attrs: map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"}, wantStatus: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
				"ServiceId": svcID, "InstanceId": tt.instanceID, "Attributes": tt.attrs,
			})
			assert.Equal(t, tt.wantStatus, regRec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				assert.NotContains(t, regRec.Body.String(), "InvalidInput:")
			}
		})
	}
}

func extractFirstID(t *testing.T, body string) string {
	t.Helper()

	var out struct {
		Namespaces []struct {
			ID string `json:"Id"`
		} `json:"Namespaces"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	require.NotEmpty(t, out.Namespaces)

	return out.Namespaces[0].ID
}

func extractServiceID(t *testing.T, body string) string {
	t.Helper()

	var out struct {
		Service struct {
			ID string `json:"Id"`
		} `json:"Service"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))

	return out.Service.ID
}
