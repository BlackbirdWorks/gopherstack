package iot_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func iotErr(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &out))

	return out.Type, out.Message
}

func TestEntityNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{name: "thing ok", path: "/things/my-thing_1:a", wantStatus: http.StatusOK},
		{name: "thing space", path: "/things/bad%20name", wantStatus: http.StatusBadRequest},
		{name: "thing bang", path: "/things/bad!name", wantStatus: http.StatusBadRequest},
		{name: "thing too long", path: "/things/" + strings.Repeat("a", 129), wantStatus: http.StatusBadRequest},
		{name: "thing type bad", path: "/thing-types/bad%20type", wantStatus: http.StatusBadRequest},
		{name: "thing group bad", path: "/thing-groups/bad%20group", wantStatus: http.StatusBadRequest},
		{name: "thing group ok", path: "/thing-groups/group-1", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := iotRequest(t, newIoTHandlerBatch1(t), http.MethodPost, tt.path, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				code, _ := iotErr(t, rec.Body.Bytes())
				assert.Equal(t, "InvalidRequestException", code)
			}
		})
	}
}

func TestCreateThingUnknownType(t *testing.T) {
	t.Parallel()

	h := newIoTHandlerBatch1(t)
	rec := iotRequest(t, h, http.MethodPost, "/things/t1", map[string]any{"thingTypeName": "ghost"})
	require.Equal(t, http.StatusNotFound, rec.Code)

	code, _ := iotErr(t, rec.Body.Bytes())
	assert.Equal(t, "ResourceNotFoundException", code)

	iotOK(t, h, http.MethodPost, "/thing-types/real", map[string]any{})
	iotOK(t, h, http.MethodPost, "/things/t2", map[string]any{"thingTypeName": "real"})
}

func TestPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		doc      string
		wantType string
	}{
		{name: "ok", path: "/policies/p1", doc: `{"Version":"2012-10-17","Statement":[]}`},
		{name: "not json", path: "/policies/p2", doc: "notjson", wantType: "MalformedPolicyException"},
		{name: "bad name", path: "/policies/bad%20name", doc: `{}`, wantType: "InvalidRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := iotRequest(t, newIoTHandlerBatch1(t), http.MethodPost, tt.path,
				map[string]any{"policyDocument": tt.doc})
			if tt.wantType == "" {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, msg := iotErr(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, code)
			assert.NotContains(t, msg, tt.wantType)
		})
	}
}

func TestPaginationQueryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{name: "garbage token", query: "?nextToken=garbage", wantStatus: http.StatusBadRequest},
		{name: "negative token", query: "?nextToken=-5", wantStatus: http.StatusBadRequest},
		{name: "zero max", query: "?maxResults=0", wantStatus: http.StatusBadRequest},
		{name: "non numeric max", query: "?maxResults=abc", wantStatus: http.StatusBadRequest},
		{name: "valid", query: "?maxResults=10&nextToken=0", wantStatus: http.StatusOK},
		{name: "absent", query: "", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := iotRequest(t, newIoTHandlerBatch1(t), http.MethodGet, "/things"+tt.query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestErrorMessagesOmitSentinelText(t *testing.T) {
	t.Parallel()

	h := newIoTHandlerBatch1(t)
	iotOK(t, h, http.MethodPost, "/things/t1", map[string]any{})

	tests := []struct {
		name     string
		method   string
		path     string
		wantType string
		banned   string
	}{
		{
			name: "duplicate", method: http.MethodPost, path: "/things/t1",
			wantType: "ResourceAlreadyExistsException", banned: "resource already exists",
		},
		{
			name: "job missing", method: http.MethodGet, path: "/jobs/ghost",
			wantType: "ResourceNotFoundException", banned: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := iotRequest(t, h, tt.method, tt.path, map[string]any{})
			code, msg := iotErr(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, code)
			assert.NotContains(t, msg, tt.banned)
		})
	}
}
