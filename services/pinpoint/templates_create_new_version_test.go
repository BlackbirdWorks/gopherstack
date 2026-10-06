package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateTemplate_CreateNewVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		templateType string
		query        string
		wantVersion  string
		wantStatus   int
		wantVersions int
	}{
		{
			name: "default_overwrites_latest", templateType: "email", body: map[string]any{"Subject": "s2"},
			wantStatus: http.StatusAccepted, wantVersion: "1", wantVersions: 1,
		},
		{
			name: "explicit_false_overwrites", templateType: "sms", query: "?create-new-version=false",
			body: map[string]any{"Body": "b2"}, wantStatus: http.StatusAccepted, wantVersion: "1", wantVersions: 1,
		},
		{
			name: "true_adds_version", templateType: "push", query: "?create-new-version=true",
			body:       map[string]any{"Default": map[string]any{"Body": "b2"}},
			wantStatus: http.StatusAccepted, wantVersion: "2", wantVersions: 2,
		},
		{
			name: "true_voice", templateType: "voice", query: "?create-new-version=true",
			body: map[string]any{"Body": "b2"}, wantStatus: http.StatusAccepted, wantVersion: "2", wantVersions: 2,
		},
		{
			name: "true_inapp", templateType: "inapp", query: "?create-new-version=true",
			body:       map[string]any{"Layout": "BOTTOM_BANNER"},
			wantStatus: http.StatusAccepted, wantVersion: "2", wantVersions: 2,
		},
		{
			name: "true_with_version_rejected", templateType: "email", query: "?create-new-version=true&version=1",
			body: map[string]any{"Subject": "s2"}, wantStatus: http.StatusBadRequest, wantVersion: "1", wantVersions: 1,
		},
		{
			name: "invalid_flag_rejected", templateType: "email", query: "?create-new-version=maybe",
			body: map[string]any{"Subject": "s2"}, wantStatus: http.StatusBadRequest, wantVersion: "1", wantVersions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			path := "/v1/templates/cnv/" + tt.templateType

			rec := doPinpointRequest(t, h, http.MethodPost, path, map[string]any{"Subject": "s1"})
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+tt.query, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.wantVersion, got["Version"])

			rec = doPinpointRequest(t, h, http.MethodGet, path+"/versions", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var vers map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vers))
			assert.Len(t, vers["Item"], tt.wantVersions)
		})
	}
}
