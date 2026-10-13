package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))

	return got
}

func TestTemplateVersionSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantFirst    any
		wantSecond   any
		first        map[string]any
		second       map[string]any
		probe        func(map[string]any) any
		name         string
		templateType string
	}{
		{
			name: "email", templateType: "email",
			first: map[string]any{"Subject": "v1"}, second: map[string]any{"Subject": "v2"},
			probe:     func(m map[string]any) any { return m["Subject"] },
			wantFirst: "v1", wantSecond: "v2",
		},
		{
			name: "sms", templateType: "sms",
			first: map[string]any{"Body": "v1"}, second: map[string]any{"Body": "v2"},
			probe:     func(m map[string]any) any { return m["Body"] },
			wantFirst: "v1", wantSecond: "v2",
		},
		{
			name: "voice", templateType: "voice",
			first: map[string]any{"Body": "v1"}, second: map[string]any{"Body": "v2"},
			probe:     func(m map[string]any) any { return m["Body"] },
			wantFirst: "v1", wantSecond: "v2",
		},
		{
			name: "push", templateType: "push",
			first:  map[string]any{"Default": map[string]any{"Body": "v1"}},
			second: map[string]any{"Default": map[string]any{"Body": "v2"}},
			probe: func(m map[string]any) any {
				d, _ := m["Default"].(map[string]any)

				return d["Body"]
			},
			wantFirst: "v1", wantSecond: "v2",
		},
		{
			name: "inapp", templateType: "inapp",
			first: map[string]any{"Layout": "BOTTOM_BANNER"}, second: map[string]any{"Layout": "TOP_BANNER"},
			probe:     func(m map[string]any) any { return m["Layout"] },
			wantFirst: "BOTTOM_BANNER", wantSecond: "TOP_BANNER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			path := "/v1/templates/ver/" + tt.templateType

			rec := doPinpointRequest(t, h, http.MethodPost, path, tt.first)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodPut, path+"?create-new-version=true", tt.second)
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodGet, path+"?version=1", nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			got := decodeBody(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantFirst, tt.probe(got))
			assert.Equal(t, "1", got["Version"])

			rec = doPinpointRequest(t, h, http.MethodGet, path, nil)
			got = decodeBody(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantFirst, tt.probe(got), "a new version does not displace the active one")
			assert.Equal(t, "1", got["Version"])

			rec = doPinpointRequest(t, h, http.MethodGet, path+"?version=2", nil)
			assert.Equal(t, tt.wantSecond, tt.probe(decodeBody(t, rec.Body.Bytes())))

			rec = doPinpointRequest(t, h, http.MethodGet, path+"?version=9", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+"/active-version", map[string]any{"Version": "1"})
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantFirst, tt.probe(decodeBody(t, rec.Body.Bytes())))

			rec = doPinpointRequest(t, h, http.MethodPut, path+"/active-version", map[string]any{"Version": "7"})
			assert.Equal(t, http.StatusNotFound, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+"/active-version", map[string]any{"Version": "latest"})
			require.Equal(t, http.StatusAccepted, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantSecond, tt.probe(decodeBody(t, rec.Body.Bytes())))
		})
	}
}

func TestTemplateVersionUpdateRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantBody   string
		wantStatus int
	}{
		{name: "latest_version_overwrites", query: "?version=2", wantStatus: http.StatusAccepted, wantBody: "edited"},
		{name: "stale_version_rejected", query: "?version=1", wantStatus: http.StatusBadRequest, wantBody: "v2"},
		{name: "unknown_version_missing", query: "?version=5", wantStatus: http.StatusNotFound, wantBody: "v2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			path := "/v1/templates/upd/sms"

			rec := doPinpointRequest(t, h, http.MethodPost, path, map[string]any{"Body": "v1"})
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+"?create-new-version=true", map[string]any{"Body": "v2"})
			require.Equal(t, http.StatusAccepted, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+tt.query, map[string]any{"Body": "edited"})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodGet, path+"?version=2", nil)
			assert.Equal(t, tt.wantBody, decodeBody(t, rec.Body.Bytes())["Body"])
		})
	}
}

func TestTemplateVersionDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deleteQuery  string
		wantBody     string
		wantVersions []string
		wantGone     bool
	}{
		{name: "older_version", deleteQuery: "?version=1", wantVersions: []string{"2"}, wantBody: "v2"},
		{name: "latest_version_rewinds", deleteQuery: "?version=2", wantVersions: []string{"1"}, wantBody: "v1"},
		{name: "whole_template", deleteQuery: "", wantGone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			path := "/v1/templates/del/sms"

			rec := doPinpointRequest(t, h, http.MethodPost, path, map[string]any{"Body": "v1"})
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodPut, path+"?create-new-version=true", map[string]any{"Body": "v2"})
			require.Equal(t, http.StatusAccepted, rec.Code)

			rec = doPinpointRequest(t, h, http.MethodDelete, path+tt.deleteQuery, nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			rec = doPinpointRequest(t, h, http.MethodGet, path, nil)
			if tt.wantGone {
				assert.Equal(t, http.StatusNotFound, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantBody, decodeBody(t, rec.Body.Bytes())["Body"])

			rec = doPinpointRequest(t, h, http.MethodGet, path+"/versions", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var versions []string

			items, _ := decodeBody(t, rec.Body.Bytes())["Item"].([]any)
			for _, it := range items {
				m, _ := it.(map[string]any)
				versions = append(versions, m["Version"].(string))
				assert.NotEmpty(t, m["CreationDate"])
				assert.NotEmpty(t, m["LastModifiedDate"])
			}

			assert.Equal(t, tt.wantVersions, versions)
		})
	}
}
