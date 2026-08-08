package iot_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// doIoTRequestRaw is like doIoTRequest but sends body verbatim, letting
// tests express JSON a Go map literal cannot (e.g. duplicate object keys).
func doIoTRequestRaw(t *testing.T, h *iot.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func decodeTagList(t *testing.T, body []byte) map[string]string {
	t.Helper()
	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	out := make(map[string]string, len(resp.Tags))
	for _, item := range resp.Tags {
		out[item.Key] = item.Value
	}

	return out
}

// envTag builds the {"Key":..,"Value":..} wire shape a real SDK client
// sends for a list of tags (types.Tag, aws-sdk-go-v2/service/iot@v1.77.4).
func envTag() map[string]string {
	return map[string]string{"Key": "env", "Value": "prod"}
}

// TestCreateOpsAcceptListShapedTags drives every services/iot Create* op
// (plus TagResource) that models inline tags with the raw JSON body a real
// SDK client sends: a "tags" array of {Key,Value} objects. Before the fix,
// each of these decoded straight into a map[string]string field and a real
// client's array failed with a JSON unmarshal type error.
func TestCreateOpsAcceptListShapedTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{
			name:   "security profile",
			method: http.MethodPost,
			path:   "/security-profiles/wire-sp",
			body:   map[string]any{"tags": []map[string]string{envTag()}},
		},
		{
			name:   "authorizer",
			method: http.MethodPost,
			path:   "/authorizer/wire-authz",
			body: map[string]any{
				"tags":                  []map[string]string{envTag()},
				"authorizerFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:wire-authz",
			},
		},
		{
			name:   "billing group",
			method: http.MethodPost,
			path:   "/billing-groups/wire-bg",
			body:   map[string]any{"tags": []map[string]string{envTag()}},
		},
		{
			name:   "command",
			method: http.MethodPut,
			path:   "/commands/wire-cmd",
			body:   map[string]any{"tags": []map[string]string{envTag()}, "namespace": "AWS-IoT"},
		},
		{
			name:   "scheduled audit",
			method: http.MethodPost,
			path:   "/audit/scheduledaudits/wire-audit",
			body:   map[string]any{"tags": []map[string]string{envTag()}, "frequency": "DAILY"},
		},
		{
			name:   "mitigation action",
			method: http.MethodPost,
			path:   "/mitigationactions/actions/wire-action",
			body: map[string]any{
				"tags":    []map[string]string{envTag()},
				"roleArn": "arn:aws:iam::000000000000:role/wire-action",
			},
		},
		{
			name:   "role alias",
			method: http.MethodPost,
			path:   "/role-aliases/wire-alias",
			body: map[string]any{
				"tags":    []map[string]string{envTag()},
				"roleArn": "arn:aws:iam::000000000000:role/wire-alias",
			},
		},
		{
			name:   "domain configuration",
			method: http.MethodPost,
			path:   "/domainConfigurations/wire-domain",
			body:   map[string]any{"tags": []map[string]string{envTag()}},
		},
		{
			name:   "provisioning template",
			method: http.MethodPost,
			path:   "/provisioning-templates",
			body: map[string]any{
				"tags":         []map[string]string{envTag()},
				"templateName": "wire-template",
				"templateBody": "{}",
			},
		},
		{
			name:   "stream",
			method: http.MethodPost,
			path:   "/streams/wire-stream",
			body:   map[string]any{"tags": []map[string]string{envTag()}, "files": []any{}},
		},
		{
			name:   "job",
			method: http.MethodPut,
			path:   "/jobs/wire-job",
			body: map[string]any{
				"tags":    []map[string]string{envTag()},
				"targets": []string{"arn:aws:iot:us-east-1:000000000000:thing/wire-thing"},
			},
		},
		{
			name:   "job template",
			method: http.MethodPut,
			path:   "/job-templates/wire-jobtemplate",
			body: map[string]any{
				"tags":     []map[string]string{envTag()},
				"document": "{}",
			},
		},
		{
			name:   "fleet metric",
			method: http.MethodPut,
			path:   "/fleet-metric/wire-fleetmetric",
			body: map[string]any{
				"tags":        []map[string]string{envTag()},
				"queryString": "status:ACTIVE",
			},
		},
		{
			name:   "custom metric",
			method: http.MethodPost,
			path:   "/custom-metric/wire-custommetric",
			body: map[string]any{
				"tags":       []map[string]string{envTag()},
				"metricType": "number",
			},
		},
		{
			name:   "dimension",
			method: http.MethodPost,
			path:   "/dimensions/wire-dimension",
			body: map[string]any{
				"tags":         []map[string]string{envTag()},
				"type":         "TOPIC_FILTER",
				"stringValues": []string{"foo/bar"},
			},
		},
		{
			name:   "tag resource",
			method: http.MethodPost,
			path:   "/tags",
			body: map[string]any{
				"resourceArn": "arn:aws:iot:us-east-1:000000000000:thing/wire-thing",
				"tags":        []map[string]string{envTag()},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := iot.NewHandler(iot.NewInMemoryBackend(), nil)

			rec := doIoTRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestCreateSecurityProfileTagsWireShape covers the exact op named in
// gopherstack-x2um: tags present, absent, explicitly empty, and duplicate
// keys. Verified against backend storage (SecurityProfile.Tags is
// json:"-", so it never appears in the HTTP response -- see
// SecurityProfile's doc comment).
func TestCreateSecurityProfileTagsWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body string
		want map[string]string
		name string
	}{
		{
			name: "tags present",
			body: `{"tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"iot"}]}`,
			want: map[string]string{"env": "prod", "team": "iot"},
		},
		{
			name: "tags absent",
			body: `{}`,
			want: map[string]string{},
		},
		{
			name: "tags explicitly empty",
			body: `{"tags":[]}`,
			want: map[string]string{},
		},
		{
			name: "duplicate keys last write wins",
			body: `{"tags":[{"Key":"env","Value":"first"},{"Key":"env","Value":"second"}]}`,
			want: map[string]string{"env": "second"},
		},
		{
			name: "empty string value preserved",
			body: `{"tags":[{"Key":"blank","Value":""}]}`,
			want: map[string]string{"blank": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iot.NewInMemoryBackend()
			h := iot.NewHandler(b, nil)

			profileName := "wire-sp-" + strings.ReplaceAll(tt.name, " ", "-")
			rec := doIoTRequestRaw(t, h, http.MethodPost, "/security-profiles/"+profileName, tt.body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			sp, err := b.DescribeSecurityProfile(profileName)
			require.NoError(t, err)
			assert.Equal(t, tt.want, sp.Tags)
		})
	}
}

// TestTagResourceListTagsRoundTrip exercises TagResource and
// ListTagsForResource together, driving both requests with the real wire
// shape and asserting the round trip through the shared resourceTags store.
func TestTagResourceListTagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]string
		reqBody string
		name    string
	}{
		{
			name:    "simple set",
			reqBody: `{"resourceArn":"arn:aws:iot:us-east-1:000000000000:thing/rt-thing","tags":[{"Key":"env","Value":"prod"}]}`,
			want:    map[string]string{"env": "prod"},
		},
		{
			name: "duplicate keys and empty value",
			reqBody: `{"resourceArn":"arn:aws:iot:us-east-1:000000000000:thing/rt-thing",` +
				`"tags":[{"Key":"env","Value":"first"},{"Key":"env","Value":"second"},{"Key":"blank","Value":""}]}`,
			want: map[string]string{"env": "second", "blank": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := iot.NewHandler(iot.NewInMemoryBackend(), nil)

			tagRec := doIoTRequestRaw(t, h, http.MethodPost, "/tags", tt.reqBody)
			require.Equal(t, http.StatusOK, tagRec.Code, "body: %s", tagRec.Body.String())

			resourceARN := "arn:aws:iot:us-east-1:000000000000:thing/rt-thing"
			listRec := doIoTRequest(t, h, http.MethodGet, "/tags?resourceArn="+url.QueryEscape(resourceARN), nil)
			require.Equal(t, http.StatusOK, listRec.Code, "body: %s", listRec.Body.String())

			got := decodeTagList(t, listRec.Body.Bytes())
			assert.Equal(t, tt.want, got)
		})
	}
}
