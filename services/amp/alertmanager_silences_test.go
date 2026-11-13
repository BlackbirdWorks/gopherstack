package amp_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/amp"
)

const silenceConfig = `route:
  receiver: team
  group_by: [alertname]
  group_wait: 10s
  group_interval: 1m
receivers:
  - name: team
    sns_configs:
      - topic_arn: arn:aws:sns:us-east-1:123456789012:alerts
        send_resolved: false
`

func amPath(ws, rest string) string { return "/workspaces/" + ws + "/alertmanager/api/v2/" + rest }

func (f *fixture) putSilence(t *testing.T, ws string, body map[string]any) string {
	t.Helper()

	raw, err := json.Marshal(body, jsonx.V1())
	require.NoError(t, err)

	r := f.do(t, http.MethodPost, amPath(ws, "silences"), strings.NewReader(string(raw)), nil)
	require.Equal(t, http.StatusOK, r.status, string(r.body))

	var out struct {
		SilenceID string `json:"silenceID"`
	}

	require.NoError(t, json.Unmarshal(r.body, &out, jsonx.V1()))
	require.NotEmpty(t, out.SilenceID)

	return out.SilenceID
}

func (f *fixture) silenceBody(name, value string) map[string]any {
	now := f.clock.now()

	return map[string]any{
		"matchers":  []map[string]any{{"name": name, "value": value, "isRegex": false, "isEqual": true}},
		"startsAt":  now.Format(time.RFC3339),
		"endsAt":    now.Add(time.Hour).Format(time.RFC3339),
		"createdBy": "tester",
		"comment":   "maintenance",
	}
}

func TestSilenceMutesNotifications(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	pub := &recordedSNS{}
	f.backend.SetSNSPublisher(pub)

	ws := f.workspace(t)
	f.putAlertManager(t, ws, silenceConfig)
	f.backend.Tick(t.Context())

	id := f.putSilence(t, ws, f.silenceBody("service", "db"))
	far := f.clock.now().Add(24 * time.Hour)
	f.postAlerts(t, ws, []map[string]any{
		alertBody(map[string]string{"alertname": "Down", "service": "db"}, far),
		alertBody(map[string]string{"alertname": "Down", "service": "web"}, far),
	})

	f.clock.advance(15 * time.Second)
	f.backend.Tick(t.Context())

	msgs := pub.take()
	require.Len(t, msgs, 1)
	assert.Contains(t, msgs[0].Subject, "FIRING:1")

	r := f.do(t, http.MethodGet, amPath(ws, "alerts?active=false"), nil, nil)
	require.Equal(t, http.StatusOK, r.status)

	var alerts []struct {
		Status struct {
			State      string   `json:"state"`
			SilencedBy []string `json:"silencedBy"`
		} `json:"status"`
	}

	require.NoError(t, json.Unmarshal(r.body, &alerts, jsonx.V1()))
	require.Len(t, alerts, 1)
	assert.Equal(t, "suppressed", alerts[0].Status.State)
	assert.Equal(t, []string{id}, alerts[0].Status.SilencedBy)

	del := f.do(t, http.MethodDelete, amPath(ws, "silence/"+id), nil, nil)
	require.Equal(t, http.StatusOK, del.status, string(del.body))

	f.clock.advance(time.Minute)
	f.backend.Tick(t.Context())
	assert.Len(t, pub.take(), 1, "the unsilenced alert notifies")
}

func TestSilenceLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	f.putAlertManager(t, ws, silenceConfig)
	f.backend.Tick(t.Context())

	id := f.putSilence(t, ws, f.silenceBody("service", "db"))

	get := f.do(t, http.MethodGet, amPath(ws, "silence/"+id), nil, nil)
	require.Equal(t, http.StatusOK, get.status, string(get.body))
	assert.Contains(t, string(get.body), `"state":"active"`)

	upd := f.silenceBody("service", "cache")
	upd["id"] = id
	assert.Equal(t, id, f.putSilence(t, ws, upd))

	list := f.do(t, http.MethodGet, amPath(ws, "silences?filter=service%3D%22cache%22"), nil, nil)
	require.Equal(t, http.StatusOK, list.status)
	assert.Contains(t, string(list.body), id)

	none := f.do(t, http.MethodGet, amPath(ws, "silences?filter=service%3D%22db%22"), nil, nil)
	assert.JSONEq(t, `[]`, string(none.body))

	require.Equal(t, http.StatusOK, f.do(t, http.MethodDelete, amPath(ws, "silence/"+id), nil, nil).status)

	get = f.do(t, http.MethodGet, amPath(ws, "silence/"+id), nil, nil)
	assert.Contains(t, string(get.body), `"state":"expired"`)

	f.clock.advance(121 * time.Hour)
	f.settle()
	f.backend.Tick(t.Context())
	assert.Equal(t, http.StatusNotFound, f.do(t, http.MethodGet, amPath(ws, "silence/"+id), nil, nil).status)
}

func TestSilenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(f *fixture, b map[string]any)
		name   string
		status int
	}{
		{name: "nomatchers", status: 400, mutate: func(_ *fixture, b map[string]any) { b["matchers"] = []any{} }},
		{name: "nocomment", status: 400, mutate: func(_ *fixture, b map[string]any) { delete(b, "comment") }},
		{name: "badregex", status: 400, mutate: func(_ *fixture, b map[string]any) {
			b["matchers"] = []map[string]any{{"name": "a", "value": "(", "isRegex": true}}
		}},
		{name: "past", status: 400, mutate: func(f *fixture, b map[string]any) {
			b["startsAt"] = f.clock.now().Add(-2 * time.Hour).Format(time.RFC3339)
			b["endsAt"] = f.clock.now().Add(-time.Hour).Format(time.RFC3339)
		}},
		{name: "unknownid", status: 404, mutate: func(_ *fixture, b map[string]any) { b["id"] = "missing" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			f.putAlertManager(t, ws, silenceConfig)
			f.backend.Tick(t.Context())

			body := f.silenceBody("a", "b")
			tt.mutate(f, body)

			raw, err := json.Marshal(body, jsonx.V1())
			require.NoError(t, err)

			r := f.do(t, http.MethodPost, amPath(ws, "silences"), strings.NewReader(string(raw)), nil)
			assert.Equal(t, tt.status, r.status, string(r.body))
		})
	}
}

func TestAlertManagerStatusReceiversGroups(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	f.putAlertManager(t, ws, silenceConfig)
	f.backend.Tick(t.Context())

	st := f.do(t, http.MethodGet, amPath(ws, "status"), nil, nil)
	require.Equal(t, http.StatusOK, st.status)
	assert.Contains(t, string(st.body), `"cluster":null`)
	assert.Contains(t, string(st.body), "receivers:")

	rc := f.do(t, http.MethodGet, amPath(ws, "receivers"), nil, nil)
	assert.JSONEq(t, `[{"name":"team"}]`, string(rc.body))

	f.postAlerts(t, ws, []map[string]any{
		alertBody(map[string]string{"alertname": "Down", "service": "db"}, f.clock.now().Add(time.Hour)),
	})

	gr := f.do(t, http.MethodGet, amPath(ws, "alerts/groups"), nil, nil)
	require.Equal(t, http.StatusOK, gr.status)

	var groups []struct {
		Labels   map[string]string `json:"labels"`
		Receiver struct {
			Name string `json:"name"`
		} `json:"receiver"`
		Alerts []jsontext.Value `json:"alerts"`
	}

	require.NoError(t, json.Unmarshal(gr.body, &groups, jsonx.V1()))
	require.Len(t, groups, 1)
	assert.Equal(t, "team", groups[0].Receiver.Name)
	assert.Equal(t, map[string]string{"alertname": "Down"}, groups[0].Labels)
	assert.Len(t, groups[0].Alerts, 1)
}

func TestSilencePersists(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	ws := src.workspace(t)
	src.putAlertManager(t, ws, silenceConfig)
	src.backend.Tick(t.Context())

	id := src.putSilence(t, ws, src.silenceBody("a", "b"))
	snap := src.backend.Snapshot(t.Context())

	dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"), &fakeClock{t: src.clock.now()})
	require.NoError(t, dst.backend.Restore(t.Context(), snap))
	dst.backend.Tick(t.Context())

	r := dst.do(t, http.MethodGet, amPath(ws, "silence/"+id), nil, nil)
	assert.Equal(t, http.StatusOK, r.status, string(r.body))
}

func TestAlertManagerTemplateFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		subject string
		message string
	}{
		{
			name: "defaulttemplates",
			def: `alertmanager_config: |
  route:
    receiver: team
    group_by: [alertname]
    group_wait: 10s
  receivers:
    - name: team
      sns_configs:
        - topic_arn: arn:aws:sns:us-east-1:123456789012:alerts
          subject: '{{ template "sns.default.subject" . }}'
          message: '{{ template "sns.default.message" . }}'
`,
			subject: "[FIRING:1] Down (db)",
			message: "disk full",
		},
		{
			name: "customfile",
			def: `alertmanager_config: |
  route:
    receiver: team
    group_by: [alertname]
    group_wait: 10s
  receivers:
    - name: team
      sns_configs:
        - topic_arn: arn:aws:sns:us-east-1:123456789012:alerts
          subject: '{{ template "my.subject" . }}'
          message: 'x'
  templates:
    - custom.tmpl
template_files:
  custom.tmpl: |
    {{ define "my.subject" }}custom {{ .CommonLabels.service }}{{ end }}
`,
			subject: "custom db",
			message: "x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			pub := &recordedSNS{}
			f.backend.SetSNSPublisher(pub)

			ws := f.workspace(t)
			_, err := f.client.CreateAlertManagerDefinition(t.Context(), &ampsdk.CreateAlertManagerDefinitionInput{
				WorkspaceId: aws.String(ws), Data: b64(tt.def),
			})
			require.NoError(t, err)
			f.settle()
			f.backend.Tick(t.Context())

			a := alertBody(map[string]string{"alertname": "Down", "service": "db"}, f.clock.now().Add(time.Hour))
			a["annotations"] = map[string]string{"summary": "disk full"}
			f.postAlerts(t, ws, []map[string]any{a})
			f.clock.advance(15 * time.Second)
			f.backend.Tick(t.Context())

			msgs := pub.take()
			require.Len(t, msgs, 1)
			assert.Equal(t, tt.subject, msgs[0].Subject)
			assert.Equal(t, tt.message, msgs[0].Message)
		})
	}
}

func TestAlertManagerBadTemplateFileRejected(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)

	def := `alertmanager_config: |
  route:
    receiver: team
  receivers:
    - name: team
template_files:
  bad.tmpl: '{{ define "x" }}'
`
	_, err := f.client.CreateAlertManagerDefinition(t.Context(), &ampsdk.CreateAlertManagerDefinitionInput{
		WorkspaceId: aws.String(ws), Data: b64(def),
	})
	require.Error(t, err)
}
