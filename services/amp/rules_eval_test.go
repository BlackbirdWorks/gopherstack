package amp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

type recordedSNS struct {
	msgs []amp.SNSMessage
}

func (r *recordedSNS) Publish(_ context.Context, m amp.SNSMessage) error {
	r.msgs = append(r.msgs, m)

	return nil
}

func (r *recordedSNS) take() []amp.SNSMessage {
	out := r.msgs
	r.msgs = nil

	return out
}

func (f *fixture) putRules(t *testing.T, ws, data string) {
	t.Helper()

	_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
		WorkspaceId: aws.String(ws), Name: aws.String("ns"), Data: b64(data),
	})
	require.NoError(t, err)
	f.settle()
}

func (f *fixture) putAlertManager(t *testing.T, ws, cfg string) {
	t.Helper()

	def := "alertmanager_config: |\n" + indent(cfg)
	_, err := f.client.CreateAlertManagerDefinition(t.Context(), &ampsdk.CreateAlertManagerDefinitionInput{
		WorkspaceId: aws.String(ws), Data: b64(def),
	})
	require.NoError(t, err)
	f.settle()
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "  " + l
	}

	return strings.Join(lines, "\n") + "\n"
}

func (f *fixture) writeNow(t *testing.T, ws string, labels map[string]string, v float64) {
	t.Helper()

	r := f.write(t, ws, amp.NewSeries(labels, promql.Sample{T: f.clock.now().UnixMilli(), V: v}))
	require.Equal(t, http.StatusOK, r.status, string(r.body))
}

type vecSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

func (f *fixture) vector(t *testing.T, ws, q string) []vecSample {
	t.Helper()

	at := strconv.FormatInt(f.clock.now().Unix(), 10)
	r, pr := f.query(t, ws, "query", url.Values{"query": {q}, "time": {at}})
	require.Equal(t, http.StatusOK, r.status, string(r.body))

	var out []vecSample
	require.NoError(t, json.Unmarshal(pr.Data.Result, &out))

	return out
}

func (f *fixture) apiData(t *testing.T, ws, path string, into any) {
	t.Helper()

	r := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/"+path, nil, nil)
	require.Equal(t, http.StatusOK, r.status, string(r.body))

	var env struct {
		Data json.RawMessage `json:"data"`
	}

	require.NoError(t, json.Unmarshal(r.body, &env))
	require.NoError(t, json.Unmarshal(env.Data, into))
}

func TestRecordingRule(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	f.putRules(t, ws, `groups:
  - name: g
    interval: 30s
    rules:
      - record: job:up:sum
        expr: sum by (job) (up)
        labels:
          team: infra
`)

	steps := []struct {
		want    map[string]string
		name    string
		advance time.Duration
		a       float64
		b       float64
	}{
		{name: "first eval", a: 1, b: 1, want: map[string]string{"api": "2", "web": "1"}},
		{name: "not due yet", advance: 10 * time.Second, a: 5, b: 5, want: map[string]string{"api": "2", "web": "1"}},
		{name: "second eval", advance: 20 * time.Second, a: 1, b: 0, want: map[string]string{"api": "1", "web": "0"}},
	}

	for _, st := range steps {
		f.clock.advance(st.advance)
		f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api", "i": "a"}, st.a)
		f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api", "i": "b"}, st.b)
		f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "web", "i": "a"}, st.b)
		f.backend.Tick(t.Context())

		got := map[string]string{}
		for _, s := range f.vector(t, ws, `job:up:sum`) {
			assert.Equal(t, "infra", s.Metric["team"], st.name)
			got[s.Metric["job"]], _ = s.Value[1].(string)
		}

		assert.Equal(t, st.want, got, st.name)
	}
}

func TestAlertLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	f.putRules(t, ws, `groups:
  - name: g
    interval: 1m
    rules:
      - alert: Down
        expr: up == 0
        for: 3m
        labels:
          severity: page
        annotations:
          summary: '{{ $labels.job }} is down ({{ $value }})'
`)

	steps := []struct {
		name         string
		state        string
		up           float64
		pending      int
		firing       int
		advanceAfter time.Duration
	}{
		{name: "pending", up: 0, state: "pending", pending: 1, advanceAfter: time.Minute},
		{name: "still pending", up: 0, state: "pending", pending: 1, advanceAfter: time.Minute},
		{name: "still pending 2", up: 0, state: "pending", pending: 1, advanceAfter: time.Minute},
		{name: "firing", up: 0, state: "firing", firing: 1, advanceAfter: time.Minute},
		{name: "recovered", up: 1, state: "inactive", advanceAfter: time.Minute},
	}

	for _, st := range steps {
		f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api"}, st.up)
		f.backend.Tick(t.Context())

		var alerts struct {
			Alerts []struct {
				Annotations map[string]string `json:"annotations"`
				Labels      map[string]string `json:"labels"`
				State       string            `json:"state"`
			} `json:"alerts"`
		}

		f.apiData(t, ws, "alerts", &alerts)
		assert.Len(t, alerts.Alerts, st.pending+st.firing, st.name)

		if len(alerts.Alerts) == 1 {
			assert.Equal(t, st.state, alerts.Alerts[0].State, st.name)
			assert.Equal(t, "api is down (0)", alerts.Alerts[0].Annotations["summary"], st.name)
			assert.Equal(t, "page", alerts.Alerts[0].Labels["severity"], st.name)
		}

		assert.Len(t, f.vector(t, ws, `ALERTS{alertstate="pending"}`), st.pending, st.name)
		assert.Len(t, f.vector(t, ws, `ALERTS{alertstate="firing"}`), st.firing, st.name)

		var rules struct {
			Groups []struct {
				Rules []struct {
					State  string `json:"state"`
					Health string `json:"health"`
				} `json:"rules"`
			} `json:"groups"`
		}

		f.apiData(t, ws, "rules", &rules)
		require.Len(t, rules.Groups, 1, st.name)
		assert.Equal(t, st.state, rules.Groups[0].Rules[0].State, st.name)
		assert.Equal(t, "ok", rules.Groups[0].Rules[0].Health, st.name)

		f.clock.advance(st.advanceAfter)
	}
}

func TestRuleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rules string
		want  string
	}{
		{
			name:  "duplicate labelset",
			rules: "groups:\n  - name: g\n    rules:\n      - record: r\n        expr: up\n        labels: {i: x, job: x}\n",
			want:  "same labelset",
		},
		{
			name:  "limit",
			rules: "groups:\n  - name: g\n    limit: 1\n    rules:\n      - record: r\n        expr: up\n",
			want:  "limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			f.putRules(t, ws, tt.rules)
			f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "a", "i": "1"}, 1)
			f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "b", "i": "2"}, 1)
			f.backend.Tick(t.Context())

			var rules struct {
				Groups []struct {
					Rules []struct {
						Health    string `json:"health"`
						LastError string `json:"lastError"`
					} `json:"rules"`
				} `json:"groups"`
			}

			f.apiData(t, ws, "rules", &rules)
			require.Len(t, rules.Groups, 1)
			assert.Equal(t, "err", rules.Groups[0].Rules[0].Health)
			assert.Contains(t, rules.Groups[0].Rules[0].LastError, tt.want)
		})
	}
}

func TestInvalidRuleExprRejected(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)

	_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
		WorkspaceId: aws.String(ws), Name: aws.String("ns"),
		Data: b64("groups:\n  - name: g\n    rules:\n      - record: r\n        expr: nosuchfn(up)\n"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestQueryTimeoutAndStatusEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		want   string
		status int
	}{
		{name: "timeout zero", path: "query?query=up&timeout=0", status: 503, want: `"errorType":"timeout"`},
		{name: "timeout bad", path: "query?query=up&timeout=abc", status: 400, want: `"errorType":"bad_data"`},
		{name: "timeout ok", path: "query?query=1&timeout=5s", status: 200, want: `"resultType":"scalar"`},
		{
			name:   "range timeout",
			path:   "query_range?query=up&start=0&end=60&step=15&timeout=0",
			status: 503,
			want:   "timeout",
		},
		{name: "buildinfo", path: "status/buildinfo", status: 200, want: `"version"`},
		{name: "exemplars", path: "query_exemplars?query=up&start=0&end=60", status: 200, want: `"data":[]`},
		{name: "rules empty", path: "rules", status: 200, want: `"groups":[]`},
		{name: "rules bad type", path: "rules?type=nope", status: 400, want: "bad_data"},
		{name: "alerts empty", path: "alerts", status: 200, want: `"alerts":[]`},
		{name: "metadata empty", path: "metadata", status: 200, want: `"data":{}`},
		{name: "metadata bad limit", path: "metadata?limit=x", status: 400, want: "bad_data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "a"}, 1)

			r := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/"+tt.path, nil, nil)
			assert.Equal(t, tt.status, r.status, string(r.body))
			assert.Contains(t, string(r.body), tt.want)
		})
	}
}

func TestRulesToSNSPipeline(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	pub := &recordedSNS{}
	f.backend.SetSNSPublisher(pub)

	ws := f.workspace(t)
	f.putRules(t, ws, `groups:
  - name: g
    interval: 15s
    rules:
      - alert: HighErrors
        expr: errors > 5
        labels:
          severity: page
        annotations:
          summary: 'errors at {{ $value | humanize }}'
`)
	f.putAlertManager(t, ws, `route:
  receiver: team
  group_by: [alertname]
  group_wait: 10s
  group_interval: 1m
receivers:
  - name: team
    sns_configs:
      - topic_arn: arn:aws:sns:us-east-1:123456789012:alerts
        subject: '{{ .Status | toUpper }}: {{ .CommonLabels.alertname }}'
        message: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
        attributes:
          env: test
`)

	f.writeNow(t, ws, map[string]string{"__name__": "errors", "job": "api"}, 1500)
	f.backend.Tick(t.Context())
	assert.Empty(t, pub.take(), "group_wait has not elapsed")

	f.clock.advance(15 * time.Second)
	f.writeNow(t, ws, map[string]string{"__name__": "errors", "job": "api"}, 1500)
	f.backend.Tick(t.Context())

	msgs := pub.take()
	require.Len(t, msgs, 1)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:alerts", msgs[0].TopicARN)
	assert.Equal(t, "FIRING: HighErrors", msgs[0].Subject)
	assert.Equal(t, "errors at 1.5k", msgs[0].Message)
	assert.Equal(t, map[string]string{"env": "test"}, msgs[0].Attributes)

	f.clock.advance(15 * time.Second)
	f.writeNow(t, ws, map[string]string{"__name__": "errors", "job": "api"}, 0)
	f.backend.Tick(t.Context())
	assert.Empty(t, pub.take(), "group_interval has not elapsed")

	f.clock.advance(time.Minute)
	f.writeNow(t, ws, map[string]string{"__name__": "errors", "job": "api"}, 0)
	f.backend.Tick(t.Context())

	msgs = pub.take()
	require.Len(t, msgs, 1)
	assert.Equal(t, "RESOLVED: HighErrors", msgs[0].Subject)
}

const groupingConfig = `route:
  receiver: default
  group_by: [alertname]
  group_wait: 10s
  group_interval: 1m
  repeat_interval: 10m
  routes:
    - matchers: ['severity="page"']
      receiver: pager
receivers:
  - name: default
    sns_configs:
      - topic_arn: arn:aws:sns:us-east-1:123456789012:default
        send_resolved: false
  - name: pager
    sns_configs:
      - topic_arn: arn:aws:sns:us-east-1:123456789012:pager
inhibit_rules:
  - source_matchers: ['severity="page"']
    target_matchers: ['severity="warn"']
    equal: [cluster]
`

func (f *fixture) postAlerts(t *testing.T, ws string, alerts []map[string]any) {
	t.Helper()

	body, err := json.Marshal(alerts)
	require.NoError(t, err)

	path := "/workspaces/" + ws + "/alertmanager/api/v2/alerts"
	r := f.do(t, http.MethodPost, path, strings.NewReader(string(body)), nil)
	require.Equal(t, http.StatusOK, r.status, string(r.body))
}

func alertBody(labels map[string]string, endsAt time.Time) map[string]any {
	return map[string]any{
		"labels":      labels,
		"endsAt":      endsAt.UTC().Format(time.RFC3339),
		"annotations": map[string]string{},
	}
}

func TestAlertManagerGroupingAndRepeat(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	pub := &recordedSNS{}
	f.backend.SetSNSPublisher(pub)

	ws := f.workspace(t)
	f.putAlertManager(t, ws, groupingConfig)
	f.backend.Tick(t.Context())

	far := f.clock.now().Add(24 * time.Hour)
	f.postAlerts(t, ws, []map[string]any{
		alertBody(map[string]string{"alertname": "X", "severity": "page", "instance": "a", "cluster": "c1"}, far),
		alertBody(map[string]string{"alertname": "X", "severity": "page", "instance": "b", "cluster": "c1"}, far),
		alertBody(map[string]string{"alertname": "Y", "severity": "info", "cluster": "c1"}, far),
		alertBody(map[string]string{"alertname": "Z", "severity": "warn", "cluster": "c1"}, far),
	})

	steps := []struct {
		want    map[string]int
		name    string
		advance time.Duration
	}{
		{name: "within group_wait", advance: 5 * time.Second, want: map[string]int{}},
		{name: "after group_wait", advance: 5 * time.Second, want: map[string]int{"pager": 2, "default": 1}},
		{name: "group_interval no change", advance: time.Minute, want: map[string]int{}},
		{name: "before repeat", advance: 5 * time.Minute, want: map[string]int{}},
		{name: "repeat", advance: 5 * time.Minute, want: map[string]int{"pager": 2, "default": 1}},
	}

	for _, st := range steps {
		f.clock.advance(st.advance)
		f.backend.Tick(t.Context())

		got := map[string]int{}

		for _, m := range pub.take() {
			topic := m.TopicARN[strings.LastIndex(m.TopicARN, ":")+1:]
			got[topic] += strings.Count(m.Message, "alertname=") // one line per alert in the default message
		}

		assert.Equal(t, st.want, got, st.name)
	}
}

func TestAlertManagerInhibitionHidesTarget(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	pub := &recordedSNS{}
	f.backend.SetSNSPublisher(pub)

	ws := f.workspace(t)
	f.putAlertManager(t, ws, groupingConfig)
	f.backend.Tick(t.Context())

	far := f.clock.now().Add(24 * time.Hour)
	f.postAlerts(t, ws, []map[string]any{
		alertBody(map[string]string{"alertname": "X", "severity": "page", "cluster": "c1"}, far),
		alertBody(map[string]string{"alertname": "Z", "severity": "warn", "cluster": "c1"}, far),
		alertBody(map[string]string{"alertname": "W", "severity": "warn", "cluster": "c2"}, far),
	})

	f.clock.advance(10 * time.Second)
	f.backend.Tick(t.Context())

	topics := map[string]string{}
	for _, m := range pub.take() {
		topics[m.TopicARN[strings.LastIndex(m.TopicARN, ":")+1:]] += m.Message
	}

	assert.Contains(t, topics["pager"], `alertname="X"`)
	assert.NotContains(t, topics["default"], `alertname="Z"`, "Z is inhibited by X in the same cluster")
	assert.Contains(t, topics["default"], `alertname="W"`, "W is in another cluster")
}

func TestAlertManagerAPIErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{name: "bad json", body: `{`, status: 400},
		{name: "no labels", body: `[{"labels":{}}]`, status: 400},
		{name: "no alertmanager", body: `[{"labels":{"a":"b"}}]`, status: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)

			r := f.do(t, http.MethodPost, "/workspaces/"+ws+"/alertmanager/api/v2/alerts",
				strings.NewReader(tt.body), nil)
			assert.Equal(t, tt.status, r.status, string(r.body))
		})
	}
}

func TestRuleStateSurvivesRestoreViaForState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	rules := `groups:
  - name: g
    interval: 1m
    rules:
      - alert: Down
        expr: up == 0
        for: 2m
`
	f.putRules(t, ws, rules)

	for range 2 {
		f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api"}, 0)
		f.backend.Tick(t.Context())
		f.clock.advance(time.Minute)
	}

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"), &fakeClock{t: f.clock.now()})
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	dst.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api"}, 0)
	dst.backend.Tick(t.Context())

	assert.Len(t, dst.vector(t, ws, `ALERTS{alertstate="firing"}`), 1,
		"the for clock resumes from ALERTS_FOR_STATE instead of restarting")
}

func TestStaleMarkersPersist(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	f.putRules(t, ws, "groups:\n  - name: g\n    rules:\n      - record: r\n        expr: up > 0\n")

	f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api"}, 1)
	f.backend.Tick(t.Context())
	assert.Len(t, f.vector(t, ws, `r`), 1)

	f.clock.advance(time.Minute)
	f.writeNow(t, ws, map[string]string{"__name__": "up", "job": "api"}, 0)
	f.backend.Tick(t.Context())
	assert.Empty(t, f.vector(t, ws, `r`), "the series ends with a stale marker, not after the lookback")

	dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"), &fakeClock{t: f.clock.now()})
	require.NoError(t, dst.backend.Restore(t.Context(), f.backend.Snapshot(t.Context())))
	assert.Empty(t, dst.vector(t, ws, `r`))
}
