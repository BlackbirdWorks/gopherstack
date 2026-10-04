package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type iotHTTPHit struct {
	header http.Header
	query  url.Values
	path   string
	body   []byte
}

type iotHTTPEndpoint struct {
	srv       *httptest.Server
	status    func(n int) int
	onConfirm func(hit iotHTTPHit)
	hits      []iotHTTPHit
	confirms  []iotHTTPHit
	mu        sync.Mutex
}

func newIoTHTTPEndpoint(t *testing.T, status func(n int) int, onConfirm func(hit iotHTTPHit)) *iotHTTPEndpoint {
	t.Helper()

	ep := &iotHTTPEndpoint{status: status, onConfirm: onConfirm}

	ep.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hit := iotHTTPHit{header: r.Header.Clone(), query: r.URL.Query(), path: r.URL.Path, body: body}

		if r.Header.Get("X-Amz-Rules-Engine-Message-Type") == "DestinationConfirmation" {
			ep.mu.Lock()
			ep.confirms = append(ep.confirms, hit)
			ep.mu.Unlock()

			if ep.onConfirm != nil {
				ep.onConfirm(hit)
			}

			w.WriteHeader(http.StatusOK)

			return
		}

		ep.mu.Lock()
		ep.hits = append(ep.hits, hit)
		n := len(ep.hits)
		ep.mu.Unlock()

		code := http.StatusOK
		if ep.status != nil {
			code = ep.status(n)
		}

		w.WriteHeader(code)
	}))
	t.Cleanup(ep.srv.Close)

	return ep
}

func (e *iotHTTPEndpoint) deliveries() []iotHTTPHit {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]iotHTTPHit(nil), e.hits...)
}

func (e *iotHTTPEndpoint) confirmationRequests() []iotHTTPHit {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]iotHTTPHit(nil), e.confirms...)
}

func (e *iotHTTPEndpoint) waitDeliveries(t *testing.T, want int) []iotHTTPHit {
	t.Helper()

	require.Eventually(t, func() bool { return len(e.deliveries()) >= want }, authzDeadline, authzTick)

	return e.deliveries()
}

type confirmBody struct {
	ARN               string `json:"arn"`
	ConfirmationToken string `json:"confirmationToken"`
	EnableURL         string `json:"enableUrl"`
	MessageType       string `json:"messageType"`
}

func decodeConfirm(t *testing.T, hit iotHTTPHit) confirmBody {
	t.Helper()

	var b confirmBody
	require.NoError(t, json.Unmarshal(hit.body, &b))

	return b
}

func (f *iotRuleFixture) confirmViaSDK(t *testing.T) func(hit iotHTTPHit) {
	t.Helper()

	return func(hit iotHTTPHit) {
		var b confirmBody
		if json.Unmarshal(hit.body, &b) != nil {
			return
		}

		_, _ = iot.NewFromConfig(f.fx.cfg).ConfirmTopicRuleDestination(t.Context(),
			&iot.ConfirmTopicRuleDestinationInput{ConfirmationToken: aws.String(b.ConfirmationToken)})
	}
}

func (f *iotRuleFixture) createDestination(t *testing.T, confirmationURL string) string {
	t.Helper()

	out, err := iot.NewFromConfig(f.fx.cfg).
		CreateTopicRuleDestination(t.Context(), &iot.CreateTopicRuleDestinationInput{
			DestinationConfiguration: &iottypes.TopicRuleDestinationConfiguration{
				HttpUrlConfiguration: &iottypes.HttpUrlDestinationConfiguration{
					ConfirmationUrl: aws.String(confirmationURL),
				},
			},
		})
	require.NoError(t, err)

	return aws.ToString(out.TopicRuleDestination.Arn)
}

func (f *iotRuleFixture) destination(t *testing.T, arn string) *iottypes.TopicRuleDestination {
	t.Helper()

	out, err := iot.NewFromConfig(f.fx.cfg).GetTopicRuleDestination(t.Context(),
		&iot.GetTopicRuleDestinationInput{Arn: aws.String(arn)})
	require.NoError(t, err)

	return out.TopicRuleDestination
}

func (f *iotRuleFixture) awaitStatus(t *testing.T, arn string, want iottypes.TopicRuleDestinationStatus) {
	t.Helper()

	require.Eventually(t, func() bool { return f.destination(t, arn).Status == want }, authzDeadline, authzTick)
}

func TestIoTTopicRuleDestinationConfirmation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		confirm func(t *testing.T, f *iotRuleFixture) func(hit iotHTTPHit)
		status  func(int) int
		want    iottypes.TopicRuleDestinationStatus
	}{
		{
			name: "confirm_operation_enables",
			confirm: func(t *testing.T, f *iotRuleFixture) func(iotHTTPHit) {
				t.Helper()

				return f.confirmViaSDK(t)
			},
			want: iottypes.TopicRuleDestinationStatusEnabled,
		},
		{
			name: "enable_url_enables",
			confirm: func(t *testing.T, _ *iotRuleFixture) func(iotHTTPHit) {
				t.Helper()

				return func(hit iotHTTPHit) {
					var b confirmBody
					if json.Unmarshal(hit.body, &b) != nil {
						return
					}

					req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, b.EnableURL, nil)
					if err != nil {
						return
					}

					if resp, derr := http.DefaultClient.Do(req); derr == nil {
						resp.Body.Close()
					}
				}
			},
			want: iottypes.TopicRuleDestinationStatusEnabled,
		},
		{
			name: "unconfirmed_stays_in_progress",
			want: iottypes.TopicRuleDestinationStatusInProgress,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")

			var onConfirm func(iotHTTPHit)
			if tt.confirm != nil {
				onConfirm = tt.confirm(t, f)
			}

			ep := newIoTHTTPEndpoint(t, nil, onConfirm)
			arn := f.createDestination(t, ep.srv.URL+"/hook")

			require.Eventually(t, func() bool { return len(ep.confirmationRequests()) == 1 }, authzDeadline, authzTick)

			hit := ep.confirmationRequests()[0]
			body := decodeConfirm(t, hit)

			assert.Equal(t, "DestinationConfirmation", hit.header.Get("X-Amz-Rules-Engine-Message-Type"))
			assert.Equal(t, arn, hit.header.Get("X-Amz-Rules-Engine-Destination-Arn"))
			assert.Equal(t, "application/json", hit.header.Get("Content-Type"))
			assert.Equal(t, "/hook", hit.path)
			assert.Equal(t, arn, body.ARN)
			assert.Equal(t, "DestinationConfirmation", body.MessageType)
			assert.NotEmpty(t, body.ConfirmationToken)
			assert.Equal(t, body.ConfirmationToken, hit.query.Get("confirmationToken"))
			assert.True(t, strings.HasSuffix(body.EnableURL, "/confirmdestination/"+body.ConfirmationToken))

			if tt.want == iottypes.TopicRuleDestinationStatusInProgress {
				assert.Equal(t, tt.want, f.destination(t, arn).Status)

				_, err := iot.NewFromConfig(f.fx.cfg).ConfirmTopicRuleDestination(t.Context(),
					&iot.ConfirmTopicRuleDestinationInput{ConfirmationToken: aws.String(body.ConfirmationToken)})
				require.NoError(t, err)

				tt.want = iottypes.TopicRuleDestinationStatusEnabled
			}

			f.awaitStatus(t, arn, tt.want)

			list, err := iot.NewFromConfig(f.fx.cfg).
				ListTopicRuleDestinations(t.Context(), &iot.ListTopicRuleDestinationsInput{})
			require.NoError(t, err)
			require.Len(t, list.DestinationSummaries, 1)
			assert.Equal(
				t,
				ep.srv.URL+"/hook",
				aws.ToString(list.DestinationSummaries[0].HttpUrlSummary.ConfirmationUrl),
			)
		})
	}
}

func TestIoTTopicRuleDestinationErrorAndResend(t *testing.T) {
	t.Parallel()

	f := newIoTRuleFixture(t, false, "*")

	var failing sync.Mutex

	rejected := true

	ep := newIoTHTTPEndpoint(t, nil, nil)
	ep.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		failing.Lock()
		reject := rejected
		failing.Unlock()

		ep.mu.Lock()
		ep.confirms = append(ep.confirms, iotHTTPHit{header: r.Header.Clone(), query: r.URL.Query(), body: body})
		ep.mu.Unlock()

		if reject {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		f.confirmViaSDK(t)(iotHTTPHit{body: body})
		w.WriteHeader(http.StatusOK)
	})

	arn := f.createDestination(t, ep.srv.URL)

	f.awaitStatus(t, arn, iottypes.TopicRuleDestinationStatusError)
	assert.NotEmpty(t, aws.ToString(f.destination(t, arn).StatusReason))

	failing.Lock()
	rejected = false
	failing.Unlock()

	_, err := iot.NewFromConfig(f.fx.cfg).UpdateTopicRuleDestination(t.Context(), &iot.UpdateTopicRuleDestinationInput{
		Arn: aws.String(arn), Status: iottypes.TopicRuleDestinationStatusInProgress,
	})
	require.NoError(t, err)

	f.awaitStatus(t, arn, iottypes.TopicRuleDestinationStatusEnabled)
	assert.Empty(t, aws.ToString(f.destination(t, arn).StatusReason))
	require.Len(t, ep.confirmationRequests(), 2)

	first, second := decodeConfirm(t, iotHTTPHit{body: ep.confirmationRequests()[0].body}),
		decodeConfirm(t, iotHTTPHit{body: ep.confirmationRequests()[1].body})
	assert.NotEqual(t, first.ConfirmationToken, second.ConfirmationToken)

	_, err = iot.NewFromConfig(f.fx.cfg).DeleteTopicRuleDestination(t.Context(),
		&iot.DeleteTopicRuleDestinationInput{Arn: aws.String(arn)})
	require.NoError(t, err)
}

type iotHTTPCase struct {
	status     func(n int) int
	prepare    func(t *testing.T, f *iotRuleFixture, ep *iotHTTPEndpoint, arn string)
	name       string
	payload    string
	sql        string
	wantReason string
	headers    []iottypes.HttpActionHeader
	wantBodies []string
	wantHits   int
	noConfirm  bool
}

func TestIoTRuleHTTPAction(t *testing.T) {
	t.Parallel()

	tests := []iotHTTPCase{
		{
			name: "projected_payload_and_templated_header", sql: "SELECT temp AS t, topic() AS tp FROM 'http/in'",
			payload: `{"temp":5,"id":"d9"}`, wantHits: 1, wantBodies: []string{`{"t":5,"tp":"http/in"}`},
			headers: []iottypes.HttpActionHeader{{Key: aws.String("X-Device"), Value: aws.String("${id}")}},
		},
		{
			name: "binary_payload_content_type", sql: "SELECT * FROM 'http/in'", payload: "plain-bytes",
			wantHits: 1, wantBodies: []string{"plain-bytes"},
		},
		{
			name: "retries_on_503", sql: "SELECT * FROM 'http/in'", payload: `{"a":1}`, wantHits: 2,
			status: func(n int) int {
				if n == 1 {
					return http.StatusServiceUnavailable
				}

				return http.StatusOK
			},
		},
		{
			name: "client_error_not_retried", sql: "SELECT * FROM 'http/in'", payload: `{"a":1}`, wantHits: 1,
			status:     func(int) int { return http.StatusBadRequest },
			wantReason: "failure status",
		},
		{
			name: "unconfirmed_destination_blocks", sql: "SELECT * FROM 'http/in'", payload: `{"a":1}`,
			noConfirm: true, wantReason: "not enabled",
		},
		{
			name: "disabled_destination_blocks", sql: "SELECT * FROM 'http/in'", payload: `{"a":1}`,
			wantReason: "not enabled",
			prepare: func(t *testing.T, f *iotRuleFixture, _ *iotHTTPEndpoint, arn string) {
				t.Helper()

				_, err := iot.NewFromConfig(f.fx.cfg).UpdateTopicRuleDestination(t.Context(),
					&iot.UpdateTopicRuleDestinationInput{
						Arn: aws.String(arn), Status: iottypes.TopicRuleDestinationStatusDisabled,
					})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runIoTHTTPCase(t, tt)
		})
	}
}

func runIoTHTTPCase(t *testing.T, tt iotHTTPCase) {
	t.Helper()

	f := newIoTRuleFixture(t, false, "*")
	errAct, errURL := f.errAction(t)

	var onConfirm func(iotHTTPHit)
	if !tt.noConfirm {
		onConfirm = f.confirmViaSDK(t)
	}

	ep := newIoTHTTPEndpoint(t, tt.status, onConfirm)
	arn := f.createDestination(t, ep.srv.URL)

	if tt.noConfirm {
		f.awaitStatus(t, arn, iottypes.TopicRuleDestinationStatusInProgress)
	} else {
		f.awaitStatus(t, arn, iottypes.TopicRuleDestinationStatusEnabled)
	}

	if tt.prepare != nil {
		tt.prepare(t, f, ep, arn)
	}

	f.createRule(t, "httprule", tt.sql, errAct, iottypes.Action{Http: &iottypes.HttpAction{
		Url: aws.String(ep.srv.URL + "/data"), ConfirmationUrl: aws.String(ep.srv.URL), Headers: tt.headers,
	}})
	f.publish(t, "http/in", tt.payload)

	if tt.wantReason != "" {
		var env map[string]string
		require.NoError(t, json.Unmarshal([]byte(f.bodies(t, errURL, 1)[0]), &env))
		assert.Equal(t, "HttpAction", env["failedAction"])
		assert.Contains(t, env["failedActionReason"], tt.wantReason)
	}

	if tt.wantHits == 0 {
		assert.Empty(t, ep.deliveries())

		return
	}

	hits := ep.waitDeliveries(t, tt.wantHits)
	last := hits[len(hits)-1]

	assert.Len(t, hits, tt.wantHits)
	assert.Equal(t, "/data", last.path)

	for i, want := range tt.wantBodies {
		assert.Equal(t, want, string(hits[i].body))
	}

	if len(tt.headers) > 0 {
		assert.Equal(t, "d9", last.header.Get("X-Device"))
	}

	wantType := "application/json"
	if tt.name == "binary_payload_content_type" {
		wantType = "application/octet-stream"
	}

	assert.Equal(t, wantType, last.header.Get("Content-Type"))
}

func TestIoTRuleHTTPActionImplicitDestination(t *testing.T) {
	t.Parallel()

	f := newIoTRuleFixture(t, false, "*")
	ep := newIoTHTTPEndpoint(t, nil, f.confirmViaSDK(t))

	f.createRule(t, "implicit", "SELECT * FROM 'http/in'", nil, iottypes.Action{Http: &iottypes.HttpAction{
		Url: aws.String(ep.srv.URL + "/data"),
	}})

	require.Eventually(t, func() bool { return len(ep.confirmationRequests()) == 1 }, authzDeadline, authzTick)
	assert.Equal(t, "/data", ep.confirmationRequests()[0].path)

	require.Eventually(t, func() bool {
		out, err := iot.NewFromConfig(f.fx.cfg).
			ListTopicRuleDestinations(t.Context(), &iot.ListTopicRuleDestinationsInput{})

		return err == nil && len(out.DestinationSummaries) == 1 &&
			out.DestinationSummaries[0].Status == iottypes.TopicRuleDestinationStatusEnabled
	}, authzDeadline, authzTick)

	f.publish(t, "http/in", `{"x":1}`)
	assert.JSONEq(t, `{"x":1}`, string(ep.waitDeliveries(t, 1)[0].body))
}

func TestIoTRuleHTTPActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		http iottypes.HttpAction
	}{
		{name: "confirmation_url_not_prefix", http: iottypes.HttpAction{
			Url: aws.String("https://a.example.com/x"), ConfirmationUrl: aws.String("https://b.example.com"),
		}},
		{name: "template_without_confirmation_url", http: iottypes.HttpAction{
			Url: aws.String("https://a.example.com/${id}"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")

			_, err := iot.NewFromConfig(f.fx.cfg).CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
				RuleName: aws.String("bad"),
				TopicRulePayload: &iottypes.TopicRulePayload{
					Sql: aws.String("SELECT * FROM 't'"), Actions: []iottypes.Action{{Http: &tt.http}},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestIoTRuleHTTPActionSigV4Enforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantKey   string
		enforce   bool
		wantHit   bool
	}{
		{name: "trusted_role_signs", principal: "iot.amazonaws.com", enforce: true, wantHit: true, wantKey: "ASIA"},
		{name: "untrusted_role_denied", principal: "lambda.amazonaws.com", enforce: true},
		{name: "enforcement_off_signs_default_key", principal: "lambda.amazonaws.com", wantHit: true, wantKey: "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, tt.enforce, "sqs:SendMessage")
			errAct, errURL := f.errAction(t)
			role := authzRole(t, f.fx, "signing-role", tt.principal, "execute-api:Invoke")
			ep := newIoTHTTPEndpoint(t, nil, f.confirmViaSDK(t))

			f.awaitStatus(t, f.createDestination(t, ep.srv.URL), iottypes.TopicRuleDestinationStatusEnabled)

			f.createRule(t, "signed", "SELECT * FROM 'http/in'", errAct, iottypes.Action{Http: &iottypes.HttpAction{
				Url: aws.String(ep.srv.URL + "/data"), ConfirmationUrl: aws.String(ep.srv.URL),
				Auth: &iottypes.HttpAuthorization{Sigv4: &iottypes.SigV4Authorization{
					RoleArn: aws.String(
						role,
					), ServiceName: aws.String("execute-api"), SigningRegion: aws.String("us-west-2"),
				}},
			}})
			f.publish(t, "http/in", `{"x":1}`)

			if !tt.wantHit {
				var env map[string]string
				require.NoError(t, json.Unmarshal([]byte(f.bodies(t, errURL, 1)[0]), &env))
				assert.Equal(t, "HttpAction", env["failedAction"])
				assert.Empty(t, ep.deliveries())

				return
			}

			auth := ep.waitDeliveries(t, 1)[0].header
			assert.Contains(t, auth.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="+tt.wantKey)
			assert.Contains(t, auth.Get("Authorization"), "/us-west-2/execute-api/aws4_request")

			if tt.enforce {
				assert.NotEmpty(t, auth.Get("X-Amz-Security-Token"))
			}
		})
	}
}
