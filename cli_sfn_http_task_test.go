package main

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const httpTaskConnARN = "arn:aws:events:us-east-1:000000000000:connection/c/"

type fakeConnections map[string]sfnbackend.ConnectionCredentials

func (f fakeConnections) ResolveConnectionCredentials(
	_ context.Context, arn string,
) (sfnbackend.ConnectionCredentials, error) {
	c, ok := f[arn]
	if !ok {
		return c, sfnbackend.ErrConnectionNotFound
	}

	return c, nil
}

type seenRequest struct {
	header http.Header
	query  url.Values
	body   string
}

type httpTarget struct {
	srv  *httptest.Server
	seen sync.Map
}

func newHTTPTarget(t *testing.T) *httpTarget {
	t.Helper()

	tg := &httpTarget{}
	mux := http.NewServeMux()

	mux.HandleFunc("/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"a":1}`))
	})
	mux.HandleFunc("/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("nope"))
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(strings.Repeat("a", 262145)))
	})
	mux.HandleFunc("/binary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte{0xff, 0xfe})
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ftp://example.invalid/x", http.StatusFound)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok"}`))
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		tg.seen.Store(
			r.URL.Query().Get("id"),
			seenRequest{header: r.Header.Clone(), query: r.URL.Query(), body: string(b)},
		)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	tg.srv = httptest.NewServer(mux)
	t.Cleanup(tg.srv.Close)

	return tg
}

func (tg *httpTarget) request(t *testing.T, id string) seenRequest {
	t.Helper()

	v, ok := tg.seen.Load(id)
	require.True(t, ok, id)

	return v.(seenRequest)
}

const formBody = `"RequestBody":{"customer":"c1","tags":["a","b"],"meta":{"k":"v w"}}`

func transform(arrayFormat string) string {
	return `"Transform":{"RequestBodyEncoding":"URL_ENCODED",` +
		`"RequestEncodingOptions":{"ArrayFormat":"` + arrayFormat + `"}}`
}

func httpTaskDefinition(params, extra string) string {
	return `{"StartAt":"H","States":{"H":{"Type":"Task","Resource":"arn:aws:states:::http:invoke",` +
		`"Parameters":` + params + extra + `,"End":true},` + caughtState + `}}`
}

func TestStepFunctionsHTTPTask(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	tg := newHTTPTarget(t)
	base := tg.srv.URL

	fx.sfn.SetSDKIntegration(sfnbackend.NewSDKIntegrationWithHTTP(fx.handler, "us-east-1", nil, fakeConnections{
		httpTaskConnARN + "plain": {},
		httpTaskConnARN + "apikey": {
			AuthType: "API_KEY", APIKeyName: "X-Api-Key", APIKeyValue: "s3cret",
			Headers: []sfnbackend.NameValue{{Key: "X-Mode", Value: "conn"}},
			Query:   []sfnbackend.NameValue{{Key: "q", Value: "conn"}},
			Body:    []sfnbackend.NameValue{{Key: "k", Value: "conn"}},
		},
		httpTaskConnARN + "basic": {AuthType: "BASIC", Username: "u", Password: "p"},
		httpTaskConnARN + "oauth": {
			AuthType: "OAUTH_CLIENT_CREDENTIALS",
			OAuth: &sfnbackend.OAuthCredentials{
				Endpoint:     base + "/token",
				Method:       "POST",
				ClientID:     "id",
				ClientSecret: "sec",
			},
		},
	}))

	call := func(conn, path, method, extra string) string {
		return `{"ApiEndpoint":"` + base + path + `","Method":"` + method +
			`","Authentication":{"ConnectionArn":"` + httpTaskConnARN + conn + `"}` + extra + `}`
	}

	tests := []struct {
		wantAt     map[string]any
		wantHeader map[string]string
		wantQuery  map[string]string
		name       string
		definition string
		wantStatus sfntypes.SyncExecutionStatus
		wantCause  string
		seenID     string
		wantJSON   string
		wantRaw    string
	}{
		{
			name:       "success_json_output_shape",
			definition: httpTaskDefinition(call("plain", "/json", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			wantAt: map[string]any{
				"StatusCode": float64(200), "StatusText": "OK", "ResponseBody.a": float64(1),
				"Headers.Content-Type": []any{"application/json"},
			},
		},
		{
			name: "non_2xx_caught_by_documented_name",
			definition: httpTaskDefinition(
				call("plain", "/missing", "GET", ""),
				","+catchTo("States.Http.StatusCode.404"),
			),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			wantAt:     map[string]any{"err.Error": "States.Http.StatusCode.404"},
		},
		{
			name:       "non_2xx_uncaught_fails_with_name",
			definition: httpTaskDefinition(call("plain", "/missing", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusFailed,
			wantCause:  "States.Http.StatusCode.404",
		},
		{
			name: "api_key_header_and_connection_overrides",
			definition: httpTaskDefinition(call(
				"apikey",
				"/echo?id=apikey",
				"POST",
				`,"Headers":{"X-Mode":"state"},"QueryParameters":{"q":"state"},"RequestBody":{"k":"state","j":"1"}`,
			), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "apikey",
			wantHeader: map[string]string{"X-Api-Key": "s3cret", "X-Mode": "conn", "Range": "bytes=0-262144"},
			wantQuery:  map[string]string{"q": "conn"},
			wantJSON:   `{"j":"1","k":"conn"}`,
		},
		{
			name:       "basic_auth_applied",
			definition: httpTaskDefinition(call("basic", "/echo?id=basic", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "basic",
			wantHeader: map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))},
		},
		{
			name:       "oauth_client_credentials_bearer",
			definition: httpTaskDefinition(call("oauth", "/echo?id=oauth", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "oauth",
			wantHeader: map[string]string{"Authorization": "Bearer tok"},
		},
		{
			name: "url_encoded_indices",
			definition: httpTaskDefinition(call("plain", "/echo?id=indices", "POST",
				`,"Headers":{"Content-Type":"application/x-www-form-urlencoded"},`+formBody+
					`,"Transform":{"RequestBodyEncoding":"URL_ENCODED"}`), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "indices",
			wantRaw:    "customer=c1&meta%5Bk%5D=v%20w&tags%5B0%5D=a&tags%5B1%5D=b",
		},
		{
			name: "url_encoded_repeat",
			definition: httpTaskDefinition(call("plain", "/echo?id=repeat", "POST",
				`,`+formBody+`,`+transform("REPEAT")), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "repeat",
			wantRaw:    "customer=c1&meta%5Bk%5D=v%20w&tags=a&tags=b",
		},
		{
			name: "url_encoded_commas",
			definition: httpTaskDefinition(call("plain", "/echo?id=commas", "POST",
				`,`+formBody+`,`+transform("COMMAS")), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "commas",
			wantRaw:    "customer=c1&meta%5Bk%5D=v%20w&tags=a,b",
		},
		{
			name: "url_encoded_brackets",
			definition: httpTaskDefinition(call("plain", "/echo?id=brackets", "POST",
				`,`+formBody+`,`+transform("BRACKETS")), ""),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			seenID:     "brackets",
			wantRaw:    "customer=c1&meta%5Bk%5D=v%20w&tags%5B%5D=a&tags%5B%5D=b",
		},
		{
			name: "forbidden_header_is_runtime_error",
			definition: httpTaskDefinition(call("plain", "/json", "GET",
				`,"Headers":{"X-Amz-Date":"x"}`), ""),
			wantStatus: sfntypes.SyncExecutionStatusFailed,
			wantCause:  "States.Runtime",
		},
		{
			name: "unknown_connection_caught",
			definition: httpTaskDefinition(
				call("absent", "/json", "GET", ""),
				","+catchTo("Events.ConnectionResource.ResourceNotFound"),
			),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			wantAt:     map[string]any{"err.Error": "Events.ConnectionResource.ResourceNotFound"},
		},
		{
			name:       "binary_response_is_runtime_error",
			definition: httpTaskDefinition(call("plain", "/binary", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusFailed,
			wantCause:  "States.Runtime",
		},
		{
			name:       "oversized_response_exceeds_data_limit",
			definition: httpTaskDefinition(call("plain", "/big", "GET", ""), ""),
			wantStatus: sfntypes.SyncExecutionStatusFailed,
			wantCause:  "States.DataLimitExceeded",
		},
		{
			name: "string_body_with_connection_body_is_runtime_error",
			definition: httpTaskDefinition(call("apikey", "/echo?id=strbody", "POST",
				`,"RequestBody":"raw"`), ""),
			wantStatus: sfntypes.SyncExecutionStatusFailed,
			wantCause:  "States.Runtime",
		},
		{
			name: "redirect_to_non_http_scheme_not_followed",
			definition: httpTaskDefinition(
				call("plain", "/redirect", "GET", ""),
				","+catchTo("States.Http.StatusCode.302"),
			),
			wantStatus: sfntypes.SyncExecutionStatusSucceeded,
			wantAt:     map[string]any{"err.Error": "States.Http.StatusCode.302"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := runSyncSFN(t, fx, "http-"+strings.ReplaceAll(tt.name, "_", "-"), tt.definition, `{}`)
			require.Equal(t, tt.wantStatus, out.Status, aws.ToString(out.Cause))
			assert.Equal(t, tt.wantCause, aws.ToString(out.Error))

			for path, want := range tt.wantAt {
				assert.Equal(t, want, jsonAt(t, out.Output, strings.Split(path, ".")...))
			}

			if tt.seenID == "" {
				return
			}

			got := tg.request(t, tt.seenID)
			for k, v := range tt.wantHeader {
				assert.Equal(t, v, got.header.Get(k), k)
			}

			for k, v := range tt.wantQuery {
				assert.Equal(t, v, got.query.Get(k), k)
			}

			if tt.wantJSON != "" {
				assert.JSONEq(t, tt.wantJSON, got.body)
			}

			if tt.wantRaw != "" {
				assert.Equal(t, tt.wantRaw, got.body)
			}
		})
	}
}

func TestStepFunctionsOptimizedGlue(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "Glue")

	_, err := glue.NewFromConfig(fx.cfg).CreateJob(t.Context(), &glue.CreateJobInput{
		Name: aws.String("sfn-job"), Role: aws.String(sfnTestRole),
		Command: &gluetypes.JobCommand{Name: aws.String("glueetl"), ScriptLocation: aws.String("s3://b/s.py")},
	})
	require.NoError(t, err)

	task := func(suffix, job, extra string) string {
		return `{"StartAt":"G","States":{"G":{"Type":"Task","Resource":"arn:aws:states:::glue:startJobRun` + suffix +
			`","Parameters":{"JobName":"` + job + `"}` + extra + `,"End":true},` + caughtState + `}}`
	}

	tests := []struct {
		wantAt     map[string]any
		name       string
		definition string
	}{
		{
			name:       "request_response_adds_job_name",
			definition: task("", "sfn-job", ""),
			wantAt:     map[string]any{"JobName": "sfn-job"},
		},
		{
			name:       "sync_waits_for_succeeded",
			definition: task(".sync", "sfn-job", ""),
			wantAt:     map[string]any{"JobRun.JobRunState": "SUCCEEDED", "JobRun.JobName": "sfn-job"},
		},
		{
			name:       "missing_job_caught",
			definition: task(".sync", "absent", ","+catchTo("Glue.EntityNotFoundException")),
			wantAt:     map[string]any{"err.Error": "Glue.EntityNotFoundException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := runSyncSFN(t, fx, "glue-"+strings.ReplaceAll(tt.name, "_", "-"), tt.definition, `{}`)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status, aws.ToString(out.Cause))

			for path, want := range tt.wantAt {
				assert.Equal(t, want, jsonAt(t, out.Output, strings.Split(path, ".")...))
			}
		})
	}
}

func TestStepFunctionsOptimizedECSSync(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ecsc := ecs.NewFromConfig(fx.cfg)

	_, err := ecsc.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("sfn-ecs")})
	require.NoError(t, err)

	_, err = ecsc.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("sfn-td"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("c"), Image: aws.String("busybox"), Essential: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	definition := `{"StartAt":"R","States":{"R":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask.sync",` +
		`"Parameters":{"Cluster":"sfn-ecs","TaskDefinition":"sfn-td"},"End":true}}}`

	sfnc := sfn.NewFromConfig(fx.cfg)
	created, err := sfnc.CreateStateMachine(t.Context(), &sfn.CreateStateMachineInput{
		Name: aws.String("ecs-sync"), Definition: aws.String(definition), RoleArn: aws.String(sfnTestRole),
		Type: sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	type result struct {
		out *sfn.StartSyncExecutionOutput
		err error
	}

	done := make(chan result, 1)

	go func() {
		out, runErr := sfnc.StartSyncExecution(t.Context(), &sfn.StartSyncExecutionInput{
			StateMachineArn: created.StateMachineArn, Input: aws.String(`{}`),
		})
		done <- result{out: out, err: runErr}
	}()

	var taskARN string

	require.Eventually(t, func() bool {
		list, listErr := ecsc.ListTasks(t.Context(), &ecs.ListTasksInput{Cluster: aws.String("sfn-ecs")})
		if listErr != nil || len(list.TaskArns) == 0 {
			return false
		}

		taskARN = list.TaskArns[0]

		return true
	}, authzDeadline, authzTick)

	select {
	case <-done:
		require.Fail(t, ".sync returned before the task stopped")
	default:
	}

	_, err = ecsc.StopTask(t.Context(), &ecs.StopTaskInput{Cluster: aws.String("sfn-ecs"), Task: aws.String(taskARN)})
	require.NoError(t, err)

	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, res.out.Status, aws.ToString(res.out.Cause))

	tasks, ok := jsonAt(t, res.out.Output, "Tasks").([]any)
	require.True(t, ok)
	require.Len(t, tasks, 1)
	assert.Equal(t, "STOPPED", tasks[0].(map[string]any)["LastStatus"])
}

func TestStepFunctionsOptimizedECSFailures(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ecsc := ecs.NewFromConfig(fx.cfg)

	_, err := ecsc.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("sfn-ecs-fail")})
	require.NoError(t, err)

	_, err = ecsc.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("sfn-td-fail"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("c"), Image: aws.String("busybox"), Essential: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	task := func(suffix, extra string) string {
		return `{"StartAt":"R","States":{"R":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask` + suffix +
			`","Parameters":{"Cluster":"sfn-ecs-fail","TaskDefinition":"sfn-td-fail","LaunchType":"EC2"}` + extra +
			`,"End":true},` + caughtState + `}}`
	}

	tests := []struct {
		wantAt     map[string]any
		name       string
		definition string
	}{
		{
			name:       "sync_failures_raise_amazon_ecs_unknown",
			definition: task(".sync", ","+catchTo("AmazonECS.Unknown")),
			wantAt:     map[string]any{"err.Error": "AmazonECS.Unknown"},
		},
		{
			name:       "request_response_returns_failures",
			definition: task("", ""),
			wantAt:     map[string]any{"Failures": nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := runSyncSFN(t, fx, "ecsf-"+strings.ReplaceAll(tt.name, "_", "-"), tt.definition, `{}`)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status, aws.ToString(out.Cause))

			for path, want := range tt.wantAt {
				got := jsonAt(t, out.Output, strings.Split(path, ".")...)
				if want == nil {
					assert.NotEmpty(t, got)

					continue
				}

				assert.Equal(t, want, got)
			}
		})
	}
}

func TestStepFunctionsHTTPTaskRoleAuthz(t *testing.T) {
	t.Parallel()

	const (
		invoke = "states:InvokeHTTPEndpoint"
		creds  = "events:RetrieveConnectionCredentials"
		get    = "secretsmanager:GetSecretValue"
		desc   = "secretsmanager:DescribeSecret"
	)

	tests := []struct {
		name      string
		wantCause string
		wantCaugh string
		actions   []string
	}{
		{name: "all_allowed", actions: []string{invoke, creds, get, desc}},
		{name: "invoke_endpoint_missing", actions: []string{creds, get, desc}, wantCause: "States.Permissions"},
		{
			name: "connection_credentials_missing", actions: []string{invoke, get, desc},
			wantCaugh: "Events.ConnectionResource.AccessDenied",
		},
		{
			name: "secret_missing", actions: []string{invoke, creds},
			wantCaugh: "Events.ConnectionResource.AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, true)
			tg := newHTTPTarget(t)
			fx.sfn.SetSDKIntegration(sfnbackend.NewSDKIntegrationWithHTTP(fx.handler, "us-east-1", nil,
				fakeConnections{httpTaskConnARN + "plain": {}}))

			role := authzRole(t, fx, "sfn-http-"+strings.ReplaceAll(tt.name, "_", "-"),
				"states.amazonaws.com", strings.Join(tt.actions, `","`))
			params := `{"ApiEndpoint":"` + tg.srv.URL + `/json","Method":"GET",` +
				`"Authentication":{"ConnectionArn":"` + httpTaskConnARN + `plain"}}`

			extra := ""
			if tt.wantCaugh != "" {
				extra = "," + catchTo("Events.ConnectionResource.AccessDenied")
			}

			out := runSyncSFNAs(t, fx, role, "sm", httpTaskDefinition(params, extra), `{}`)

			switch {
			case tt.wantCause != "":
				require.Equal(t, sfntypes.SyncExecutionStatusFailed, out.Status)
				assert.Equal(t, tt.wantCause, aws.ToString(out.Error))
			case tt.wantCaugh != "":
				require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status, aws.ToString(out.Cause))
				assert.Equal(t, tt.wantCaugh, jsonAt(t, out.Output, "err", "Error"))
			default:
				require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status, aws.ToString(out.Cause))
				assert.InDelta(t, 200, jsonAt(t, out.Output, "StatusCode"), 0)
			}
		})
	}
}

func TestStepFunctionsHTTPTaskEventBridgeConnections(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	tg := newHTTPTarget(t)
	ebc := eventbridge.NewFromConfig(fx.cfg)

	type authParams = ebtypes.CreateConnectionAuthRequestParameters

	create := func(name string, authType ebtypes.ConnectionAuthorizationType, p *authParams) string {
		out, err := ebc.CreateConnection(t.Context(), &eventbridge.CreateConnectionInput{
			Name: aws.String(name), AuthorizationType: authType, AuthParameters: p,
		})
		require.NoError(t, err)

		return aws.ToString(out.ConnectionArn)
	}

	hdr := &ebtypes.ConnectionHttpParameters{HeaderParameters: []ebtypes.ConnectionHeaderParameter{
		{Key: aws.String("X-From-Conn"), Value: aws.String("yes")},
	}}

	arns := map[string]string{
		"apikey": create(
			"sfn-apikey",
			ebtypes.ConnectionAuthorizationTypeApiKey,
			&authParams{
				ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
					ApiKeyName:  aws.String("X-Api-Key"),
					ApiKeyValue: aws.String("s3cret"),
				},
				InvocationHttpParameters: hdr,
			},
		),
		"basic": create(
			"sfn-basic",
			ebtypes.ConnectionAuthorizationTypeBasic,
			&authParams{
				BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
					Username: aws.String("u"),
					Password: aws.String("p"),
				},
			},
		),
		"oauth": create(
			"sfn-oauth",
			ebtypes.ConnectionAuthorizationTypeOauthClientCredentials,
			&authParams{
				OAuthParameters: &ebtypes.CreateConnectionOAuthRequestParameters{
					AuthorizationEndpoint: aws.String(tg.srv.URL + "/token"),
					HttpMethod:            ebtypes.ConnectionOAuthHttpMethodPost,
					ClientParameters: &ebtypes.CreateConnectionOAuthClientRequestParameters{
						ClientID: aws.String("id"), ClientSecret: aws.String("sec"),
					},
				},
			},
		),
		"revoked": create(
			"sfn-revoked",
			ebtypes.ConnectionAuthorizationTypeBasic,
			&authParams{
				BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
					Username: aws.String("u"),
					Password: aws.String("p"),
				},
			},
		),
	}

	_, err := ebc.DeauthorizeConnection(
		t.Context(),
		&eventbridge.DeauthorizeConnectionInput{Name: aws.String("sfn-revoked")},
	)
	require.NoError(t, err)

	tests := []struct {
		wantHeader map[string]string
		name       string
		conn       string
		wantCaught string
	}{
		{
			name: "api_key", conn: "apikey",
			wantHeader: map[string]string{"X-Api-Key": "s3cret", "X-From-Conn": "yes"},
		},
		{
			name: "basic", conn: "basic",
			wantHeader: map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))},
		},
		{name: "oauth", conn: "oauth", wantHeader: map[string]string{"Authorization": "Bearer tok"}},
		{name: "deauthorized", conn: "revoked", wantCaught: "Events.ConnectionResource.InvalidConnectionState"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := `{"ApiEndpoint":"` + tg.srv.URL + `/echo?id=eb-` + tt.name + `","Method":"GET",` +
				`"Authentication":{"ConnectionArn":"` + arns[tt.conn] + `"}}`

			extra := ""
			if tt.wantCaught != "" {
				extra = "," + catchTo(tt.wantCaught)
			}

			out := runSyncSFN(
				t,
				fx,
				"ebconn-"+strings.ReplaceAll(tt.name, "_", "-"),
				httpTaskDefinition(params, extra),
				`{}`,
			)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status, aws.ToString(out.Cause))

			if tt.wantCaught != "" {
				assert.Equal(t, tt.wantCaught, jsonAt(t, out.Output, "err", "Error"))

				return
			}

			assert.InDelta(t, 200, jsonAt(t, out.Output, "StatusCode"), 0)

			got := tg.request(t, "eb-"+tt.name)
			for k, v := range tt.wantHeader {
				assert.Equal(t, v, got.header.Get(k), k)
			}
		})
	}
}
