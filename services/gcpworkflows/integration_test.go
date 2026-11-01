package gcpworkflows_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestHTTPCallsCarryServiceAccountTokens(t *testing.T) {
	t.Parallel()

	rec := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	e := newEnv(t)
	got := e.run(t, "http_wf", `
main:
  steps:
    - oidc:
        call: http.post
        args:
          url: `+rec.srv.URL+`/oidc
          body: {hello: world}
          auth: {type: OIDC, audience: "https://aud.example"}
        result: a
    - oauth:
        call: http.get
        args:
          url: `+rec.srv.URL+`/oauth
          auth: {type: OAuth2}
        result: b
    - r:
        return: ${a.body.ok and b.body.ok}
`, "")

	assert.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
	assert.Equal(t, "true", got.GetResult())

	reqs := rec.requests()
	require.Len(t, reqs, 2)
	assert.Contains(t, reqs[0], "POST /oidc auth=Bearer ")
	assert.JSONEq(t, `{"hello":"world"}`, string(<-rec.got))
	assert.Contains(t, reqs[1], "auth=Bearer gopherstack-unsigned-oauth.")
}

func TestCallbacks(t *testing.T) {
	t.Parallel()

	announce := newRecorder(t, nil)
	e := newEnv(t)
	e.clock.holdSleeps()
	w := e.createWorkflow(t, "cb_wf", `
main:
  steps:
    - mk:
        call: events.create_callback_endpoint
        args: {http_callback_method: POST}
        result: cb
    - tell:
        call: http.post
        args:
          url: `+announce.srv.URL+`
          body: {url: "${cb.url}"}
    - wait:
        call: events.await_callback
        args: {callback: "${cb}", timeout: 600}
        result: got
    - r:
        return: {body: "${got.http_request.body}", method: "${got.http_request.method}"}
`)
	ex := e.execute(t, w.GetName(), "")

	var told struct{ URL string }

	raw := <-announce.got
	require.NoError(t, jsonUnmarshal(raw, &told))
	require.Contains(t, told.URL, "/callbacks/")

	resp, err := testclient.New(t).Get(told.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	resp, err = testclient.New(t).Post(
		told.URL,
		"application/json",
		strings.NewReader(`{"approved":true}`),
	)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	done := e.await(t, ex.GetName())
	assert.Equal(t, executionspb.Execution_SUCCEEDED, done.GetState(), done.GetError().GetPayload())
	assert.JSONEq(t, `{"body":{"approved":true},"method":"POST"}`, done.GetResult())

	resp, err = testclient.New(t).Post(told.URL, "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "a spent callback is gone")
}

func TestCallbackTimeout(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()

	w := e.createWorkflow(t, "cb_timeout", `
main:
  steps:
    - mk:
        call: events.create_callback_endpoint
        args: {http_callback_method: POST}
        result: cb
    - wait:
        try:
          call: events.await_callback
          args: {callback: "${cb}", timeout: 5}
        except:
          as: err
          steps:
            - r:
                return: ${err.tags}
`)
	ex := e.execute(t, w.GetName(), "")

	<-e.clock.sleeping
	e.clock.release()

	got := e.await(t, ex.GetName())
	assert.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
	assert.JSONEq(t, `["TimeoutError"]`, got.GetResult())
}

type captureSink struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (c *captureSink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, req.GetEntries()...)

	return nil
}

func TestSysLogReachesCloudLogging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sink := &captureSink{}
	e.handler.Backend.SetLogSink(sink)

	ex := e.run(t, "log_wf", `
main:
  steps:
    - a:
        call: sys.log
        args: {text: "plain line", severity: WARNING}
    - b:
        call: sys.log
        args: {json: {k: "v"}}
`, "")
	require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState())

	sink.mu.Lock()
	defer sink.mu.Unlock()

	require.Len(t, sink.entries, 2)
	assert.Equal(t, "projects/test-project/logs/Workflows", sink.entries[0].GetLogName())
	assert.Equal(t, "workflows.googleapis.com/Workflow", sink.entries[0].GetResource().GetType())
	assert.Equal(t, "log_wf", sink.entries[0].GetResource().GetLabels()["workflow_id"])
	assert.Equal(t, "plain line", sink.entries[0].GetTextPayload())
	assert.Equal(t, "WARNING", sink.entries[0].GetSeverity().String())
	assert.Equal(t, "v", sink.entries[1].GetJsonPayload().AsMap()["k"])
}

type endpoints map[string]string

func (e endpoints) BaseURL(service string) string { return e[service] }

func TestConnectorsReachTheOtherEmulators(t *testing.T) {
	t.Parallel()

	ps := gcppubsub.NewHandler(gcppubsub.NewInMemoryBackend())
	ps.Port = 0
	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })

	sm := gcpsecretmanager.NewHandler(gcpsecretmanager.NewInMemoryBackend())
	sm.Port = 0
	require.NoError(t, sm.StartWorker(t.Context()))
	t.Cleanup(func() { sm.Shutdown(context.Background()) })

	fs := gcpfirestore.NewHandler(gcpfirestore.NewInMemoryBackend())
	fs.Port = 0
	require.NoError(t, fs.StartWorker(t.Context()))
	t.Cleanup(func() { fs.Shutdown(context.Background()) })

	echoSrv := echo.New()
	echoSrv.Any("/*", gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend()).Handler())

	gcs := httptest.NewServer(echoSrv)
	t.Cleanup(gcs.Close)

	e := newEnv(t)
	e.handler.Backend.SetEndpoints(endpoints{
		"pubsub": "http://" + ps.Addr(), "secretmanager": "http://" + sm.Addr(),
		"firestore": "http://" + fs.Addr(), "storage": gcs.URL,
	})

	payload := base64.StdEncoding.EncodeToString([]byte("s3cret"))
	got := e.run(t, "connectors", `
main:
  steps:
    - mkTopic:
        call: googleapis.pubsub.v1.projects.topics.create
        args: {name: projects/test-project/topics/topic1, body: {}}
    - publish:
        call: googleapis.pubsub.v1.projects.topics.publish
        args:
          topic: projects/test-project/topics/topic1
          body: {messages: [{data: aGk=}]}
        result: pub
    - mkSecret:
        call: googleapis.secretmanager.v1.projects.secrets.create
        args:
          parent: projects/test-project
          secretId: s1
          body: {replication: {automatic: {}}}
    - addVersion:
        call: googleapis.secretmanager.v1.projects.secrets.addVersion
        args:
          parent: projects/test-project/secrets/s1
          body: {payload: {data: `+payload+`}}
    - access:
        call: googleapis.secretmanager.v1.projects.secrets.versions.access
        args: {name: projects/test-project/secrets/s1/versions/latest}
        result: secret
    - mkBucket:
        call: googleapis.storage.v1.buckets.insert
        args: {project: test-project, body: {name: bkt}}
    - put:
        call: googleapis.storage.v1.objects.insert
        args: {bucket: bkt, name: dir/obj.txt, body: "file body"}
    - meta:
        call: googleapis.storage.v1.objects.get
        args: {bucket: bkt, object: dir/obj.txt}
        result: obj
    - listing:
        call: googleapis.storage.v1.objects.list
        args: {bucket: bkt}
        result: objs
    - doc:
        call: googleapis.firestore.v1.projects.databases.documents.createDocument
        args:
          parent: projects/test-project/databases/(default)/documents
          collectionId: things
          documentId: d1
          body: {fields: {n: {integerValue: "5"}}}
    - read:
        call: googleapis.firestore.v1.projects.databases.documents.get
        args: {name: projects/test-project/databases/(default)/documents/things/d1}
        result: fdoc
    - r:
        return:
          messageIds: "${len(pub.messageIds)}"
          secret: "${secret.payload.data}"
          object: "${obj.name}"
          objects: "${len(objs.items)}"
          field: "${fdoc.fields.n.integerValue}"
`, "")

	require.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
	assert.JSONEq(
		t,
		`{"messageIds":1,"secret":"`+payload+`","object":"dir/obj.txt","objects":1,"field":"5"}`,
		got.GetResult(),
	)
}

func TestConnectorErrorsAreHTTPErrors(t *testing.T) {
	t.Parallel()

	sm := gcpsecretmanager.NewHandler(gcpsecretmanager.NewInMemoryBackend())
	sm.Port = 0
	require.NoError(t, sm.StartWorker(t.Context()))
	t.Cleanup(func() { sm.Shutdown(context.Background()) })

	tests := []struct {
		name string
		eps  endpoints
		want string
	}{
		{"missing secret", endpoints{"secretmanager": "http://" + sm.Addr()}, `{"code":404,"tags":["HttpError"]}`},
		{"emulator not connected", endpoints{}, `{"code":null,"tags":["OperationError"]}`},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetEndpoints(tt.eps)

			got := e.run(t, "conn_err_"+string(rune('a'+i)), `
main:
  steps:
    - t:
        try:
          call: googleapis.secretmanager.v1.projects.secrets.versions.access
          args: {name: projects/test-project/secrets/ghost/versions/1}
        except:
          as: err
          steps:
            - r:
                return: {code: "${map.get(err, \"code\")}", tags: "${err.tags}"}
`, "")
			require.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
			assert.JSONEq(t, tt.want, got.GetResult())
		})
	}
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	w := src.createWorkflow(t, "kept", hello, func(w *workflowspb.Workflow) { w.Labels = map[string]string{"a": "b"} })
	done := src.await(t, src.execute(t, w.GetName(), `{"name":"x"}`).GetName())

	src.clock.holdSleeps()

	sleeper := src.createWorkflow(
		t,
		"sleeper",
		"main:\n  steps:\n    - nap:\n        call: sys.sleep\n        args: {seconds: 60}\n",
	)
	running := src.execute(t, sleeper.GetName(), "")
	<-src.clock.sleeping

	snap := src.handler.Backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), snap))

	got, err := dst.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: w.GetName()})
	require.NoError(t, err)
	sameProto(t, w, got)

	ex, err := dst.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: done.GetName()})
	require.NoError(t, err)
	assert.Equal(t, `"hello x"`, ex.GetResult())

	interrupted, err := dst.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: running.GetName()})
	require.NoError(t, err)
	assert.Equal(t, executionspb.Execution_FAILED, interrupted.GetState())
	assert.Contains(t, interrupted.GetError().GetPayload(), "emulator restart")

	again := dst.await(t, dst.execute(t, w.GetName(), `{"name":"y"}`).GetName())
	assert.Equal(t, `"hello y"`, again.GetResult(), "restored workflows are runnable")

	op, err := dst.wf.LROClient.GetOperation(t.Context(), opRequest(parent+"/operations/unknown"))
	require.Error(t, err)
	assert.Nil(t, op)

	src.clock.release()
}

func TestRestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createWorkflow(t, "doomed", hello)

	require.NoError(t, e.handler.Backend.Restore(t.Context(), []byte(`{"version":999,"workflows":{}}`)))

	_, err := e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: parent + "/workflows/doomed"})
	assert.Equal(t, 5, int(code(err)))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "gone_wf", hello)
	ex := e.await(t, e.execute(t, w.GetName(), `{"name":"x"}`).GetName())
	e.handler.Reset()

	_, err := e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: w.GetName()})
	assert.Equal(t, 5, int(code(err)))

	_, err = e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: ex.GetName()})
	assert.Equal(t, 5, int(code(err)))

	again := e.createWorkflow(t, "gone_wf", hello)
	assert.Equal(t, w.GetName(), again.GetName(), "the backend works after a reset")
}

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	const wf = "/v1/projects/test-project/locations/us-central1/workflows/rest_wf"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create workflow", Method: http.MethodPost,
			Path:   "/v1/projects/test-project/locations/us-central1/workflows?workflowId=rest_wf",
			Body:   `{"sourceContents":"main:\n  params: [args]\n  steps:\n    - r:\n        return: ${args.n * 2}\n"}`,
			Status: 200, Contains: []string{`"done":true`, `rest_wf`, `OperationMetadata`, `"state":"ACTIVE"`},
		},
		{
			Name:     "get workflow",
			Method:   http.MethodGet,
			Path:     wf,
			Status:   200,
			Contains: []string{`"revisionId":"000001-`},
		},
		{
			Name: "list revisions", Method: http.MethodGet, Path: wf + ":listRevisions",
			Status: 200, Contains: []string{`"workflows":[`, `000001-`},
		},
		{
			Name: "create execution", Method: http.MethodPost, Path: wf + "/executions",
			Body: `{"argument":"{\"n\":21}"}`, Status: 200, Contains: []string{`"state":"ACTIVE"`, `/executions/`},
		},
		{
			Name: "list executions", Method: http.MethodGet, Path: wf + "/executions?view=FULL",
			Status: 200, Contains: []string{`"executions":[`, `"argument":"{\"n\":21}"`},
		},
		{
			Name: "missing workflow", Method: http.MethodGet, Path: wf + "_ghost",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "list locations", Method: http.MethodGet, Path: "/v1/projects/test-project/locations",
			Status: 200, Contains: []string{`"locationId":"us-central1"`},
		},
		{Name: "delete workflow", Method: http.MethodDelete, Path: wf, Status: 200, Contains: []string{`"done":true`}},
	})
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(bytes.TrimSpace(raw), v) }
