package gcpfunctions_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	functions "cloud.google.com/go/functions/apiv2"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

const parent = "projects/test-project/locations/us-central1"

type fakeBus struct {
	topics map[string]bool
	subs   map[string]*pubsubpb.Subscription
	mu     sync.Mutex
}

func newFakeBus(topics ...string) *fakeBus {
	b := &fakeBus{topics: map[string]bool{}, subs: map[string]*pubsubpb.Subscription{}}
	for _, t := range topics {
		b.topics[t] = true
	}

	return b
}

func (f *fakeBus) GetTopic(name string) (*pubsubpb.Topic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.topics[name] {
		return nil, status.Error(codes.NotFound, "no topic")
	}

	return &pubsubpb.Topic{Name: name}, nil
}

func (f *fakeBus) CreateSubscription(in *pubsubpb.Subscription) (*pubsubpb.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.subs[in.GetName()] = in

	return in, nil
}

func (f *fakeBus) DeleteSubscription(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.subs, name)

	return nil
}

func (f *fakeBus) sub(name string) *pubsubpb.Subscription {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.subs[name]
}

type fakeObjects map[string][]byte

func (o fakeObjects) ReadObject(bucket, object string) ([]byte, error) {
	if d, ok := o[bucket+"/"+object]; ok {
		return d, nil
	}

	return nil, status.Error(codes.NotFound, "no such object")
}

type env struct {
	rt      *fakeRuntime
	bus     *fakeBus
	handler *gcpfunctions.Handler
	client  *functions.FunctionClient
	http    *http.Client
}

func newEnv(t *testing.T, withRuntime bool) *env {
	t.Helper()

	rt := newFakeRuntime()
	bus := newFakeBus("projects/test-project/topics/events")
	h := gcpfunctions.NewHandler(gcpfunctions.NewInMemoryBackend())
	h.Port, h.IngressPort = 0, 0
	h.Backend.SetEventBus(bus)

	if withRuntime {
		h.Backend.SetRuntime(func() (gcpfunctions.Runner, error) { return rt, nil })
	}

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	c, err := functions.NewFunctionClient(t.Context(),
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	e := &env{rt: rt, bus: bus, handler: h, client: c, http: &http.Client{Transport: &http.Transport{}}}
	t.Cleanup(e.http.CloseIdleConnections)

	return e
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func nodeFilesFn() map[string]string {
	return map[string]string{"index.js": "exports.hello = (req, res) => res.send('hi');"}
}

// upload obtains an upload URL, PUTs data to it and returns the storage source to reference.
func (e *env) upload(t *testing.T, data []byte) *functionspb.StorageSource {
	t.Helper()

	resp, err := e.client.GenerateUploadUrl(t.Context(), &functionspb.GenerateUploadUrlRequest{Parent: parent})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, resp.GetUploadUrl(), bytes.NewReader(data))
	require.NoError(t, err)

	res, err := e.http.Do(req)
	require.NoError(t, err)
	_ = res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	return resp.GetStorageSource()
}

func fnSpec(src *functionspb.StorageSource) *functionspb.Function {
	return &functionspb.Function{
		BuildConfig: &functionspb.BuildConfig{
			Runtime: "nodejs22", EntryPoint: "hello",
			Source: &functionspb.Source{Source: &functionspb.Source_StorageSource{StorageSource: src}},
		},
		ServiceConfig: &functionspb.ServiceConfig{
			EnvironmentVariables: map[string]string{"GREETING": "hi"},
		},
	}
}

func (e *env) createFunction(t *testing.T, id string, fn *functionspb.Function) *functionspb.Function {
	t.Helper()

	op, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: id, Function: fn,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

// call sends a request to the ingress with host as the Host header.
func (e *env) call(t *testing.T, method, host, path string, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+e.handler.IngressAddr()+path,
		strings.NewReader(body))
	require.NoError(t, err)

	req.Host = host

	resp, err := e.http.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func hostOf(fn *functionspb.Function) string {
	return strings.TrimPrefix(fn.GetUrl(), "http://")
}

func decode(t *testing.T, body string) received {
	t.Helper()

	var r received

	require.NoError(t, json.Unmarshal([]byte(body), &r))

	return r
}
