package app

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type seenEvent struct {
	typ, subject, source string
	contentType          string
	body                 []byte
}

// eventRuntime serves each container's published port and reports every CloudEvent it receives.
type eventRuntime struct {
	events chan seenEvent
	specs  chan container.Spec
	boxes  map[string]*http.Server
	mu     sync.Mutex
	seq    int
}

func (r *eventRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	_, host, _, err := container.ParsePortSpec(spec.Ports[0])
	if err != nil {
		return "", err
	}

	l, err := net.Listen("tcp", net.JoinHostPort(container.LoopbackHost, host))
	if err != nil {
		return "", err
	}

	srv := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		if req.UserAgent() == gcpserve.ProbeUserAgent {
			return
		}

		body, _ := io.ReadAll(req.Body)
		r.events <- seenEvent{
			typ: req.Header.Get("Ce-Type"), subject: req.Header.Get("Ce-Subject"),
			source: req.Header.Get("Ce-Source"), contentType: req.Header.Get("Content-Type"), body: body,
		}
	})}

	go func() { _ = srv.Serve(l) }()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.seq++
	id := fmt.Sprintf("ev-%d", r.seq)

	if r.boxes == nil {
		r.boxes = map[string]*http.Server{}
	}

	r.boxes[id] = srv
	r.specs <- spec

	return id, nil
}

func (r *eventRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	srv := r.boxes[id]
	delete(r.boxes, id)
	r.mu.Unlock()

	if srv != nil {
		_ = srv.Close()
	}

	return nil
}

func nextSeen(t *testing.T, ch <-chan seenEvent) seenEvent {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(15 * time.Second):
		require.FailNow(t, "no event arrived")

		return seenEvent{}
	}
}

func TestWireGCPFunctions_StorageFirestoreAndSecrets(t *testing.T) {
	t.Parallel()

	const (
		loc    = "projects/p/locations/us-central1"
		dbPath = "projects/p/databases/(default)"
	)

	gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())
	fs := gcpfirestorebackend.NewHandler(gcpfirestorebackend.NewInMemoryBackend())
	sm := gcpsecretmanagerbackend.NewHandler(gcpsecretmanagerbackend.NewInMemoryBackend())
	fns := gcpfunctionsbackend.NewHandler(gcpfunctionsbackend.NewInMemoryBackend())
	rt := &eventRuntime{events: make(chan seenEvent, 16), specs: make(chan container.Spec, 16)}
	fns.Backend.SetRuntime(func() (gcpfunctionsbackend.Runner, error) { return rt, nil })
	t.Cleanup(fns.Backend.Close)

	wireGCPFunctions(map[string]service.Registerable{
		"GCPFunctions": fns, "GCPStorage": gcs, "GCPFirestore": fs, "GCPSecretManager": sm,
	})

	_, err := gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": json.RawMessage(`"src"`)})
	require.NoError(t, err)
	_, err = gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": json.RawMessage(`"inbox"`)})
	require.NoError(t, err)

	_, err = sm.Backend.CreateSecret(&secretmanagerpb.CreateSecretRequest{
		Parent: "projects/p", SecretId: "token", Secret: automaticSecret(),
	})
	require.NoError(t, err)
	_, err = sm.Backend.AddSecretVersion(&secretmanagerpb.AddSecretVersionRequest{
		Parent: "projects/p/secrets/token", Payload: &secretmanagerpb.SecretPayload{Data: []byte("hunter2")},
	})
	require.NoError(t, err)

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.js")
	require.NoError(t, err)
	_, err = w.Write([]byte("exports.h = () => {};"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	_, err = gcs.Backend.PutObject("src", "fn.zip", buf.Bytes(), gcpstoragebackend.Patch{},
		gcpstoragebackend.Preconds{})
	require.NoError(t, err)

	create := func(id string, et *functionspb.EventTrigger, secrets bool) {
		t.Helper()

		fn := &functionspb.Function{
			BuildConfig: &functionspb.BuildConfig{
				Runtime: "nodejs22", EntryPoint: "h",
				Source: &functionspb.Source{Source: &functionspb.Source_StorageSource{
					StorageSource: &functionspb.StorageSource{Bucket: "src", Object: "fn.zip"},
				}},
			},
			ServiceConfig: &functionspb.ServiceConfig{},
			EventTrigger:  et,
		}
		if secrets {
			fn.ServiceConfig.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "TOKEN", Secret: "token"}}
		}

		_, cerr := fns.Backend.CreateFunction(
			&functionspb.CreateFunctionRequest{Parent: loc, FunctionId: id, Function: fn},
		)
		require.NoError(t, cerr)
		fns.Backend.Drain()
	}

	create("onobject", &functionspb.EventTrigger{
		EventType:    "google.cloud.storage.object.v1.finalized",
		EventFilters: []*functionspb.EventFilter{{Attribute: "bucket", Value: "inbox"}},
	}, true)

	spec := <-rt.specs
	assert.Contains(t, spec.Env, "TOKEN=hunter2")

	_, err = gcs.Backend.PutObject(
		"inbox",
		"a.txt",
		[]byte("hello"),
		gcpstoragebackend.Patch{},
		gcpstoragebackend.Preconds{},
	)
	require.NoError(t, err)

	ev := nextSeen(t, rt.events)
	assert.Equal(t, "google.cloud.storage.object.v1.finalized", ev.typ)
	assert.Equal(t, "objects/a.txt", ev.subject)

	var obj map[string]any

	require.NoError(t, json.Unmarshal(ev.body, &obj))
	assert.Equal(t, "inbox", obj["bucket"])
	assert.Equal(t, "a.txt", obj["name"])

	_, err = fns.Backend.CreateFunction(&functionspb.CreateFunctionRequest{
		Parent: loc, FunctionId: "nobucket", Function: &functionspb.Function{
			BuildConfig: &functionspb.BuildConfig{
				Runtime: "nodejs22", EntryPoint: "h",
				Source: &functionspb.Source{Source: &functionspb.Source_StorageSource{
					StorageSource: &functionspb.StorageSource{Bucket: "src", Object: "fn.zip"},
				}},
			},
			EventTrigger: &functionspb.EventTrigger{
				EventType:    "google.cloud.storage.object.v1.finalized",
				EventFilters: []*functionspb.EventFilter{{Attribute: "bucket", Value: "ghost"}},
			},
		},
	})
	require.Error(t, err, "a trigger on a bucket that does not exist is rejected")

	create("ondoc", &functionspb.EventTrigger{
		EventType: "google.cloud.firestore.document.v1.created",
		EventFilters: []*functionspb.EventFilter{
			{Attribute: "database", Value: "(default)"},
			{Attribute: "document", Value: "users/{id}", Operator: "match-path-pattern"},
		},
	}, false)
	<-rt.specs

	_, err = fs.Backend.CreateDocument(&firestorepb.CreateDocumentRequest{
		Parent: dbPath + "/documents", CollectionId: "users", DocumentId: "alice",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"name": {ValueType: &firestorepb.Value_StringValue{StringValue: "Alice"}},
		}},
	})
	require.NoError(t, err)

	ev = nextSeen(t, rt.events)
	assert.Equal(t, "google.cloud.firestore.document.v1.created", ev.typ)
	assert.Equal(t, "documents/users/alice", ev.subject)
	assert.Equal(t, "application/protobuf", ev.contentType)
	assert.Contains(t, string(ev.body), dbPath+"/documents/users/alice")
}
