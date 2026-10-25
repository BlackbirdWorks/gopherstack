package app

import (
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

// workflowEndpoints resolves the REST base URLs of the emulators googleapis.* connectors call, at call time,
// because the listeners bind after wiring.
type workflowEndpoints struct {
	pubsub    *gcppubsubbackend.Handler
	secrets   *gcpsecretmanagerbackend.Handler
	firestore *gcpfirestorebackend.Handler
	storage   *gcpstoragebackend.Handler
}

func loopbackBase(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" || port == "0" {
		return ""
	}

	return "http://" + net.JoinHostPort("127.0.0.1", port)
}

func (e workflowEndpoints) BaseURL(svc string) string {
	switch {
	case svc == "pubsub" && e.pubsub != nil:
		return loopbackBase(e.pubsub.Addr())
	case svc == "secretmanager" && e.secrets != nil:
		return loopbackBase(e.secrets.Addr())
	case svc == "firestore" && e.firestore != nil:
		return loopbackBase(e.firestore.Addr())
	case svc == "storage" && e.storage != nil && e.storage.Port > 0:
		return "http://127.0.0.1:" + strconv.Itoa(e.storage.Port)
	default:
		return ""
	}
}

// wireGCPWorkflows connects Workflows connectors to Pub/Sub, Cloud Storage, Firestore and Secret Manager, and
// sys.log output to Cloud Logging.
func wireGCPWorkflows(byName map[string]service.Registerable) {
	wf, ok := byName["GCPWorkflows"].(*gcpworkflowsbackend.Handler)
	if !ok {
		return
	}

	var eps workflowEndpoints

	eps.pubsub, _ = byName["GCPPubSub"].(*gcppubsubbackend.Handler)
	eps.secrets, _ = byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler)
	eps.firestore, _ = byName["GCPFirestore"].(*gcpfirestorebackend.Handler)
	eps.storage, _ = byName["GCPStorage"].(*gcpstoragebackend.Handler)
	wf.Backend.SetEndpoints(eps)

	if logH, isLog := byName["GCPLogging"].(*gcploggingbackend.Handler); isLog {
		wf.Backend.SetLogSink(logH.Backend)
	}
}
