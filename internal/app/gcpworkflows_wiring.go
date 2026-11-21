package app

import (
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcpcloudtasksbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	gcpkmsbackend "github.com/blackbirdworks/gopherstack/services/gcpkms"
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
	tasks     *gcpcloudtasksbackend.Handler
	kms       *gcpkmsbackend.Handler
	build     *gcpcloudbuildbackend.Handler
	logging   *gcploggingbackend.Handler
}

func loopbackBase(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" || port == "0" {
		return ""
	}

	return "http://" + net.JoinHostPort("127.0.0.1", port)
}

func (e workflowEndpoints) BaseURL(svc string) string {
	if svc == "storage" {
		if e.storage != nil && e.storage.Port > 0 {
			return "http://127.0.0.1:" + strconv.Itoa(e.storage.Port)
		}

		return ""
	}

	var addr func() string

	switch svc {
	case "pubsub":
		addr = addrOf(e.pubsub)
	case "secretmanager":
		addr = addrOf(e.secrets)
	case "firestore":
		addr = addrOf(e.firestore)
	case "cloudtasks":
		addr = addrOf(e.tasks)
	case "kms":
		addr = addrOf(e.kms)
	case "cloudbuild":
		addr = addrOf(e.build)
	case "logging":
		addr = addrOf(e.logging)
	}

	if addr == nil {
		return ""
	}

	return loopbackBase(addr())
}

// addrOf returns h's Addr method, or nil when the handler is not wired.
func addrOf[T any, P interface {
	*T
	Addr() string
}](h P) func() string {
	if h == nil {
		return nil
	}

	return h.Addr
}

// wireGCPWorkflows connects Workflows connectors to Pub/Sub, Cloud Storage, Firestore, Secret Manager, Cloud Tasks,
// Cloud KMS, Cloud Build and Cloud Logging, and
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
	eps.tasks, _ = byName["GCPCloudTasks"].(*gcpcloudtasksbackend.Handler)
	eps.kms, _ = byName["GCPKMS"].(*gcpkmsbackend.Handler)
	eps.build, _ = byName["GCPCloudBuild"].(*gcpcloudbuildbackend.Handler)
	eps.logging, _ = byName["GCPLogging"].(*gcploggingbackend.Handler)
	wf.Backend.SetEndpoints(eps)

	if logH, isLog := byName["GCPLogging"].(*gcploggingbackend.Handler); isLog {
		wf.Backend.SetLogSink(logH.Backend)
	}
}
