package app

import (
	"errors"
	"fmt"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpresourcemanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

var errNoEventarcTarget = errors.New("no Cloud Run service or function")

// serviceGetter is the part of the Cloud Run backend that resolves a service to its serving URI.
type serviceGetter interface {
	GetService(name string) (*runpb.Service, error)
}

// functionGetter is the part of the Cloud Functions backend that resolves a function to its serving URL.
type functionGetter interface {
	GetFunction(name string) (*functionspb.Function, error)
}

// eventarcTargets resolves Cloud Run destinations against the Cloud Run emulator, then against the Cloud
// Functions emulator, whose 2nd gen functions are Cloud Run services of the same id.
type eventarcTargets struct {
	run serviceGetter
	fns functionGetter
}

func (t eventarcTargets) ServiceURL(project, region, service string) (string, error) {
	base := "projects/" + project + "/locations/" + region

	if t.run != nil {
		if svc, err := t.run.GetService(base + "/services/" + service); err == nil && svc.GetUri() != "" {
			return svc.GetUri(), nil
		}
	}

	if t.fns != nil {
		if fn, err := t.fns.GetFunction(base + "/functions/" + service); err == nil && fn.GetUrl() != "" {
			return fn.GetUrl(), nil
		}
	}

	return "", fmt.Errorf("%w: %s/%s", errNoEventarcTarget, region, service)
}

// wireGCPEventarc connects Eventarc to Pub/Sub (event transport), Cloud Run and Cloud Functions
// and Workflows (destinations) and Cloud Storage (object events).
func wireGCPEventarc(byName map[string]service.Registerable) {
	ea, ok := byName["GCPEventarc"].(*gcpeventarcbackend.Handler)
	if !ok {
		return
	}

	var svc gcpeventarcbackend.Services

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		svc.PubSub = ps.Backend
	}

	if wf, isWF := byName["GCPWorkflows"].(*gcpworkflowsbackend.Handler); isWF {
		svc.Workflows = wf.Backend
	}

	var targets eventarcTargets

	if run, isRun := byName["GCPRun"].(*gcprunbackend.Handler); isRun {
		targets.run = run.Backend
	}

	if fns, isFn := byName["GCPFunctions"].(*gcpfunctionsbackend.Handler); isFn {
		targets.fns = fns.Backend
	}

	svc.Targets = targets
	ea.Backend.SetServices(svc)

	wireGCPAudit(byName, ea)

	if fs, isFS := byName["GCPFirestore"].(*gcpfirestorebackend.Handler); isFS {
		fs.Backend.ObserveDocuments(func(ev gcpfirestorebackend.DocumentEvent) {
			ea.Backend.HandleDocumentEvent(gcpeventarcbackend.DocumentEvent{
				Kind: string(ev.Kind), Old: ev.Old, New: ev.New, UpdateMask: ev.UpdateMask, Time: ev.Time,
			})
		})
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		gcs.Backend.ObserveObjects(func(ev gcpstoragebackend.ObjectEvent) {
			ea.Backend.HandleStorageEvent(gcpeventarcbackend.StorageEvent{
				Type: string(ev.Type), Bucket: ev.Object.Bucket, Name: ev.Object.Name, Data: ev.Data(), Time: ev.Time,
			})
		})
	}
}

// wireGCPAudit points the audit emitters of the emulated services at the Logging emulator, and feeds the audit
// log entries it stores to Eventarc.
func wireGCPAudit(byName map[string]service.Registerable, ea *gcpeventarcbackend.Handler) {
	lg, ok := byName["GCPLogging"].(*gcploggingbackend.Handler)
	if !ok {
		return
	}

	lg.Backend.ObserveEntries(ea.Backend.HandleLogEntry)

	if h, isH := byName["GCPStorage"].(*gcpstoragebackend.Handler); isH {
		h.Backend.Audit.SetSink(lg.Backend)
	}

	if h, isH := byName["GCPBigQuery"].(*gcpbigquerybackend.Handler); isH {
		h.Backend.Audit.SetSink(lg.Backend)
	}

	if h, isH := byName["GCPFirestore"].(*gcpfirestorebackend.Handler); isH {
		h.Backend.Audit.SetSink(lg.Backend)
	}

	if h, isH := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isH {
		h.Backend.Audit.SetSink(lg.Backend)
	}

	if h, isH := byName["GCPResourceManager"].(*gcpresourcemanagerbackend.Handler); isH {
		h.Backend.Audit.SetSink(lg.Backend)
	}
}
