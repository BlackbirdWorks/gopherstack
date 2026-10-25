package app

import (
	"errors"
	"fmt"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
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
// (destinations) and Cloud Storage (object events).
func wireGCPEventarc(byName map[string]service.Registerable) {
	ea, ok := byName["GCPEventarc"].(*gcpeventarcbackend.Handler)
	if !ok {
		return
	}

	var svc gcpeventarcbackend.Services

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		svc.PubSub = ps.Backend
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

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		gcs.Backend.ObserveObjects(func(ev gcpstoragebackend.ObjectEvent) {
			ea.Backend.HandleStorageEvent(gcpeventarcbackend.StorageEvent{
				Type: string(ev.Type), Bucket: ev.Object.Bucket, Name: ev.Object.Name, Data: ev.Data(), Time: ev.Time,
			})
		})
	}
}
