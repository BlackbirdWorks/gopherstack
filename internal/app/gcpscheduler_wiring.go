package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpschedulerbackend "github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

// wireGCPScheduler lets Cloud Scheduler PubsubTarget jobs publish into the in-process Pub/Sub emulator.
func wireGCPScheduler(byName map[string]service.Registerable) {
	sched, ok := byName["GCPScheduler"].(*gcpschedulerbackend.Handler)
	if !ok {
		return
	}

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		sched.Backend.SetPublisher(ps.Backend)
	}
}
