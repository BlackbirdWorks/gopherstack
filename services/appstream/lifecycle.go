package appstream

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	fleetStateStarting = "STARTING"
	fleetStateStopping = "STOPPING"

	imageBuilderStatePending  = "PENDING"
	imageBuilderStateStopping = "STOPPING"

	minFleetUserDurationSecs = 600
	maxFleetUserDurationSecs = 432000
	minFleetTimeoutSecs      = 60
	maxFleetTimeoutSecs      = 36000
)

type stateTransition struct {
	until   time.Time
	interim string
}

func fleetKey(name string) string { return "fleet/" + name }

func builderKey(name string) string { return "builder/" + name }

// SetStateTransitionDelay holds fleets and image builders in their interim
// state (STARTING/STOPPING/PENDING) for d after a lifecycle call (default 0:
// the final state is visible immediately).
func (b *InMemoryBackend) SetStateTransitionDelay(d time.Duration) {
	b.mu.Lock("SetStateTransitionDelay")
	defer b.mu.Unlock()

	b.transitionDelay = d
}

// beginTransition records an interim state; must hold b.mu.
func (b *InMemoryBackend) beginTransition(key, interim string) {
	if b.transitionDelay <= 0 {
		delete(b.transitions, key)

		return
	}

	if b.transitions == nil {
		b.transitions = make(map[string]stateTransition)
	}

	b.transitions[key] = stateTransition{interim: interim, until: b.now().Add(b.transitionDelay)}
}

// effectiveState returns the interim state while a transition is in flight,
// else final.
func (b *InMemoryBackend) effectiveState(key, final string) string {
	if t, ok := b.transitions[key]; ok && b.now().Before(t.until) {
		return t.interim
	}

	return final
}

func (b *InMemoryBackend) fleetView(f *storedFleet) *Fleet {
	out := f.toFleet()
	out.State = b.effectiveState(fleetKey(f.Name), f.State)

	for _, sess := range b.sessions.All() {
		if sess.FleetName == f.Name {
			out.InUseInstances++
		}
	}

	return out
}

func (b *InMemoryBackend) builderView(ib *storedImageBuilder) *ImageBuilder {
	out := ib.toImageBuilder()
	out.State = b.effectiveState(builderKey(ib.Name), ib.State)

	return out
}

func invalidParam(format string, args ...any) error {
	return fmt.Errorf("%w: %s", awserr.ErrInvalidParameter, fmt.Sprintf(format, args...))
}

// validateFleetSettings checks the documented CreateFleet/UpdateFleet ranges;
// zero means "unset".
func validateFleetSettings(instanceType string, desired, maxUser, disconnect, idle int) error {
	if instanceType != "" && !strings.HasPrefix(instanceType, "stream.") && !strings.HasPrefix(instanceType, "rt.") {
		return invalidParam("InstanceType %q is not a valid instance type", instanceType)
	}

	if desired < 0 {
		return invalidParam("ComputeCapacity.DesiredInstances must be at least 0")
	}

	if maxUser != 0 && (maxUser < minFleetUserDurationSecs || maxUser > maxFleetUserDurationSecs) {
		return invalidParam("MaxUserDurationInSeconds must be between %d and %d",
			minFleetUserDurationSecs, maxFleetUserDurationSecs)
	}

	if disconnect != 0 && (disconnect < minFleetTimeoutSecs || disconnect > maxFleetTimeoutSecs) {
		return invalidParam("DisconnectTimeoutInSeconds must be between %d and %d",
			minFleetTimeoutSecs, maxFleetTimeoutSecs)
	}

	if idle != 0 && (idle < minFleetTimeoutSecs || idle > maxFleetTimeoutSecs) {
		return invalidParam("IdleDisconnectTimeoutInSeconds must be 0 or between %d and %d",
			minFleetTimeoutSecs, maxFleetTimeoutSecs)
	}

	return nil
}
