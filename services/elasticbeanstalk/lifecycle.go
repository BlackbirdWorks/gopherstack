package elasticbeanstalk

import (
	"regexp"
	"time"
)

const (
	minEnvironmentNameLen = 4
	maxEnvironmentNameLen = 40
	minCNAMEPrefixLen     = 4
	maxCNAMEPrefixLen     = 63
	envStatusLaunching    = "Launching"
	envStatusUpdating     = "Updating"
	envStatusTerminating  = "Terminating"
	envStatusTerminated   = "Terminated"
	envHealthGrey         = "Grey"
)

var envLabelPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$`)

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// SetLifecycleDelay sets how long environments report Launching, Updating and
// Terminating. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// SetClock overrides the backend clock for deterministic lifecycle tests; nil restores time.Now.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

func (b *InMemoryBackend) beginTransition(env *Environment, status string) {
	if b.lifecycleDelay <= 0 {
		env.transitionStatus = ""
		env.transitionUntil = time.Time{}

		return
	}

	env.transitionStatus = status
	env.transitionUntil = b.now().Add(b.lifecycleDelay)
}

func (b *InMemoryBackend) transitioning(env *Environment) bool {
	return env.transitionStatus != "" && b.now().Before(env.transitionUntil)
}

// terminated reports whether env finished terminating and is awaiting reaping.
func (b *InMemoryBackend) terminated(env *Environment) bool {
	return env.transitionStatus == envStatusTerminating && !b.transitioning(env)
}

// observe returns a copy of env as a client sees it right now.
func (b *InMemoryBackend) observe(env *Environment) *Environment {
	cp := cloneEnvironment(env)

	switch {
	case b.terminated(env):
		cp.Status = envStatusTerminated
		cp.Health = envHealthGrey
	case b.transitioning(env):
		cp.Status = env.transitionStatus
		if env.transitionStatus != envStatusUpdating {
			cp.Health = envHealthGrey
		}
	}

	return cp
}

// reapLocked moves environments whose termination finished into the deleted list. Caller holds the write lock.
func (b *InMemoryBackend) reapLocked(region string) {
	for _, env := range append([]*Environment(nil), b.environmentsInRegion(region)...) {
		if !b.terminated(env) {
			continue
		}

		out := b.observe(env)
		out.transitionStatus = ""
		b.deletedEnvironments[region] = append(b.deletedEnvironments[region], out)

		if n := len(b.deletedEnvironments[region]); n > maxDeletedEnvironmentsPerRegion {
			b.deletedEnvironments[region] = b.deletedEnvironments[region][n-maxDeletedEnvironmentsPerRegion:]
		}

		b.environmentDeleteKey(region, env.ApplicationName, env.EnvironmentName)
		delete(b.managedActionHistory[region], env.EnvironmentName)
	}
}

func (b *InMemoryBackend) visibleAt() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return b.now().Add(b.lifecycleDelay)
}

func validEnvLabel(s string, minLen, maxLen int) bool {
	return len(s) >= minLen && len(s) <= maxLen && envLabelPattern.MatchString(s)
}
