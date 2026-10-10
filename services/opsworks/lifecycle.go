package opsworks

import "time"

const instancePhases = 4

var bootPhases = [instancePhases]string{ //nolint:gochecknoglobals // fixed phase order
	instanceStatusRequested, instanceStatusPending, instanceStatusBooting, instanceStatusRunningSetup,
}

// SetLifecycleDelay sets how long instance start/stop, deployments and commands stay transitional; zero disables.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

// setInstanceStatus records a target status and starts its transition window. Caller holds b.mu.
func (b *InMemoryBackend) setInstanceStatus(i *storedInstance, status string) {
	i.Status = status
	i.changedAt = time.Now()
	i.delay = b.lifecycleDelay
}

func (i *storedInstance) effectiveStatus() string {
	if i.delay <= 0 || i.changedAt.IsZero() {
		return i.Status
	}

	elapsed := time.Since(i.changedAt)
	if elapsed >= i.delay {
		return i.Status
	}

	switch i.Status {
	case instanceStatusOnline:
		step := i.delay / instancePhases
		if step <= 0 {
			return i.Status
		}

		return bootPhases[min(int(elapsed/step), len(bootPhases)-1)]
	case instanceStatusStopped:
		return instanceStatusStopping
	default:
		return i.Status
	}
}

func (d *storedDeployment) running() bool {
	return d.delay > 0 && time.Since(d.CreatedAt) < d.delay
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return formatOpsWorksTime(t)
}
