package autoscaling

import (
	"slices"
	"strings"
	"time"
)

const launchingActivityPrefix = "Launching a new EC2 instance"

// SetLaunchDelay makes instances launched through CreateAutoScalingGroup, SetDesiredCapacity,
// UpdateAutoScalingGroup and ExecutePolicy dwell in Pending (with an InProgress activity) for d
// before turning InService. 0, the default, launches them InService immediately.
func (b *InMemoryBackend) SetLaunchDelay(d time.Duration) {
	b.launchDelay.Store(int64(d))
}

// beginLaunchDwellLocked moves the named instances that are plainly InService to Pending and arms
// the timer that promotes them. Must be called with b.mu held.
func (b *InMemoryBackend) beginLaunchDwellLocked(g *AutoScalingGroup, ids []string) {
	d := time.Duration(b.launchDelay.Load())
	if d <= 0 || len(ids) == 0 {
		return
	}

	pending := make(map[string]bool, len(ids))

	for i := range g.Instances {
		inst := &g.Instances[i]
		if inst.LifecycleState == lifecycleStateInService && slicesContains(ids, inst.InstanceID) {
			inst.LifecycleState = lifecycleStatePending
			pending[inst.InstanceID] = true
		}
	}

	if len(pending) == 0 {
		return
	}

	group := g.AutoScalingGroupName

	b.launchTimers = append(b.launchTimers, time.AfterFunc(d, func() {
		b.mu.Lock("launchDwell")
		defer b.mu.Unlock()

		b.finishLaunchesLocked(group, pending)
	}))
}

// finishLaunchesLocked promotes still-Pending instances in pending and completes their activities.
func (b *InMemoryBackend) finishLaunchesLocked(group string, pending map[string]bool) {
	if g, ok := b.groups.Get(group); ok {
		for i := range g.Instances {
			if pending[g.Instances[i].InstanceID] && g.Instances[i].LifecycleState == lifecycleStatePending {
				g.Instances[i].LifecycleState = lifecycleStateInService
			}
		}
	}

	now := time.Now()
	acts := b.activities[group]

	for i := range acts {
		a := &acts[i]
		if a.StatusCode != statusInProgress || !strings.HasPrefix(a.Description, launchingActivityPrefix) {
			continue
		}

		if id, ok := strings.CutPrefix(a.Description, launchingActivityPrefix+": "); ok && !pending[id] {
			continue
		}

		a.StatusCode, a.Progress, a.EndTime = statusCodeSuccessful, completedProgress, now
	}
}

// settleLaunchesLocked completes every in-flight launch; used when state is restored, since timers
// do not survive a snapshot. Must be called with b.mu held.
func (b *InMemoryBackend) settleLaunchesLocked() {
	for _, g := range b.groups.All() {
		all := make(map[string]bool, len(g.Instances))
		for _, inst := range g.Instances {
			all[inst.InstanceID] = true
		}

		b.finishLaunchesLocked(g.AutoScalingGroupName, all)
	}
}

func slicesContains(ids []string, id string) bool {
	return slices.Contains(ids, id)
}
