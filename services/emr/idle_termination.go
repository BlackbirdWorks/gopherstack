package emr

import (
	"time"
)

const stateChangeMessageIdleTimeout = "Terminated after the auto-termination idle timeout elapsed"

// clusterIdleSince returns when a cluster went idle. A cluster is idle while
// WAITING with no pending step and no live session; the idle clock starts at
// the later of its ready time, its last step end and its last session end.
// AWS publishes no finer definition of "idle", so YARN activity is not modeled.
func clusterIdleSince(c *Cluster) (time.Time, bool) {
	if c.Status.State != StateWaiting {
		return time.Time{}, false
	}

	since := timelineSeconds(c.Status.Timeline, timelineKeyReady)

	for _, s := range c.steps {
		st := effectiveStepStatus(s.Status)
		if st.State == StepStatePending {
			return time.Time{}, false
		}

		since = max(since, st.Timeline.EndDateTime)
	}

	for _, s := range c.sessions {
		if s.State != SessionStateTerminated && s.State != SessionStateFailed {
			return time.Time{}, false
		}

		since = max(since, s.EndedAt)
	}

	return time.Unix(0, int64(since*float64(time.Second))), true
}

// idleTimeoutElapsed reports whether the cluster's AutoTerminationPolicy
// idle timeout has passed as of now.
func idleTimeoutElapsed(c *Cluster, now time.Time) bool {
	if c.autoTerminationPolicy == nil {
		return false
	}

	since, idle := clusterIdleSince(c)
	if !idle {
		return false
	}

	timeout := time.Duration(c.autoTerminationPolicy.IdleTimeout) * time.Second

	return !now.Before(since.Add(timeout))
}
