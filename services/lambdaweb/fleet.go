package lambdaweb

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	nsWeb      = "AWS/Lambda/Web"
	nsWebUsage = "AWS/Lambda/Web/Usage"

	unitCount = "Count"
	unitMs    = "Milliseconds"
	unitBytes = "Bytes"
)

// startJanitorLocked starts the background sweep on first use; callers hold r.mu.
func (r *runtimeState) startJanitorLocked() {
	if r.janitor || r.tick == nil {
		return
	}

	r.janitor = true
	tick := r.tick

	r.wg.Go(func() {
		t := time.NewTicker(sweepInterval)
		defer t.Stop()

		for {
			select {
			case <-r.stopped:
				return
			case <-t.C:
				tick()
			}
		}
	})
}

// SweepEnvironments drains environments of revisions no endpoint routes to, scales idle environments down
// (to zero when none is busy) and emits the periodic environment reports.
func (b *InMemoryBackend) SweepEnvironments() {
	routed := b.routedRevisionKeys()
	b.run.sweep(routed)
}

func (b *InMemoryBackend) routedRevisionKeys() map[string]bool {
	b.mu.RLock("routedRevisionKeys")
	defer b.mu.RUnlock()

	out := map[string]bool{}

	for _, e := range b.endpoints.All() {
		if e.State == stateDeleting {
			continue
		}

		for _, w := range b.effectiveWeightsLocked(e) {
			out[envKey(e.Region, e.FunctionName, w.RevisionID)] = true
		}
	}

	return out
}

type drainJob struct {
	in    *instance
	limit time.Duration
}

func (r *runtimeState) sweep(routed map[string]bool) {
	now := r.now()

	var (
		drains  []drainJob
		reports []*instance
	)

	r.mu.Lock("sweep")

	for key, list := range r.insts {
		keep := make([]*instance, 0, len(list))

		for _, in := range list {
			switch {
			case in.env == nil:
				keep = append(keep, in)
			case !routed[key] || (in.inflight == 0 && now.Sub(in.lastUsed) >= r.idle):
				in.draining = true
				if in.inflight == 0 {
					close(in.drained)
				}

				drains = append(drains, drainJob{in, time.Duration(in.rev.ServiceConfig.TimeoutSeconds) * time.Second})
			default:
				keep = append(keep, in)

				if now.Sub(in.lastReport) >= reportInterval {
					reports = append(reports, in)
				}
			}
		}

		if len(keep) == 0 {
			delete(r.insts, key)
		} else {
			r.insts[key] = keep
		}
	}

	r.mu.Unlock()

	for _, d := range drains {
		r.drain(d.in, d.limit)
	}

	for _, in := range reports {
		r.report(in, now)
	}
}

type windowStats struct {
	start time.Time
	reqs  int64
	bytes int64
	peak  int32
}

func (r *runtimeState) takeWindow(in *instance, now time.Time) windowStats {
	r.mu.Lock("takeWindow")
	defer r.mu.Unlock()

	st := windowStats{start: in.lastReport, reqs: in.windowReqs, bytes: in.winBytes, peak: in.peak}
	in.lastReport, in.windowReqs, in.winBytes, in.peak = now, 0, 0, in.inflight

	return st
}

// report emits the environment's invokeSummary event and execution environment metrics for the closing window.
func (r *runtimeState) report(in *instance, now time.Time) {
	st := r.takeWindow(in, now)

	in.ship.system("platform.invokeSummary", "INFO", map[string]any{
		"startTime": st.start.UTC().Format(time.RFC3339Nano), "instanceId": in.id,
		"metrics": map[string]any{"requests": st.reqs, "concurrentRequests": st.peak},
	})
	in.emitEnvironmentMetrics(st, now)
}

func (in *instance) emitEnvironmentMetrics(st windowStats, now time.Time) {
	dims := []cwmetric.Dimension{{Name: "FunctionName", Value: in.rev.FunctionName}}

	emitMetric(in.metrics, in.rev.Region, nsWeb, "EnvironmentActiveTime", unitMs,
		float64(now.Sub(st.start).Milliseconds()), dims)
	emitMetric(in.metrics, in.rev.Region, nsWeb, "EnvironmentConcurrentRequests", unitCount, float64(st.peak), dims)
	emitMetric(in.metrics, in.rev.Region, nsWeb, "ResponseBytes", unitBytes, float64(st.bytes), dims)
}

func emitMetric(em cwmetric.Emitter, region, ns, name, unit string, value float64, dims []cwmetric.Dimension) {
	if em == nil {
		return
	}

	_ = em.EmitMetric(cwmetric.Point{
		Region: region, Namespace: ns, Name: name, Unit: unit, Value: value, Dimensions: dims,
	})
}
