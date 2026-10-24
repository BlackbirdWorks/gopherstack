package amp

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	pruneInterval = time.Minute
)

// retentionCutoffMs is the oldest sample timestamp (ms) a workspace still keeps; 0 keeps everything.
func (b *InMemoryBackend) retentionCutoffMs(w *Workspace) int64 {
	if w == nil || w.Config.RetentionPeriodInDays <= 0 {
		return 0
	}

	keep := time.Duration(w.Config.RetentionPeriodInDays) * hoursPerDay * time.Hour

	return b.now().Add(-keep).UnixMilli()
}

// pruneExpiredLocked drops samples past each workspace's retention, and series left empty.
func (b *InMemoryBackend) pruneExpiredLocked(now time.Time) {
	if now.Sub(b.lastPrune) < pruneInterval {
		return
	}

	b.lastPrune = now

	b.workspaces.Range(func(w *Workspace) bool {
		cutoff := b.retentionCutoffMs(w)

		for _, rec := range slices.Clone(b.seriesByWS.Get(w.ID)) {
			rec.Samples = slices.DeleteFunc(rec.Samples, func(s promql.Sample) bool { return s.T < cutoff })

			if len(rec.Samples) == 0 {
				b.series.Delete(rec.Key)
			}
		}

		return true
	})
}

// overLabelSetLimit reports whether adding a series with labels would exceed a limitsPerLabelSet entry.
func (b *InMemoryBackend) overLabelSetLimit(w *Workspace, labels promql.Labels) bool {
	if w == nil {
		return false
	}

	for _, lim := range w.Config.LimitsPerLabelSet {
		if lim.MaxSeries == nil || !labelSetMatches(lim.LabelSet, labels) {
			continue
		}

		var n int64

		for _, rec := range b.seriesByWS.Get(w.ID) {
			if len(rec.Samples) > 0 && labelSetMatches(lim.LabelSet, rec.Labels) {
				n++
			}
		}

		if n >= *lim.MaxSeries {
			return true
		}
	}

	return false
}

func labelSetMatches(set map[string]string, labels promql.Labels) bool {
	for k, v := range set {
		if labels[k] != v {
			return false
		}
	}

	return true
}
