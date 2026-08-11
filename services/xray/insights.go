package xray

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// maybeResetInsightWindow resets the window when it has expired, closing any
// active insight whose rate has normalised. Must be called with mu held.
func (b *InMemoryBackend) maybeResetInsightWindow(w *serviceInsightWindow, now time.Time) {
	if now.Sub(w.WindowStart) <= insightWindowDuration {
		return
	}

	if w.InsightID != "" && w.Total > 0 {
		rate := float64(w.FaultCount) / float64(w.Total)
		if rate < insightFaultThreshold {
			if ins, exists := b.insights.Get(w.InsightID); exists {
				ins.State = "CLOSED"
				ins.EndTime = now
				ins.LastUpdateTime = now
			}

			w.InsightID = ""
		}
	}

	w.Total = 0
	w.FaultCount = 0
	w.WindowStart = now
}

// maybeOpenInsight creates a new ACTIVE insight when the window has enough
// data and the fault rate exceeds the threshold. Must be called with mu held.
func (b *InMemoryBackend) maybeOpenInsight(w *serviceInsightWindow, svcName string, now time.Time) {
	if w.Total < insightMinRequests || w.InsightID != "" {
		return
	}

	rate := float64(w.FaultCount) / float64(w.Total)
	if rate < insightFaultThreshold {
		return
	}

	insightID := uuid.NewString()
	b.insights.Put(&Insight{
		InsightID:      insightID,
		GroupARN:       b.groupARN("default"),
		GroupName:      "default",
		State:          statusActive,
		StartTime:      now,
		LastUpdateTime: now,
		// gopherstack only detects fault-rate anomalies (see the threshold check
		// above); InsightCategory has no other value in the SDK either
		// (types/enums.go: InsightCategoryFault is the sole InsightCategory constant).
		Categories: []string{"FAULT"},
		ClientRequestImpactStatistics: &RequestImpactStatistics{
			OkCount:    w.Total - w.FaultCount,
			FaultCount: w.FaultCount,
			TotalCount: w.Total,
		},
		Summary: fmt.Sprintf(
			"Elevated fault rate detected for service %q (%.0f%%)",
			svcName, rate*pctMultiplier,
		),
	})
	b.insightEvents[insightID] = []*InsightEvent{{
		InsightID: insightID,
		EventTime: now,
		Summary: fmt.Sprintf(
			"Fault rate %.0f%% exceeded threshold for %q",
			rate*pctMultiplier, svcName,
		),
	}}
	w.InsightID = insightID
}

// detectInsights checks per-service fault rates and creates/closes insights as needed.
// Must be called while the backend mutex is held.
func (b *InMemoryBackend) detectInsights(newSegs []*Segment) {
	now := time.Now()

	byService := map[string][]*Segment{}
	for _, seg := range newSegs {
		byService[seg.Name] = append(byService[seg.Name], seg)
	}

	for svcName, segs := range byService {
		w, ok := b.serviceWindows.Get(svcName)
		if !ok {
			w = &serviceInsightWindow{Name: svcName, WindowStart: now}
			b.serviceWindows.Put(w)
		}

		b.maybeResetInsightWindow(w, now)

		for _, seg := range segs {
			w.Total++
			if seg.Fault || seg.Error {
				w.FaultCount++
			}
		}

		b.maybeOpenInsight(w, svcName, now)
	}
}

func cloneInsight(i *Insight) *Insight {
	cp := *i

	return &cp
}

// AddInsightInternal seeds an insight directly for testing.
func (b *InMemoryBackend) AddInsightInternal(insight Insight) {
	b.mu.Lock("AddInsightInternal")
	defer b.mu.Unlock()

	b.insights.Put(&insight)
}

// AddInsightEventInternal seeds an event for an insight directly for testing.
func (b *InMemoryBackend) AddInsightEventInternal(event InsightEvent) {
	b.mu.Lock("AddInsightEventInternal")
	defer b.mu.Unlock()

	b.insightEvents[event.InsightID] = append(b.insightEvents[event.InsightID], &event)
}

// GetInsight returns the insight with the given ID.
func (b *InMemoryBackend) GetInsight(insightID string) (*Insight, error) {
	b.mu.RLock("GetInsight")
	defer b.mu.RUnlock()

	i, ok := b.insights.Get(insightID)
	if !ok {
		return nil, fmt.Errorf("%w: insight %s not found", ErrInsightNotFound, insightID)
	}

	return cloneInsight(i), nil
}

// GetInsightEvents returns all events for the given insight ID.
func (b *InMemoryBackend) GetInsightEvents(insightID string) ([]*InsightEvent, error) {
	b.mu.RLock("GetInsightEvents")
	defer b.mu.RUnlock()

	if !b.insights.Has(insightID) {
		return nil, fmt.Errorf("%w: insight %s not found", ErrInsightNotFound, insightID)
	}

	events := b.insightEvents[insightID]
	out := make([]*InsightEvent, len(events))

	for idx, e := range events {
		cp := *e
		out[idx] = &cp
	}

	return out, nil
}

// isValidInsightState returns true if s is a recognised insight state name.
func isValidInsightState(s string) bool {
	return s == statusActive || s == "CLOSED"
}

// GetInsightSummaries returns all insights as summaries, optionally filtered by state.
// If states is empty, all insights are returned. "ALL" matches both ACTIVE and CLOSED.
// Unknown states return ErrValidation.
func (b *InMemoryBackend) GetInsightSummaries(states []string) ([]Insight, error) {
	b.mu.RLock("GetInsightSummaries")
	defer b.mu.RUnlock()

	// Validate states and resolve ALL.
	wantAll := len(states) == 0
	stateSet := make(map[string]bool, len(states))

	for _, s := range states {
		if s == "ALL" {
			wantAll = true

			continue
		}

		if !isValidInsightState(s) {
			return nil, fmt.Errorf("%w: unknown insight state %q", ErrValidation, s)
		}

		stateSet[s] = true
	}

	all := b.insights.All()
	out := make([]Insight, 0, len(all))

	for _, i := range all {
		if !wantAll && !stateSet[i.State] {
			continue
		}

		out = append(out, *cloneInsight(i))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].InsightID < out[j].InsightID
	})

	return out, nil
}

const (
	// insightFaultThreshold is the fault rate that triggers an insight (5%).
	insightFaultThreshold = 0.05
)

const (
	// insightMinRequests is the minimum number of requests before an insight fires.
	insightMinRequests = int64(10)
)

const (
	// insightWindowDuration is the rolling window for fault rate tracking.
	insightWindowDuration = 60 * time.Second
)

const (
	// pctMultiplier converts a 0-1 fraction to a percentage for display.
	pctMultiplier = 100.0
)
