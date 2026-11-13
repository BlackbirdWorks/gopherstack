package ocimonitoring

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const ruleBase = "BASE"

// DimState is the last transition of one metric stream of an alarm.
type DimState struct {
	Since  time.Time         `json:"since"`
	Dims   map[string]string `json:"dimensions"`
	Status string            `json:"status"`
}

func dimKey(d map[string]string) string {
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	var sb strings.Builder

	for _, k := range keys {
		sb.WriteString(k + "=" + d[k] + "\x00")
	}

	return sb.String()
}

func firingDims(a *Alarm) []map[string]string {
	var out []map[string]string

	for _, s := range a.DimStates {
		if s.Status == statusFiring {
			out = append(out, s.Dims)
		}
	}

	return out
}

// updateDimStates records the per-stream status after an evaluation; the alarm status decides FIRING.
func (b *InMemoryBackend) updateDimStates(a *Alarm, q *MQL, hits []firing, now time.Time) {
	old := make(map[string]DimState, len(a.DimStates))
	for _, s := range a.DimStates {
		old[dimKey(s.Dims)] = s
	}

	hit := map[string]bool{}

	if a.Status == statusFiring {
		for _, h := range hits {
			hit[dimKey(h.dims)] = true
		}
	}

	seen := map[string]bool{}
	next := make([]DimState, 0, len(a.DimStates))

	add := func(dims map[string]string) {
		k := dimKey(dims)
		if seen[k] {
			return
		}

		seen[k] = true

		status := statusOK
		if hit[k] {
			status = statusFiring
		}

		st := DimState{Dims: dims, Status: status, Since: now}
		if prev, ok := old[k]; ok && prev.Status == status {
			st.Since = prev.Since
		}

		next = append(next, st)
	}

	for _, g := range b.collectGroups(q, a.Namespace, a.MetricCompartmentID, a.ResourceGroup,
		a.InSubtree != nil && *a.InSubtree) {
		add(g.dims)
	}

	for _, h := range hits {
		if a.Status == statusFiring {
			add(h.dims)
		}
	}

	a.DimStates = next
}

// DimensionStatesIn is the RetrieveDimensionStates request body.
type DimensionStatesIn struct {
	DimensionFilters map[string]string `json:"dimensionFilters"`
	Status           string            `json:"status"`
}

type dimStateEntryWire struct {
	Timestamp    time.Time         `json:"timestamp"`
	AlarmSummary string            `json:"alarmSummary"`
	Dimensions   map[string]string `json:"dimensions"`
	Status       string            `json:"status"`
	RuleName     string            `json:"ruleName"`
}

// DimensionStatesWire is the AlarmDimensionStatesCollection body.
type DimensionStatesWire struct {
	AlarmID      string              `json:"alarmId"`
	Items        []dimStateEntryWire `json:"items"`
	Enabled      bool                `json:"isEnabled"`
	PerDimension bool                `json:"isNotificationsPerMetricDimensionEnabled"`
}

// RetrieveDimensionStates lists the current status of each metric stream of an alarm.
func (b *InMemoryBackend) RetrieveDimensionStates(
	alarmID string,
	in DimensionStatesIn,
	o ListOpts,
) (DimensionStatesWire, string, error) {
	status := strings.ToUpper(in.Status)
	if status != "" && status != statusOK && status != statusFiring {
		return DimensionStatesWire{}, "", errParam("status must be FIRING or OK")
	}

	b.mu.RLock("RetrieveDimensionStates")
	defer b.mu.RUnlock()

	a, aerr := b.liveAlarm(alarmID)
	if aerr != nil {
		return DimensionStatesWire{}, "", aerr
	}

	summary := a.Name
	if v := extra(a, "alarmSummary"); v != nil {
		summary = *v
	}

	var all []dimStateEntryWire

	for _, s := range a.DimStates {
		if (status != "" && s.Status != status) || (len(in.DimensionFilters) > 0 &&
			dimKey(in.DimensionFilters) != dimKey(s.Dims)) {
			continue
		}

		all = append(all, dimStateEntryWire{
			Timestamp: s.Since, AlarmSummary: summary, Dimensions: s.Dims, Status: s.Status, RuleName: ruleBase,
		})
	}

	slices.SortStableFunc(
		all,
		func(x, y dimStateEntryWire) int { return cmp.Compare(dimKey(x.Dimensions), dimKey(y.Dimensions)) },
	)

	pg := page.New(all, o.Page, o.Limit, defaultListLimit)
	if pg.Data == nil {
		pg.Data = []dimStateEntryWire{}
	}

	return DimensionStatesWire{
		AlarmID: a.ID, Enabled: a.Enabled, PerDimension: a.PerDimension != nil && *a.PerDimension, Items: pg.Data,
	}, pg.Next, nil
}
