package ocinosql

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	gigabyte    = 1 << 30
	defaultSpan = time.Hour
)

// UsageWire is one ListTableUsage item.
type UsageWire struct {
	TimeStarted          time.Time `json:"timeStarted"`
	SecondsInPeriod      int       `json:"secondsInPeriod"`
	ReadUnits            int       `json:"readUnits"`
	WriteUnits           int       `json:"writeUnits"`
	StorageInGBs         int       `json:"storageInGBs"`
	ReadThrottleCount    int       `json:"readThrottleCount"`
	WriteThrottleCount   int       `json:"writeThrottleCount"`
	StorageThrottleCount int       `json:"storageThrottleCount"`
}

func parseTime(s, what string) (time.Time, *APIError) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errParam(what + " must be an RFC 3339 timestamp")
	}

	return t.UTC(), nil
}

// ListTableUsage reports per-minute consumption recorded for a table.
func (b *InMemoryBackend) ListTableUsage(
	ref, compartment, start, end string,
	o ListOpts,
) (page.Page[UsageWire], error) {
	b.mu.RLock("ListTableUsage")
	defer b.mu.RUnlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return page.Page[UsageWire]{}, aerr
	}

	to := b.nowUTC()
	from := to.Add(-defaultSpan)

	if start != "" {
		if from, aerr = parseTime(start, "timeStart"); aerr != nil {
			return page.Page[UsageWire]{}, aerr
		}
	}

	if end != "" {
		if to, aerr = parseTime(end, "timeEnd"); aerr != nil {
			return page.Page[UsageWire]{}, aerr
		}
	}

	if to.Before(from) {
		return page.Page[UsageWire]{}, errParam("timeEnd must not be before timeStart")
	}

	size := 0
	for _, r := range t.Rows {
		size += len(r.Value)
	}

	storage := (size + gigabyte - 1) / gigabyte

	minutes := make([]int64, 0, len(t.Usage))
	for m := range t.Usage {
		if ts := time.Unix(m, 0); !ts.Before(from.Truncate(time.Minute)) && !ts.After(to) {
			minutes = append(minutes, m)
		}
	}

	slices.Sort(minutes)

	items := make([]UsageWire, 0, len(minutes))
	for _, m := range minutes {
		u := t.Usage[m]
		items = append(items, UsageWire{
			TimeStarted: time.Unix(m, 0).UTC(), SecondsInPeriod: secondsPerMin, ReadUnits: u.Reads,
			WriteUnits: u.Writes, StorageInGBs: storage,
		})
	}

	return page.New(items, o.Page, o.Limit, defaultListLimit), nil
}

func (h *Handler) listTableUsage(q *req) error {
	pg, err := h.Backend.ListTableUsage(q.param("tableNameOrId"), q.query("compartmentId"), q.query("timeStart"),
		q.query("timeEnd"), q.listOpts())

	return collectionReply(q, pg, err)
}
