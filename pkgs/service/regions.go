package service

import (
	"encoding/json"
	"sort"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

//nolint:gochecknoglobals // process-wide set of regions seen across all services; fed by RegionTrackingMiddleware
var (
	regionsMu    = lockmetrics.New("service.regions")
	knownRegions = make(map[string]struct{})
)

// RecordRegion adds region to the set of regions known to hold data. It
// checks under RLock first so the steady-state hot path (the region has
// already been recorded) never contends for the write lock.
func RecordRegion(region string) {
	if region == "" {
		return
	}

	regionsMu.RLock("RecordRegion")
	_, seen := knownRegions[region]
	regionsMu.RUnlock()

	if seen {
		return
	}

	regionsMu.Lock("RecordRegion")
	knownRegions[region] = struct{}{}
	regionsMu.Unlock()
}

// SeedRegions records every non-empty region in regions as known to hold
// data. Intended for backfilling the set from persisted state on restore,
// since resources restored from disk have not (yet) had a live request
// touch their region.
func SeedRegions(regions ...string) {
	for _, r := range regions {
		RecordRegion(r)
	}
}

// KnownRegions returns a sorted, deduplicated snapshot of every region
// recorded so far. It returns an empty (non-nil) slice if nothing has been
// recorded yet.
func KnownRegions() []string {
	regionsMu.RLock("KnownRegions")
	defer regionsMu.RUnlock()

	out := make([]string, 0, len(knownRegions))
	for r := range knownRegions {
		out = append(out, r)
	}

	sort.Strings(out)

	return out
}

// RegionTrackingMiddleware records the region of every request that reaches
// a registered service, so the dashboard can discover which regions hold
// data without fanning out to every AWS region. Register it via Registry.Use
// before any services are registered.
func RegionTrackingMiddleware(defaultRegion string) Middleware {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			RecordRegion(httputils.ExtractRegionFromRequest(c.Request(), defaultRegion))

			return next(c)
		}
	}
}

// SnapshotKnownRegions serializes the known-region set as a JSON array, or
// returns nil if nothing has been recorded. It implements the persistence
// package's Persistable.Snapshot shape so the tracker's own history --
// rather than a guess reconstructed from other services' state -- survives
// a restart.
func SnapshotKnownRegions() []byte {
	regions := KnownRegions()
	if len(regions) == 0 {
		return nil
	}

	data, err := json.Marshal(regions)
	if err != nil {
		return nil
	}

	return data
}

// RestoreKnownRegions seeds the known-region set from JSON produced by
// SnapshotKnownRegions. Malformed data is ignored: this is a best-effort
// backfill, not a correctness-critical load.
func RestoreKnownRegions(data []byte) {
	var regions []string
	if err := json.Unmarshal(data, &regions); err != nil {
		return
	}

	SeedRegions(regions...)
}
