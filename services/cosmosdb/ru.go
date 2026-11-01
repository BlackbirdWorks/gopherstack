package cosmosdb

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// Request-unit model, from the Azure documentation:
//
//   - request-units: a point read of a 1 KB item is 1 RU; strong and bounded staleness reads cost twice as much.
//   - optimize-cost-reads-writes: a point read is 1 RU at 1 KB and 10 RU at 100 KB; inserting a 1 KB item without
//     indexing is about 5.5 RU; a replace costs twice an insert; a query that returns a thousand 1 KB items costs
//     1000 RU.
//   - the same page's query-metrics sample: one retrieved document of 9,963 bytes cost 3.19 RU, which fixes the
//     query overhead at 3.19 less the 1.79 RU load of that document, 1.4 RU.
//   - partitioning: a query without the partition key pays 2-3 RU for every physical partition (2.5 here).
//
// Cost grows linearly with item size (1 -> 10 RU over 1 -> 100 KB), for writes too. The documents give no per-path
// index cost: the 0.3 RU per indexed property path is a calibration that makes a write cost more the more of the
// item the indexing policy covers; indexing mode none pays only the documented 5.5 RU. Index lookups, query
// predicates and UDFs are listed as cost factors but with no numbers, so they add nothing.
const (
	ruAt100KB  = 10.0
	ruKBAtCost = 100.0
)

const (
	ruReadBase         = 1.0
	ruReadPerKB        = (ruAt100KB - ruReadBase) / (ruKBAtCost - 1.0)
	ruWriteBase        = 5.5
	ruIndexedPath      = 0.3
	ruReplaceFactor    = 2.0
	ruDelete           = ruWriteBase
	ruQueryBase        = 1.4
	ruCrossPartition   = 2.5
	ruStrongReadFactor = 2.0
	ruMetadata         = 1.0
	bytesPerKB         = 1024.0
)

func kbBeyondFirst(size int) float64 {
	return max(float64(size)/bytesPerKB-1, 0)
}

// sizeFactor scales a one-KB cost to an item of size bytes.
func sizeFactor(size int) float64 { return 1 + ruReadPerKB*kbBeyondFirst(size) }

func readCharge(size int) float64 { return ruReadBase * sizeFactor(size) }

// createCharge is the insert cost of an item of size bytes whose indexing policy covers indexed property paths.
func createCharge(size, indexed int) float64 {
	return ruWriteBase*sizeFactor(size) + ruIndexedPath*float64(indexed)
}

func replaceCharge(size, indexed int) float64 {
	return ruReplaceFactor * createCharge(size, indexed)
}

// queryCharge is the cost of one query page: the overhead, the load of the documents it retrieved (loaded, already
// summed from readCharge) and, without a partition key on a multi-partition container, the per-partition check.
func queryCharge(loaded float64, partitions int) float64 {
	ru := ruQueryBase + loaded
	if partitions > 1 {
		ru += ruCrossPartition * float64(partitions)
	}

	return ru
}

// formatCharge renders RUs with at most two decimals, as the service does.
func formatCharge(ru float64) string {
	s := strconv.FormatFloat(ru, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")

	if s == "" || s == "0" {
		return "0.01"
	}

	return s
}

func setCharge(c *echo.Context, ru float64) {
	c.Response().Header().Set(headerRequestCharge, formatCharge(ru))
}

// writeJSONCharged marshals v, sets x-ms-request-charge from the payload size, and writes the response.
func (h *Handler) writeJSONCharged(c *echo.Context, status int, v any, charge func(size int) float64) error {
	body, err := jsonMarshal(v)
	if err != nil {
		return h.writeErrorNoRecurse(c, http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	setCharge(c, charge(len(body)))

	return c.Blob(status, "application/json", body)
}

// readFactor is 2 when the request reads at strong or bounded staleness consistency, else 1.
func (h *Handler) readFactor(c *echo.Context) float64 {
	pol := h.accountRegions().consistency()
	level, err := checkRequestConsistency(canonicalConsistency(c.Request().Header.Get(headerConsistency)), pol)

	if err == nil && (level == ConsistencyStrong || level == ConsistencyBoundedStaleness) {
		return ruStrongReadFactor
	}

	return 1
}

// readCharger prices a read by payload size at the request's consistency level.
func (h *Handler) readCharger(c *echo.Context) func(int) float64 {
	f := h.readFactor(c)

	return func(size int) float64 { return f * readCharge(size) }
}

// feedCharger prices a read feed as one read per returned item.
func (h *Handler) feedCharger(c *echo.Context, values []map[string]any) func(int) float64 {
	f := h.readFactor(c)

	return func(int) float64 {
		var total float64

		for _, v := range values {
			raw, _ := jsonMarshal(v)
			total += readCharge(len(raw))
		}

		return f * max(total, ruReadBase)
	}
}

// indexedPaths counts the property paths of body the container's indexing policy indexes.
func (h *Handler) indexedPaths(dbID, collID string, body map[string]any) int {
	info, err := h.Backend.GetContainer(dbID, collID)
	if err != nil {
		return 0
	}

	return countIndexedPaths(parseIndexingPolicy(info.IndexingPolicy), body)
}

// countIndexedPaths counts the distinct leaf property paths of body (array positions are "[]") that pol indexes;
// indexing mode none indexes no property.
func countIndexedPaths(pol indexingPolicy, body map[string]any) int {
	if strings.EqualFold(pol.IndexingMode, indexModeNone) {
		return 0
	}

	seen := map[string]struct{}{}
	collectLeafPaths("", body, seen, 0)

	n := 0

	for p := range seen {
		if pol.indexed(p) {
			n++
		}
	}

	return n
}

const maxLeafDepth = 32

func collectLeafPaths(prefix string, v any, out map[string]struct{}, depth int) {
	if depth > maxLeafDepth {
		return
	}

	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if depth == 0 && strings.HasPrefix(k, "_") {
				continue
			}

			collectLeafPaths(prefix+"/"+k, child, out, depth+1)
		}
	case []any:
		for _, child := range t {
			collectLeafPaths(prefix+"/[]", child, out, depth+1)
		}
	default:
		out[prefix] = struct{}{}
	}
}

// writeCharger prices a create or upsert of body (or, with replace, a replace or patch).
func (h *Handler) writeCharger(dbID, collID string, body map[string]any, replace bool) func(int) float64 {
	indexed := h.indexedPaths(dbID, collID, body)

	return func(size int) float64 {
		if replace {
			return replaceCharge(size, indexed)
		}

		return createCharge(size, indexed)
	}
}

// queryPartitions is the number of physical partitions a query has to check: every one unless the partition key
// scopes it.
func (h *Handler) queryPartitions(dbID, collID, scope string) int {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok || scope != "" {
		return 1
	}

	return tb.ThroughputBudget(dbID, collID).Partitions()
}
