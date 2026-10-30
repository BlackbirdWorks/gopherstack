package cosmosdb

import (
	"math/big"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"
)

// Physical partition limits (Learn, "Partitioning and horizontal scaling" and "Best practices for scaling
// provisioned throughput"): up to 10,000 RU/s and 50 GB each.
const (
	partitionMaxRUs         = 10_000
	partitionMaxBytes       = int64(50) << 30
	pkRangeMax              = "FF"
	pkRangeRIDLetters       = 26
	maxStorageSplitsPerPass = 256
)

// partitionsForRU is the physical partition count an offer needs: ROUNDUP(RU / 10000), at least one.
func partitionsForRU(ru int) int {
	return max(1, (ru+partitionMaxRUs-1)/partitionMaxRUs)
}

// rangeIndex maps a canonical partition key to its owner in an even split of the hash space into n ranges.
func rangeIndex(pkJSON string, n int) int {
	l := evenLayout(n)

	return l.indexOf(epkHash(pkJSON))
}

func rangeBound(i, n int) string {
	switch {
	case i <= 0:
		return ""
	case i >= n || n <= 0:
		return pkRangeMax
	}

	return evenLow(i, n).String()
}

func rangeRIDSuffix(i int) string {
	return string([]byte{'A', 'A', byte('A' + i/pkRangeRIDLetters%pkRangeRIDLetters), byte('A' + i%pkRangeRIDLetters)})
}

// pkSpan is one physical partition: it owns the hashes from Lo up to the next span's Lo.
type pkSpan struct {
	Parents []int `json:"parents,omitempty"`
	Lo      u128  `json:"lo"`
	ID      int   `json:"id"`
}

// pkLayout is a container's physical partitions. Splits only add partitions (merge is a separate preview feature),
// a child lists its ancestors, and ids are never reused. A throughput increase that needs more partitions than
// exist is pending until PendingDue (the documented asynchronous scale-up).
type pkLayout struct {
	PendingDue    time.Time `json:"pendingDue,omitzero"`
	Spans         []pkSpan  `json:"spans"`
	NextID        int       `json:"nextId"`
	PendingTarget int       `json:"pendingTarget,omitempty"`
}

func evenLayout(n int) *pkLayout {
	n = max(1, n)
	l := &pkLayout{Spans: make([]pkSpan, n), NextID: n}

	for i := range l.Spans {
		l.Spans[i] = pkSpan{ID: i, Lo: evenLow(i, n)}
	}

	return l
}

func (l *pkLayout) clone() *pkLayout {
	out := *l
	out.Spans = slices.Clone(l.Spans)

	return &out
}

// indexOf is the position of the span owning hash h.
func (l *pkLayout) indexOf(h u128) int {
	return max(0, sort.Search(len(l.Spans), func(i int) bool { return l.Spans[i].Lo.cmp(h) > 0 })-1)
}

func (l *pkLayout) owner(pkJSON string) pkSpan { return l.Spans[l.indexOf(epkHash(pkJSON))] }

// upper is the exclusive upper bound of span i; open is true for the last span (the whole top of the space).
func (l *pkLayout) upper(i int) (u128, bool) {
	if i+1 >= len(l.Spans) {
		return u128{}, true
	}

	return l.Spans[i+1].Lo, false
}

func (l *pkLayout) byID(id int) (int, bool) {
	for i, s := range l.Spans {
		if s.ID == id {
			return i, true
		}
	}

	return 0, false
}

// splitAt replaces span i with two children divided at hash at, which must fall strictly inside the span.
func (l *pkLayout) splitAt(i int, at u128) bool {
	hi, open := l.upper(i)
	if at.cmp(l.Spans[i].Lo) <= 0 || (!open && at.cmp(hi) >= 0) {
		return false
	}

	parent := l.Spans[i]
	lineage := append(slices.Clone(parent.Parents), parent.ID)
	left := pkSpan{ID: l.NextID, Lo: parent.Lo, Parents: lineage}
	right := pkSpan{ID: l.NextID + 1, Lo: at, Parents: slices.Clone(lineage)}
	l.NextID += 2
	l.Spans = slices.Replace(l.Spans, i, i+1, left, right)

	return true
}

func (l *pkLayout) width(i int) *big.Int {
	hi, open := l.upper(i)
	top := epkSpace()
	if !open {
		top = hi.big()
	}

	return new(big.Int).Sub(top, l.Spans[i].Lo.big())
}

func (l *pkLayout) splitMid(i int) bool {
	hi, open := l.upper(i)
	if open {
		hi = u128FromBig(epkSpace())
	}

	return l.splitAt(i, midpoint(l.Spans[i].Lo, hi))
}

// growTo splits the widest partition (lowest first on a tie) until n partitions exist.
func (l *pkLayout) growTo(n int) int {
	splits := 0

	for len(l.Spans) < n {
		widest := 0
		for i := range l.Spans {
			if l.width(i).Cmp(l.width(widest)) > 0 {
				widest = i
			}
		}

		if !l.splitMid(widest) {
			break
		}

		splits++
	}

	return splits
}

func (l *pkLayout) pending() bool { return l.PendingTarget > len(l.Spans) }

// gone reports whether id once named a partition that has since split.
func (l *pkLayout) gone(id int) bool {
	_, active := l.byID(id)

	return !active && id >= 0 && id < l.NextID
}

func (l *pkLayout) etag() string { return `"` + strconv.Itoa(l.NextID) + `"` }

func hashBound(h u128) string { return h.String() }

// pkRangeBody is one partition key range document.
func (l *pkLayout) pkRangeBody(containerRID, dbID, collID string, i int) map[string]any {
	s := l.Spans[i]
	minB, maxB := "", pkRangeMax

	if i > 0 {
		minB = hashBound(s.Lo)
	}

	if hi, open := l.upper(i); !open {
		maxB = hashBound(hi)
	}

	parents := make([]string, len(s.Parents))
	for j, p := range s.Parents {
		parents[j] = strconv.Itoa(p)
	}

	return map[string]any{
		"id": strconv.Itoa(s.ID), sysPropRID: containerRID + rangeRIDSuffix(s.ID), sysPropETag: l.etag(),
		"minInclusive": minB, "maxExclusive": maxB, "ridPrefix": s.ID,
		sysPropSelf:          "dbs/" + dbID + "/colls/" + collID + "/pkranges/" + strconv.Itoa(s.ID) + "/",
		"throughputFraction": 1 / float64(len(l.Spans)), "status": "online", "parents": parents, "lsn": 1,
	}
}

// hashRange is the hash interval of one partition: [lo, hi), or [lo, top] when open.
type hashRange struct {
	lo, hi u128
	open   bool
	set    bool
}

func (r hashRange) contains(pkJSON string) bool {
	if !r.set {
		return true
	}

	h := epkHash(pkJSON)

	return h.cmp(r.lo) >= 0 && (r.open || h.cmp(r.hi) < 0)
}

func (l *pkLayout) rangeOf(i int) hashRange {
	hi, open := l.upper(i)

	return hashRange{lo: l.Spans[i].Lo, hi: hi, open: open, set: true}
}

// partitionSlots lists the physical partitions a request touches: the key's owner, or all for a cross-partition
// request.
func partitionSlots(r *http.Request, spans []pkSpan) []pkSpan {
	l := pkLayout{Spans: spans}

	if len(spans) > 1 {
		if pk, err := queryPartitionScope(r); err == nil && pk != "" {
			return []pkSpan{l.owner(pk)}
		}
	}

	return spans
}

func (h *Handler) slotsFor(r *http.Request, bud RUBudget) []meterSlot {
	spans := bud.Ranges
	if len(spans) == 0 {
		spans = []pkSpan{{}}
	}

	rate := bud.Rate / float64(len(spans))
	touched := partitionSlots(r, spans)
	slots := make([]meterSlot, len(touched))

	for i, p := range touched {
		slots[i] = meterSlot{
			key: bud.Key + "#" + strconv.Itoa(p.ID), rate: rate, burst: h.BurstCapacity, weight: 1,
		}
	}

	return slots
}
