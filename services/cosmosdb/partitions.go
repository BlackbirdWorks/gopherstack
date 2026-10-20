package cosmosdb

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"net/http"
	"strconv"
)

// A physical partition serves up to 10,000 RU/s (and 50 GB, which the emulator does not model).
const (
	partitionMaxRUs   = 10_000
	pkRangeMax        = "FF"
	pkRangeRIDLetters = 26
)

// partitionsForRU is the physical partition count of an offer: ceil(RU / 10000), at least one.
func partitionsForRU(ru int) int {
	return max(1, (ru+partitionMaxRUs-1)/partitionMaxRUs)
}

func epkHash(pkJSON string) uint64 {
	sum := sha256.Sum256([]byte(pkJSON))

	return binary.BigEndian.Uint64(sum[:8])
}

// rangeIndex maps a canonical partition key to the physical partition owning it, splitting the hash space evenly.
func rangeIndex(pkJSON string, n int) int {
	if n <= 1 {
		return 0
	}

	hi, _ := bits.Mul64(epkHash(pkJSON), uint64(n))

	return int(hi & math.MaxInt32)
}

// rangeBound is the inclusive lower EPK bound of range i of n: the smallest hash h with rangeIndex(h) >= i.
func rangeBound(i, n int) string {
	if i <= 0 {
		return ""
	}

	if i >= n || n <= 0 {
		return pkRangeMax
	}

	q, r := bits.Div64(uint64(i), 0, uint64(n))
	if r != 0 {
		q++
	}

	return fmt.Sprintf("%016X", q)
}

func rangeRIDSuffix(i int) string {
	return string([]byte{'A', 'A', byte('A' + i/pkRangeRIDLetters%pkRangeRIDLetters), byte('A' + i%pkRangeRIDLetters)})
}

func pkRangeETagFor(n int) string { return `"` + strconv.Itoa(n) + `"` }

// pkRangeBody is one partition key range document.
func pkRangeBody(containerRID, dbID, collID string, i, n int) map[string]any {
	return map[string]any{
		"id": strconv.Itoa(i), sysPropRID: containerRID + rangeRIDSuffix(i), sysPropETag: pkRangeETagFor(n),
		"minInclusive": rangeBound(i, n), "maxExclusive": rangeBound(i+1, n), "ridPrefix": i,
		sysPropSelf:          "dbs/" + dbID + "/colls/" + collID + "/pkranges/" + strconv.Itoa(i) + "/",
		"throughputFraction": 1 / float64(n), "status": "online", "parents": []string{}, "lsn": 1,
	}
}

// partitionIndexes lists the physical partitions a request touches: the key's owner, or all for a cross-partition
// request.
func partitionIndexes(r *http.Request, n int) []int {
	if n > 1 {
		if pk, err := queryPartitionScope(r); err == nil && pk != "" {
			return []int{rangeIndex(pk, n)}
		}
	}

	out := make([]int, n)
	for i := range out {
		out[i] = i
	}

	return out
}

func (h *Handler) slotsFor(r *http.Request, bud RUBudget) []meterSlot {
	n := max(1, bud.Partitions)
	rate := bud.Rate / float64(n)
	idx := partitionIndexes(r, n)
	slots := make([]meterSlot, len(idx))

	for i, p := range idx {
		slots[i] = meterSlot{key: bud.Key + "#" + strconv.Itoa(p), rate: rate, burst: h.BurstCapacity}
	}

	return slots
}
