package dynamodb

import (
	"maps"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CapacityUsage is the request units a table consumed since the previous drain.
type CapacityUsage struct {
	Tags       map[string]string
	Region     string
	Table      string
	TableARN   string
	ReadUnits  float64
	WriteUnits float64
}

// EnableCapacityMetering starts recording consumed request units for DrainCapacityUsage.
func (db *InMemoryDB) EnableCapacityMetering() { db.metering.Store(true) }

type capacityKey struct{ region, table string }

type capacityCounter struct{ read, write atomic.Uint64 }

func addUnits(a *atomic.Uint64, v float64) {
	if v == 0 {
		return
	}

	for {
		old := a.Load()
		if a.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

func (db *InMemoryDB) capacityCounterFor(k capacityKey) *capacityCounter {
	if m := db.meterCounters.Load(); m != nil {
		if c, ok := (*m)[k]; ok {
			return c
		}
	}

	db.meterMu.Lock("meterCapacity")
	defer db.meterMu.Unlock()

	var cur map[capacityKey]*capacityCounter
	if m := db.meterCounters.Load(); m != nil {
		cur = *m
	}

	if c, ok := cur[k]; ok {
		return c
	}

	next := make(map[capacityKey]*capacityCounter, len(cur)+1)
	maps.Copy(next, cur)

	c := &capacityCounter{}
	next[capacityKey{region: strings.Clone(k.region), table: strings.Clone(k.table)}] = c
	db.meterCounters.Store(&next)

	return c
}

func (db *InMemoryDB) meterCapacity(region, table string, read, write float64) {
	if !db.metering.Load() {
		return
	}

	c := db.capacityCounterFor(capacityKey{region: region, table: table})
	addUnits(&c.read, read)
	addUnits(&c.write, write)
}

func (db *InMemoryDB) pruneCapacityCounters(dead []capacityKey) {
	db.meterMu.Lock("pruneCapacityCounters")
	defer db.meterMu.Unlock()

	m := db.meterCounters.Load()
	if m == nil {
		return
	}

	next := maps.Clone(*m)
	for _, k := range dead {
		delete(next, k)
	}

	db.meterCounters.Store(&next)
}

// DrainCapacityUsage returns and clears the request units consumed by on-demand tables.
func (db *InMemoryDB) DrainCapacityUsage() []CapacityUsage {
	m := db.meterCounters.Load()
	if m == nil {
		return []CapacityUsage{}
	}

	out := make([]CapacityUsage, 0, len(*m))

	var dead []capacityKey

	for k, c := range *m {
		read := math.Float64frombits(c.read.Swap(0))
		write := math.Float64frombits(c.write.Swap(0))

		table := db.getTableInRegionRLocked(k.region, k.table, "DrainCapacityUsage")
		if table == nil {
			dead = append(dead, k)

			continue
		}

		if read == 0 && write == 0 {
			continue
		}

		table.mu.RLock("DrainCapacityUsage")
		onDemand := table.BillingMode == string(types.BillingModePayPerRequest)
		table.mu.RUnlock()

		if !onDemand {
			continue
		}

		out = append(out, CapacityUsage{
			Region: k.region, Table: k.table, ReadUnits: read, WriteUnits: write,
			TableARN: table.TableArn, Tags: cloneTagsRLocked(table),
		})
	}

	if len(dead) > 0 {
		db.pruneCapacityCounters(dead)
	}

	slices.SortFunc(out, func(a, b CapacityUsage) int { return strings.Compare(a.TableARN, b.TableARN) })

	return out
}
