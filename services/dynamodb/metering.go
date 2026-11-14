package dynamodb

import (
	"slices"
	"strings"

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

func (db *InMemoryDB) meterCapacity(region, table string, read, write float64) {
	if !db.metering.Load() {
		return
	}

	db.meterMu.Lock("meterCapacity")
	defer db.meterMu.Unlock()

	if db.meterUsage == nil {
		db.meterUsage = map[string]*CapacityUsage{}
	}

	key := tableKey(region, table)

	u, ok := db.meterUsage[key]
	if !ok {
		u = &CapacityUsage{Region: region, Table: table}
		db.meterUsage[key] = u
	}

	u.ReadUnits += read
	u.WriteUnits += write
}

// DrainCapacityUsage returns and clears the request units consumed by on-demand tables.
func (db *InMemoryDB) DrainCapacityUsage() []CapacityUsage {
	db.meterMu.Lock("DrainCapacityUsage")
	pending := db.meterUsage
	db.meterUsage = nil
	db.meterMu.Unlock()

	out := make([]CapacityUsage, 0, len(pending))

	for _, u := range pending {
		table := db.getTableInRegionRLocked(u.Region, u.Table, "DrainCapacityUsage")
		if table == nil {
			continue
		}

		table.mu.RLock("DrainCapacityUsage")
		onDemand := table.BillingMode == string(types.BillingModePayPerRequest)
		table.mu.RUnlock()

		if !onDemand {
			continue
		}

		u.TableARN = table.TableArn
		u.Tags = cloneTagsRLocked(table)
		out = append(out, *u)
	}

	slices.SortFunc(out, func(a, b CapacityUsage) int { return strings.Compare(a.TableARN, b.TableARN) })

	return out
}
