// Package dynamodb implements the AWS DynamoDB mock service.
// capacity.go builds ConsumedCapacity responses (table/index breakdowns and the
// ConsistentRead RCU multiplier).
package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// consistentReadMultiplier is the RCU multiplier for strongly-consistent reads.
const consistentReadMultiplier = 2.0

// buildConsumedCapacityWithIndexes constructs a ConsumedCapacity response that
// includes per-index breakdowns when req == INDEXES, and a table-level summary
// when req == TOTAL. Returns nil for NONE or empty.
func buildConsumedCapacityWithIndexes(
	tableName string,
	req types.ReturnConsumedCapacity,
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	totalRCU, totalWCU := sumCapacityMaps(tableRCU, tableWCU, gsiRCU, gsiWCU, lsiRCU, lsiWCU)

	cc := buildBaseConsumedCapacity(tableName, totalRCU, totalWCU)

	if req == types.ReturnConsumedCapacityIndexes {
		applyIndexBreakdowns(cc, tableRCU, tableWCU, gsiRCU, gsiWCU, lsiRCU, lsiWCU)
	}

	return cc
}

// sumCapacityMaps totals RCU and WCU across table and all index maps.
func sumCapacityMaps(
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) (float64, float64) {
	totalRCU := tableRCU
	totalWCU := tableWCU

	for _, v := range gsiRCU {
		totalRCU += v
	}

	for _, v := range gsiWCU {
		totalWCU += v
	}

	for _, v := range lsiRCU {
		totalRCU += v
	}

	for _, v := range lsiWCU {
		totalWCU += v
	}

	return totalRCU, totalWCU
}

// buildBaseConsumedCapacity creates the base ConsumedCapacity with table name and totals.
func buildBaseConsumedCapacity(
	tableName string,
	totalRCU, totalWCU float64,
) *types.ConsumedCapacity {
	cc := &types.ConsumedCapacity{
		TableName:     aws.String(tableName),
		CapacityUnits: aws.Float64(totalRCU + totalWCU),
	}

	if totalRCU > 0 {
		cc.ReadCapacityUnits = aws.Float64(totalRCU)
	}

	if totalWCU > 0 {
		cc.WriteCapacityUnits = aws.Float64(totalWCU)
	}

	return cc
}

// applyIndexBreakdowns populates INDEXES-granularity fields on cc.
func applyIndexBreakdowns(
	cc *types.ConsumedCapacity,
	tableRCU, tableWCU float64,
	gsiRCU, gsiWCU map[string]float64,
	lsiRCU, lsiWCU map[string]float64,
) {
	if tableRCU > 0 || tableWCU > 0 {
		cc.Table = buildTableCapacity(tableRCU, tableWCU)
	}

	if len(gsiRCU) > 0 || len(gsiWCU) > 0 {
		cc.GlobalSecondaryIndexes = buildIndexCapacityMap(gsiRCU, gsiWCU)
	}

	if len(lsiRCU) > 0 || len(lsiWCU) > 0 {
		cc.LocalSecondaryIndexes = buildIndexCapacityMap(lsiRCU, lsiWCU)
	}
}

// buildTableCapacity constructs the per-table Capacity breakdown for INDEXES granularity.
func buildTableCapacity(rcu, wcu float64) *types.Capacity {
	c := &types.Capacity{CapacityUnits: aws.Float64(rcu + wcu)}

	if rcu > 0 {
		c.ReadCapacityUnits = aws.Float64(rcu)
	}

	if wcu > 0 {
		c.WriteCapacityUnits = aws.Float64(wcu)
	}

	return c
}

// buildIndexCapacityMap merges separate RCU/WCU index maps into a map[string]types.Capacity.
func buildIndexCapacityMap(rcuMap, wcuMap map[string]float64) map[string]types.Capacity {
	keys := make(map[string]struct{})
	for k := range rcuMap {
		keys[k] = struct{}{}
	}

	for k := range wcuMap {
		keys[k] = struct{}{}
	}

	out := make(map[string]types.Capacity, len(keys))

	for k := range keys {
		c := types.Capacity{}
		r := rcuMap[k]
		w := wcuMap[k]

		if r > 0 {
			c.ReadCapacityUnits = aws.Float64(r)
		}

		if w > 0 {
			c.WriteCapacityUnits = aws.Float64(w)
		}

		c.CapacityUnits = aws.Float64(r + w)
		out[k] = c
	}

	return out
}

// applyConsistentReadMultiplier doubles the RCU cost when ConsistentRead is true.
// AWS DynamoDB charges 1 RCU per 4 KB for strongly-consistent reads vs
// 0.5 RCU per 4 KB for eventually-consistent reads.
func applyConsistentReadMultiplier(rcu float64, consistentRead bool) float64 {
	if consistentRead {
		return rcu * consistentReadMultiplier
	}

	return rcu
}

// consumedCapacityForReadOp returns a populated ConsumedCapacity for Query or Scan operations.
func consumedCapacityForReadOp(
	tableName string,
	req types.ReturnConsumedCapacity,
	count int,
	consistentRead bool,
	indexName string,
	table *Table,
) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}
	const halfRCU = 0.5
	cu := float64(count) * halfRCU
	if cu < halfRCU {
		cu = halfRCU
	}
	cu = applyConsistentReadMultiplier(cu, consistentRead)

	var (
		tableRCU float64
		gsiRCU   map[string]float64
		lsiRCU   map[string]float64
	)

	if indexName != "" && table != nil {
		if isIndexGSI(table, indexName) {
			gsiRCU = map[string]float64{indexName: cu}
		} else {
			lsiRCU = map[string]float64{indexName: cu}
		}
	} else {
		tableRCU = cu
	}

	return buildConsumedCapacityWithIndexes(
		tableName,
		req,
		tableRCU, 0,
		gsiRCU, nil,
		lsiRCU, nil,
	)
}

func isIndexGSI(table *Table, indexName string) bool {
	for i := range table.GlobalSecondaryIndexes {
		if table.GlobalSecondaryIndexes[i].IndexName == indexName {
			return true
		}
	}

	return false
}

// calculateWriteIndexBreakdowns determines WCU consumed on GSIs and LSIs populated by any of the items.
func calculateWriteIndexBreakdowns(
	table *Table,
	writeUnits float64,
	items ...map[string]any,
) (map[string]float64, map[string]float64) {
	if len(items) == 0 || writeUnits <= 0 || table == nil {
		return nil, nil
	}

	return calculateGSIWriteBreakdowns(
			table,
			writeUnits,
			items...), calculateLSIWriteBreakdowns(
			table,
			writeUnits,
			items...)
}

func calculateGSIWriteBreakdowns(table *Table, writeUnits float64, items ...map[string]any) map[string]float64 {
	if len(table.GlobalSecondaryIndexes) == 0 {
		return nil
	}

	var gsiWCU map[string]float64
	for i := range table.GlobalSecondaryIndexes {
		gsi := &table.GlobalSecondaryIndexes[i]
		pkDef, skDef := getPKAndSK(gsi.KeySchema)
		for _, item := range items {
			if item == nil {
				continue
			}
			if _, _, ok := secondaryItemKeyValues(item, pkDef, skDef); ok {
				if gsiWCU == nil {
					gsiWCU = make(map[string]float64)
				}
				gsiWCU[gsi.IndexName] = writeUnits

				break
			}
		}
	}

	return gsiWCU
}

func calculateLSIWriteBreakdowns(table *Table, writeUnits float64, items ...map[string]any) map[string]float64 {
	if len(table.LocalSecondaryIndexes) == 0 {
		return nil
	}

	var lsiWCU map[string]float64
	for i := range table.LocalSecondaryIndexes {
		lsi := &table.LocalSecondaryIndexes[i]
		pkDef, skDef := getPKAndSK(lsi.KeySchema)
		for _, item := range items {
			if item == nil {
				continue
			}
			if _, _, ok := secondaryItemKeyValues(item, pkDef, skDef); ok {
				if lsiWCU == nil {
					lsiWCU = make(map[string]float64)
				}
				lsiWCU[lsi.IndexName] = writeUnits

				break
			}
		}
	}

	return lsiWCU
}
