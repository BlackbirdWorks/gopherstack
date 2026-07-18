package xray

import (
	"slices"
	"sort"
	"time"
)

// accumulateServiceNodes builds nodeMap and segToService from the trace segments.
func accumulateServiceNodes(
	traceSegs map[string][]*Segment,
) (map[serviceKey]*serviceNode, map[string]serviceKey) {
	nodeMap := map[serviceKey]*serviceNode{}
	segToService := map[string]serviceKey{}
	refID := 0

	for _, segs := range traceSegs {
		for _, seg := range segs {
			svcType := seg.Origin
			if svcType == "" {
				svcType = seg.Namespace
			}

			key := serviceKey{Name: seg.Name, Type: svcType}

			node, ok := nodeMap[key]
			if !ok {
				node = &serviceNode{Name: seg.Name, Type: svcType, ReferenceID: refID}
				refID++
				nodeMap[key] = node
			}

			accumulateNodeStats(node, seg)
			segToService[seg.ID] = key
		}
	}

	return nodeMap, segToService
}

// accumulateNodeStats updates a service node with stats from a single segment.
func accumulateNodeStats(node *serviceNode, seg *Segment) {
	if seg.StartTime > 0 && (node.StartTime == 0 || seg.StartTime < node.StartTime) {
		node.StartTime = seg.StartTime
	}

	if seg.EndTime > node.EndTime {
		node.EndTime = seg.EndTime
	}

	node.TotalCount++

	switch {
	case seg.Fault:
		node.FaultCount++
	case seg.Error && seg.Throttle:
		node.ThrottleCount++
	case seg.Error:
		node.ErrorCount++
	default:
		node.OkCount++
	}

	if seg.EndTime > 0 && seg.StartTime > 0 {
		node.TotalRespTime += seg.EndTime - seg.StartTime
	}

	if seg.ParentID == "" {
		node.IsRoot = true
	}
}

// buildEdgeSet returns the set of directed edges between service nodes.
func buildEdgeSet(
	traceSegs map[string][]*Segment,
	segToService map[string]serviceKey,
) map[edgeKey]bool {
	edgeSet := map[edgeKey]bool{}

	for _, segs := range traceSegs {
		for _, seg := range segs {
			if seg.ParentID == "" {
				continue
			}

			parentKey, ok := segToService[seg.ParentID]
			if !ok {
				continue
			}

			childKey := segToService[seg.ID]
			if childKey != parentKey {
				edgeSet[edgeKey{From: childKey, To: parentKey}] = true
			}
		}
	}

	return edgeSet
}

// nodeToView converts a service node to its JSON output representation.
func nodeToView(
	key serviceKey,
	node *serviceNode,
	edgeSet map[edgeKey]bool,
	nodeMap map[serviceKey]*serviceNode,
) map[string]any {
	nodeEdges := make([]map[string]any, 0)

	for e := range edgeSet {
		if e.From == key {
			to := nodeMap[e.To]
			nodeEdges = append(nodeEdges, map[string]any{
				"ReferenceId": to.ReferenceID,
			})
		}
	}

	return map[string]any{
		"ReferenceId": node.ReferenceID,
		"Name":        node.Name,
		"Type":        node.Type,
		"State":       "active",
		"Root":        node.IsRoot,
		"StartTime":   node.StartTime,
		"EndTime":     node.EndTime,
		"Edges":       nodeEdges,
		"SummaryStatistics": map[string]any{
			"OkCount": node.OkCount,
			"ErrorStatistics": map[string]any{
				"ThrottleCount":        node.ThrottleCount,
				"OtherCount":           node.ErrorCount,
				serviceGraphTotalCount: node.ThrottleCount + node.ErrorCount,
			},
			"FaultStatistics": map[string]any{
				serviceGraphTotalCount: node.FaultCount,
			},
			serviceGraphTotalCount: node.TotalCount,
			"TotalResponseTime":    node.TotalRespTime,
			"DurationHistogram":    []any{},
		},
	}
}

// buildServiceGraph builds service nodes from a map of traceID → segments.
func buildServiceGraph(traceSegs map[string][]*Segment) []map[string]any {
	nodeMap, segToService := accumulateServiceNodes(traceSegs)
	edgeSet := buildEdgeSet(traceSegs, segToService)

	nodes := make([]map[string]any, 0, len(nodeMap))

	for key, node := range nodeMap {
		nodes = append(nodes, nodeToView(key, node, edgeSet, nodeMap))
	}

	sort.Slice(nodes, func(i, j int) bool {
		ri, _ := nodes[i]["ReferenceId"].(int)
		rj, _ := nodes[j]["ReferenceId"].(int)

		return ri < rj
	})

	return nodes
}

// GetServiceGraph returns a service graph derived from stored traces in the time window.
func (b *InMemoryBackend) GetServiceGraph(startTime, endTime time.Time) []map[string]any {
	b.mu.RLock("GetServiceGraph")
	defer b.mu.RUnlock()

	// Filter segments to those within the time window.
	filtered := map[string][]*Segment{}

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)

		var inWindow []*Segment

		for _, seg := range segs {
			if seg.StartTime == 0 {
				inWindow = append(inWindow, seg)

				continue
			}

			segTime := time.Unix(int64(seg.StartTime), 0)
			if !segTime.Before(startTime) && !segTime.After(endTime) {
				inWindow = append(inWindow, seg)
			}
		}

		if len(inWindow) > 0 {
			filtered[t.TraceID] = inWindow
		}
	}

	if len(filtered) == 0 {
		return []map[string]any{}
	}

	return buildServiceGraph(filtered)
}

// GetTraceGraph returns a service graph scoped to the given trace IDs.
func (b *InMemoryBackend) GetTraceGraph(traceIDs []string) []map[string]any {
	b.mu.RLock("GetTraceGraph")
	defer b.mu.RUnlock()

	filtered := map[string][]*Segment{}

	for _, id := range traceIDs {
		if segs := b.traceSegments.Get(id); len(segs) > 0 {
			filtered[id] = segs
		}
	}

	if len(filtered) == 0 {
		return []map[string]any{}
	}

	return buildServiceGraph(filtered)
}

// accumulateToBucket adds one segment's stats into the appropriate time bucket.
func accumulateToBucket(buckets map[int64]*tsBucket, seg *Segment, period int) {
	bk := (int64(seg.StartTime) / int64(period)) * int64(period)

	bkt := buckets[bk]
	if bkt == nil {
		bkt = &tsBucket{}
		buckets[bk] = bkt
	}

	bkt.TotalCount++

	switch {
	case seg.Fault:
		bkt.FaultCount++
	case seg.Error && seg.Throttle:
		bkt.ThrottleCount++
	case seg.Error:
		bkt.ErrorCount++
	default:
		bkt.OkCount++
	}

	if seg.EndTime > seg.StartTime {
		bkt.TotalRespTime += seg.EndTime - seg.StartTime
	}
}

// tsBucketToView converts a tsBucket to its JSON output map.
func tsBucketToView(k int64, bkt *tsBucket) map[string]any {
	return map[string]any{
		"Timestamp": float64(k),
		"ServiceSummaryStatistics": map[string]any{
			"OkCount": bkt.OkCount,
			"ErrorStatistics": map[string]any{
				"ThrottleCount":        bkt.ThrottleCount,
				"OtherCount":           bkt.ErrorCount,
				serviceGraphTotalCount: bkt.ThrottleCount + bkt.ErrorCount,
			},
			"FaultStatistics": map[string]any{
				serviceGraphTotalCount: bkt.FaultCount,
			},
			serviceGraphTotalCount: bkt.TotalCount,
			"TotalResponseTime":    bkt.TotalRespTime,
		},
	}
}

// GetTimeSeriesServiceStatistics returns per-period bucketed statistics for segments in the time window.
func (b *InMemoryBackend) GetTimeSeriesServiceStatistics(startTime, endTime time.Time, period int) []map[string]any {
	b.mu.RLock("GetTimeSeriesServiceStatistics")
	defer b.mu.RUnlock()

	if period <= 0 {
		period = 60
	}

	buckets := map[int64]*tsBucket{}

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)

		for _, seg := range segs {
			if seg.StartTime == 0 {
				continue
			}

			segTime := time.Unix(int64(seg.StartTime), 0)
			if segTime.Before(startTime) || segTime.After(endTime) {
				continue
			}

			accumulateToBucket(buckets, seg, period)
		}
	}

	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, tsBucketToView(k, buckets[k]))
	}

	return out
}

const (
	// serviceGraphTotalCount is the key for the TotalCount stat in service graph nodes.
	serviceGraphTotalCount = "TotalCount"
)
