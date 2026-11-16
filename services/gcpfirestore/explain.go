package gcpfirestore

import (
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func orderProperty(t orderTerm) string {
	name := nameField
	if !t.name {
		name = strings.Join(t.path, ".")
	}

	dir := "ASC"
	if t.desc {
		dir = "DESC"
	}

	return name + " " + dir
}

// planIndexes names the automatic index a query runs on: its ordering fields, then the document name.
func planIndexes(parent string, q *firestorepb.StructuredQuery) ([]*structpb.Struct, error) {
	cq, err := compileQuery(parent, q)
	if err != nil {
		return nil, err
	}

	scopeName := "Collection"
	if cq.scope.descendants {
		scopeName = "Collection group"
	}

	props := make([]string, 0, len(cq.orders)+1)

	if fn := q.GetFindNearest(); fn != nil {
		dims := 0
		if vec, ok := vectorOf(fn.GetQueryVector()); ok {
			dims = len(vec)
		}

		props = append(props, fn.GetVectorField().GetFieldPath()+" VECTOR<"+strconv.Itoa(dims)+">")
	}

	for _, t := range cq.orders {
		props = append(props, orderProperty(t))
	}

	if len(cq.orders) == 0 || !cq.orders[len(cq.orders)-1].name {
		props = append(props, nameField+" ASC")
	}

	idx, err := structpb.NewStruct(map[string]any{
		"query_scope": scopeName, "properties": "(" + strings.Join(props, ", ") + ")",
	})
	if err != nil {
		return nil, errInvalid("plan summary: %v", err)
	}

	return []*structpb.Struct{idx}, nil
}

// explainMetrics reports the plan and, when analyzing, what the run did. read_operations and the scanned
// counts are the documents the query returned or skipped: the emulator keeps no index to count entries against.
func explainMetrics(
	plan []*structpb.Struct, analyze bool, returned, scanned int, took time.Duration,
) (*firestorepb.ExplainMetrics, error) {
	m := &firestorepb.ExplainMetrics{PlanSummary: &firestorepb.PlanSummary{IndexesUsed: plan}}
	if !analyze {
		return m, nil
	}

	debug, err := structpb.NewStruct(map[string]any{
		"documents_scanned": float64(scanned), "index_entries_scanned": float64(scanned),
	})
	if err != nil {
		return nil, errInvalid("execution stats: %v", err)
	}

	m.ExecutionStats = &firestorepb.ExecutionStats{
		ResultsReturned: int64(returned), ExecutionDuration: durationpb.New(took), ReadOperations: int64(scanned),
		DebugStats: debug,
	}

	return m, nil
}
