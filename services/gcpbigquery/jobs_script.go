package gcpbigquery

import (
	"context"
	"encoding/json/v2"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	stmtScript     = "SCRIPT"
	litParentJobID = "parentJobId"
	typeQueryJob   = "QUERY"
	litCacheHit    = "cacheHit"
)

// scriptHost applies a running script's effects and records its child jobs; the caller holds the write lock.
type scriptHost struct {
	b       *InMemoryBackend
	project string
	parent  string
	loc     string
	backup  []byte
	n       int
}

func (h *scriptHost) Apply(_ context.Context, effects []bqsql.Effect) error {
	return h.b.applyEffects(effects)
}

func (h *scriptHost) BeginTx() error {
	data, err := json.Marshal(h.b.datasets, jsonx.V1())
	if err != nil {
		return errInvalidQuery("Could not start a transaction: " + err.Error())
	}

	h.backup = data

	return nil
}

func (h *scriptHost) CommitTx() error {
	h.backup = nil

	return nil
}

func (h *scriptHost) RollbackTx() error {
	var restored map[string]*Dataset

	if err := json.Unmarshal(h.backup, &restored, jsonx.V1()); err != nil {
		return errInvalidQuery("Could not roll back the transaction: " + err.Error())
	}

	for _, d := range restored {
		normalizeDataset(d)
	}

	h.b.datasets, h.backup = restored, nil

	return nil
}

func (h *scriptHost) Child(ev bqsql.ChildEvent) {
	now := nowMs()
	id := h.parent + "_" + strconv.Itoa(h.n)
	h.n++

	cfg := map[string]any{
		jobQuery:  map[string]any{jobQuery: ev.Text, litUselegacysql: false},
		"jobType": typeQueryJob,
	}

	stats := map[string]any{
		litCreationtime: msString(now), "startTime": msString(now), "endTime": msString(now),
		litTotalbytesprocessed: "0", litParentJobID: h.parent,
		"scriptStatistics": map[string]any{"evaluationKind": "STATEMENT"},
	}

	job := &Job{
		Config: cfg, Stats: stats, Project: h.project, ID: id, Location: h.loc, Type: typeQueryJob, State: stateDone,
		User: userEmail, Created: now, Started: now, Ended: now,
	}

	if ev.Err != nil {
		e := fromSQL(ev.Err)
		job.ErrorResult = &jobError{Reason: e.Reason, Message: e.Message}
		stats[jobQuery] = map[string]any{}
	} else {
		stats[jobQuery] = queryStats(ev.Result)
		job.NumDML = ev.Result.Affected
		job.HasResult = ev.Result.Kind == bqsql.StmtQuery
		job.Schema, job.Rows = ev.Result.Fields, ev.Result.Rows
	}

	h.b.storeJob(job)
}

// execScript runs a multi-statement script; the caller holds the write lock.
func (b *InMemoryBackend) execScript(
	ctx context.Context,
	project, parent string,
	sc *bqsql.Script,
	in *bqsql.Input,
	dryRun bool,
) (*outcome, error) {
	if dryRun {
		return &outcome{Stats: scriptStats(nil)}, nil
	}

	host := &scriptHost{b: b, project: project, parent: parent, loc: defaultLocation}

	res, err := bqsql.RunScript(ctx, sc, in, host)
	if err != nil {
		return b.queryFailure(err)
	}

	out := &outcome{Stats: scriptStats(res.Last), Extra: map[string]any{"numChildJobs": strconv.Itoa(res.Children)}}

	if last := res.Last; last != nil {
		out.Fields, out.Rows = last.Fields, last.Rows
		out.IsQuery = last.Kind == bqsql.StmtQuery
		out.NumDML = last.Affected
	}

	return out, nil
}

func scriptStats(last *bqsql.Result) map[string]any {
	stats := map[string]any{
		"statementType": stmtScript, litTotalbytesprocessed: "0", "totalBytesBilled": "0", litCacheHit: false,
		"billingTier": 1,
	}

	if last != nil && last.Kind.DML() {
		stats["numDmlAffectedRows"] = strconv.FormatInt(last.Affected, 10)
	}

	return stats
}
