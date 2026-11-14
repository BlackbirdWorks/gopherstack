package ocimysql

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	wrGenerateDBSystemStatus = "GENERATE_DBSYSTEM_STATUS"
	wrGenerateChannelStatus  = "GENERATE_CHANNEL_STATUS"
	wrGenerateEstimate       = "GENERATE_HEATWAVE_CLUSTER_MEMORY_ESTIMATE"
	gtidExecuted             = "GTID_EXECUTED"
	gtidAvailable            = "GTID_AVAILABLE"
	estimateAccepted         = "ACCEPTED"
	estimateSucceeded        = "SUCCEEDED"
	estimateFailed           = "FAILED"
	mebibyte                 = int64(1) << 20
)

var gtidSetRe = regexp.MustCompile(`^[0-9a-fA-F:,\s-]+$`)

// StatusRec is the last status collected for a DB system or channel.
type StatusRec struct {
	TimeCreated time.Time      `json:"timeCreated"`
	TimeUpdated time.Time      `json:"timeUpdated"`
	Result      map[string]any `json:"result,omitempty"`
}

// TableEstimate is the estimate for one user table.
type TableEstimate struct {
	Name         string `json:"name"`
	ErrorComment string `json:"errorComment,omitempty"`
	Columns      int    `json:"columns"`
	VarlenCols   int    `json:"varlenColumns"`
	Rows         int64  `json:"rows"`
	FootprintMB  int64  `json:"footprintMB"`
}

// SchemaEstimate groups the table estimates of a schema.
type SchemaEstimate struct {
	Name   string          `json:"name"`
	Tables []TableEstimate `json:"tables"`
}

// EstimateRec is the last HeatWave memory estimate of a DB system.
type EstimateRec struct {
	TimeCreated time.Time        `json:"timeCreated"`
	TimeUpdated time.Time        `json:"timeUpdated"`
	State       string           `json:"state"`
	Schemas     []SchemaEstimate `json:"schemas"`
}

// GenerateStatusIn is the GenerateDbSystemStatus body.
type GenerateStatusIn struct {
	GtidSetType    string   `json:"gtidSetType"`
	GtidSetToApply string   `json:"gtidSetToApply"`
	RetryToken     string   `json:"-"`
	ChannelIDs     []string `json:"channelIds"`
}

func (b *InMemoryBackend) activeOrInactive(id string) (*DBSystem, *APIError) {
	b.settleID(id)

	s, ok := b.systems[id]
	if !ok || s.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return s, nil
}

func (b *InMemoryBackend) validateStatusIn(s *DBSystem, in GenerateStatusIn) *APIError {
	if aerr := validEnum("gtidSetType", in.GtidSetType, gtidExecuted, gtidAvailable); aerr != nil {
		return aerr
	}

	if in.GtidSetToApply != "" && !gtidSetRe.MatchString(in.GtidSetToApply) {
		return errParam("gtidSetToApply must be a GTID set such as uuid:1-5")
	}

	for _, id := range in.ChannelIDs {
		if c, ok := b.channels[id]; !ok || c.State == stateDeleted || c.Target.DBSystemID != s.ID {
			return errParam("channelIds: " + id + " is not a channel of DB system " + s.ID)
		}
	}

	return nil
}

func (b *InMemoryBackend) channelStatusResult(c *Channel) map[string]any {
	healthy := c.State == stateActive
	errs := []string{}

	if c.State == stateNeedsAttention && c.Details != "" {
		errs = append(errs, c.Details)
	}

	return map[string]any{
		"channelId": c.ID, "isHealthy": healthy, "isReceivedGtidSetApplied": healthy, "errors": errs,
	}
}

// stateStatus is what a DB system without a running server can report: its availability follows its lifecycle.
func stateStatus(s *DBSystem) map[string]any {
	up := s.State == stateActive

	return map[string]any{
		"canAcceptClientConnections": up, "isInOfflineMode": false,
		"isWritable": up && s.DatabaseMode == modeReadWrite, "areAllMysqlInstancesHealthy": up, "isStorageFull": false,
	}
}

// GenerateDBSystemStatus collects the status of a DB system and returns it with its work request.
func (b *InMemoryBackend) GenerateDBSystemStatus(id string, in GenerateStatusIn) (Doc, string, error) {
	b.mu.Lock("GenerateDbSystemStatus")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("GenerateDbSystemStatus/"+id, in.RetryToken, b.nowUTC()); ok {
		return b.statusDoc(b.systems[id].Status, id), v, nil
	}

	s, aerr := b.activeOrInactive(id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = b.validateStatusIn(s, in); aerr != nil {
		return nil, "", aerr
	}

	now := b.nowUTC()
	rec := &StatusRec{TimeCreated: now, TimeUpdated: now}
	chans := b.channelStatuses(in.ChannelIDs)
	wr := b.newWR(wrGenerateDBSystemStatus, s.CompartmentID, entityDBSystem, id, actionUpdated, nil)

	b.retry.Remember("GenerateDbSystemStatus/"+id, in.RetryToken, wr, now)

	inst := b.eng.liveInstance(id)
	if inst == nil || s.State != stateActive {
		rec.Result = stateStatus(s)
		b.finishStatus(rec, chans, wr)
		s.Status = rec

		return b.statusDoc(rec, id), wr, nil
	}

	s.Status = rec
	sys := *s

	b.goAsync(wr, func(ctx context.Context) (map[string]any, error) {
		return queryStatus(ctx, inst, &sys, in)
	}, func(res map[string]any, err error) {
		cur, ok := b.systems[id]
		if !ok || cur.Status != rec {
			return
		}

		if err != nil {
			res = stateStatus(&sys)
			res["canAcceptClientConnections"], res["isWritable"], res["areAllMysqlInstancesHealthy"] = false, false, false
		}

		rec.Result = res
		b.finishStatus(rec, b.channelStatuses(in.ChannelIDs), wr)
	})

	return b.statusDoc(rec, id), wr, nil
}

func (e *dbEngine) liveInstance(id string) Instance {
	if e == nil {
		return nil
	}

	return e.live[id]
}

func (b *InMemoryBackend) channelStatuses(ids []string) []any {
	var out []any

	for _, id := range ids {
		if c, ok := b.channels[id]; ok {
			out = append(out, b.channelStatusResult(c))
		}
	}

	return out
}

func (b *InMemoryBackend) finishStatus(rec *StatusRec, chans []any, wr string) {
	if len(chans) > 0 {
		rec.Result["channelsStatus"] = chans
	}

	rec.TimeUpdated = b.nowUTC()
	b.finishWR(wr, "")
}

func (b *InMemoryBackend) statusDoc(rec *StatusRec, id string) Doc {
	d := Doc{paramDBSystem: id, keyTimeCreated: rec.TimeCreated, keyTimeUpdated: rec.TimeUpdated}
	if rec.Result != nil {
		d["dbSystemStatusResult"] = rec.Result
	}

	return d
}

// goAsync runs compute off the lock, then apply under it.
func (b *InMemoryBackend) goAsync(
	_ string,
	compute func(ctx context.Context) (map[string]any, error),
	apply func(res map[string]any, err error),
) {
	ctx := b.runCtx

	b.wg.Go(func() {
		res, err := compute(ctx)

		b.mu.Lock("goAsync")
		defer b.mu.Unlock()

		if ctx.Err() != nil {
			return
		}

		apply(res, err)
	})
}

func scalar(rows [][]string) string {
	if len(rows) < 2 || len(rows[1]) == 0 {
		return ""
	}

	return rows[1][0]
}

func queryStatus(ctx context.Context, inst Instance, s *DBSystem, in GenerateStatusIn) (map[string]any, error) {
	q, ok := inst.(Querier)
	if !ok {
		return stateStatus(s), nil
	}

	res := map[string]any{}

	if _, err := q.Query(ctx, "", "SELECT 1"); err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	flags, err := q.Query(ctx, "", "SELECT @@offline_mode, @@read_only OR @@super_read_only")
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	res["canAcceptClientConnections"], res["areAllMysqlInstancesHealthy"] = true, true
	res["isInOfflineMode"] = flags[1][0] == "1"
	res["isWritable"] = flags[1][1] != "1" && flags[1][0] != "1"

	if err = fillStatusGTIDs(ctx, q, in, res); err != nil {
		return nil, err
	}

	used, err := q.Query(ctx, "", "SELECT COALESCE(SUM(DATA_LENGTH+INDEX_LENGTH),0) FROM information_schema.TABLES")
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	n, _ := strconv.ParseInt(scalar(used), 10, 64)
	res["isStorageFull"] = n >= int64(s.DataGB)*gib

	return res, nil
}

func fillStatusGTIDs(ctx context.Context, q Querier, in GenerateStatusIn, res map[string]any) error {
	set := "@@GLOBAL.gtid_executed"
	if in.GtidSetType == gtidAvailable {
		set = "GTID_SUBTRACT(@@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged)"
	}

	rows, err := q.Query(ctx, "", "SELECT "+set)
	if err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	res["gtidSet"] = strings.ReplaceAll(strings.Join(strings.Fields(scalar(rows)), ""), `\n`, "")

	if in.GtidSetToApply == "" {
		return nil
	}

	rows, err = q.Query(ctx, "", "SELECT GTID_SUBSET('"+in.GtidSetToApply+"', @@GLOBAL.gtid_executed)")
	if err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	res["isGtidSetApplied"] = scalar(rows) == "1"

	return nil
}

// GetDBSystemStatus returns the status most recently collected.
func (b *InMemoryBackend) GetDBSystemStatus(id string) (Doc, error) {
	b.mu.Lock("GetDbSystemStatus")
	defer b.mu.Unlock()

	s, aerr := b.activeOrInactive(id)
	if aerr != nil {
		return nil, aerr
	}

	if s.Status == nil {
		return nil, errNotFound("status of " + id)
	}

	return b.statusDoc(s.Status, id), nil
}

// GenerateChannelStatus collects the status of a channel and returns it with its work request.
func (b *InMemoryBackend) GenerateChannelStatus(id, token string) (Doc, string, error) {
	b.mu.Lock("GenerateChannelStatus")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("GenerateChannelStatus/"+id, token, b.nowUTC()); ok {
		return b.channelStatusDoc(b.channels[id]), v, nil
	}

	c, aerr := b.channelFor(id, "")
	if aerr != nil {
		return nil, "", aerr
	}

	now := b.nowUTC()
	c.Status = &StatusRec{TimeCreated: now, TimeUpdated: now, Result: b.channelStatusResult(c)}
	wr := b.newWR(wrGenerateChannelStatus, c.CompartmentID, entityChannel, id, actionUpdated, &now)
	b.retry.Remember("GenerateChannelStatus/"+id, token, wr, now)

	return b.channelStatusDoc(c), wr, nil
}

func (b *InMemoryBackend) channelStatusDoc(c *Channel) Doc {
	return Doc{
		keyTimeCreated: c.Status.TimeCreated, keyTimeUpdated: c.Status.TimeUpdated,
		"channelStatusResult": c.Status.Result,
	}
}

// GetChannelStatus returns the status most recently collected.
func (b *InMemoryBackend) GetChannelStatus(id string) (Doc, error) {
	b.mu.Lock("GetChannelStatus")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, "")
	if aerr != nil {
		return nil, aerr
	}

	if c.Status == nil {
		return nil, errNotFound("status of " + id)
	}

	return b.channelStatusDoc(c), nil
}

const estimateSQL = `SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COALESCE(t.TABLE_ROWS,0),
 (SELECT COUNT(*) FROM information_schema.COLUMNS c WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME),
 (SELECT COUNT(*) FROM information_schema.COLUMNS c WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME
   AND c.DATA_TYPE IN ('char','varchar','binary','varbinary','tinytext','text','mediumtext','longtext',
   'tinyblob','blob','mediumblob','longblob')),
 COALESCE(t.DATA_LENGTH,0)
FROM information_schema.TABLES t
WHERE t.TABLE_TYPE='BASE TABLE' AND t.TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')
ORDER BY 1,2`

func queryEstimate(ctx context.Context, inst Instance) ([]SchemaEstimate, error) {
	q, ok := inst.(Querier)
	if !ok {
		return nil, errNoServer
	}

	rows, err := q.Query(ctx, "", estimateSQL)
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	var out []SchemaEstimate

	for _, r := range rows[1:] {
		t := tableEstimate(r)

		if len(out) == 0 || out[len(out)-1].Name != r[0] {
			out = append(out, SchemaEstimate{Name: r[0]})
		}

		out[len(out)-1].Tables = append(out[len(out)-1].Tables, t)
	}

	return out, nil
}

func tableEstimate(r []string) TableEstimate {
	rows, _ := strconv.ParseInt(r[2], 10, 64)
	cols, _ := strconv.Atoi(r[3])
	varlen, _ := strconv.Atoi(r[4])
	bytes, _ := strconv.ParseInt(r[5], 10, 64)

	return TableEstimate{
		Name: r[1], Rows: rows, Columns: cols, VarlenCols: varlen,
		FootprintMB: max(1, (bytes+mebibyte-1)/mebibyte),
	}
}

// GenerateHeatWaveEstimate starts a memory estimate for the user tables of a DB system.
func (b *InMemoryBackend) GenerateHeatWaveEstimate(id, token string) (Doc, string, error) {
	b.mu.Lock("GenerateHeatWaveEstimate")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("GenerateHeatWaveEstimate/"+id, token, b.nowUTC()); ok {
		return b.estimateDoc(b.systems[id]), v, nil
	}

	s, aerr := b.activeOrInactive(id)
	if aerr != nil {
		return nil, "", aerr
	}

	if s.State != stateActive {
		return nil, "", errState("DB system " + id + " is in state " + s.State + "; an estimate requires ACTIVE")
	}

	now := b.nowUTC()
	rec := &EstimateRec{TimeCreated: now, TimeUpdated: now, State: estimateAccepted, Schemas: []SchemaEstimate{}}
	s.Estimate = rec
	inst := b.eng.liveInstance(id)
	wr := b.newWR(wrGenerateEstimate, s.CompartmentID, entityDBSystem, id, actionUpdated, nil)
	b.retry.Remember("GenerateHeatWaveEstimate/"+id, token, wr, now)

	if inst == nil {
		rec.State = estimateSucceeded
		b.finishWR(wr, "")

		return b.estimateDoc(s), wr, nil
	}

	doc := b.estimateDoc(s)
	ctx := b.runCtx

	b.wg.Go(func() {
		schemas, err := queryEstimate(ctx, inst)

		b.mu.Lock("GenerateHeatWaveEstimate")
		defer b.mu.Unlock()

		if cur, ok := b.systems[id]; !ok || cur.Estimate != rec || ctx.Err() != nil {
			return
		}

		rec.TimeUpdated, rec.State = b.nowUTC(), estimateSucceeded

		if err != nil {
			rec.State = estimateFailed
			b.finishWR(wr, "Estimate failed: "+err.Error())

			return
		}

		rec.Schemas = schemas
		b.finishWR(wr, "")
	})

	return doc, wr, nil
}

func (b *InMemoryBackend) estimateDoc(s *DBSystem) Doc {
	rec := s.Estimate
	schemas := make([]Doc, 0, len(rec.Schemas))

	for _, sc := range rec.Schemas {
		tables := make([]Doc, 0, len(sc.Tables))
		for _, t := range sc.Tables {
			tables = append(tables, Doc{
				"tableName": t.Name, "toLoadColumnCount": t.Columns, "varlenColumnCount": t.VarlenCols,
				"estimatedRowCount": t.Rows, "analyticalFootprintInMbs": t.FootprintMB, "errorComment": t.ErrorComment,
			})
		}

		schemas = append(schemas, Doc{"schemaName": sc.Name, "perTableEstimates": tables})
	}

	return Doc{
		paramDBSystem: s.ID, keyStatus: rec.State, keyTimeCreated: rec.TimeCreated, keyTimeUpdated: rec.TimeUpdated,
		"tableSchemas": schemas,
	}
}

// GetHeatWaveEstimate returns the most recent memory estimate.
func (b *InMemoryBackend) GetHeatWaveEstimate(id string) (Doc, error) {
	b.mu.Lock("GetHeatWaveEstimate")
	defer b.mu.Unlock()

	s, aerr := b.activeOrInactive(id)
	if aerr != nil {
		return nil, aerr
	}

	if s.Estimate == nil {
		return nil, errNotFound("memory estimate of " + id)
	}

	return b.estimateDoc(s), nil
}

func (h *Handler) generateDBSystemStatus(q *req) error {
	var in GenerateStatusIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	d, wr, err := h.Backend.GenerateDBSystemStatus(q.param(paramDBSystem), in)

	return acceptedDoc(q, d, wr, err)
}

func acceptedDoc(q *req, d Doc, wr string, err error) error {
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	out, merr := json.Marshal(d, jsonx.V1())
	if merr != nil {
		return merr //nolint:wrapcheck // encoding a map cannot fail
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(http.StatusAccepted)
	_, _ = q.w.Write(out)

	return nil
}

func (h *Handler) getDBSystemStatus(q *req) error {
	d, err := h.Backend.GetDBSystemStatus(q.param(paramDBSystem))

	return reply(q, d, "", err)
}

func (h *Handler) generateChannelStatus(q *req) error {
	d, wr, err := h.Backend.GenerateChannelStatus(q.param("channelId"), q.r.Header.Get(headerRetry))

	return acceptedDoc(q, d, wr, err)
}

func (h *Handler) getChannelStatus(q *req) error {
	d, err := h.Backend.GetChannelStatus(q.param("channelId"))

	return reply(q, d, "", err)
}

func (h *Handler) generateHeatWaveEstimate(q *req) error {
	d, wr, err := h.Backend.GenerateHeatWaveEstimate(q.param(paramDBSystem), q.r.Header.Get(headerRetry))

	return acceptedDoc(q, d, wr, err)
}

func (h *Handler) getHeatWaveEstimate(q *req) error {
	d, err := h.Backend.GetHeatWaveEstimate(q.param(paramDBSystem))

	return reply(q, d, "", err)
}
