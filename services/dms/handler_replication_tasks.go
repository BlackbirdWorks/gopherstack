package dms

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createReplicationTaskInput struct {
	ReplicationTaskIdentifier *string    `json:"ReplicationTaskIdentifier"`
	SourceEndpointArn         *string    `json:"SourceEndpointArn"`
	TargetEndpointArn         *string    `json:"TargetEndpointArn"`
	ReplicationInstanceArn    *string    `json:"ReplicationInstanceArn"`
	MigrationType             *string    `json:"MigrationType"`
	TableMappings             *string    `json:"TableMappings"`
	ReplicationTaskSettings   *string    `json:"ReplicationTaskSettings"`
	CdcStartPosition          *string    `json:"CdcStartPosition"`
	CdcStartTime              *float64   `json:"CdcStartTime"`
	CdcStopPosition           *string    `json:"CdcStopPosition"`
	TaskData                  *string    `json:"TaskData"`
	ResourceIdentifier        *string    `json:"ResourceIdentifier"`
	Tags                      []tagEntry `json:"Tags"`
}

type createReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleCreateReplicationTask(
	ctx context.Context, in *createReplicationTaskInput,
) (*createReplicationTaskOutput, error) {
	identifier := ptrconv.String(in.ReplicationTaskIdentifier)
	sourceEndpointArn := ptrconv.String(in.SourceEndpointArn)
	targetEndpointArn := ptrconv.String(in.TargetEndpointArn)
	replicationInstanceArn := ptrconv.String(in.ReplicationInstanceArn)
	migrationType := ptrconv.String(in.MigrationType)

	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationTaskIdentifier is required", ErrValidation)
	}

	if sourceEndpointArn == "" {
		return nil, fmt.Errorf("%w: SourceEndpointArn is required", ErrValidation)
	}

	if targetEndpointArn == "" {
		return nil, fmt.Errorf("%w: TargetEndpointArn is required", ErrValidation)
	}

	if replicationInstanceArn == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceArn is required", ErrValidation)
	}

	if migrationType == "" {
		return nil, fmt.Errorf("%w: MigrationType is required", ErrValidation)
	}

	if !isValidStartMigrationType(migrationType) {
		return nil, fmt.Errorf(
			"%w: invalid MigrationType %q; valid: full-load, cdc, full-load-and-cdc",
			ErrValidation,
			migrationType,
		)
	}

	if err := validateCdcStartExclusive(in.CdcStartPosition, in.CdcStartTime); err != nil {
		return nil, err
	}

	if err := validateTableMappings(ptrconv.String(in.TableMappings)); err != nil {
		return nil, err
	}

	kv := tagsToMap(in.Tags)
	rt, err := h.Backend.CreateReplicationTask(
		ctx,
		identifier,
		sourceEndpointArn,
		targetEndpointArn,
		replicationInstanceArn,
		migrationType,
		ptrconv.String(in.TableMappings),
		ptrconv.String(in.ReplicationTaskSettings),
		kv,
		ReplicationTaskCDCSettings{
			CdcStartPosition:   ptrconv.String(in.CdcStartPosition),
			CdcStopPosition:    ptrconv.String(in.CdcStopPosition),
			TaskData:           ptrconv.String(in.TaskData),
			ResourceIdentifier: ptrconv.String(in.ResourceIdentifier),
		},
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type describeReplicationTasksInput struct {
	Marker          *string       `json:"Marker"`
	MaxRecords      *int32        `json:"MaxRecords"`
	WithoutSettings *bool         `json:"WithoutSettings"`
	Filters         []filterEntry `json:"Filters"`
}

type describeReplicationTasksOutput struct {
	Marker           *string               `json:"Marker,omitempty"`
	ReplicationTasks []replicationTaskJSON `json:"ReplicationTasks"`
}

func (h *Handler) handleDescribeReplicationTasks(
	ctx context.Context, in *describeReplicationTasksInput,
) (*describeReplicationTasksOutput, error) {
	list, err := h.Backend.DescribeReplicationTasks(ctx, newDescribeFilters(in.Filters))
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationTaskIdentifier < list[j].ReplicationTaskIdentifier
	})

	withoutSettings := ptrconv.Bool(in.WithoutSettings)

	all := make([]replicationTaskJSON, 0, len(list))
	for _, rt := range list {
		item := rtToJSON(rt)
		if withoutSettings {
			item.ReplicationTaskSettings = ""
		}

		all = append(all, item)
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationTasksOutput{ReplicationTasks: data, Marker: nextMarker}, nil
}

type startReplicationTaskInput struct {
	ReplicationTaskArn       *string  `json:"ReplicationTaskArn"`
	StartReplicationTaskType *string  `json:"StartReplicationTaskType"`
	CdcStartPosition         *string  `json:"CdcStartPosition"`
	CdcStartTime             *float64 `json:"CdcStartTime"`
	CdcStopPosition          *string  `json:"CdcStopPosition"`
}

// validateCdcStartExclusive enforces the documented rule that CdcStartPosition
// and CdcStartTime can't both be specified.
func validateCdcStartExclusive(position *string, startTime *float64) error {
	if ptrconv.String(position) != "" && startTime != nil {
		return fmt.Errorf("%w: CdcStartPosition and CdcStartTime can't both be specified", ErrValidation)
	}

	return nil
}

type startReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func isValidStartReplicationTaskType(s string) bool {
	return s == "start-replication" || s == "resume-processing" || s == "reload-target"
}

func isValidStartMigrationType(s string) bool {
	return s == "full-load" || s == "cdc" || s == "full-load-and-cdc"
}

func (h *Handler) handleStartReplicationTask(
	ctx context.Context, in *startReplicationTaskInput,
) (*startReplicationTaskOutput, error) {
	taskType := ptrconv.String(in.StartReplicationTaskType)
	if taskType == "" {
		taskType = "start-replication"
	}

	if !isValidStartReplicationTaskType(taskType) {
		return nil, fmt.Errorf(
			"%w: invalid StartReplicationTaskType %q; valid: start-replication, resume-processing, reload-target",
			ErrValidation,
			taskType,
		)
	}

	if err := validateCdcStartExclusive(in.CdcStartPosition, in.CdcStartTime); err != nil {
		return nil, err
	}

	rt, err := h.Backend.StartReplicationTask(ctx, ptrconv.String(in.ReplicationTaskArn), ReplicationTaskCDCSettings{
		CdcStartPosition: ptrconv.String(in.CdcStartPosition),
		CdcStopPosition:  ptrconv.String(in.CdcStopPosition),
		StartType:        taskType,
	})
	if err != nil {
		return nil, err
	}

	return &startReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type stopReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type stopReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleStopReplicationTask(
	ctx context.Context, in *stopReplicationTaskInput,
) (*stopReplicationTaskOutput, error) {
	rt, err := h.Backend.StopReplicationTask(ctx, ptrconv.String(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &stopReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type deleteReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type deleteReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleDeleteReplicationTask(
	ctx context.Context, in *deleteReplicationTaskInput,
) (*deleteReplicationTaskOutput, error) {
	rt, err := h.Backend.DeleteReplicationTask(ctx, ptrconv.String(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &deleteReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type replicationTaskJSON struct {
	ReplicationTaskStats        *replicationTaskStatsJSON `json:"ReplicationTaskStats,omitempty"`
	Status                      string                    `json:"Status"`
	CdcStopPosition             string                    `json:"CdcStopPosition,omitempty"`
	TargetEndpointArn           string                    `json:"TargetEndpointArn"`
	ReplicationInstanceArn      string                    `json:"ReplicationInstanceArn"`
	MigrationType               string                    `json:"MigrationType"`
	TableMappings               string                    `json:"TableMappings,omitempty"`
	ReplicationTaskSettings     string                    `json:"ReplicationTaskSettings,omitempty"`
	ReplicationTaskIdentifier   string                    `json:"ReplicationTaskIdentifier"`
	SourceEndpointArn           string                    `json:"SourceEndpointArn"`
	TaskData                    string                    `json:"TaskData,omitempty"`
	CdcStartPosition            string                    `json:"CdcStartPosition,omitempty"`
	StopReason                  string                    `json:"StopReason,omitempty"`
	LastFailureMessage          string                    `json:"LastFailureMessage,omitempty"`
	RecoveryCheckpoint          string                    `json:"RecoveryCheckpoint,omitempty"`
	ReplicationTaskArn          string                    `json:"ReplicationTaskArn"`
	ReplicationTaskStartDate    float64                   `json:"ReplicationTaskStartDate,omitempty"`
	ReplicationTaskCreationDate float64                   `json:"ReplicationTaskCreationDate,omitempty"`
}

const percentComplete = 100

// replicationTaskStatsJSON mirrors types.ReplicationTaskStats; dates are epoch seconds.
type replicationTaskStatsJSON struct {
	ElapsedTimeMillis       int64   `json:"ElapsedTimeMillis"`
	FreshStartDate          float64 `json:"FreshStartDate,omitempty"`
	FullLoadFinishDate      float64 `json:"FullLoadFinishDate,omitempty"`
	FullLoadStartDate       float64 `json:"FullLoadStartDate,omitempty"`
	StartDate               float64 `json:"StartDate,omitempty"`
	StopDate                float64 `json:"StopDate,omitempty"`
	FullLoadProgressPercent int32   `json:"FullLoadProgressPercent"`
	TablesErrored           int32   `json:"TablesErrored"`
	TablesLoaded            int32   `json:"TablesLoaded"`
	TablesLoading           int32   `json:"TablesLoading"`
	TablesQueued            int32   `json:"TablesQueued"`
}

func taskStats(rt *ReplicationTask) *replicationTaskStatsJSON {
	return runStats(rt.StartDate, rt.StopDate, rt.FullLoadStartDate, rt.FullLoadFinishDate, rt.ElapsedTimeMillis,
		rt.TableStats)
}

// runStats derives the statistics members shared by ReplicationTaskStats and ReplicationStats.
func runStats(
	start, stop, flStart, flFinish time.Time, elapsed int64, tables []TableStatistic,
) *replicationTaskStatsJSON {
	if start.IsZero() {
		return nil
	}

	st := &replicationTaskStatsJSON{
		ElapsedTimeMillis:  elapsed,
		FreshStartDate:     awstime.Epoch(start),
		FullLoadStartDate:  awstime.Epoch(flStart),
		FullLoadFinishDate: awstime.Epoch(flFinish),
		StartDate:          awstime.Epoch(start),
		StopDate:           awstime.Epoch(stop),
	}

	for _, ts := range tables {
		switch ts.TableState {
		case "Table completed":
			st.TablesLoaded++
		case "Table error":
			st.TablesErrored++
		case "Full load", "Table is being reloaded":
			st.TablesLoading++
		default:
			st.TablesQueued++
		}
	}

	switch total := len(tables); {
	case !flFinish.IsZero():
		st.FullLoadProgressPercent = percentComplete
	case total > 0:
		st.FullLoadProgressPercent = int32(int(st.TablesLoaded) * percentComplete / total) //nolint:gosec // 0..100
	}

	return st
}

func rtToJSON(rt *ReplicationTask) replicationTaskJSON {
	return replicationTaskJSON{
		ReplicationTaskIdentifier:   rt.ReplicationTaskIdentifier,
		ReplicationTaskArn:          rt.ReplicationTaskArn,
		SourceEndpointArn:           rt.SourceEndpointArn,
		TargetEndpointArn:           rt.TargetEndpointArn,
		ReplicationInstanceArn:      rt.ReplicationInstanceArn,
		MigrationType:               rt.MigrationType,
		TableMappings:               rt.TableMappings,
		ReplicationTaskSettings:     rt.ReplicationTaskSettings,
		Status:                      rt.Status,
		CdcStartPosition:            rt.CdcStartPosition,
		CdcStopPosition:             rt.CdcStopPosition,
		TaskData:                    rt.TaskData,
		StopReason:                  rt.StopReason,
		LastFailureMessage:          rt.LastFailureMessage,
		RecoveryCheckpoint:          rt.RecoveryCheckpoint,
		ReplicationTaskCreationDate: awstime.Epoch(rt.CreationTime),
		ReplicationTaskStartDate:    awstime.Epoch(rt.StartDate),
		ReplicationTaskStats:        taskStats(rt),
	}
}

// tableStatisticJSON represents a single table statistic entry.
type tableStatisticJSON struct {
	SchemaName                   string `json:"SchemaName"`
	TableName                    string `json:"TableName"`
	ValidationState              string `json:"ValidationState"`
	TableState                   string `json:"TableState"`
	FullLoadRows                 int64  `json:"FullLoadRows"`
	FullLoadCondtnlChkFailedRows int64  `json:"FullLoadCondtnlChkFailedRows"`
	FullLoadErrorRows            int64  `json:"FullLoadErrorRows"`
	ValidationPendingRecords     int64  `json:"ValidationPendingRecords"`
	ValidationFailedRecords      int64  `json:"ValidationFailedRecords"`
	ValidationSuspendedRecords   int64  `json:"ValidationSuspendedRecords"`
	Inserts                      int64  `json:"Inserts"`
	Updates                      int64  `json:"Updates"`
	Deletes                      int64  `json:"Deletes"`
	Ddls                         int64  `json:"Ddls"`
	AppliedInserts               int64  `json:"AppliedInserts"`
	AppliedUpdates               int64  `json:"AppliedUpdates"`
	AppliedDeletes               int64  `json:"AppliedDeletes"`
	AppliedDdls                  int64  `json:"AppliedDdls"`
	FullLoadReloaded             bool   `json:"FullLoadReloaded"`
	// Times are epoch seconds on the awsjson1.1 wire.
	FullLoadStartTime float64 `json:"FullLoadStartTime,omitempty"`
	FullLoadEndTime   float64 `json:"FullLoadEndTime,omitempty"`
	LastUpdateTime    float64 `json:"LastUpdateTime,omitempty"`
}

func statJSON(ts TableStatistic) tableStatisticJSON {
	return tableStatisticJSON{
		SchemaName: ts.SchemaName, TableName: ts.TableName, TableState: ts.TableState,
		ValidationState: "Not enabled", FullLoadRows: ts.FullLoadRows, FullLoadErrorRows: ts.FullLoadErrorRows,
		Inserts: ts.Inserts, Updates: ts.Updates, Deletes: ts.Deletes,
		AppliedInserts: ts.Inserts, AppliedUpdates: ts.Updates, AppliedDeletes: ts.Deletes,
		FullLoadReloaded:  ts.FullLoadReloaded,
		FullLoadStartTime: awstime.Epoch(ts.FullLoadStartTime), FullLoadEndTime: awstime.Epoch(ts.FullLoadEndTime),
		LastUpdateTime: awstime.Epoch(ts.LastUpdateTime),
	}
}

// tableMappingRule is used to parse table mappings JSON.
type tableMappingRule struct {
	SchemaName string `json:"schema-name"`
	TableName  string `json:"table-name"`
	RuleType   string `json:"rule-type"`
}

// tableMappings is the top-level table mappings structure.
type tableMappingsDoc struct {
	Rules []tableMappingRule `json:"rules"`
}

// buildTableStatistics parses TableMappings JSON and returns mock table statistics.
func buildTableStatistics(tableMappings string) []tableStatisticJSON {
	if tableMappings == "" {
		return []tableStatisticJSON{}
	}

	var doc tableMappingsDoc
	if err := json.Unmarshal([]byte(tableMappings), &doc, jsonx.V1()); err != nil {
		return []tableStatisticJSON{}
	}

	stats := make([]tableStatisticJSON, 0, len(doc.Rules))
	for _, rule := range doc.Rules {
		if rule.RuleType == "selection" || rule.RuleType == "" {
			stats = append(stats, tableStatisticJSON{
				SchemaName:      rule.SchemaName,
				TableName:       rule.TableName,
				ValidationState: "Not enabled",
				TableState:      "Not started",
			})
		}
	}

	return stats
}

type describeReplicationTableStatisticsInput struct {
	ReplicationConfigArn *string       `json:"ReplicationConfigArn"`
	Marker               *string       `json:"Marker"`
	MaxRecords           *int32        `json:"MaxRecords"`
	Filters              []filterEntry `json:"Filters"`
}

type describeReplicationTableStatisticsOutput struct {
	ReplicationConfigArn       string               `json:"ReplicationConfigArn,omitempty"`
	Marker                     *string              `json:"Marker,omitempty"`
	ReplicationTableStatistics []tableStatisticJSON `json:"ReplicationTableStatistics"`
}

// handleDescribeReplicationTableStatistics reports per-table statistics of the config's engine run; a config that
// has not run reports none.
func (h *Handler) handleDescribeReplicationTableStatistics(
	ctx context.Context, in *describeReplicationTableStatisticsInput,
) (*describeReplicationTableStatisticsOutput, error) {
	configArn := ptrconv.String(in.ReplicationConfigArn)

	configs, err := h.Backend.DescribeReplicationConfigs(ctx)
	if err != nil {
		return nil, err
	}

	var found *ReplicationConfig
	for _, rc := range configs {
		if rc.ReplicationConfigArn == configArn || rc.ReplicationConfigIdentifier == configArn {
			found = rc

			break
		}
	}

	if found == nil {
		return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, configArn)
	}

	df := newDescribeFilters(in.Filters)
	stats := make([]tableStatisticJSON, 0, len(found.TableStats))

	for _, ts := range found.TableStats {
		s := statJSON(ts)
		if df.Matches("schema-name", s.SchemaName) && df.Matches("table-name", s.TableName) &&
			df.Matches("table-state", s.TableState) {
			stats = append(stats, s)
		}
	}

	data, nextMarker := dmsPaginate(stats, in.Marker, in.MaxRecords)

	return &describeReplicationTableStatisticsOutput{
		ReplicationConfigArn:       found.ReplicationConfigArn,
		ReplicationTableStatistics: data,
		Marker:                     nextMarker,
	}, nil
}

type describeTableStatisticsInput struct {
	ReplicationTaskArn *string       `json:"ReplicationTaskArn"`
	Marker             *string       `json:"Marker"`
	MaxRecords         *int32        `json:"MaxRecords"`
	Filters            []filterEntry `json:"Filters"`
}

type describeTableStatisticsOutput struct {
	ReplicationTaskArn string               `json:"ReplicationTaskArn,omitempty"`
	Marker             *string              `json:"Marker,omitempty"`
	TableStatistics    []tableStatisticJSON `json:"TableStatistics"`
}

func (h *Handler) handleDescribeTableStatistics(
	ctx context.Context, in *describeTableStatisticsInput,
) (*describeTableStatisticsOutput, error) {
	taskArn := ptrconv.String(in.ReplicationTaskArn)

	tasks, err := h.Backend.DescribeReplicationTasks(ctx, NewIdentifierFilter("replication-task-arn", taskArn))
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return &describeTableStatisticsOutput{
			ReplicationTaskArn: taskArn,
			TableStatistics:    []tableStatisticJSON{},
		}, nil
	}

	var all []tableStatisticJSON
	if len(tasks[0].TableStats) > 0 {
		for _, ts := range tasks[0].TableStats {
			all = append(all, statJSON(ts))
		}
	} else {
		all = buildTableStatistics(tasks[0].TableMappings)
	}

	df := newDescribeFilters(in.Filters)

	stats := make([]tableStatisticJSON, 0, len(all))
	for _, s := range all {
		if !df.Matches("schema-name", s.SchemaName) {
			continue
		}

		if !df.Matches("table-name", s.TableName) {
			continue
		}

		if !df.Matches("table-state", s.TableState) {
			continue
		}

		stats = append(stats, s)
	}

	data, nextMarker := dmsPaginate(stats, in.Marker, in.MaxRecords)

	return &describeTableStatisticsOutput{
		ReplicationTaskArn: taskArn,
		TableStatistics:    data,
		Marker:             nextMarker,
	}, nil
}

type modifyReplicationTaskInput struct {
	ReplicationTaskArn        *string  `json:"ReplicationTaskArn"`
	ReplicationTaskIdentifier *string  `json:"ReplicationTaskIdentifier"`
	MigrationType             *string  `json:"MigrationType"`
	TableMappings             *string  `json:"TableMappings"`
	ReplicationTaskSettings   *string  `json:"ReplicationTaskSettings"`
	CdcStartPosition          *string  `json:"CdcStartPosition"`
	CdcStartTime              *float64 `json:"CdcStartTime"`
	CdcStopPosition           *string  `json:"CdcStopPosition"`
	TaskData                  *string  `json:"TaskData"`
}

type modifyReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleModifyReplicationTask(
	ctx context.Context, in *modifyReplicationTaskInput,
) (*modifyReplicationTaskOutput, error) {
	if err := validateCdcStartExclusive(in.CdcStartPosition, in.CdcStartTime); err != nil {
		return nil, err
	}

	rt, err := h.Backend.ModifyReplicationTask(
		ctx,
		ptrconv.String(in.ReplicationTaskArn),
		ptrconv.String(in.MigrationType),
		ptrconv.String(in.TableMappings),
		ptrconv.String(in.ReplicationTaskSettings),
		ReplicationTaskCDCSettings{
			CdcStartPosition: ptrconv.String(in.CdcStartPosition),
			CdcStopPosition:  ptrconv.String(in.CdcStopPosition),
			TaskData:         ptrconv.String(in.TaskData),
			NewIdentifier:    ptrconv.String(in.ReplicationTaskIdentifier),
		},
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type moveReplicationTaskInput struct {
	ReplicationTaskArn           *string `json:"ReplicationTaskArn"`
	TargetReplicationInstanceArn *string `json:"TargetReplicationInstanceArn"`
}

type moveReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleMoveReplicationTask(
	ctx context.Context, in *moveReplicationTaskInput,
) (*moveReplicationTaskOutput, error) {
	rt, err := h.Backend.MoveReplicationTask(
		ctx,
		ptrconv.String(in.ReplicationTaskArn),
		ptrconv.String(in.TargetReplicationInstanceArn),
	)
	if err != nil {
		return nil, err
	}

	return &moveReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

// reloadOption is used on both ReloadTables and ReloadReplicationTables.
func isValidReloadOption(s string) bool {
	return s == "" || s == "data-reload" || s == "validate-only"
}

// reloadReplicationTablesInput mirrors ReloadReplicationTablesInput: the
// resource identifier field is ReplicationConfigArn (a DMS Serverless
// replication config), NOT ReplicationTaskArn -- a previous implementation
// used the wrong field name, which silently discarded the real ARN sent by
// SDK clients targeting a serverless replication.
type reloadReplicationTablesInput struct {
	ReplicationConfigArn *string          `json:"ReplicationConfigArn"`
	ReloadOption         *string          `json:"ReloadOption"`
	TablesToReload       []map[string]any `json:"TablesToReload"`
}

type reloadReplicationTablesOutput struct {
	ReplicationConfigArn string `json:"ReplicationConfigArn"`
}

func (h *Handler) handleReloadReplicationTables(
	ctx context.Context, in *reloadReplicationTablesInput,
) (*reloadReplicationTablesOutput, error) {
	configArn := ptrconv.String(in.ReplicationConfigArn)
	if configArn == "" {
		return nil, fmt.Errorf("%w: ReplicationConfigArn is required", ErrValidation)
	}

	if len(in.TablesToReload) == 0 {
		return nil, fmt.Errorf("%w: TablesToReload is required", ErrValidation)
	}

	reloadOption := ptrconv.String(in.ReloadOption)
	if !isValidReloadOption(reloadOption) {
		return nil, fmt.Errorf(
			"%w: invalid ReloadOption %q; valid: data-reload, validate-only",
			ErrValidation,
			reloadOption,
		)
	}

	rc, err := h.Backend.ReloadReplicationTables(ctx, configArn, tableRefs(in.TablesToReload))
	if err != nil {
		return nil, err
	}

	return &reloadReplicationTablesOutput{ReplicationConfigArn: rc.ReplicationConfigArn}, nil
}

type reloadTablesInput struct {
	ReplicationTaskArn *string          `json:"ReplicationTaskArn"`
	ReloadOption       *string          `json:"ReloadOption"`
	TablesToReload     []map[string]any `json:"TablesToReload"`
}

func tableRefs(raw []map[string]any) []dmsengine.TableRef {
	out := make([]dmsengine.TableRef, 0, len(raw))

	for _, m := range raw {
		schema, _ := m["SchemaName"].(string)
		table, _ := m["TableName"].(string)
		out = append(out, dmsengine.TableRef{Schema: schema, Name: table})
	}

	return out
}

type reloadTablesOutput struct {
	ReplicationTaskArn string `json:"ReplicationTaskArn"`
}

func (h *Handler) handleReloadTables(
	ctx context.Context, in *reloadTablesInput,
) (*reloadTablesOutput, error) {
	taskArn := ptrconv.String(in.ReplicationTaskArn)
	if taskArn == "" {
		return nil, fmt.Errorf("%w: ReplicationTaskArn is required", ErrValidation)
	}

	if len(in.TablesToReload) == 0 {
		return nil, fmt.Errorf("%w: TablesToReload is required", ErrValidation)
	}

	reloadOption := ptrconv.String(in.ReloadOption)
	if !isValidReloadOption(reloadOption) {
		return nil, fmt.Errorf(
			"%w: invalid ReloadOption %q; valid: data-reload, validate-only",
			ErrValidation,
			reloadOption,
		)
	}

	rt, err := h.Backend.ReloadTables(ctx, taskArn, tableRefs(in.TablesToReload))
	if err != nil {
		return nil, err
	}

	return &reloadTablesOutput{ReplicationTaskArn: rt.ReplicationTaskArn}, nil
}

type startReplicationTaskAssessmentInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type startReplicationTaskAssessmentOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

// opsReplicationTasks returns the dispatch-table entries for the replication_tasks operation family.
func (h *Handler) opsReplicationTasks() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateReplicationTask: service.WrapOp(
			h.handleCreateReplicationTask,
		),
		opDescribeReplicationTasks: service.WrapOp(
			h.handleDescribeReplicationTasks,
		),
		opStartReplicationTask: service.WrapOp(
			h.handleStartReplicationTask,
		),
		opStopReplicationTask: service.WrapOp(h.handleStopReplicationTask),
		opDeleteReplicationTask: service.WrapOp(
			h.handleDeleteReplicationTask,
		),
		opDescribeReplicationTableStatistics: service.WrapOp(
			h.handleDescribeReplicationTableStatistics,
		),
		opDescribeTableStatistics: service.WrapOp(
			h.handleDescribeTableStatistics,
		),
		opModifyReplicationTask: service.WrapOp(
			h.handleModifyReplicationTask,
		),
		opMoveReplicationTask: service.WrapOp(h.handleMoveReplicationTask),
		opReloadReplicationTables: service.WrapOp(
			h.handleReloadReplicationTables,
		),
		opReloadTables: service.WrapOp(h.handleReloadTables),
	}
}
