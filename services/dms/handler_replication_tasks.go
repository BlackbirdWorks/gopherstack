package dms

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type describeReplicationTasksInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTasksOutput struct {
	Marker           *string               `json:"Marker,omitempty"`
	ReplicationTasks []replicationTaskJSON `json:"ReplicationTasks"`
}

func (h *Handler) handleDescribeReplicationTasks(
	ctx context.Context, in *describeReplicationTasksInput,
) (*describeReplicationTasksOutput, error) {
	arnOrID := extractFilterValue(in.Filters, "replication-task-id", "replication-task-arn")
	list, err := h.Backend.DescribeReplicationTasks(ctx, arnOrID)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationTaskIdentifier < list[j].ReplicationTaskIdentifier
	})

	all := make([]replicationTaskJSON, 0, len(list))
	for _, rt := range list {
		all = append(all, rtToJSON(rt))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationTasksOutput{ReplicationTasks: data, Marker: nextMarker}, nil
}

type startReplicationTaskInput struct {
	ReplicationTaskArn       *string `json:"ReplicationTaskArn"`
	StartReplicationTaskType *string `json:"StartReplicationTaskType"`
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

	rt, err := h.Backend.StartReplicationTask(ctx, ptrconv.String(in.ReplicationTaskArn))
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
	ReplicationTaskIdentifier string `json:"ReplicationTaskIdentifier"`
	ReplicationTaskArn        string `json:"ReplicationTaskArn"`
	SourceEndpointArn         string `json:"SourceEndpointArn"`
	TargetEndpointArn         string `json:"TargetEndpointArn"`
	ReplicationInstanceArn    string `json:"ReplicationInstanceArn"`
	MigrationType             string `json:"MigrationType"`
	TableMappings             string `json:"TableMappings,omitempty"`
	ReplicationTaskSettings   string `json:"ReplicationTaskSettings,omitempty"`
	Status                    string `json:"Status"`
	// ReplicationTaskCreationDate is wire-encoded as epoch seconds
	// (awsjson1.1 unixTimestamp format) -- see pkgs/awstime.Epoch.
	ReplicationTaskCreationDate float64 `json:"ReplicationTaskCreationDate,omitempty"`
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
		ReplicationTaskCreationDate: awstime.Epoch(rt.CreationTime),
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
	if err := json.Unmarshal([]byte(tableMappings), &doc); err != nil {
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
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
	Marker               *string `json:"Marker"`
	MaxRecords           *int32  `json:"MaxRecords"`
	// Filters is accepted for wire-shape parity (real DescribeReplicationTableStatisticsInput
	// carries []types.Filter) but is never applied: ReplicationTableStatistics is always
	// empty in this emulation (see the handler doc below), so there is no per-table state
	// for a filter to narrow.
	Filters []filterEntry `json:"Filters"`
}

type describeReplicationTableStatisticsOutput struct {
	ReplicationConfigArn       string               `json:"ReplicationConfigArn,omitempty"`
	Marker                     *string              `json:"Marker,omitempty"`
	ReplicationTableStatistics []tableStatisticJSON `json:"ReplicationTableStatistics"`
}

// handleDescribeReplicationTableStatistics reports per-table statistics for a
// DMS Serverless replication config. ReplicationConfig carries no TableMappings
// state in this emulation (see models.go), so a config that exists but has no
// tracked mappings legitimately returns an empty list rather than borrowing
// data from an unrelated classic replication task.
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

	return &describeReplicationTableStatisticsOutput{
		ReplicationConfigArn:       found.ReplicationConfigArn,
		ReplicationTableStatistics: []tableStatisticJSON{},
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

	tasks, err := h.Backend.DescribeReplicationTasks(ctx, taskArn)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return &describeTableStatisticsOutput{
			ReplicationTaskArn: taskArn,
			TableStatistics:    []tableStatisticJSON{},
		}, nil
	}

	stats := buildTableStatistics(tasks[0].TableMappings)

	return &describeTableStatisticsOutput{
		ReplicationTaskArn: taskArn,
		TableStatistics:    stats,
	}, nil
}

type modifyReplicationTaskInput struct {
	ReplicationTaskArn      *string `json:"ReplicationTaskArn"`
	MigrationType           *string `json:"MigrationType"`
	TableMappings           *string `json:"TableMappings"`
	ReplicationTaskSettings *string `json:"ReplicationTaskSettings"`
}

type modifyReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleModifyReplicationTask(
	ctx context.Context, in *modifyReplicationTaskInput,
) (*modifyReplicationTaskOutput, error) {
	rt, err := h.Backend.ModifyReplicationTask(
		ctx,
		ptrconv.String(in.ReplicationTaskArn),
		ptrconv.String(in.MigrationType),
		ptrconv.String(in.TableMappings),
		ptrconv.String(in.ReplicationTaskSettings),
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

	rc, err := h.Backend.ReloadReplicationTables(ctx, configArn)
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

	rt, err := h.Backend.ReloadTables(ctx, taskArn)
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
