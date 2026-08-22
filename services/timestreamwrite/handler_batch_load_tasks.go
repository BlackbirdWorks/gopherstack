package timestreamwrite

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type dataSourceS3ConfigInput struct {
	BucketName      string `json:"BucketName"`
	ObjectKeyPrefix string `json:"ObjectKeyPrefix,omitempty"`
}

type dataSourceConfigInput struct {
	DataSourceS3Configuration *dataSourceS3ConfigInput `json:"DataSourceS3Configuration,omitempty"`
	DataFormat                string                   `json:"DataFormat,omitempty"`
}

type reportConfigInput struct {
	ReportS3Configuration *dataSourceS3ConfigInput `json:"ReportS3Configuration,omitempty"`
}

type dataModelS3ConfigInput struct {
	BucketName string `json:"BucketName,omitempty"`
	ObjectKey  string `json:"ObjectKey,omitempty"`
}

type dimensionMappingInput struct {
	SourceColumn      string `json:"SourceColumn,omitempty"`
	DestinationColumn string `json:"DestinationColumn,omitempty"`
}

type multiMeasureAttributeMappingInput struct {
	SourceColumn                    string `json:"SourceColumn,omitempty"`
	TargetMultiMeasureAttributeName string `json:"TargetMultiMeasureAttributeName,omitempty"`
	MeasureValueType                string `json:"MeasureValueType,omitempty"`
}

type multiMeasureMappingsInput struct {
	TargetMultiMeasureName        string                              `json:"TargetMultiMeasureName,omitempty"`
	MultiMeasureAttributeMappings []multiMeasureAttributeMappingInput `json:"MultiMeasureAttributeMappings,omitempty"`
}

type mixedMeasureMappingInput struct {
	MeasureName                   string                              `json:"MeasureName,omitempty"`
	SourceColumn                  string                              `json:"SourceColumn,omitempty"`
	TargetMeasureName             string                              `json:"TargetMeasureName,omitempty"`
	MeasureValueType              string                              `json:"MeasureValueType,omitempty"`
	MultiMeasureAttributeMappings []multiMeasureAttributeMappingInput `json:"MultiMeasureAttributeMappings,omitempty"`
}

type dataModelInput struct {
	MultiMeasureMappings *multiMeasureMappingsInput `json:"MultiMeasureMappings,omitempty"`
	MeasureNameColumn    string                     `json:"MeasureNameColumn,omitempty"`
	TimeColumn           string                     `json:"TimeColumn,omitempty"`
	TimeUnit             string                     `json:"TimeUnit,omitempty"`
	DimensionMappings    []dimensionMappingInput    `json:"DimensionMappings,omitempty"`
	MixedMeasureMappings []mixedMeasureMappingInput `json:"MixedMeasureMappings,omitempty"`
}

type dataModelConfigInput struct {
	DataModel                *dataModelInput         `json:"DataModel,omitempty"`
	DataModelS3Configuration *dataModelS3ConfigInput `json:"DataModelS3Configuration,omitempty"`
}

type createBatchLoadTaskInput struct {
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	DataModelConfiguration  *dataModelConfigInput  `json:"DataModelConfiguration,omitempty"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	ClientToken             string                 `json:"ClientToken"`
	RecordVersion           int64                  `json:"RecordVersion,omitempty"`
}

type createBatchLoadTaskOutput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskIDInput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskDescriptionView struct {
	ResumableUntil          *float64               `json:"ResumableUntil,omitempty"`
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	DataModelConfiguration  *dataModelConfigInput  `json:"DataModelConfiguration,omitempty"`
	ProgressReport          *progressReportView    `json:"ProgressReport,omitempty"`
	TaskID                  string                 `json:"TaskId"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	TaskStatus              string                 `json:"TaskStatus"`
	ErrorMessage            string                 `json:"ErrorMessage,omitempty"`
	CreationTime            float64                `json:"CreationTime"`
	LastUpdatedTime         float64                `json:"LastUpdatedTime"`
	RecordVersion           int64                  `json:"RecordVersion,omitempty"`
}

type describeBatchLoadTaskOutput struct {
	BatchLoadTaskDescription batchLoadTaskDescriptionView `json:"BatchLoadTaskDescription"`
}

type listBatchLoadTasksInput struct {
	NextToken  string `json:"NextToken"`
	TaskStatus string `json:"TaskStatus"`
	MaxResults int    `json:"MaxResults"`
}

type batchLoadTaskSummaryView struct {
	ResumableUntil  *float64 `json:"ResumableUntil,omitempty"`
	TaskID          string   `json:"TaskId"`
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	TaskStatus      string   `json:"TaskStatus"`
	CreationTime    float64  `json:"CreationTime"`
	LastUpdatedTime float64  `json:"LastUpdatedTime"`
}

type listBatchLoadTasksOutput struct {
	NextToken      string                     `json:"NextToken,omitempty"`
	BatchLoadTasks []batchLoadTaskSummaryView `json:"BatchLoadTasks"`
}

type progressReportView struct {
	BytesMetered            int64 `json:"BytesMetered,omitempty"`
	FileFailures            int64 `json:"FileFailures,omitempty"`
	ParseFailures           int64 `json:"ParseFailures,omitempty"`
	RecordIngestionFailures int64 `json:"RecordIngestionFailures,omitempty"`
	RecordsIngested         int64 `json:"RecordsIngested,omitempty"`
	RecordsProcessed        int64 `json:"RecordsProcessed,omitempty"`
}

func toMultiMeasureAttributeMappingViews(
	in []MultiMeasureAttributeMapping,
) []multiMeasureAttributeMappingInput {
	if len(in) == 0 {
		return nil
	}

	out := make([]multiMeasureAttributeMappingInput, 0, len(in))
	for _, m := range in {
		out = append(out, multiMeasureAttributeMappingInput(m))
	}

	return out
}

//nolint:dupl // mirrors dataModelFromInput's reverse (wire->backend) direction field-for-field
func toDataModelView(dm *DataModel) *dataModelInput {
	if dm == nil {
		return nil
	}

	v := &dataModelInput{
		MeasureNameColumn: dm.MeasureNameColumn,
		TimeColumn:        dm.TimeColumn,
		TimeUnit:          dm.TimeUnit,
	}

	if len(dm.DimensionMappings) > 0 {
		v.DimensionMappings = make([]dimensionMappingInput, 0, len(dm.DimensionMappings))
		for _, m := range dm.DimensionMappings {
			v.DimensionMappings = append(v.DimensionMappings, dimensionMappingInput(m))
		}
	}

	if len(dm.MixedMeasureMappings) > 0 {
		v.MixedMeasureMappings = make([]mixedMeasureMappingInput, 0, len(dm.MixedMeasureMappings))

		for _, m := range dm.MixedMeasureMappings {
			v.MixedMeasureMappings = append(v.MixedMeasureMappings, mixedMeasureMappingInput{
				MeasureName:                   m.MeasureName,
				SourceColumn:                  m.SourceColumn,
				TargetMeasureName:             m.TargetMeasureName,
				MeasureValueType:              m.MeasureValueType,
				MultiMeasureAttributeMappings: toMultiMeasureAttributeMappingViews(m.MultiMeasureAttributeMappings),
			})
		}
	}

	if dm.MultiMeasureMappings != nil {
		v.MultiMeasureMappings = &multiMeasureMappingsInput{
			TargetMultiMeasureName: dm.MultiMeasureMappings.TargetMultiMeasureName,
			MultiMeasureAttributeMappings: toMultiMeasureAttributeMappingViews(
				dm.MultiMeasureMappings.MultiMeasureAttributeMappings,
			),
		}
	}

	return v
}

func toDataModelConfigView(cfg *DataModelConfiguration) *dataModelConfigInput {
	if cfg == nil {
		return nil
	}

	v := &dataModelConfigInput{DataModel: toDataModelView(cfg.DataModel)}

	if cfg.DataModelS3Configuration != nil {
		v.DataModelS3Configuration = &dataModelS3ConfigInput{
			BucketName: cfg.DataModelS3Configuration.BucketName,
			ObjectKey:  cfg.DataModelS3Configuration.ObjectKey,
		}
	}

	return v
}

func multiMeasureAttributeMappingsFromInput(in []multiMeasureAttributeMappingInput) []MultiMeasureAttributeMapping {
	if len(in) == 0 {
		return nil
	}

	out := make([]MultiMeasureAttributeMapping, 0, len(in))
	for _, m := range in {
		out = append(out, MultiMeasureAttributeMapping(m))
	}

	return out
}

//nolint:dupl // mirrors toDataModelView's reverse (backend->wire) direction field-for-field
func dataModelFromInput(in *dataModelInput) *DataModel {
	if in == nil {
		return nil
	}

	dm := &DataModel{
		MeasureNameColumn: in.MeasureNameColumn,
		TimeColumn:        in.TimeColumn,
		TimeUnit:          in.TimeUnit,
	}

	if len(in.DimensionMappings) > 0 {
		dm.DimensionMappings = make([]DimensionMapping, 0, len(in.DimensionMappings))
		for _, m := range in.DimensionMappings {
			dm.DimensionMappings = append(dm.DimensionMappings, DimensionMapping(m))
		}
	}

	if len(in.MixedMeasureMappings) > 0 {
		dm.MixedMeasureMappings = make([]MixedMeasureMapping, 0, len(in.MixedMeasureMappings))

		for _, m := range in.MixedMeasureMappings {
			dm.MixedMeasureMappings = append(dm.MixedMeasureMappings, MixedMeasureMapping{
				MeasureName:                   m.MeasureName,
				SourceColumn:                  m.SourceColumn,
				TargetMeasureName:             m.TargetMeasureName,
				MeasureValueType:              m.MeasureValueType,
				MultiMeasureAttributeMappings: multiMeasureAttributeMappingsFromInput(m.MultiMeasureAttributeMappings),
			})
		}
	}

	if in.MultiMeasureMappings != nil {
		dm.MultiMeasureMappings = &MultiMeasureMappings{
			TargetMultiMeasureName: in.MultiMeasureMappings.TargetMultiMeasureName,
			MultiMeasureAttributeMappings: multiMeasureAttributeMappingsFromInput(
				in.MultiMeasureMappings.MultiMeasureAttributeMappings,
			),
		}
	}

	return dm
}

func dataModelConfigFromInput(in *dataModelConfigInput) *DataModelConfiguration {
	if in == nil {
		return nil
	}

	cfg := &DataModelConfiguration{DataModel: dataModelFromInput(in.DataModel)}

	if in.DataModelS3Configuration != nil {
		cfg.DataModelS3Configuration = &DataModelS3Configuration{
			BucketName: in.DataModelS3Configuration.BucketName,
			ObjectKey:  in.DataModelS3Configuration.ObjectKey,
		}
	}

	return cfg
}

func toBatchLoadTaskDescriptionView(task *BatchLoadTask) batchLoadTaskDescriptionView {
	v := batchLoadTaskDescriptionView{
		TaskID:             task.TaskID,
		TargetDatabaseName: task.TargetDatabaseName,
		TargetTableName:    task.TargetTableName,
		TaskStatus:         task.TaskStatus,
		CreationTime:       float64(task.CreationTime.Unix()),
		LastUpdatedTime:    float64(task.LastUpdatedTime.Unix()),
		ErrorMessage:       task.ErrorMessage,
		RecordVersion:      task.RecordVersion,
	}

	if task.ProgressReport != nil {
		v.ProgressReport = &progressReportView{
			BytesMetered:            task.ProgressReport.BytesMetered,
			FileFailures:            task.ProgressReport.FileFailures,
			ParseFailures:           task.ProgressReport.ParseFailures,
			RecordIngestionFailures: task.ProgressReport.RecordIngestionFailures,
			RecordsIngested:         task.ProgressReport.RecordsIngested,
			RecordsProcessed:        task.ProgressReport.RecordsProcessed,
		}
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	if task.DataSourceConfiguration != nil {
		cfg := &dataSourceConfigInput{
			DataFormat: task.DataSourceConfiguration.DataFormat,
		}

		if task.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := task.DataSourceConfiguration.DataSourceS3Configuration
			cfg.DataSourceS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}

		v.DataSourceConfiguration = cfg
	}

	if task.ReportConfiguration != nil {
		rpt := &reportConfigInput{}

		if task.ReportConfiguration.ReportS3Configuration != nil {
			s3 := task.ReportConfiguration.ReportS3Configuration
			rpt.ReportS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}

		v.ReportConfiguration = rpt
	}

	v.DataModelConfiguration = toDataModelConfigView(task.DataModelConfiguration)

	return v
}

func toBatchLoadTaskSummaryView(task *BatchLoadTask) batchLoadTaskSummaryView {
	v := batchLoadTaskSummaryView{
		TaskID:          task.TaskID,
		DatabaseName:    task.TargetDatabaseName,
		TableName:       task.TargetTableName,
		TaskStatus:      task.TaskStatus,
		CreationTime:    float64(task.CreationTime.Unix()),
		LastUpdatedTime: float64(task.LastUpdatedTime.Unix()),
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	return v
}

func (h *Handler) handleCreateBatchLoadTask(
	_ context.Context,
	in *createBatchLoadTaskInput,
) (*createBatchLoadTaskOutput, error) {
	if in.TargetDatabaseName == "" || in.TargetTableName == "" {
		return nil, fmt.Errorf("%w: TargetDatabaseName and TargetTableName are required", errInvalidRequest)
	}

	if in.DataSourceConfiguration == nil {
		return nil, fmt.Errorf("%w: DataSourceConfiguration is required", errInvalidRequest)
	}

	var dataSourceCfg *DataSourceConfiguration

	if in.DataSourceConfiguration != nil {
		dataSourceCfg = &DataSourceConfiguration{
			DataFormat: in.DataSourceConfiguration.DataFormat,
		}

		if in.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := in.DataSourceConfiguration.DataSourceS3Configuration
			dataSourceCfg.DataSourceS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}
	}

	var reportCfg *ReportConfiguration

	if in.ReportConfiguration != nil {
		reportCfg = &ReportConfiguration{}

		if in.ReportConfiguration.ReportS3Configuration != nil {
			s3 := in.ReportConfiguration.ReportS3Configuration
			reportCfg.ReportS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}
	}

	dataModelCfg := dataModelConfigFromInput(in.DataModelConfiguration)

	task, err := h.Backend.CreateBatchLoadTask(
		in.TargetDatabaseName, in.TargetTableName, dataSourceCfg, reportCfg, dataModelCfg, in.RecordVersion,
	)
	if err != nil {
		return nil, err
	}

	return &createBatchLoadTaskOutput{TaskID: task.TaskID}, nil
}

func (h *Handler) handleDescribeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*describeBatchLoadTaskOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	task, err := h.Backend.DescribeBatchLoadTask(in.TaskID)
	if err != nil {
		return nil, err
	}

	return &describeBatchLoadTaskOutput{
		BatchLoadTaskDescription: toBatchLoadTaskDescriptionView(task),
	}, nil
}

func (h *Handler) handleListBatchLoadTasks(
	_ context.Context,
	in *listBatchLoadTasksInput,
) (*listBatchLoadTasksOutput, error) {
	tasks := h.Backend.ListBatchLoadTasks(in.TaskStatus)
	pg := page.New(tasks, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]batchLoadTaskSummaryView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toBatchLoadTaskSummaryView(&pg.Data[i]))
	}

	return &listBatchLoadTasksOutput{BatchLoadTasks: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleResumeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*emptyOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	if err := h.Backend.ResumeBatchLoadTask(in.TaskID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
