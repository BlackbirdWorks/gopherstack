package glue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// awserrFromDetail converts an ErrorDetail to an error wrapping ErrNotFound.
func awserrFromDetail(d ErrorDetail) error {
	return awserr.New(d.ErrorCode+": "+d.ErrorMessage, awserr.ErrNotFound)
}

// This file contains stub implementations for Glue operations acknowledged in
// the SDK completeness test. Each stub returns a valid empty response (HTTP 200).

// batchGetJobsInput holds input for BatchGetJobs.
type batchGetJobsInput struct {
	JobNames []string `json:"JobNames"`
}

// batchGetJobsOutput holds the result for BatchGetJobs.
type batchGetJobsOutput struct {
	Jobs         []*Job   `json:"Jobs"`
	JobsNotFound []string `json:"JobsNotFound"`
}

func (h *Handler) handleBatchGetJobs(
	_ context.Context,
	in *batchGetJobsInput,
) (*batchGetJobsOutput, error) {
	var found []*Job
	var missing []string

	for _, name := range in.JobNames {
		j, err := h.Backend.GetJob(name)
		if err != nil {
			missing = append(missing, name)
		} else {
			found = append(found, j)
		}
	}

	return &batchGetJobsOutput{Jobs: found, JobsNotFound: missing}, nil
}

// batchGetPartitionInput holds input for BatchGetPartition.
type batchGetPartitionInput struct {
	DatabaseName    string               `json:"DatabaseName"`
	TableName       string               `json:"TableName"`
	PartitionsToGet []PartitionValueList `json:"PartitionsToGet"`
}

// batchGetPartitionOutput holds the result for BatchGetPartition.
type batchGetPartitionOutput struct {
	Partitions      []*Partition `json:"Partitions"`
	UnprocessedKeys []any        `json:"UnprocessedKeys"`
}

// handleBatchGetPartition validates that the referenced database/table exist
// before returning. AWS Glue returns an EntityNotFoundException when the
// table is missing rather than silently returning an empty list. The mock
// backend has no partition storage, so on success the response remains empty.
func (h *Handler) handleBatchGetPartition(
	_ context.Context,
	in *batchGetPartitionInput,
) (*batchGetPartitionOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	if _, err := h.Backend.GetTable(in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}

	return &batchGetPartitionOutput{Partitions: []*Partition{}, UnprocessedKeys: []any{}}, nil
}

// batchGetTableOptimizerInput holds input for BatchGetTableOptimizer.
type batchGetTableOptimizerInput struct {
	Entries []BatchGetTableOptimizerEntry `json:"Entries"`
}

// batchGetTableOptimizerOutput holds the result for BatchGetTableOptimizer.
type batchGetTableOptimizerOutput struct {
	TableOptimizers []*TableOptimizer             `json:"TableOptimizers"`
	Failures        []BatchGetTableOptimizerError `json:"Failures"`
}

func (h *Handler) handleBatchGetTableOptimizer(
	_ context.Context,
	in *batchGetTableOptimizerInput,
) (*batchGetTableOptimizerOutput, error) {
	found, errs := h.Backend.BatchGetTableOptimizer(in.Entries)
	if found == nil {
		found = []*TableOptimizer{}
	}
	if errs == nil {
		errs = []BatchGetTableOptimizerError{}
	}

	return &batchGetTableOptimizerOutput{TableOptimizers: found, Failures: errs}, nil
}

// batchGetTriggersInput holds input for BatchGetTriggers.
type batchGetTriggersInput struct {
	TriggerNames []string `json:"TriggerNames"`
}

// batchGetTriggersOutput holds the result for BatchGetTriggers.
type batchGetTriggersOutput struct {
	Triggers         []*Trigger `json:"Triggers"`
	TriggersNotFound []string   `json:"TriggersNotFound"`
}

func (h *Handler) handleBatchGetTriggers(
	_ context.Context,
	in *batchGetTriggersInput,
) (*batchGetTriggersOutput, error) {
	found, missing := h.Backend.BatchGetTriggers(in.TriggerNames)

	return &batchGetTriggersOutput{Triggers: found, TriggersNotFound: missing}, nil
}

// batchGetWorkflowsInput holds input for BatchGetWorkflows.
type batchGetWorkflowsInput struct {
	Names []string `json:"Names"`
}

// batchGetWorkflowsOutput holds the result for BatchGetWorkflows.
type batchGetWorkflowsOutput struct {
	Workflows        []*Workflow `json:"Workflows"`
	MissingWorkflows []string    `json:"MissingWorkflows"`
}

func (h *Handler) handleBatchGetWorkflows(
	_ context.Context,
	in *batchGetWorkflowsInput,
) (*batchGetWorkflowsOutput, error) {
	found, missing := h.Backend.BatchGetWorkflows(in.Names)

	return &batchGetWorkflowsOutput{Workflows: found, MissingWorkflows: missing}, nil
}

// batchPutDataQualityStatisticAnnotationInput holds input for BatchPutDataQualityStatisticAnnotation.
type batchPutDataQualityStatisticAnnotationInput struct{}

// batchPutDataQualityStatisticAnnotationOutput holds the result for BatchPutDataQualityStatisticAnnotation.
type batchPutDataQualityStatisticAnnotationOutput struct {
	FailedEntries []any `json:"FailedEntries"`
}

func (h *Handler) handleBatchPutDataQualityStatisticAnnotation(
	_ context.Context,
	_ *batchPutDataQualityStatisticAnnotationInput,
) (*batchPutDataQualityStatisticAnnotationOutput, error) {
	return &batchPutDataQualityStatisticAnnotationOutput{FailedEntries: []any{}}, nil
}

// batchUpdatePartitionEntry is a single entry in a BatchUpdatePartition request.
type batchUpdatePartitionEntry struct {
	PartitionValueList []string       `json:"PartitionValueList"`
	PartitionInput     PartitionInput `json:"PartitionInput"`
}

// batchUpdatePartitionInput holds input for BatchUpdatePartition.
type batchUpdatePartitionInput struct {
	DatabaseName string                      `json:"DatabaseName"`
	TableName    string                      `json:"TableName"`
	Entries      []batchUpdatePartitionEntry `json:"Entries"`
}

// batchUpdatePartitionError holds per-entry error detail for BatchUpdatePartition.
type batchUpdatePartitionError struct {
	ErrorDetail        ErrorDetail `json:"ErrorDetail"`
	PartitionValueList []string    `json:"PartitionValueList"`
}

// batchUpdatePartitionOutput holds the result for BatchUpdatePartition.
type batchUpdatePartitionOutput struct {
	Errors []batchUpdatePartitionError `json:"Errors"`
}

func (h *Handler) handleBatchUpdatePartition(
	_ context.Context,
	in *batchUpdatePartitionInput,
) (*batchUpdatePartitionOutput, error) {
	errs := make([]batchUpdatePartitionError, 0, len(in.Entries))

	for _, entry := range in.Entries {
		if err := h.Backend.UpdatePartition(
			in.DatabaseName, in.TableName,
			entry.PartitionValueList, entry.PartitionInput,
		); err != nil {
			errs = append(errs, batchUpdatePartitionError{
				PartitionValueList: entry.PartitionValueList,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: err.Error(),
				},
			})
		}
	}

	return &batchUpdatePartitionOutput{Errors: errs}, nil
}

// cancelDataQualityRuleRecommendationRunInput holds input for CancelDataQualityRuleRecommendationRun.
type cancelDataQualityRuleRecommendationRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleCancelDataQualityRuleRecommendationRun(
	_ context.Context,
	in *cancelDataQualityRuleRecommendationRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CancelDataQualityRuleRecommendationRun(in.RunID)
}

// cancelMLTaskRunInput holds input for CancelMLTaskRun.
type cancelMLTaskRunInput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
}

func (h *Handler) handleCancelMLTaskRun(
	_ context.Context,
	in *cancelMLTaskRunInput,
) (*emptyOutput, error) {
	if in.TransformID == "" || in.TaskRunID == "" {
		return nil, fmt.Errorf("%w: TransformId and TaskRunId are required", ErrValidation)
	}

	return &emptyOutput{}, h.Backend.CancelMLTaskRun(in.TransformID, in.TaskRunID)
}

// cancelStatementInput holds input for CancelStatement.
type cancelStatementInput struct {
	SessionID   string `json:"SessionId"`
	StatementID int32  `json:"Id"`
}

func (h *Handler) handleCancelStatement(
	_ context.Context,
	in *cancelStatementInput,
) (*emptyOutput, error) {
	if in.SessionID == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	return &emptyOutput{}, h.Backend.CancelStatement(in.SessionID, in.StatementID)
}

// checkSchemaVersionValidityInput holds input for CheckSchemaVersionValidity.
type checkSchemaVersionValidityInput struct{}

// checkSchemaVersionValidityOutput holds the result for CheckSchemaVersionValidity.
type checkSchemaVersionValidityOutput struct {
	Error string `json:"Error,omitempty"`
	Valid bool   `json:"Valid"`
}

func (h *Handler) handleCheckSchemaVersionValidity(
	_ context.Context,
	_ *checkSchemaVersionValidityInput,
) (*checkSchemaVersionValidityOutput, error) {
	return &checkSchemaVersionValidityOutput{Valid: true}, nil
}

// createBlueprintInput holds input for CreateBlueprint.
type createBlueprintInput struct {
	Name string `json:"Name"`
}

// createBlueprintOutput holds the result for CreateBlueprint.
type createBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateBlueprint(
	_ context.Context,
	in *createBlueprintInput,
) (*createBlueprintOutput, error) {
	if err := h.Backend.CreateBlueprint(in.Name); err != nil {
		return nil, err
	}

	return &createBlueprintOutput{Name: in.Name}, nil
}

// createCatalogInput holds input for CreateCatalog.
type createCatalogInput struct {
	CatalogInput struct {
		Parameters  map[string]string `json:"Parameters,omitzero"`
		Name        string            `json:"Name,omitempty"`
		Description string            `json:"Description,omitempty"`
	} `json:"CatalogInput"`
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleCreateCatalog(
	_ context.Context,
	in *createCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateCatalog(
		in.CatalogID,
		in.CatalogInput.Name,
		in.CatalogInput.Description,
		in.CatalogInput.Parameters,
	)
}

// createClassifierInput holds input for CreateClassifier.
type createClassifierInput struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JSONClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}

func (h *Handler) handleCreateClassifier(
	_ context.Context,
	in *createClassifierInput,
) (*emptyOutput, error) {
	c := Classifier{
		GrokClassifier: in.GrokClassifier,
		XMLClassifier:  in.XMLClassifier,
		JSONClassifier: in.JSONClassifier,
		CsvClassifier:  in.CsvClassifier,
	}
	if err := h.Backend.CreateClassifier(c); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// createColumnStatisticsTaskSettingsInput holds input for CreateColumnStatisticsTaskSettings.
type createColumnStatisticsTaskSettingsInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	TableName      string   `json:"TableName"`
	RoleArn        string   `json:"RoleArn,omitempty"`
	ColumnNameList []string `json:"ColumnNameList,omitempty"`
}

func (h *Handler) handleCreateColumnStatisticsTaskSettings(
	_ context.Context,
	in *createColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	_, err := h.Backend.CreateColumnStatisticsTaskSettings(
		in.DatabaseName, in.TableName, in.RoleArn, in.ColumnNameList,
	)

	return &emptyOutput{}, err
}

// createCustomEntityTypeInput holds input for CreateCustomEntityType.
type createCustomEntityTypeInput struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString,omitempty"`
	ContextWords []string `json:"ContextWords,omitempty"`
}

// createCustomEntityTypeOutput holds the result for CreateCustomEntityType.
type createCustomEntityTypeOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateCustomEntityType(
	_ context.Context,
	in *createCustomEntityTypeInput,
) (*createCustomEntityTypeOutput, error) {
	if _, err := h.Backend.CreateCustomEntityType(in.Name, in.RegexString, in.ContextWords); err != nil {
		return nil, err
	}

	return &createCustomEntityTypeOutput{Name: in.Name}, nil
}

// createDevEndpointInput holds input for CreateDevEndpoint.
type createDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// createDevEndpointOutput holds the result for CreateDevEndpoint.
type createDevEndpointOutput struct {
	EndpointName string `json:"EndpointName"`
	Status       string `json:"Status"`
}

func (h *Handler) handleCreateDevEndpoint(
	_ context.Context,
	in *createDevEndpointInput,
) (*createDevEndpointOutput, error) {
	dep, err := h.Backend.CreateDevEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return &createDevEndpointOutput{EndpointName: dep.EndpointName, Status: dep.Status}, nil
}

// createIdentityCenterConfigurationInput holds input for CreateGlueIdentityCenterConfiguration.
type createIdentityCenterConfigurationInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
}

func (h *Handler) handleCreateGlueIdentityCenterConfiguration(
	_ context.Context,
	in *createIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateGlueIdentityCenterConfiguration(in.InstanceArn)
}

// createIntegrationInput holds input for CreateIntegration.
type createIntegrationInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	IntegrationName string            `json:"IntegrationName"`
}

// createIntegrationOutput holds the result for CreateIntegration.
type createIntegrationOutput struct {
	IntegrationName string `json:"IntegrationName"`
	Status          string `json:"Status"`
}

func (h *Handler) handleCreateIntegration(
	_ context.Context,
	in *createIntegrationInput,
) (*createIntegrationOutput, error) {
	ig, err := h.Backend.CreateIntegration(in.IntegrationName, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createIntegrationOutput{IntegrationName: ig.IntegrationName, Status: ig.Status}, nil
}

// createIntegrationResourcePropertyInput holds input for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyInput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// createIntegrationResourcePropertyOutput holds the result for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyOutput struct {
	ResourceArn      string            `json:"ResourceArn"`
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	CreateTime       string            `json:"CreateTime,omitempty"`
}

func (h *Handler) handleCreateIntegrationResourceProperty(
	_ context.Context,
	in *createIntegrationResourcePropertyInput,
) (*createIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.CreateIntegrationResourceProperty(in.ResourceArn, in.SourceProperties, in.TargetProperties)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResourcePropertyOutput{
		ResourceArn:      prop.ResourceArn,
		SourceProperties: prop.SourceProperties,
		TargetProperties: prop.TargetProperties,
		CreateTime:       prop.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}

// createIntegrationTablePropertiesInput holds input for CreateIntegrationTableProperties.
type createIntegrationTablePropertiesInput struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

func (h *Handler) handleCreateIntegrationTableProperties(
	_ context.Context,
	in *createIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateIntegrationTableProperties(
		in.ResourceArn, in.TableName, in.SourceTableConfig, in.TargetTableConfig,
	)
}

// createMLTransformInput holds input for CreateMLTransform.
type createMLTransformInput struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	Tags              map[string]string    `json:"Tags,omitempty"`
	Name              string               `json:"Name"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
}

// createMLTransformOutput holds the result for CreateMLTransform.
type createMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleCreateMLTransform(
	_ context.Context,
	in *createMLTransformInput,
) (*createMLTransformOutput, error) {
	m, err := h.Backend.CreateMLTransform(
		in.Name,
		in.Description,
		in.Role,
		in.InputRecordTables,
		in.Parameters,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createMLTransformOutput{TransformID: m.TransformID}, nil
}

// createPartitionInput holds input for CreatePartition.
type createPartitionInput struct {
	DatabaseName   string         `json:"DatabaseName"`
	TableName      string         `json:"TableName"`
	PartitionInput PartitionInput `json:"PartitionInput"`
}

func (h *Handler) handleCreatePartition(
	_ context.Context,
	in *createPartitionInput,
) (*emptyOutput, error) {
	_, errs := h.Backend.BatchCreatePartition(
		in.DatabaseName,
		in.TableName,
		[]PartitionInput{in.PartitionInput},
	)
	if len(errs) > 0 {
		return nil, awserrFromDetail(errs[0].ErrorDetail)
	}

	return &emptyOutput{}, nil
}

// createPartitionIndexInput holds input for CreatePartitionIndex.
type createPartitionIndexInput struct {
	DatabaseName   string         `json:"DatabaseName"`
	TableName      string         `json:"TableName"`
	PartitionIndex PartitionIndex `json:"PartitionIndex"`
}

func (h *Handler) handleCreatePartitionIndex(
	_ context.Context,
	in *createPartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreatePartitionIndex(in.DatabaseName, in.TableName, in.PartitionIndex)
}

// createRegistryInput holds input for CreateRegistry.
type createRegistryInput struct {
	Tags         map[string]string `json:"Tags"`
	RegistryName string            `json:"RegistryName"`
	Description  string            `json:"Description"`
}

// createRegistryOutput holds the result for CreateRegistry.
type createRegistryOutput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryName string            `json:"RegistryName"`
	RegistryArn  string            `json:"RegistryArn"`
	Status       string            `json:"Status"`
}

func (h *Handler) handleCreateRegistry(
	_ context.Context,
	in *createRegistryInput,
) (*createRegistryOutput, error) {
	reg, err := h.Backend.CreateRegistry(in.RegistryName, in.Description, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createRegistryOutput{
		RegistryName: reg.Name,
		RegistryArn:  reg.ARN,
		Status:       reg.Status,
		Tags:         reg.Tags,
	}, nil
}

// createSchemaInput holds input for CreateSchema.
type createSchemaInput struct {
	RegistryID    *registryIDInput  `json:"RegistryId"`
	Tags          map[string]string `json:"Tags"`
	SchemaName    string            `json:"SchemaName"`
	DataFormat    string            `json:"DataFormat"`
	Compatibility string            `json:"Compatibility"`
	Description   string            `json:"Description"`
}

// registryIDInput holds registry identification fields.
type registryIDInput struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
}

// createSchemaOutput holds the result for CreateSchema.
type createSchemaOutput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	RegistryName  string            `json:"RegistryName"`
	RegistryArn   string            `json:"RegistryArn"`
	SchemaName    string            `json:"SchemaName"`
	SchemaArn     string            `json:"SchemaArn"`
	DataFormat    string            `json:"DataFormat"`
	Compatibility string            `json:"Compatibility"`
	SchemaStatus  string            `json:"SchemaStatus"`
}

func (h *Handler) handleCreateSchema(
	_ context.Context,
	in *createSchemaInput,
) (*createSchemaOutput, error) {
	registryName := ""
	if in.RegistryID != nil {
		registryName = in.RegistryID.RegistryName
	}

	s, err := h.Backend.CreateSchema(
		registryName,
		in.SchemaName,
		in.DataFormat,
		in.Compatibility,
		in.Description,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createSchemaOutput{
		RegistryName:  s.RegistryName,
		RegistryArn:   s.RegistryARN,
		SchemaName:    s.SchemaName,
		SchemaArn:     s.SchemaARN,
		DataFormat:    s.DataFormat,
		Compatibility: s.Compatibility,
		SchemaStatus:  s.SchemaStatus,
		Tags:          s.Tags,
	}, nil
}

// createScriptInput holds input for CreateScript.
type createScriptInput struct{}

// createScriptOutput holds the result for CreateScript.
type createScriptOutput struct {
	PythonScript string `json:"PythonScript"`
	ScalaCode    string `json:"ScalaCode"`
}

func (h *Handler) handleCreateScript(
	_ context.Context,
	_ *createScriptInput,
) (*createScriptOutput, error) {
	return &createScriptOutput{PythonScript: "", ScalaCode: ""}, nil
}

// createSecurityConfigurationInput holds input for CreateSecurityConfiguration.
type createSecurityConfigurationInput struct {
	Name                    string                  `json:"Name"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration"`
}

// createSecurityConfigurationOutput holds the result for CreateSecurityConfiguration.
type createSecurityConfigurationOutput struct {
	Name      string  `json:"Name"`
	CreatedOn float64 `json:"CreatedOn"`
}

func (h *Handler) handleCreateSecurityConfiguration(
	_ context.Context,
	in *createSecurityConfigurationInput,
) (*createSecurityConfigurationOutput, error) {
	sc, err := h.Backend.CreateSecurityConfiguration(in.Name, in.EncryptionConfiguration)
	if err != nil {
		return nil, err
	}

	return &createSecurityConfigurationOutput{Name: sc.Name, CreatedOn: sc.CreatedTimeStamp}, nil
}

// createSessionInput holds input for CreateSession.
type createSessionInput struct {
	DefaultArguments map[string]string `json:"DefaultArguments,omitempty"`
	Command          SessionCommand    `json:"Command"`
	ID               string            `json:"Id"`
	Role             string            `json:"Role,omitempty"`
	Description      string            `json:"Description,omitempty"`
	Timeout          int32             `json:"Timeout,omitempty"`
	MaxCapacity      float64           `json:"MaxCapacity,omitempty"`
}

// createSessionOutput holds the result for CreateSession.
type createSessionOutput struct {
	Session *Session `json:"Session"`
}

func (h *Handler) handleCreateSession(
	_ context.Context,
	in *createSessionInput,
) (*createSessionOutput, error) {
	opts := Session{
		Timeout:          in.Timeout,
		MaxCapacity:      in.MaxCapacity,
		Description:      in.Description,
		DefaultArguments: in.DefaultArguments,
	}
	s, err := h.Backend.CreateSession(in.ID, in.Role, in.Command, opts)
	if err != nil {
		return nil, err
	}

	return &createSessionOutput{Session: s}, nil
}

// createTableOptimizerInput holds input for CreateTableOptimizer.
type createTableOptimizerInput struct {
	CatalogID                   string                      `json:"CatalogId,omitempty"`
	DatabaseName                string                      `json:"DatabaseName"`
	TableName                   string                      `json:"TableName"`
	Type                        string                      `json:"Type"`
	TableOptimizerConfiguration TableOptimizerConfiguration `json:"TableOptimizerConfiguration"`
}

func (h *Handler) handleCreateTableOptimizer(
	_ context.Context,
	in *createTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateTableOptimizer(
		in.CatalogID,
		in.DatabaseName,
		in.TableName,
		in.Type,
		in.TableOptimizerConfiguration,
	)
}

// createTriggerInput holds input for CreateTrigger.
type createTriggerInput struct {
	Tags      map[string]string `json:"Tags,omitempty"`
	Predicate *TriggerPredicate `json:"Predicate,omitempty"`
	Schedule  string            `json:"Schedule,omitempty"`
	Name      string            `json:"Name"`
	Type      string            `json:"Type,omitempty"`
	Actions   []TriggerAction   `json:"Actions,omitempty"`
}

// createTriggerOutput holds the result for CreateTrigger.
type createTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateTrigger(
	_ context.Context,
	in *createTriggerInput,
) (*createTriggerOutput, error) {
	t := Trigger{
		Name:      in.Name,
		Type:      in.Type,
		Schedule:  in.Schedule,
		Actions:   in.Actions,
		Predicate: in.Predicate,
	}

	created, err := h.Backend.CreateTrigger(t, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createTriggerOutput{Name: created.Name}, nil
}

// createUsageProfileInput holds input for CreateUsageProfile.
type createUsageProfileInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
}

// createUsageProfileOutput holds the result for CreateUsageProfile.
type createUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateUsageProfile(
	_ context.Context,
	in *createUsageProfileInput,
) (*createUsageProfileOutput, error) {
	if _, err := h.Backend.CreateUsageProfile(in.Name, in.Description, in.Tags); err != nil {
		return nil, err
	}

	return &createUsageProfileOutput{Name: in.Name}, nil
}

// createUserDefinedFunctionInput holds input for CreateUserDefinedFunction.
type createUserDefinedFunctionInput struct {
	Tags          map[string]string   `json:"Tags,omitempty"`
	DatabaseName  string              `json:"DatabaseName"`
	FunctionInput UserDefinedFunction `json:"FunctionInput"`
}

func (h *Handler) handleCreateUserDefinedFunction(
	_ context.Context,
	in *createUserDefinedFunctionInput,
) (*emptyOutput, error) {
	_, err := h.Backend.CreateUserDefinedFunction(in.DatabaseName, in.FunctionInput, in.Tags)

	return &emptyOutput{}, err
}

// createWorkflowInput holds input for CreateWorkflow.
type createWorkflowInput struct {
	Tags                 map[string]string `json:"Tags,omitempty"`
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
}

// createWorkflowOutput holds the result for CreateWorkflow.
type createWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateWorkflow(
	_ context.Context,
	in *createWorkflowInput,
) (*createWorkflowOutput, error) {
	w := Workflow{
		Name:                 in.Name,
		Description:          in.Description,
		DefaultRunProperties: in.DefaultRunProperties,
	}

	created, err := h.Backend.CreateWorkflow(w, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createWorkflowOutput{Name: created.Name}, nil
}

// deleteBlueprintInput holds input for DeleteBlueprint.
type deleteBlueprintInput struct {
	Name string `json:"Name"`
}

// deleteBlueprintOutput holds the result for DeleteBlueprint.
type deleteBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteBlueprint(
	_ context.Context,
	in *deleteBlueprintInput,
) (*deleteBlueprintOutput, error) {
	if err := h.Backend.DeleteBlueprint(in.Name); err != nil {
		return nil, err
	}

	return &deleteBlueprintOutput{Name: in.Name}, nil
}

// deleteCatalogInput holds input for DeleteCatalog.
type deleteCatalogInput struct {
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleDeleteCatalog(
	_ context.Context,
	in *deleteCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteCatalog(in.CatalogID)
}

// deleteClassifierInput holds input for DeleteClassifier.
type deleteClassifierInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteClassifier(
	_ context.Context,
	in *deleteClassifierInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteClassifier(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// deleteColumnStatisticsForPartitionInput holds input for DeleteColumnStatisticsForPartition.
type deleteColumnStatisticsForPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	ColumnName      string   `json:"ColumnName"`
	PartitionValues []string `json:"PartitionValues"`
}

func (h *Handler) handleDeleteColumnStatisticsForPartition(
	_ context.Context,
	in *deleteColumnStatisticsForPartitionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnName,
	)
}

// deleteColumnStatisticsForTableInput holds input for DeleteColumnStatisticsForTable.
type deleteColumnStatisticsForTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	ColumnName   string `json:"ColumnName"`
}

func (h *Handler) handleDeleteColumnStatisticsForTable(
	_ context.Context,
	in *deleteColumnStatisticsForTableInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnName,
	)
}

// deleteColumnStatisticsTaskSettingsInput holds input for DeleteColumnStatisticsTaskSettings.
type deleteColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleDeleteColumnStatisticsTaskSettings(
	_ context.Context,
	in *deleteColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsTaskSettings(in.DatabaseName, in.TableName)
}

// deleteConnectionTypeInput holds input for DeleteConnectionType.
type deleteConnectionTypeInput struct{}

func (h *Handler) handleDeleteConnectionType(
	_ context.Context,
	_ *deleteConnectionTypeInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteCustomEntityTypeInput holds input for DeleteCustomEntityType.
type deleteCustomEntityTypeInput struct {
	Name string `json:"Name"`
}

// deleteCustomEntityTypeOutput holds the result for DeleteCustomEntityType.
type deleteCustomEntityTypeOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteCustomEntityType(
	_ context.Context,
	in *deleteCustomEntityTypeInput,
) (*deleteCustomEntityTypeOutput, error) {
	if err := h.Backend.DeleteCustomEntityType(in.Name); err != nil {
		return nil, err
	}

	return &deleteCustomEntityTypeOutput{Name: in.Name}, nil
}

// deleteDevEndpointInput holds input for DeleteDevEndpoint.
type deleteDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleDeleteDevEndpoint(
	_ context.Context,
	in *deleteDevEndpointInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDevEndpoint(in.EndpointName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// deleteIdentityCenterConfigurationInput holds input for DeleteGlueIdentityCenterConfiguration.
type deleteIdentityCenterConfigurationInput struct{}

func (h *Handler) handleDeleteGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *deleteIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteGlueIdentityCenterConfiguration()
}

// deleteIntegrationInput holds input for DeleteIntegration.
type deleteIntegrationInput struct {
	IntegrationIdentifier string `json:"IntegrationIdentifier"`
}

// deleteIntegrationOutput holds the result for DeleteIntegration.
type deleteIntegrationOutput struct {
	IntegrationName string `json:"IntegrationName"`
}

func (h *Handler) handleDeleteIntegration(
	_ context.Context,
	in *deleteIntegrationInput,
) (*deleteIntegrationOutput, error) {
	if err := h.Backend.DeleteIntegration(in.IntegrationIdentifier); err != nil {
		return nil, err
	}

	return &deleteIntegrationOutput{IntegrationName: in.IntegrationIdentifier}, nil
}

// deleteIntegrationResourcePropertyInput holds input for DeleteIntegrationResourceProperty.
type deleteIntegrationResourcePropertyInput struct{}

func (h *Handler) handleDeleteIntegrationResourceProperty(
	_ context.Context,
	_ *deleteIntegrationResourcePropertyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteIntegrationTablePropertiesInput holds input for DeleteIntegrationTableProperties.
type deleteIntegrationTablePropertiesInput struct{}

func (h *Handler) handleDeleteIntegrationTableProperties(
	_ context.Context,
	_ *deleteIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteMLTransformInput holds input for DeleteMLTransform.
type deleteMLTransformInput struct {
	TransformID string `json:"TransformId"`
}

// deleteMLTransformOutput holds the result for DeleteMLTransform.
type deleteMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleDeleteMLTransform(
	_ context.Context,
	in *deleteMLTransformInput,
) (*deleteMLTransformOutput, error) {
	if err := h.Backend.DeleteMLTransform(in.TransformID); err != nil {
		return nil, err
	}

	return &deleteMLTransformOutput{TransformID: in.TransformID}, nil
}

// deletePartitionInput holds input for DeletePartition.
type deletePartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
}

func (h *Handler) handleDeletePartition(
	_ context.Context,
	in *deletePartitionInput,
) (*emptyOutput, error) {
	errs := h.Backend.BatchDeletePartition(
		in.DatabaseName,
		in.TableName,
		[]PartitionValueList{{Values: in.PartitionValues}},
	)
	if len(errs) > 0 {
		return nil, awserrFromDetail(errs[0].ErrorDetail)
	}

	return &emptyOutput{}, nil
}

// deletePartitionIndexInput holds input for DeletePartitionIndex.
type deletePartitionIndexInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	IndexName    string `json:"IndexName"`
}

func (h *Handler) handleDeletePartitionIndex(
	_ context.Context,
	in *deletePartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeletePartitionIndex(in.DatabaseName, in.TableName, in.IndexName)
}

// deleteRegistryInput holds input for DeleteRegistry.
type deleteRegistryInput struct {
	RegistryID *registryIDInput `json:"RegistryId"`
}

// deleteRegistryOutput holds the result for DeleteRegistry.
type deleteRegistryOutput struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
	Status       string `json:"Status"`
}

func (h *Handler) handleDeleteRegistry(
	_ context.Context,
	in *deleteRegistryInput,
) (*deleteRegistryOutput, error) {
	name := ""
	if in.RegistryID != nil {
		name = in.RegistryID.RegistryName
	}

	if err := h.Backend.DeleteRegistry(name); err != nil {
		return nil, err
	}

	return &deleteRegistryOutput{RegistryName: name, Status: stateDeleting}, nil
}

// deleteResourcePolicyInput holds input for DeleteResourcePolicy.
type deleteResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
	PolicyHash  string `json:"PolicyHashCondition,omitempty"`
}

func (h *Handler) handleDeleteResourcePolicy(
	_ context.Context,
	in *deleteResourcePolicyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteResourcePolicy(in.ResourceArn, in.PolicyHash)
}

// schemaIDInput identifies a schema by registry + schema name or by ARN.
type schemaIDInput struct {
	RegistryName string `json:"RegistryName"`
	SchemaName   string `json:"SchemaName"`
	SchemaArn    string `json:"SchemaArn"`
}

// deleteSchemaInput holds input for DeleteSchema.
type deleteSchemaInput struct {
	SchemaID *schemaIDInput `json:"SchemaId"`
}

// deleteSchemaOutput holds the result for DeleteSchema.
type deleteSchemaOutput struct {
	SchemaArn  string `json:"SchemaArn"`
	SchemaName string `json:"SchemaName"`
	Status     string `json:"Status"`
}

func (h *Handler) handleDeleteSchema(
	_ context.Context,
	in *deleteSchemaInput,
) (*deleteSchemaOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	if err := h.Backend.DeleteSchema(registryName, schemaName); err != nil {
		return nil, err
	}

	return &deleteSchemaOutput{SchemaName: schemaName, Status: stateDeleting}, nil
}

// deleteSchemaVersionsInput holds input for DeleteSchemaVersions.
type deleteSchemaVersionsInput struct{}

// deleteSchemaVersionsOutput holds the result for DeleteSchemaVersions.
type deleteSchemaVersionsOutput struct {
	SchemaVersionErrors []any `json:"SchemaVersionErrors"`
}

func (h *Handler) handleDeleteSchemaVersions(
	_ context.Context,
	_ *deleteSchemaVersionsInput,
) (*deleteSchemaVersionsOutput, error) {
	return &deleteSchemaVersionsOutput{SchemaVersionErrors: []any{}}, nil
}

// deleteSecurityConfigurationInput holds input for DeleteSecurityConfiguration.
type deleteSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteSecurityConfiguration(
	_ context.Context,
	in *deleteSecurityConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteSecurityConfiguration(in.Name)
}

// deleteSessionInput holds input for DeleteSession.
type deleteSessionInput struct {
	ID string `json:"Id"`
}

// deleteSessionOutput holds the result for DeleteSession.
type deleteSessionOutput struct {
	ID string `json:"Id"`
}

func (h *Handler) handleDeleteSession(
	_ context.Context,
	in *deleteSessionInput,
) (*deleteSessionOutput, error) {
	if err := h.Backend.DeleteSession(in.ID); err != nil {
		return nil, err
	}

	return &deleteSessionOutput{ID: in.ID}, nil
}

// deleteTableOptimizerInput holds input for DeleteTableOptimizer.
type deleteTableOptimizerInput struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

func (h *Handler) handleDeleteTableOptimizer(
	_ context.Context,
	in *deleteTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTableOptimizer(in.DatabaseName, in.TableName, in.Type)
}

// deleteTableVersionInput holds input for DeleteTableVersion.
type deleteTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
}

func (h *Handler) handleDeleteTableVersion(
	_ context.Context,
	in *deleteTableVersionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTableVersion(in.DatabaseName, in.TableName, in.VersionID)
}

// deleteTriggerInput holds input for DeleteTrigger.
type deleteTriggerInput struct {
	Name string `json:"Name"`
}

// deleteTriggerOutput holds the result for DeleteTrigger.
type deleteTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteTrigger(
	_ context.Context,
	in *deleteTriggerInput,
) (*deleteTriggerOutput, error) {
	if err := h.Backend.DeleteTrigger(in.Name); err != nil {
		return nil, err
	}

	return &deleteTriggerOutput{Name: in.Name}, nil
}

// deleteUsageProfileInput holds input for DeleteUsageProfile.
type deleteUsageProfileInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteUsageProfile(
	_ context.Context,
	in *deleteUsageProfileInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteUsageProfile(in.Name)
}

// deleteUserDefinedFunctionInput holds input for DeleteUserDefinedFunction.
type deleteUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
}

func (h *Handler) handleDeleteUserDefinedFunction(
	_ context.Context,
	in *deleteUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteUserDefinedFunction(in.DatabaseName, in.FunctionName)
}

// deleteWorkflowInput holds input for DeleteWorkflow.
type deleteWorkflowInput struct {
	Name string `json:"Name"`
}

// deleteWorkflowOutput holds the result for DeleteWorkflow.
type deleteWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteWorkflow(
	_ context.Context,
	in *deleteWorkflowInput,
) (*deleteWorkflowOutput, error) {
	if err := h.Backend.DeleteWorkflow(in.Name); err != nil {
		return nil, err
	}

	return &deleteWorkflowOutput{Name: in.Name}, nil
}

// describeConnectionTypeInput holds input for DescribeConnectionType.
type describeConnectionTypeInput struct {
	ConnectionType string `json:"ConnectionType"`
}

// describeConnectionTypeOutput holds the result for DescribeConnectionType.
type describeConnectionTypeOutput struct {
	ConnectionType string `json:"ConnectionType"`
}

func (h *Handler) handleDescribeConnectionType(
	_ context.Context,
	in *describeConnectionTypeInput,
) (*describeConnectionTypeOutput, error) {
	if in.ConnectionType == "" {
		return nil, fmt.Errorf("%w: ConnectionType is required", ErrValidation)
	}

	return &describeConnectionTypeOutput{ConnectionType: in.ConnectionType}, nil
}

// describeEntityInput holds input for DescribeEntity.
type describeEntityInput struct{}

// describeEntityOutput holds the result for DescribeEntity.
type describeEntityOutput struct {
	Fields []any `json:"Fields"`
}

func (h *Handler) handleDescribeEntity(
	_ context.Context,
	_ *describeEntityInput,
) (*describeEntityOutput, error) {
	return &describeEntityOutput{Fields: []any{}}, nil
}

// describeInboundIntegrationsInput holds input for DescribeInboundIntegrations.
type describeInboundIntegrationsInput struct{}

// describeInboundIntegrationsOutput holds the result for DescribeInboundIntegrations.
type describeInboundIntegrationsOutput struct {
	Integrations []any `json:"Integrations"`
}

func (h *Handler) handleDescribeInboundIntegrations(
	_ context.Context,
	_ *describeInboundIntegrationsInput,
) (*describeInboundIntegrationsOutput, error) {
	return &describeInboundIntegrationsOutput{Integrations: []any{}}, nil // inbound integrations are always empty
}

// describeIntegrationsInput holds input for DescribeIntegrations.
type describeIntegrationsInput struct{}

// describeIntegrationsOutput holds the result for DescribeIntegrations.
type describeIntegrationsOutput struct {
	Integrations []any `json:"Integrations"`
}

func (h *Handler) handleDescribeIntegrations(
	_ context.Context,
	_ *describeIntegrationsInput,
) (*describeIntegrationsOutput, error) {
	list := h.Backend.ListIntegrations()
	result := make([]any, 0, len(list))
	for _, ig := range list {
		result = append(result, ig)
	}

	return &describeIntegrationsOutput{Integrations: result}, nil
}

// getBlueprintInput holds input for GetBlueprint.
type getBlueprintInput struct {
	Name string `json:"Name"`
}

// getBlueprintOutput holds the result for GetBlueprint.
type getBlueprintOutput struct {
	Blueprint *Blueprint `json:"Blueprint"`
}

func (h *Handler) handleGetBlueprint(
	_ context.Context,
	in *getBlueprintInput,
) (*getBlueprintOutput, error) {
	found, missing := h.Backend.BatchGetBlueprints([]string{in.Name})
	if len(missing) > 0 {
		return nil, fmt.Errorf("blueprint %q not found: %w", in.Name, ErrNotFound)
	}

	return &getBlueprintOutput{Blueprint: found[0]}, nil
}

// getBlueprintRunInput holds input for GetBlueprintRun.
type getBlueprintRunInput struct {
	BlueprintName string `json:"BlueprintName"`
	RunID         string `json:"RunId"`
}

// getBlueprintRunOutput holds the result for GetBlueprintRun.
type getBlueprintRunOutput struct {
	Run any `json:"Run"`
}

func (h *Handler) handleGetBlueprintRun(
	_ context.Context,
	in *getBlueprintRunInput,
) (*getBlueprintRunOutput, error) {
	if in.RunID == "" {
		return nil, fmt.Errorf("%w: RunId is required", ErrValidation)
	}

	run, err := h.Backend.GetBlueprintRun(in.BlueprintName, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getBlueprintRunOutput{Run: run}, nil
}

// getBlueprintRunsInput holds input for GetBlueprintRuns.
type getBlueprintRunsInput struct {
	BlueprintName string `json:"BlueprintName"`
}

// getBlueprintRunsOutput holds the result for GetBlueprintRuns.
type getBlueprintRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleGetBlueprintRuns(
	_ context.Context,
	in *getBlueprintRunsInput,
) (*getBlueprintRunsOutput, error) {
	runs := h.Backend.GetBlueprintRuns(in.BlueprintName)
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getBlueprintRunsOutput{Runs: result}, nil
}

// getCatalogInput holds input for GetCatalog.
type getCatalogInput struct {
	CatalogID string `json:"CatalogId"`
}

// getCatalogOutput holds the result for GetCatalog.
type getCatalogOutput struct {
	Catalog *CatalogEntry `json:"Catalog"`
}

func (h *Handler) handleGetCatalog(
	_ context.Context,
	in *getCatalogInput,
) (*getCatalogOutput, error) {
	c, err := h.Backend.GetCatalog(in.CatalogID)
	if err != nil {
		return nil, err
	}

	return &getCatalogOutput{Catalog: c}, nil
}

// getCatalogImportStatusInput holds input for GetCatalogImportStatus.
type getCatalogImportStatusInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

// getCatalogImportStatusOutput holds the result for GetCatalogImportStatus.
type getCatalogImportStatusOutput struct {
	ImportStatus *CatalogImportStatus `json:"ImportStatus"`
}

func (h *Handler) handleGetCatalogImportStatus(
	_ context.Context,
	in *getCatalogImportStatusInput,
) (*getCatalogImportStatusOutput, error) {
	status := h.Backend.GetCatalogImportStatus(in.CatalogID)

	return &getCatalogImportStatusOutput{ImportStatus: status}, nil
}

// getCatalogsInput holds input for GetCatalogs.
type getCatalogsInput struct{}

// getCatalogsOutput holds the result for GetCatalogs.
type getCatalogsOutput struct {
	CatalogList []*CatalogEntry `json:"CatalogList"`
}

func (h *Handler) handleGetCatalogs(
	_ context.Context,
	_ *getCatalogsInput,
) (*getCatalogsOutput, error) {
	catalogs := h.Backend.GetCatalogs()
	if catalogs == nil {
		catalogs = []*CatalogEntry{}
	}

	return &getCatalogsOutput{CatalogList: catalogs}, nil
}

// getClassifierInput holds input for GetClassifier.
type getClassifierInput struct {
	Name string `json:"Name"`
}

// getClassifierOutput holds the result for GetClassifier.
type getClassifierOutput struct {
	Classifier *Classifier `json:"Classifier"`
}

func (h *Handler) handleGetClassifier(
	_ context.Context,
	in *getClassifierInput,
) (*getClassifierOutput, error) {
	c, err := h.Backend.GetClassifier(in.Name)
	if err != nil {
		return nil, err
	}

	return &getClassifierOutput{Classifier: c}, nil
}

// getClassifiersInput holds input for GetClassifiers.
type getClassifiersInput struct{}

// getClassifiersOutput holds the result for GetClassifiers.
type getClassifiersOutput struct {
	Classifiers []*Classifier `json:"Classifiers"`
}

func (h *Handler) handleGetClassifiers(
	_ context.Context,
	_ *getClassifiersInput,
) (*getClassifiersOutput, error) {
	return &getClassifiersOutput{Classifiers: h.Backend.GetClassifiers()}, nil
}

// getColumnStatisticsForPartitionInput holds input for GetColumnStatisticsForPartition.
type getColumnStatisticsForPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
	ColumnNames     []string `json:"ColumnNames,omitempty"`
}

// getColumnStatisticsForPartitionOutput holds the result for GetColumnStatisticsForPartition.
type getColumnStatisticsForPartitionOutput struct {
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
	Errors               []any               `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForPartition(
	_ context.Context,
	in *getColumnStatisticsForPartitionInput,
) (*getColumnStatisticsForPartitionOutput, error) {
	stats, err := h.Backend.GetColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnNames,
	)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		stats = []*ColumnStatistics{}
	}

	return &getColumnStatisticsForPartitionOutput{ColumnStatisticsList: stats, Errors: []any{}}, nil
}

// getColumnStatisticsForTableInput holds input for GetColumnStatisticsForTable.
type getColumnStatisticsForTableInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	ColumnNames  []string `json:"ColumnNames,omitempty"`
}

// getColumnStatisticsForTableOutput holds the result for GetColumnStatisticsForTable.
type getColumnStatisticsForTableOutput struct {
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
	Errors               []any               `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForTable(
	_ context.Context,
	in *getColumnStatisticsForTableInput,
) (*getColumnStatisticsForTableOutput, error) {
	stats, err := h.Backend.GetColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnNames,
	)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		stats = []*ColumnStatistics{}
	}

	return &getColumnStatisticsForTableOutput{ColumnStatisticsList: stats, Errors: []any{}}, nil
}

// getColumnStatisticsTaskRunInput holds input for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunInput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

// getColumnStatisticsTaskRunOutput holds the result for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRun any `json:"ColumnStatisticsTaskRun"`
}

func (h *Handler) handleGetColumnStatisticsTaskRun(
	_ context.Context,
	in *getColumnStatisticsTaskRunInput,
) (*getColumnStatisticsTaskRunOutput, error) {
	if in.ColumnStatisticsTaskRunID == "" {
		return nil, fmt.Errorf("%w: ColumnStatisticsTaskRunId is required", ErrValidation)
	}

	run, err := h.Backend.GetColumnStatisticsTaskRun(in.ColumnStatisticsTaskRunID)
	if err != nil {
		return nil, err
	}

	return &getColumnStatisticsTaskRunOutput{ColumnStatisticsTaskRun: run}, nil
}

// getColumnStatisticsTaskRunsInput holds input for GetColumnStatisticsTaskRuns.
type getColumnStatisticsTaskRunsInput struct{}

// getColumnStatisticsTaskRunsOutput holds the result for GetColumnStatisticsTaskRuns.
type getColumnStatisticsTaskRunsOutput struct {
	ColumnStatisticsTaskRuns []any `json:"ColumnStatisticsTaskRuns"`
}

func (h *Handler) handleGetColumnStatisticsTaskRuns(
	_ context.Context,
	_ *getColumnStatisticsTaskRunsInput,
) (*getColumnStatisticsTaskRunsOutput, error) {
	runs := h.Backend.GetColumnStatisticsTaskRuns()
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRuns: result}, nil
}

// getColumnStatisticsTaskSettingsInput holds input for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getColumnStatisticsTaskSettingsOutput holds the result for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsOutput struct {
	ColumnStatisticsTaskSettings any `json:"ColumnStatisticsTaskSettings"`
}

func (h *Handler) handleGetColumnStatisticsTaskSettings(
	_ context.Context,
	in *getColumnStatisticsTaskSettingsInput,
) (*getColumnStatisticsTaskSettingsOutput, error) {
	s, _ := h.Backend.GetColumnStatisticsTaskSettings(in.DatabaseName, in.TableName)

	return &getColumnStatisticsTaskSettingsOutput{ColumnStatisticsTaskSettings: s}, nil
}

// getCrawlerMetricsInput holds input for GetCrawlerMetrics.
type getCrawlerMetricsInput struct {
	CrawlerNameList []string `json:"CrawlerNameList"`
}

// getCrawlerMetricsOutput holds the result for GetCrawlerMetrics.
type getCrawlerMetricsOutput struct {
	CrawlerMetricsList []*CrawlerMetrics `json:"CrawlerMetricsList"`
}

func (h *Handler) handleGetCrawlerMetrics(
	_ context.Context,
	in *getCrawlerMetricsInput,
) (*getCrawlerMetricsOutput, error) {
	metrics := h.Backend.GetCrawlerMetrics(in.CrawlerNameList)

	return &getCrawlerMetricsOutput{CrawlerMetricsList: metrics}, nil
}

// getCustomEntityTypeInput holds input for GetCustomEntityType.
type getCustomEntityTypeInput struct {
	Name string `json:"Name"`
}

// getCustomEntityTypeOutput holds the result for GetCustomEntityType.
type getCustomEntityTypeOutput struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString"`
	ContextWords []string `json:"ContextWords"`
}

func (h *Handler) handleGetCustomEntityType(
	_ context.Context,
	in *getCustomEntityTypeInput,
) (*getCustomEntityTypeOutput, error) {
	found, missing := h.Backend.BatchGetCustomEntityTypes([]string{in.Name})
	if len(missing) > 0 {
		return nil, awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	}

	return &getCustomEntityTypeOutput{
		Name:         found[0].Name,
		RegexString:  found[0].RegexString,
		ContextWords: found[0].ContextWords,
	}, nil
}

// getDataCatalogEncryptionSettingsInput holds input for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

// getDataCatalogEncryptionSettingsOutput holds the result for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsOutput struct {
	DataCatalogEncryptionSettings *DataCatalogEncryptionSettings `json:"DataCatalogEncryptionSettings"`
}

func (h *Handler) handleGetDataCatalogEncryptionSettings(
	_ context.Context,
	in *getDataCatalogEncryptionSettingsInput,
) (*getDataCatalogEncryptionSettingsOutput, error) {
	s, err := h.Backend.GetDataCatalogEncryptionSettings(in.CatalogID)
	if err != nil {
		return nil, err
	}

	return &getDataCatalogEncryptionSettingsOutput{DataCatalogEncryptionSettings: s}, nil
}

// getDataQualityModelInput holds input for GetDataQualityModel.
type getDataQualityModelInput struct{}

// getDataQualityModelOutput holds the result for GetDataQualityModel.
type getDataQualityModelOutput struct {
	Status string `json:"Status"`
}

func (h *Handler) handleGetDataQualityModel(
	_ context.Context,
	_ *getDataQualityModelInput,
) (*getDataQualityModelOutput, error) {
	return &getDataQualityModelOutput{Status: stateSucceeded}, nil
}

// getDataQualityModelResultInput holds input for GetDataQualityModelResult.
type getDataQualityModelResultInput struct {
	ProfileID   string `json:"ProfileId"`
	StatisticID string `json:"StatisticId,omitempty"`
}

// getDataQualityModelResultOutput holds the result for GetDataQualityModelResult.
type getDataQualityModelResultOutput struct {
	ProfileID   string  `json:"ProfileId,omitempty"`
	Status      string  `json:"Status,omitempty"`
	CompletedOn float64 `json:"CompletedOn"`
}

func (h *Handler) handleGetDataQualityModelResult(
	_ context.Context,
	in *getDataQualityModelResultInput,
) (*getDataQualityModelResultOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", ErrValidation)
	}

	status, err := h.Backend.GetDataQualityModelResult(in.ProfileID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityModelResultOutput{ProfileID: in.ProfileID, Status: status}, nil
}

// getDataQualityResultInput holds input for GetDataQualityResult.
type getDataQualityResultInput struct {
	ResultID string `json:"ResultId"`
}

// getDataQualityResultOutput holds the result for GetDataQualityResult.
type getDataQualityResultOutput struct {
	ResultID string  `json:"ResultId"`
	Score    float64 `json:"Score"`
}

func (h *Handler) handleGetDataQualityResult(
	_ context.Context,
	in *getDataQualityResultInput,
) (*getDataQualityResultOutput, error) {
	found, errs := h.Backend.BatchGetDataQualityResult([]string{in.ResultID})
	if len(errs) > 0 {
		return nil, awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	}

	return &getDataQualityResultOutput{ResultID: found[0].ResultID, Score: found[0].Score}, nil
}

// getDataQualityRuleRecommendationRunInput holds input for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunInput struct {
	RunID string `json:"RunId"`
}

// getDataQualityRuleRecommendationRunOutput holds the result for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetDataQualityRuleRecommendationRun(
	_ context.Context,
	in *getDataQualityRuleRecommendationRunInput,
) (*getDataQualityRuleRecommendationRunOutput, error) {
	if in.RunID == "" {
		return &getDataQualityRuleRecommendationRunOutput{Status: stateSucceeded}, nil
	}

	run, err := h.Backend.GetDataQualityRuleRecommendationRun(in.RunID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRuleRecommendationRunOutput{RunID: run.RecommendationRunID, Status: run.Status}, nil
}

// getDataflowGraphInput holds input for GetDataflowGraph.
type getDataflowGraphInput struct{}

// getDataflowGraphOutput holds the result for GetDataflowGraph.
type getDataflowGraphOutput struct {
	DagNodes []any `json:"DagNodes"`
	DagEdges []any `json:"DagEdges"`
}

func (h *Handler) handleGetDataflowGraph(
	_ context.Context,
	_ *getDataflowGraphInput,
) (*getDataflowGraphOutput, error) {
	return &getDataflowGraphOutput{DagNodes: []any{}, DagEdges: []any{}}, nil
}

// getDevEndpointInput holds input for GetDevEndpoint.
type getDevEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// getDevEndpointOutput holds the result for GetDevEndpoint.
type getDevEndpointOutput struct {
	DevEndpoint *DevEndpoint `json:"DevEndpoint"`
}

func (h *Handler) handleGetDevEndpoint(
	_ context.Context,
	in *getDevEndpointInput,
) (*getDevEndpointOutput, error) {
	dep, err := h.Backend.GetDevEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return &getDevEndpointOutput{DevEndpoint: dep}, nil
}

// getDevEndpointsInput holds input for GetDevEndpoints.
type getDevEndpointsInput struct{}

// getDevEndpointsOutput holds the result for GetDevEndpoints.
type getDevEndpointsOutput struct {
	DevEndpoints []*DevEndpoint `json:"DevEndpoints"`
}

func (h *Handler) handleGetDevEndpoints(
	_ context.Context,
	_ *getDevEndpointsInput,
) (*getDevEndpointsOutput, error) {
	return &getDevEndpointsOutput{DevEndpoints: h.Backend.GetAllDevEndpoints()}, nil
}

// getEntityRecordsInput holds input for GetEntityRecords.
type getEntityRecordsInput struct{}

// getEntityRecordsOutput holds the result for GetEntityRecords.
type getEntityRecordsOutput struct {
	Records []any `json:"Records"`
}

func (h *Handler) handleGetEntityRecords(
	_ context.Context,
	_ *getEntityRecordsInput,
) (*getEntityRecordsOutput, error) {
	return &getEntityRecordsOutput{Records: []any{}}, nil
}

// getIdentityCenterConfigurationInput holds input for GetGlueIdentityCenterConfiguration.
type getIdentityCenterConfigurationInput struct{}

// getIdentityCenterConfigurationOutput holds the result for GetGlueIdentityCenterConfiguration.
type getIdentityCenterConfigurationOutput struct {
	InstanceArn string `json:"InstanceArn"`
}

func (h *Handler) handleGetGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *getIdentityCenterConfigurationInput,
) (*getIdentityCenterConfigurationOutput, error) {
	cfg, _ := h.Backend.GetGlueIdentityCenterConfiguration()
	if cfg == nil {
		return &getIdentityCenterConfigurationOutput{}, nil
	}

	return &getIdentityCenterConfigurationOutput{InstanceArn: cfg.InstanceARN}, nil
}

// getIntegrationResourcePropertyInput holds input for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// getIntegrationResourcePropertyOutput holds the result for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyOutput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

func (h *Handler) handleGetIntegrationResourceProperty(
	_ context.Context,
	in *getIntegrationResourcePropertyInput,
) (*getIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.GetIntegrationResourceProperty(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getIntegrationResourcePropertyOutput{
		ResourceArn:      prop.ResourceArn,
		SourceProperties: prop.SourceProperties,
		TargetProperties: prop.TargetProperties,
	}, nil
}

// getIntegrationTablePropertiesInput holds input for GetIntegrationTableProperties.
type getIntegrationTablePropertiesInput struct {
	ResourceArn string `json:"ResourceArn"`
	TableName   string `json:"TableName"`
}

// getIntegrationTablePropertiesOutput holds the result for GetIntegrationTableProperties.
type getIntegrationTablePropertiesOutput struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

func (h *Handler) handleGetIntegrationTableProperties(
	_ context.Context,
	in *getIntegrationTablePropertiesInput,
) (*getIntegrationTablePropertiesOutput, error) {
	prop, err := h.Backend.GetIntegrationTableProperties(in.ResourceArn, in.TableName)
	if err != nil {
		return nil, err
	}

	return &getIntegrationTablePropertiesOutput{
		ResourceArn:       prop.ResourceArn,
		TableName:         prop.TableName,
		SourceTableConfig: prop.SourceTableConfig,
		TargetTableConfig: prop.TargetTableConfig,
	}, nil
}

// getMLTaskRunInput holds input for GetMLTaskRun.
type getMLTaskRunInput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
}

// getMLTaskRunOutput holds the result for GetMLTaskRun.
type getMLTaskRunOutput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
	Status      string `json:"Status"`
}

func (h *Handler) handleGetMLTaskRun(
	_ context.Context,
	in *getMLTaskRunInput,
) (*getMLTaskRunOutput, error) {
	if in.TransformID == "" || in.TaskRunID == "" {
		return &getMLTaskRunOutput{Status: stateSucceeded}, nil
	}

	run, err := h.Backend.GetMLTaskRun(in.TransformID, in.TaskRunID)
	if err != nil {
		return nil, err
	}

	return &getMLTaskRunOutput{
		TransformID: run.TransformID,
		TaskRunID:   run.TaskRunID,
		Status:      run.Status,
	}, nil
}

// getMLTaskRunsInput holds input for GetMLTaskRuns.
type getMLTaskRunsInput struct {
	TransformID string `json:"TransformId"`
}

// getMLTaskRunsOutput holds the result for GetMLTaskRuns.
type getMLTaskRunsOutput struct {
	TaskRuns []any `json:"TaskRuns"`
}

func (h *Handler) handleGetMLTaskRuns(
	_ context.Context,
	in *getMLTaskRunsInput,
) (*getMLTaskRunsOutput, error) {
	if in.TransformID == "" {
		return &getMLTaskRunsOutput{TaskRuns: []any{}}, nil
	}

	runs, err := h.Backend.GetMLTaskRuns(in.TransformID)
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getMLTaskRunsOutput{TaskRuns: result}, nil
}

// getMLTransformInput holds input for GetMLTransform.
type getMLTransformInput struct {
	TransformID string `json:"TransformId"`
}

// getMLTransformOutput holds the result for GetMLTransform.
type getMLTransformOutput struct {
	*MLTransform
}

func (h *Handler) handleGetMLTransform(
	_ context.Context,
	in *getMLTransformInput,
) (*getMLTransformOutput, error) {
	m, err := h.Backend.GetMLTransform(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &getMLTransformOutput{MLTransform: m}, nil
}

// getMLTransformsInput holds input for GetMLTransforms.
type getMLTransformsInput struct{}

// getMLTransformsOutput holds the result for GetMLTransforms.
type getMLTransformsOutput struct {
	Transforms []*MLTransform `json:"Transforms"`
}

func (h *Handler) handleGetMLTransforms(
	_ context.Context,
	_ *getMLTransformsInput,
) (*getMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()
	if transforms == nil {
		transforms = []*MLTransform{}
	}

	return &getMLTransformsOutput{Transforms: transforms}, nil
}

// getMappingInput holds input for GetMapping.
type getMappingInput struct{}

// getMappingOutput holds the result for GetMapping.
type getMappingOutput struct {
	Mapping []any `json:"Mapping"`
}

func (h *Handler) handleGetMapping(
	_ context.Context,
	_ *getMappingInput,
) (*getMappingOutput, error) {
	return &getMappingOutput{Mapping: []any{}}, nil
}

// getMaterializedViewRefreshTaskRunInput holds input for GetMaterializedViewRefreshTaskRun.
type getMaterializedViewRefreshTaskRunInput struct {
	RunID string `json:"RunId"`
}

// getMaterializedViewRefreshTaskRunOutput holds the result for GetMaterializedViewRefreshTaskRun.
type getMaterializedViewRefreshTaskRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *getMaterializedViewRefreshTaskRunInput,
) (*getMaterializedViewRefreshTaskRunOutput, error) {
	if in.RunID != "" {
		run, err := h.Backend.GetMaterializedViewRefreshTaskRun(in.RunID)
		if err != nil {
			return nil, err
		}

		return &getMaterializedViewRefreshTaskRunOutput{RunID: run.TaskRunID, Status: run.Status}, nil
	}

	runs := h.Backend.ListMaterializedViewRefreshTaskRuns()
	if len(runs) == 0 {
		return &getMaterializedViewRefreshTaskRunOutput{Status: stateSucceeded}, nil
	}

	return &getMaterializedViewRefreshTaskRunOutput{RunID: runs[0].TaskRunID, Status: runs[0].Status}, nil
}

// getPartitionInput holds input for GetPartition.
type getPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
}

// getPartitionOutput holds the result for GetPartition.
type getPartitionOutput struct {
	Partition *Partition `json:"Partition"`
}

func (h *Handler) handleGetPartition(
	_ context.Context,
	in *getPartitionInput,
) (*getPartitionOutput, error) {
	p, err := h.Backend.GetPartition(in.DatabaseName, in.TableName, in.PartitionValues)
	if err != nil {
		return nil, err
	}

	return &getPartitionOutput{Partition: p}, nil
}

// getPartitionIndexesInput holds input for GetPartitionIndexes.
type getPartitionIndexesInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getPartitionIndexesOutput holds the result for GetPartitionIndexes.
type getPartitionIndexesOutput struct {
	PartitionIndexDescriptorList []*PartitionIndex `json:"PartitionIndexDescriptorList"`
}

func (h *Handler) handleGetPartitionIndexes(
	_ context.Context,
	in *getPartitionIndexesInput,
) (*getPartitionIndexesOutput, error) {
	indexes, err := h.Backend.GetPartitionIndexes(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &getPartitionIndexesOutput{PartitionIndexDescriptorList: indexes}, nil
}

// getPartitionsInput holds input for GetPartitions.
type getPartitionsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getPartitionsOutput holds the result for GetPartitions.
type getPartitionsOutput struct {
	Partitions []*Partition `json:"Partitions"`
}

func (h *Handler) handleGetPartitions(
	_ context.Context,
	in *getPartitionsInput,
) (*getPartitionsOutput, error) {
	partitions, err := h.Backend.GetPartitions(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &getPartitionsOutput{Partitions: partitions}, nil
}

// getPlanCatalogEntry holds a catalog source/sink reference.
type getPlanCatalogEntry struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
}

// getPlanInput holds input for GetPlan.
type getPlanInput struct {
	Source   *getPlanCatalogEntry `json:"Source,omitempty"`
	Language string               `json:"Language"`
	Mapping  []MappingEntry       `json:"Mapping"`
}

// getPlanOutput holds the result for GetPlan.
type getPlanOutput struct {
	PythonScript string `json:"PythonScript"`
	ScalaCode    string `json:"ScalaCode"`
}

func (h *Handler) handleGetPlan(_ context.Context, in *getPlanInput) (*getPlanOutput, error) {
	python, scala := h.Backend.GetPlan(in.Language)
	if in.Source != nil && in.Source.TableName != "" {
		if python != "" {
			python += fmt.Sprintf("# Source: %s\n", in.Source.TableName)
		}
		if scala != "" {
			scala += fmt.Sprintf("// Source: %s\n", in.Source.TableName)
		}
	}

	return &getPlanOutput{PythonScript: python, ScalaCode: scala}, nil
}

// getRegistryInput holds input for GetRegistry.
type getRegistryInput struct {
	RegistryID *registryIDInput `json:"RegistryId"`
}

// getRegistryOutput holds the result for GetRegistry.
type getRegistryOutput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryName string            `json:"RegistryName"`
	RegistryArn  string            `json:"RegistryArn"`
	Description  string            `json:"Description,omitempty"`
	Status       string            `json:"Status"`
	CreatedTime  float64           `json:"CreatedTime,omitempty"`
	UpdatedTime  float64           `json:"UpdatedTime,omitempty"`
}

func (h *Handler) handleGetRegistry(
	_ context.Context,
	in *getRegistryInput,
) (*getRegistryOutput, error) {
	name := ""
	if in.RegistryID != nil {
		name = in.RegistryID.RegistryName
	}

	reg, err := h.Backend.DescribeRegistry(name)
	if err != nil {
		return nil, err
	}

	return &getRegistryOutput{
		RegistryName: reg.Name,
		RegistryArn:  reg.ARN,
		Status:       reg.Status,
		Description:  reg.Description,
		CreatedTime:  reg.CreatedTime,
		UpdatedTime:  reg.UpdatedTime,
		Tags:         reg.Tags,
	}, nil
}

// getResourcePoliciesInput holds input for GetResourcePolicies.
type getResourcePoliciesInput struct{}

// getResourcePoliciesOutput holds the result for GetResourcePolicies.
type getResourcePoliciesOutput struct {
	GetResourcePoliciesResponseList []any `json:"GetResourcePoliciesResponseList"`
}

func (h *Handler) handleGetResourcePolicies(
	_ context.Context,
	_ *getResourcePoliciesInput,
) (*getResourcePoliciesOutput, error) {
	return &getResourcePoliciesOutput{GetResourcePoliciesResponseList: []any{}}, nil
}

// getResourcePolicyInput holds input for GetResourcePolicy.
type getResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
}

// getResourcePolicyOutput holds the result for GetResourcePolicy.
type getResourcePolicyOutput struct {
	PolicyInJSON string `json:"PolicyInJson"`
	PolicyHash   string `json:"PolicyHash,omitempty"`
}

func (h *Handler) handleGetResourcePolicy(
	_ context.Context,
	in *getResourcePolicyInput,
) (*getResourcePolicyOutput, error) {
	policy, hash, err := h.Backend.GetResourcePolicy(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getResourcePolicyOutput{PolicyInJSON: policy, PolicyHash: hash}, nil
}

// getSchemaInput holds input for GetSchema.
type getSchemaInput struct {
	SchemaID *schemaIDInput `json:"SchemaId"`
}

// getSchemaOutput holds the result for GetSchema.
type getSchemaOutput struct {
	RegistryName  string  `json:"RegistryName"`
	RegistryArn   string  `json:"RegistryArn"`
	SchemaName    string  `json:"SchemaName"`
	SchemaArn     string  `json:"SchemaArn"`
	DataFormat    string  `json:"DataFormat"`
	Compatibility string  `json:"Compatibility"`
	SchemaStatus  string  `json:"SchemaStatus"`
	Description   string  `json:"Description,omitempty"`
	CreatedTime   float64 `json:"CreatedTime,omitempty"`
	UpdatedTime   float64 `json:"UpdatedTime,omitempty"`
}

func (h *Handler) handleGetSchema(_ context.Context, in *getSchemaInput) (*getSchemaOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	s, err := h.Backend.DescribeSchema(registryName, schemaName)
	if err != nil {
		return nil, err
	}

	return &getSchemaOutput{
		RegistryName:  s.RegistryName,
		RegistryArn:   s.RegistryARN,
		SchemaName:    s.SchemaName,
		SchemaArn:     s.SchemaARN,
		DataFormat:    s.DataFormat,
		Compatibility: s.Compatibility,
		SchemaStatus:  s.SchemaStatus,
		Description:   s.Description,
		CreatedTime:   s.CreatedTime,
		UpdatedTime:   s.UpdatedTime,
	}, nil
}

// getSchemaByDefinitionInput holds input for GetSchemaByDefinition.
type getSchemaByDefinitionInput struct {
	SchemaID         *schemaIDInput `json:"SchemaId"`
	SchemaDefinition string         `json:"SchemaDefinition"`
}

// getSchemaByDefinitionOutput holds the result for GetSchemaByDefinition.
type getSchemaByDefinitionOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	DataFormat      string `json:"DataFormat"`
	Status          string `json:"Status"`
}

func (h *Handler) handleGetSchemaByDefinition(
	_ context.Context,
	in *getSchemaByDefinitionInput,
) (*getSchemaByDefinitionOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	sv, err := h.Backend.GetSchemaByDefinition(registryName, schemaName, in.SchemaDefinition)
	if err != nil {
		return nil, err
	}

	return &getSchemaByDefinitionOutput{
		SchemaVersionID: sv.SchemaVersionID,
		SchemaArn:       sv.SchemaARN,
		DataFormat:      sv.DataFormat,
		Status:          sv.Status,
	}, nil
}

// getSchemaVersionInput holds input for GetSchemaVersion.
type getSchemaVersionInput struct {
	SchemaID            *schemaIDInput `json:"SchemaId"`
	SchemaVersionNumber *struct {
		VersionNumber int64 `json:"VersionNumber"`
		LatestVersion bool  `json:"LatestVersion"`
	} `json:"SchemaVersionNumber"`
	SchemaVersionID string `json:"SchemaVersionId"`
}

// getSchemaVersionOutput holds the result for GetSchemaVersion.
type getSchemaVersionOutput struct {
	SchemaVersionID  string  `json:"SchemaVersionId"`
	SchemaArn        string  `json:"SchemaArn"`
	SchemaDefinition string  `json:"SchemaDefinition,omitempty"`
	DataFormat       string  `json:"DataFormat,omitempty"`
	Status           string  `json:"Status"`
	VersionNumber    int64   `json:"VersionNumber"`
	CreatedTime      float64 `json:"CreatedTime,omitempty"`
}

func (h *Handler) handleGetSchemaVersion(
	_ context.Context,
	in *getSchemaVersionInput,
) (*getSchemaVersionOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	versionNumber := int64(1)
	if in.SchemaVersionNumber != nil && in.SchemaVersionNumber.VersionNumber > 0 {
		versionNumber = in.SchemaVersionNumber.VersionNumber
	}

	sv, err := h.Backend.GetSchemaVersion(registryName, schemaName, versionNumber)
	if err != nil {
		return nil, err
	}

	return &getSchemaVersionOutput{
		SchemaVersionID:  sv.SchemaVersionID,
		SchemaArn:        sv.SchemaARN,
		SchemaDefinition: sv.SchemaDefinition,
		DataFormat:       sv.DataFormat,
		Status:           sv.Status,
		VersionNumber:    sv.VersionNumber,
		CreatedTime:      sv.CreatedTime,
	}, nil
}

// schemaVersionNumberInput accepts either a plain integer or AWS-style
// {"VersionNumber": N} object, matching both API call formats in use.
type schemaVersionNumberInput struct {
	Number int64
}

func (s *schemaVersionNumberInput) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		s.Number = n

		return nil
	}

	var v struct {
		VersionNumber int64 `json:"VersionNumber"`
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	s.Number = v.VersionNumber

	return nil
}

// getSchemaVersionsDiffInput holds input for GetSchemaVersionsDiff.
type getSchemaVersionsDiffInput struct {
	SchemaID                  *schemaIDInput            `json:"SchemaId,omitempty"`
	FirstSchemaVersionNumber  *schemaVersionNumberInput `json:"FirstSchemaVersionNumber,omitempty"`
	SecondSchemaVersionNumber *schemaVersionNumberInput `json:"SecondSchemaVersionNumber,omitempty"`
	SchemaDiffType            string                    `json:"SchemaDiffType,omitempty"`
}

// getSchemaVersionsDiffOutput holds the result for GetSchemaVersionsDiff.
type getSchemaVersionsDiffOutput struct {
	Diff string `json:"Diff"`
}

func (h *Handler) handleGetSchemaVersionsDiff(
	_ context.Context,
	in *getSchemaVersionsDiffInput,
) (*getSchemaVersionsDiffOutput, error) {
	if in.SchemaID == nil {
		return &getSchemaVersionsDiffOutput{}, nil
	}

	var v1, v2 int64
	if in.FirstSchemaVersionNumber != nil {
		v1 = in.FirstSchemaVersionNumber.Number
	}
	if in.SecondSchemaVersionNumber != nil {
		v2 = in.SecondSchemaVersionNumber.Number
	}

	diff, err := h.Backend.GetSchemaVersionsDiff(in.SchemaID.RegistryName, in.SchemaID.SchemaName, v1, v2)
	if err != nil {
		return nil, err
	}

	return &getSchemaVersionsDiffOutput{Diff: diff}, nil
}

// getSecurityConfigurationInput holds input for GetSecurityConfiguration.
type getSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

// getSecurityConfigurationOutput holds the result for GetSecurityConfiguration.
type getSecurityConfigurationOutput struct {
	SecurityConfiguration *SecurityConfiguration `json:"SecurityConfiguration"`
}

func (h *Handler) handleGetSecurityConfiguration(
	_ context.Context,
	in *getSecurityConfigurationInput,
) (*getSecurityConfigurationOutput, error) {
	sc, err := h.Backend.GetSecurityConfiguration(in.Name)
	if err != nil {
		return nil, err
	}

	return &getSecurityConfigurationOutput{SecurityConfiguration: sc}, nil
}

// getSecurityConfigurationsInput holds input for GetSecurityConfigurations.
type getSecurityConfigurationsInput struct{}

// getSecurityConfigurationsOutput holds the result for GetSecurityConfigurations.
type getSecurityConfigurationsOutput struct {
	SecurityConfigurations []*SecurityConfiguration `json:"SecurityConfigurations"`
}

func (h *Handler) handleGetSecurityConfigurations(
	_ context.Context,
	_ *getSecurityConfigurationsInput,
) (*getSecurityConfigurationsOutput, error) {
	configs := h.Backend.ListSecurityConfigurations()
	if configs == nil {
		configs = []*SecurityConfiguration{}
	}

	return &getSecurityConfigurationsOutput{SecurityConfigurations: configs}, nil
}

// getSessionInput holds input for GetSession.
type getSessionInput struct {
	ID string `json:"Id"`
}

// getSessionOutput holds the result for GetSession.
type getSessionOutput struct {
	Session *Session `json:"Session"`
}

func (h *Handler) handleGetSession(
	_ context.Context,
	in *getSessionInput,
) (*getSessionOutput, error) {
	s, err := h.Backend.GetSession(in.ID)
	if err != nil {
		return nil, err
	}

	return &getSessionOutput{Session: s}, nil
}

// getStatementInput holds input for GetStatement.
type getStatementInput struct {
	SessionID   string `json:"SessionId"`
	StatementID int32  `json:"Id"`
}

// getStatementOutput holds the result for GetStatement.
type getStatementOutput struct {
	Statement *Statement `json:"Statement"`
}

func (h *Handler) handleGetStatement(
	_ context.Context,
	in *getStatementInput,
) (*getStatementOutput, error) {
	st, err := h.Backend.GetStatement(in.SessionID, in.StatementID)
	if err != nil {
		return nil, err
	}

	return &getStatementOutput{Statement: st}, nil
}

// getTableOptimizerInput holds input for GetTableOptimizer.
type getTableOptimizerInput struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

// getTableOptimizerOutput holds the result for GetTableOptimizer.
type getTableOptimizerOutput struct {
	TableOptimizer *TableOptimizer `json:"TableOptimizer"`
	CatalogID      string          `json:"CatalogId,omitempty"`
	DatabaseName   string          `json:"DatabaseName,omitempty"`
	TableName      string          `json:"TableName,omitempty"`
}

func (h *Handler) handleGetTableOptimizer(
	_ context.Context,
	in *getTableOptimizerInput,
) (*getTableOptimizerOutput, error) {
	to, err := h.Backend.GetTableOptimizer(in.DatabaseName, in.TableName, in.Type)
	if err != nil {
		return nil, err
	}

	return &getTableOptimizerOutput{
		TableOptimizer: to,
		CatalogID:      in.CatalogID,
		DatabaseName:   in.DatabaseName,
		TableName:      in.TableName,
	}, nil
}

// getTableVersionInput holds input for GetTableVersion.
type getTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
}

// getTableVersionOutput holds the result for GetTableVersion.
type getTableVersionOutput struct {
	TableVersion *TableVersion `json:"TableVersion"`
}

func (h *Handler) handleGetTableVersion(
	_ context.Context,
	in *getTableVersionInput,
) (*getTableVersionOutput, error) {
	tv, err := h.Backend.GetTableVersion(in.DatabaseName, in.TableName, in.VersionID)
	if err != nil {
		return nil, err
	}

	return &getTableVersionOutput{TableVersion: tv}, nil
}

// getTableVersionsInput holds input for GetTableVersions.
type getTableVersionsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getTableVersionsOutput holds the result for GetTableVersions.
type getTableVersionsOutput struct {
	TableVersions []*TableVersion `json:"TableVersions"`
}

func (h *Handler) handleGetTableVersions(
	_ context.Context,
	in *getTableVersionsInput,
) (*getTableVersionsOutput, error) {
	versions := h.Backend.GetTableVersions(in.DatabaseName, in.TableName)

	return &getTableVersionsOutput{TableVersions: versions}, nil
}

// getTriggerInput holds input for GetTrigger.
type getTriggerInput struct {
	Name string `json:"Name"`
}

// getTriggerOutput holds the result for GetTrigger.
type getTriggerOutput struct {
	Trigger *Trigger `json:"Trigger"`
}

func (h *Handler) handleGetTrigger(
	_ context.Context,
	in *getTriggerInput,
) (*getTriggerOutput, error) {
	t, err := h.Backend.GetTrigger(in.Name)
	if err != nil {
		return nil, err
	}

	return &getTriggerOutput{Trigger: t}, nil
}

// getTriggersInput holds input for GetTriggers.
type getTriggersInput struct{}

// getTriggersOutput holds the result for GetTriggers.
type getTriggersOutput struct {
	Triggers []*Trigger `json:"Triggers"`
}

func (h *Handler) handleGetTriggers(
	_ context.Context,
	_ *getTriggersInput,
) (*getTriggersOutput, error) {
	return &getTriggersOutput{Triggers: h.Backend.GetTriggers()}, nil
}

// getUnfilteredPartitionMetadataInput holds input for GetUnfilteredPartitionMetadata.
type getUnfilteredPartitionMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	TableName                string   `json:"TableName"`
	PartitionValues          []string `json:"PartitionValues"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// getUnfilteredPartitionMetadataOutput holds the result for GetUnfilteredPartitionMetadata.
type getUnfilteredPartitionMetadataOutput struct {
	Partition                     *Partition `json:"Partition"`
	AuthorizedColumns             []string   `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool       `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredPartitionMetadata(
	_ context.Context,
	in *getUnfilteredPartitionMetadataInput,
) (*getUnfilteredPartitionMetadataOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return &getUnfilteredPartitionMetadataOutput{AuthorizedColumns: []string{}}, nil
	}

	p, err := h.Backend.GetPartition(in.DatabaseName, in.TableName, in.PartitionValues)
	if err != nil {
		return nil, err
	}

	return &getUnfilteredPartitionMetadataOutput{
		Partition:         p,
		AuthorizedColumns: []string{},
	}, nil
}

// getUnfilteredPartitionsMetadataInput holds input for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	TableName                string   `json:"TableName"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// unfilteredPartitionEntry wraps a Partition for the unfiltered metadata response.
type unfilteredPartitionEntry struct {
	Partition                     *Partition `json:"Partition"`
	AuthorizedColumns             []string   `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool       `json:"IsRegisteredWithLakeFormation"`
}

// getUnfilteredPartitionsMetadataOutput holds the result for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataOutput struct {
	UnfilteredPartitions []any `json:"UnfilteredPartitions"`
}

func (h *Handler) handleGetUnfilteredPartitionsMetadata(
	_ context.Context,
	in *getUnfilteredPartitionsMetadataInput,
) (*getUnfilteredPartitionsMetadataOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return &getUnfilteredPartitionsMetadataOutput{UnfilteredPartitions: []any{}}, nil
	}

	partitions, err := h.Backend.GetPartitions(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, unfilteredPartitionEntry{
			Partition:         p,
			AuthorizedColumns: []string{},
		})
	}

	return &getUnfilteredPartitionsMetadataOutput{UnfilteredPartitions: result}, nil
}

// getUnfilteredTableMetadataInput holds input for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	Name                     string   `json:"Name"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// getUnfilteredTableMetadataOutput holds the result for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataOutput struct {
	Table                         *Table   `json:"Table"`
	AuthorizedColumns             []string `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool     `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredTableMetadata(
	_ context.Context,
	in *getUnfilteredTableMetadataInput,
) (*getUnfilteredTableMetadataOutput, error) {
	if in.DatabaseName == "" || in.Name == "" {
		return &getUnfilteredTableMetadataOutput{AuthorizedColumns: []string{}}, nil
	}

	tbl, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	return &getUnfilteredTableMetadataOutput{
		Table:             tbl,
		AuthorizedColumns: []string{},
	}, nil
}

// getUsageProfileInput holds input for GetUsageProfile.
type getUsageProfileInput struct {
	Name string `json:"Name"`
}

// getUsageProfileOutput holds the result for GetUsageProfile.
type getUsageProfileOutput struct {
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
	CreatedOn      float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn float64           `json:"LastModifiedOn,omitempty"`
}

func (h *Handler) handleGetUsageProfile(
	_ context.Context,
	in *getUsageProfileInput,
) (*getUsageProfileOutput, error) {
	if in.Name == "" {
		return &getUsageProfileOutput{}, nil
	}

	p, err := h.Backend.GetUsageProfile(in.Name)
	if err != nil {
		return nil, err
	}

	return &getUsageProfileOutput{
		Name:           p.Name,
		Description:    p.Description,
		CreatedOn:      float64(p.CreatedOn.Unix()),
		LastModifiedOn: float64(p.LastModifiedOn.Unix()),
		Tags:           p.Tags,
	}, nil
}

// getUserDefinedFunctionInput holds input for GetUserDefinedFunction.
type getUserDefinedFunctionInput struct {
	DatabaseName string `json:"DatabaseName"`
	FunctionName string `json:"FunctionName"`
}

// getUserDefinedFunctionOutput holds the result for GetUserDefinedFunction.
type getUserDefinedFunctionOutput struct {
	UserDefinedFunction *UserDefinedFunction `json:"UserDefinedFunction"`
}

func (h *Handler) handleGetUserDefinedFunction(
	_ context.Context,
	in *getUserDefinedFunctionInput,
) (*getUserDefinedFunctionOutput, error) {
	u, err := h.Backend.GetUserDefinedFunction(in.DatabaseName, in.FunctionName)
	if err != nil {
		return nil, err
	}

	return &getUserDefinedFunctionOutput{UserDefinedFunction: u}, nil
}

// getUserDefinedFunctionsInput holds input for GetUserDefinedFunctions.
type getUserDefinedFunctionsInput struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
}

// getUserDefinedFunctionsOutput holds the result for GetUserDefinedFunctions.
type getUserDefinedFunctionsOutput struct {
	UserDefinedFunctions []*UserDefinedFunction `json:"UserDefinedFunctions"`
}

func (h *Handler) handleGetUserDefinedFunctions(
	_ context.Context,
	in *getUserDefinedFunctionsInput,
) (*getUserDefinedFunctionsOutput, error) {
	udfs := h.Backend.GetUserDefinedFunctions(in.DatabaseName)
	if udfs == nil {
		udfs = []*UserDefinedFunction{}
	}

	return &getUserDefinedFunctionsOutput{UserDefinedFunctions: udfs}, nil
}

// getWorkflowInput holds input for GetWorkflow.
type getWorkflowInput struct {
	Name string `json:"Name"`
}

// getWorkflowOutput holds the result for GetWorkflow.
type getWorkflowOutput struct {
	Workflow *Workflow `json:"Workflow"`
}

func (h *Handler) handleGetWorkflow(
	_ context.Context,
	in *getWorkflowInput,
) (*getWorkflowOutput, error) {
	w, err := h.Backend.GetWorkflow(in.Name)
	if err != nil {
		return nil, err
	}

	return &getWorkflowOutput{Workflow: w}, nil
}

// getWorkflowRunInput holds input for GetWorkflowRun.
type getWorkflowRunInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

// getWorkflowRunOutput holds the result for GetWorkflowRun.
type getWorkflowRunOutput struct {
	Run *WorkflowRun `json:"Run"`
}

func (h *Handler) handleGetWorkflowRun(
	_ context.Context,
	in *getWorkflowRunInput,
) (*getWorkflowRunOutput, error) {
	run, err := h.Backend.GetWorkflowRun(in.Name, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getWorkflowRunOutput{Run: run}, nil
}

// getWorkflowRunPropertiesInput holds input for GetWorkflowRunProperties.
type getWorkflowRunPropertiesInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

// getWorkflowRunPropertiesOutput holds the result for GetWorkflowRunProperties.
type getWorkflowRunPropertiesOutput struct {
	RunProperties map[string]string `json:"RunProperties"`
}

func (h *Handler) handleGetWorkflowRunProperties(
	_ context.Context,
	in *getWorkflowRunPropertiesInput,
) (*getWorkflowRunPropertiesOutput, error) {
	if in.Name != "" && in.RunID != "" {
		run, err := h.Backend.GetWorkflowRun(in.Name, in.RunID)
		if err == nil && run.Properties != nil {
			return &getWorkflowRunPropertiesOutput{RunProperties: run.Properties}, nil
		}
	}

	return &getWorkflowRunPropertiesOutput{RunProperties: map[string]string{}}, nil
}

// getWorkflowRunsInput holds input for GetWorkflowRuns.
type getWorkflowRunsInput struct {
	Name string `json:"Name"`
}

// getWorkflowRunsOutput holds the result for GetWorkflowRuns.
type getWorkflowRunsOutput struct {
	Runs []*WorkflowRun `json:"Runs"`
}

func (h *Handler) handleGetWorkflowRuns(
	_ context.Context,
	in *getWorkflowRunsInput,
) (*getWorkflowRunsOutput, error) {
	runs, err := h.Backend.GetWorkflowRuns(in.Name)
	if err != nil {
		return nil, err
	}

	return &getWorkflowRunsOutput{Runs: runs}, nil
}

// importCatalogToGlueInput holds input for ImportCatalogToGlue.
type importCatalogToGlueInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleImportCatalogToGlue(
	_ context.Context,
	in *importCatalogToGlueInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ImportCatalogToGlue(in.CatalogID)
}

// listBlueprintsInput holds input for ListBlueprints.
type listBlueprintsInput struct{}

// listBlueprintsOutput holds the result for ListBlueprints.
type listBlueprintsOutput struct {
	Blueprints []string `json:"Blueprints"`
}

func (h *Handler) handleListBlueprints(
	_ context.Context,
	_ *listBlueprintsInput,
) (*listBlueprintsOutput, error) {
	return &listBlueprintsOutput{Blueprints: h.Backend.ListBlueprints()}, nil
}

// listColumnStatisticsTaskRunsInput holds input for ListColumnStatisticsTaskRuns.
type listColumnStatisticsTaskRunsInput struct{}

// listColumnStatisticsTaskRunsOutput holds the result for ListColumnStatisticsTaskRuns.
type listColumnStatisticsTaskRunsOutput struct {
	ColumnStatisticsTaskRunIDs []string `json:"ColumnStatisticsTaskRunIds"`
}

func (h *Handler) handleListColumnStatisticsTaskRuns(
	_ context.Context,
	_ *listColumnStatisticsTaskRunsInput,
) (*listColumnStatisticsTaskRunsOutput, error) {
	runs := h.Backend.ListColumnStatisticsTaskRuns()
	ids := make([]string, 0, len(runs))
	for _, r := range runs {
		ids = append(ids, r.ColumnStatisticsTaskRunID)
	}

	return &listColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRunIDs: ids}, nil
}

// listConnectionTypesInput holds input for ListConnectionTypes.
type listConnectionTypesInput struct{}

// listConnectionTypesOutput holds the result for ListConnectionTypes.
type listConnectionTypesOutput struct {
	ConnectionTypes []any `json:"ConnectionTypes"`
}

func (h *Handler) handleListConnectionTypes(
	_ context.Context,
	_ *listConnectionTypesInput,
) (*listConnectionTypesOutput, error) {
	return &listConnectionTypesOutput{ConnectionTypes: []any{}}, nil
}

// listCrawlsInput holds input for ListCrawls.
type listCrawlsInput struct{}

// listCrawlsOutput holds the result for ListCrawls.
type listCrawlsOutput struct {
	Crawls []any `json:"Crawls"`
}

func (h *Handler) handleListCrawls(
	_ context.Context,
	_ *listCrawlsInput,
) (*listCrawlsOutput, error) {
	return &listCrawlsOutput{Crawls: []any{}}, nil
}

// listCustomEntityTypesInput holds input for ListCustomEntityTypes.
type listCustomEntityTypesInput struct{}

// listCustomEntityTypesOutput holds the result for ListCustomEntityTypes.
type listCustomEntityTypesOutput struct {
	CustomEntityTypes []*CustomEntityType `json:"CustomEntityTypes"`
}

func (h *Handler) handleListCustomEntityTypes(
	_ context.Context,
	_ *listCustomEntityTypesInput,
) (*listCustomEntityTypesOutput, error) {
	return &listCustomEntityTypesOutput{CustomEntityTypes: h.Backend.ListCustomEntityTypes()}, nil
}

// listDataQualityResultsInput holds input for ListDataQualityResults.
type listDataQualityResultsInput struct{}

// listDataQualityResultsOutput holds the result for ListDataQualityResults.
type listDataQualityResultsOutput struct {
	Results []any `json:"Results"`
}

func (h *Handler) handleListDataQualityResults(
	_ context.Context,
	_ *listDataQualityResultsInput,
) (*listDataQualityResultsOutput, error) {
	results := h.Backend.ListDataQualityResults()
	list := make([]any, 0, len(results))

	for _, r := range results {
		list = append(list, r)
	}

	return &listDataQualityResultsOutput{Results: list}, nil
}

// listDataQualityRuleRecommendationRunsInput holds input for ListDataQualityRuleRecommendationRuns.
type listDataQualityRuleRecommendationRunsInput struct{}

// listDataQualityRuleRecommendationRunsOutput holds the result for ListDataQualityRuleRecommendationRuns.
type listDataQualityRuleRecommendationRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListDataQualityRuleRecommendationRuns(
	_ context.Context,
	_ *listDataQualityRuleRecommendationRunsInput,
) (*listDataQualityRuleRecommendationRunsOutput, error) {
	runs := h.Backend.ListDataQualityRuleRecommendationRuns()
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &listDataQualityRuleRecommendationRunsOutput{Runs: result}, nil
}

// listDataQualityRulesetEvaluationRunsInput holds input for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsInput struct{}

// listDataQualityRulesetEvaluationRunsOutput holds the result for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListDataQualityRulesetEvaluationRuns(
	_ context.Context,
	_ *listDataQualityRulesetEvaluationRunsInput,
) (*listDataQualityRulesetEvaluationRunsOutput, error) {
	runs := h.Backend.ListDataQualityEvaluationRuns()
	result := make([]any, 0, len(runs))

	for _, r := range runs {
		result = append(result, r)
	}

	return &listDataQualityRulesetEvaluationRunsOutput{Runs: result}, nil
}

// listDataQualityStatisticAnnotationsInput holds input for ListDataQualityStatisticAnnotations.
type listDataQualityStatisticAnnotationsInput struct{}

// listDataQualityStatisticAnnotationsOutput holds the result for ListDataQualityStatisticAnnotations.
type listDataQualityStatisticAnnotationsOutput struct {
	StatisticAnnotationList []any `json:"StatisticAnnotationList"`
}

func (h *Handler) handleListDataQualityStatisticAnnotations(
	_ context.Context,
	_ *listDataQualityStatisticAnnotationsInput,
) (*listDataQualityStatisticAnnotationsOutput, error) {
	return &listDataQualityStatisticAnnotationsOutput{StatisticAnnotationList: []any{}}, nil
}

// listDataQualityStatisticsInput holds input for ListDataQualityStatistics.
type listDataQualityStatisticsInput struct{}

// listDataQualityStatisticsOutput holds the result for ListDataQualityStatistics.
type listDataQualityStatisticsOutput struct {
	Statistics []any `json:"Statistics"`
}

func (h *Handler) handleListDataQualityStatistics(
	_ context.Context,
	_ *listDataQualityStatisticsInput,
) (*listDataQualityStatisticsOutput, error) {
	return &listDataQualityStatisticsOutput{Statistics: []any{}}, nil
}

// listDevEndpointsInput holds input for ListDevEndpoints.
type listDevEndpointsInput struct{}

// listDevEndpointsOutput holds the result for ListDevEndpoints.
type listDevEndpointsOutput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

func (h *Handler) handleListDevEndpoints(
	_ context.Context,
	_ *listDevEndpointsInput,
) (*listDevEndpointsOutput, error) {
	deps := h.Backend.GetAllDevEndpoints()
	names := make([]string, 0, len(deps))
	for _, d := range deps {
		names = append(names, d.EndpointName)
	}

	return &listDevEndpointsOutput{DevEndpointNames: names}, nil
}

// listEntitiesInput holds input for ListEntities.
type listEntitiesInput struct{}

// listEntitiesOutput holds the result for ListEntities.
type listEntitiesOutput struct {
	Entities []any `json:"Entities"`
}

func (h *Handler) handleListEntities(
	_ context.Context,
	_ *listEntitiesInput,
) (*listEntitiesOutput, error) {
	return &listEntitiesOutput{Entities: []any{}}, nil
}

// listIntegrationResourcePropertiesInput holds input for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesInput struct{}

// listIntegrationResourcePropertiesOutput holds the result for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesOutput struct {
	ResourcePropertiesList []any `json:"ResourcePropertiesList"`
}

func (h *Handler) handleListIntegrationResourceProperties(
	_ context.Context,
	_ *listIntegrationResourcePropertiesInput,
) (*listIntegrationResourcePropertiesOutput, error) {
	return &listIntegrationResourcePropertiesOutput{ResourcePropertiesList: []any{}}, nil
}

// listJobsInput holds input for ListJobs.
type listJobsInput struct{}

// listJobsOutput holds the result for ListJobs.
type listJobsOutput struct {
	JobNames []string `json:"JobNames"`
}

func (h *Handler) handleListJobs(_ context.Context, _ *listJobsInput) (*listJobsOutput, error) {
	jobs := h.Backend.GetJobs()
	names := make([]string, 0, len(jobs))

	for _, j := range jobs {
		names = append(names, j.Name)
	}

	return &listJobsOutput{JobNames: names}, nil
}

// listMLTransformsInput holds input for ListMLTransforms.
type listMLTransformsInput struct{}

// listMLTransformsOutput holds the result for ListMLTransforms.
type listMLTransformsOutput struct {
	TransformIDs []string `json:"TransformIds"`
}

func (h *Handler) handleListMLTransforms(
	_ context.Context,
	_ *listMLTransformsInput,
) (*listMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()
	ids := make([]string, 0, len(transforms))

	for _, m := range transforms {
		ids = append(ids, m.TransformID)
	}

	return &listMLTransformsOutput{TransformIDs: ids}, nil
}

// listMaterializedViewRefreshTaskRunsInput holds input for ListMaterializedViewRefreshTaskRuns.
type listMaterializedViewRefreshTaskRunsInput struct{}

// listMaterializedViewRefreshTaskRunsOutput holds the result for ListMaterializedViewRefreshTaskRuns.
type listMaterializedViewRefreshTaskRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListMaterializedViewRefreshTaskRuns(
	_ context.Context,
	_ *listMaterializedViewRefreshTaskRunsInput,
) (*listMaterializedViewRefreshTaskRunsOutput, error) {
	runs := h.Backend.ListMaterializedViewRefreshTaskRuns()
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &listMaterializedViewRefreshTaskRunsOutput{Runs: result}, nil
}

// listRegistriesInput holds input for ListRegistries.
type listRegistriesInput struct{}

// listRegistriesOutput holds the result for ListRegistries.
type listRegistriesOutput struct {
	Registries []*Registry `json:"Registries"`
}

func (h *Handler) handleListRegistries(
	_ context.Context,
	_ *listRegistriesInput,
) (*listRegistriesOutput, error) {
	regs := h.Backend.ListRegistries()

	return &listRegistriesOutput{Registries: regs}, nil
}

// listSchemaVersionsInput holds input for ListSchemaVersions.
type listSchemaVersionsInput struct {
	SchemaID *schemaIDInput `json:"SchemaId"`
}

// listSchemaVersionsOutput holds the result for ListSchemaVersions.
type listSchemaVersionsOutput struct {
	SchemaVersions []*SchemaVersion `json:"SchemaVersions"`
}

func (h *Handler) handleListSchemaVersions(
	_ context.Context,
	in *listSchemaVersionsInput,
) (*listSchemaVersionsOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	versions := h.Backend.ListSchemaVersions(registryName, schemaName)
	if versions == nil {
		versions = []*SchemaVersion{}
	}

	return &listSchemaVersionsOutput{SchemaVersions: versions}, nil
}

// listSchemasInput holds input for ListSchemas.
type listSchemasInput struct {
	RegistryID *registryIDInput `json:"RegistryId"`
}

// listSchemasOutput holds the result for ListSchemas.
type listSchemasOutput struct {
	Schemas []*Schema `json:"Schemas"`
}

func (h *Handler) handleListSchemas(
	_ context.Context,
	in *listSchemasInput,
) (*listSchemasOutput, error) {
	registryName := ""
	if in.RegistryID != nil {
		registryName = in.RegistryID.RegistryName
	}

	schemas := h.Backend.ListSchemas(registryName)

	return &listSchemasOutput{Schemas: schemas}, nil
}

// listSessionsInput holds input for ListSessions.
type listSessionsInput struct{}

// listSessionsOutput holds the result for ListSessions.
type listSessionsOutput struct {
	IDs      []string   `json:"Ids"`
	Sessions []*Session `json:"Sessions"`
}

func (h *Handler) handleListSessions(
	_ context.Context,
	_ *listSessionsInput,
) (*listSessionsOutput, error) {
	sessions := h.Backend.ListSessions()
	ids := make([]string, len(sessions))
	for i, s := range sessions {
		ids[i] = s.SessionID
	}

	return &listSessionsOutput{IDs: ids, Sessions: sessions}, nil
}

// listStatementsInput holds input for ListStatements.
type listStatementsInput struct {
	SessionID string `json:"SessionId"`
}

// listStatementsOutput holds the result for ListStatements.
type listStatementsOutput struct {
	Statements []*Statement `json:"Statements"`
}

func (h *Handler) handleListStatements(
	_ context.Context,
	in *listStatementsInput,
) (*listStatementsOutput, error) {
	stmts, err := h.Backend.GetStatements(in.SessionID)
	if err != nil {
		return nil, err
	}
	if stmts == nil {
		stmts = []*Statement{}
	}

	return &listStatementsOutput{Statements: stmts}, nil
}

// listTableOptimizerRunsInput holds input for ListTableOptimizerRuns.
type listTableOptimizerRunsInput struct{}

// listTableOptimizerRunsOutput holds the result for ListTableOptimizerRuns.
type listTableOptimizerRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListTableOptimizerRuns(
	_ context.Context,
	_ *listTableOptimizerRunsInput,
) (*listTableOptimizerRunsOutput, error) {
	return &listTableOptimizerRunsOutput{Runs: []any{}}, nil
}

// listTriggersInput holds input for ListTriggers.
type listTriggersInput struct{}

// listTriggersOutput holds the result for ListTriggers.
type listTriggersOutput struct {
	TriggerNames []string `json:"TriggerNames"`
}

func (h *Handler) handleListTriggers(
	_ context.Context,
	_ *listTriggersInput,
) (*listTriggersOutput, error) {
	triggers := h.Backend.GetTriggers()
	names := make([]string, 0, len(triggers))
	for _, t := range triggers {
		names = append(names, t.Name)
	}

	return &listTriggersOutput{TriggerNames: names}, nil
}

// listUsageProfilesInput holds input for ListUsageProfiles.
type listUsageProfilesInput struct{}

// listUsageProfilesOutput holds the result for ListUsageProfiles.
type listUsageProfilesOutput struct {
	Profiles []any `json:"Profiles"`
}

func (h *Handler) handleListUsageProfiles(
	_ context.Context,
	_ *listUsageProfilesInput,
) (*listUsageProfilesOutput, error) {
	profiles := h.Backend.ListUsageProfiles()
	result := make([]any, 0, len(profiles))
	for _, p := range profiles {
		result = append(result, p)
	}

	return &listUsageProfilesOutput{Profiles: result}, nil
}

// listWorkflowsInput holds input for ListWorkflows.
type listWorkflowsInput struct{}

// listWorkflowsOutput holds the result for ListWorkflows.
type listWorkflowsOutput struct {
	Workflows []string `json:"Workflows"`
}

func (h *Handler) handleListWorkflows(
	_ context.Context,
	_ *listWorkflowsInput,
) (*listWorkflowsOutput, error) {
	return &listWorkflowsOutput{Workflows: h.Backend.GetWorkflows()}, nil
}

// modifyIntegrationInput holds input for ModifyIntegration.
type modifyIntegrationInput struct {
	IntegrationIdentifier string `json:"IntegrationIdentifier"`
}

// modifyIntegrationOutput holds the result for ModifyIntegration.
type modifyIntegrationOutput struct {
	IntegrationArn string `json:"IntegrationArn"`
	Status         string `json:"Status"`
}

func (h *Handler) handleModifyIntegration(
	_ context.Context,
	in *modifyIntegrationInput,
) (*modifyIntegrationOutput, error) {
	if err := h.Backend.ModifyIntegration(in.IntegrationIdentifier); err != nil {
		return nil, err
	}

	return &modifyIntegrationOutput{Status: stateActive}, nil
}

// putDataCatalogEncryptionSettingsInput holds input for PutDataCatalogEncryptionSettings.
type putDataCatalogEncryptionSettingsInput struct {
	DataCatalogEncryptionSettings DataCatalogEncryptionSettings `json:"DataCatalogEncryptionSettings"`
	CatalogID                     string                        `json:"CatalogId,omitempty"`
}

func (h *Handler) handlePutDataCatalogEncryptionSettings(
	_ context.Context,
	in *putDataCatalogEncryptionSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutDataCatalogEncryptionSettings(
		in.CatalogID,
		in.DataCatalogEncryptionSettings,
	)
}

// putDataQualityProfileAnnotationInput holds input for PutDataQualityProfileAnnotation.
type putDataQualityProfileAnnotationInput struct{}

func (h *Handler) handlePutDataQualityProfileAnnotation(
	_ context.Context,
	_ *putDataQualityProfileAnnotationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// putResourcePolicyInput holds input for PutResourcePolicy.
type putResourcePolicyInput struct {
	PolicyInJSON string `json:"PolicyInJson"`
	ResourceArn  string `json:"ResourceArn,omitempty"`
}

// putResourcePolicyOutput holds the result for PutResourcePolicy.
type putResourcePolicyOutput struct {
	PolicyHash string `json:"PolicyHash"`
}

func (h *Handler) handlePutResourcePolicy(
	_ context.Context,
	in *putResourcePolicyInput,
) (*putResourcePolicyOutput, error) {
	hash, err := h.Backend.PutResourcePolicy(in.PolicyInJSON, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &putResourcePolicyOutput{PolicyHash: hash}, nil
}

// putSchemaVersionMetadataInput holds input for PutSchemaVersionMetadata.
type putSchemaVersionMetadataInput struct {
	MetadataKeyValue *struct {
		MetadataKey   string `json:"MetadataKey"`
		MetadataValue string `json:"MetadataValue"`
	} `json:"MetadataKeyValue"`
	SchemaVersionID string `json:"SchemaVersionId"`
}

// putSchemaVersionMetadataOutput holds the result for PutSchemaVersionMetadata.
type putSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	VersionNumber   int64  `json:"VersionNumber"`
	LatestVersion   bool   `json:"LatestVersion"`
}

func (h *Handler) handlePutSchemaVersionMetadata(
	_ context.Context,
	in *putSchemaVersionMetadataInput,
) (*putSchemaVersionMetadataOutput, error) {
	key, value := "", ""
	if in.MetadataKeyValue != nil {
		key = in.MetadataKeyValue.MetadataKey
		value = in.MetadataKeyValue.MetadataValue
	}

	if err := h.Backend.PutSchemaVersionMetadata(in.SchemaVersionID, key, value); err != nil {
		return nil, err
	}

	return &putSchemaVersionMetadataOutput{SchemaVersionID: in.SchemaVersionID}, nil
}

// putWorkflowRunPropertiesInput holds input for PutWorkflowRunProperties.
type putWorkflowRunPropertiesInput struct {
	RunProperties map[string]string `json:"RunProperties"`
	Name          string            `json:"Name"`
	RunID         string            `json:"RunId"`
}

func (h *Handler) handlePutWorkflowRunProperties(
	_ context.Context,
	in *putWorkflowRunPropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutWorkflowRunProperties(in.Name, in.RunID, in.RunProperties)
}

// querySchemaVersionMetadataInput holds input for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataInput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
}

// querySchemaVersionMetadataOutput holds the result for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataOutput struct {
	MetadataInfo    map[string]any `json:"MetadataInfo"`
	SchemaVersionID string         `json:"SchemaVersionId"`
}

func (h *Handler) handleQuerySchemaVersionMetadata(
	_ context.Context,
	in *querySchemaVersionMetadataInput,
) (*querySchemaVersionMetadataOutput, error) {
	raw := h.Backend.QuerySchemaVersionMetadata(in.SchemaVersionID)

	meta := make(map[string]any, len(raw))
	for k, v := range raw {
		meta[k] = map[string]any{"MetadataValue": v, "CreatedTime": ""}
	}

	return &querySchemaVersionMetadataOutput{
		MetadataInfo:    meta,
		SchemaVersionID: in.SchemaVersionID,
	}, nil
}

// registerConnectionTypeInput holds input for RegisterConnectionType.
type registerConnectionTypeInput struct{}

// registerConnectionTypeOutput holds the result for RegisterConnectionType.
type registerConnectionTypeOutput struct {
	ConnectionType string `json:"ConnectionType"`
	Status         string `json:"Status"`
}

func (h *Handler) handleRegisterConnectionType(
	_ context.Context,
	_ *registerConnectionTypeInput,
) (*registerConnectionTypeOutput, error) {
	return &registerConnectionTypeOutput{Status: stateReady}, nil
}

// registerSchemaVersionInput holds input for RegisterSchemaVersion.
type registerSchemaVersionInput struct {
	SchemaID         *schemaIDInput `json:"SchemaId"`
	SchemaDefinition string         `json:"SchemaDefinition"`
}

// registerSchemaVersionOutput holds the result for RegisterSchemaVersion.
type registerSchemaVersionOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	Status          string `json:"Status"`
	VersionNumber   int64  `json:"VersionNumber"`
}

func (h *Handler) handleRegisterSchemaVersion(
	_ context.Context,
	in *registerSchemaVersionInput,
) (*registerSchemaVersionOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	sv, err := h.Backend.RegisterSchemaVersion(registryName, schemaName, in.SchemaDefinition)
	if err != nil {
		return nil, err
	}

	return &registerSchemaVersionOutput{
		SchemaVersionID: sv.SchemaVersionID,
		SchemaArn:       sv.SchemaARN,
		Status:          sv.Status,
		VersionNumber:   sv.VersionNumber,
	}, nil
}

// removeSchemaVersionMetadataInput holds input for RemoveSchemaVersionMetadata.
type removeSchemaVersionMetadataInput struct {
	MetadataKeyValue *struct {
		MetadataKey   string `json:"MetadataKey"`
		MetadataValue string `json:"MetadataValue"`
	} `json:"MetadataKeyValue"`
	SchemaVersionID string `json:"SchemaVersionId"`
}

// removeSchemaVersionMetadataOutput holds the result for RemoveSchemaVersionMetadata.
type removeSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	VersionNumber   int64  `json:"VersionNumber"`
	LatestVersion   bool   `json:"LatestVersion"`
}

func (h *Handler) handleRemoveSchemaVersionMetadata(
	_ context.Context,
	in *removeSchemaVersionMetadataInput,
) (*removeSchemaVersionMetadataOutput, error) {
	key := ""
	if in.MetadataKeyValue != nil {
		key = in.MetadataKeyValue.MetadataKey
	}

	if err := h.Backend.RemoveSchemaVersionMetadata(in.SchemaVersionID, key); err != nil {
		return nil, err
	}

	return &removeSchemaVersionMetadataOutput{SchemaVersionID: in.SchemaVersionID}, nil
}

// resumeWorkflowRunInput holds input for ResumeWorkflowRun.
type resumeWorkflowRunInput struct {
	Name    string   `json:"Name"`
	RunID   string   `json:"RunId"`
	NodeIDs []string `json:"NodeIds"`
}

// resumeWorkflowRunOutput holds the result for ResumeWorkflowRun.
type resumeWorkflowRunOutput struct {
	RunID   string   `json:"RunId"`
	NodeIDs []string `json:"NodeIds"`
}

func (h *Handler) handleResumeWorkflowRun(
	_ context.Context,
	in *resumeWorkflowRunInput,
) (*resumeWorkflowRunOutput, error) {
	if in.Name == "" || in.RunID == "" {
		return &resumeWorkflowRunOutput{NodeIDs: []string{}}, nil
	}

	runID, nodeIDs, err := h.Backend.ResumeWorkflowRun(in.Name, in.RunID)
	if err != nil {
		return nil, err
	}

	return &resumeWorkflowRunOutput{RunID: runID, NodeIDs: nodeIDs}, nil
}

// runStatementInput holds input for RunStatement.
type runStatementInput struct {
	SessionID string `json:"SessionId"`
	Code      string `json:"Code"`
}

// runStatementOutput holds the result for RunStatement.
type runStatementOutput struct {
	ID int32 `json:"Id"`
}

func (h *Handler) handleRunStatement(
	_ context.Context,
	in *runStatementInput,
) (*runStatementOutput, error) {
	st, err := h.Backend.RunStatement(in.SessionID, in.Code)
	if err != nil {
		return nil, err
	}

	return &runStatementOutput{ID: st.Id}, nil
}

// searchTablesInput holds input for SearchTables.
type searchTablesInput struct {
	SearchText string `json:"SearchText,omitempty"`
}

// searchTablesOutput holds the result for SearchTables.
type searchTablesOutput struct {
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleSearchTables(
	_ context.Context,
	in *searchTablesInput,
) (*searchTablesOutput, error) {
	tables := h.Backend.SearchTables(in.SearchText)

	return &searchTablesOutput{TableList: tables}, nil
}

// startBlueprintRunInput holds input for StartBlueprintRun.
type startBlueprintRunInput struct {
	BlueprintName string `json:"BlueprintName"`
}

// startBlueprintRunOutput holds the result for StartBlueprintRun.
type startBlueprintRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartBlueprintRun(
	_ context.Context,
	in *startBlueprintRunInput,
) (*startBlueprintRunOutput, error) {
	run, err := h.Backend.StartBlueprintRun(in.BlueprintName)
	if err != nil {
		return nil, err
	}

	return &startBlueprintRunOutput{RunID: run.RunID}, nil
}

// startColumnStatisticsTaskRunInput holds input for StartColumnStatisticsTaskRun.
type startColumnStatisticsTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// startColumnStatisticsTaskRunOutput holds the result for StartColumnStatisticsTaskRun.
type startColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

func (h *Handler) handleStartColumnStatisticsTaskRun(
	_ context.Context,
	in *startColumnStatisticsTaskRunInput,
) (*startColumnStatisticsTaskRunOutput, error) {
	run, err := h.Backend.StartColumnStatisticsTaskRun(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &startColumnStatisticsTaskRunOutput{ColumnStatisticsTaskRunID: run.ColumnStatisticsTaskRunID}, nil
}

// startColumnStatisticsTaskRunScheduleInput holds input for StartColumnStatisticsTaskRunSchedule.
type startColumnStatisticsTaskRunScheduleInput struct{}

func (h *Handler) handleStartColumnStatisticsTaskRunSchedule(
	_ context.Context,
	_ *startColumnStatisticsTaskRunScheduleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// startDataQualityRuleRecommendationRunInput holds input for StartDataQualityRuleRecommendationRun.
type startDataQualityRuleRecommendationRunInput struct {
	DataSource struct {
		GlueTable *GlueTable `json:"GlueTable,omitempty"`
	} `json:"DataSource,omitzero"`
	OutputS3Path string `json:"OutputS3Path,omitempty"`
	Role         string `json:"Role,omitempty"`
}

// startDataQualityRuleRecommendationRunOutput holds the result for StartDataQualityRuleRecommendationRun.
type startDataQualityRuleRecommendationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRuleRecommendationRun(
	_ context.Context,
	in *startDataQualityRuleRecommendationRunInput,
) (*startDataQualityRuleRecommendationRunOutput, error) {
	run, err := h.Backend.StartDataQualityRuleRecommendationRun(in.OutputS3Path)
	if err != nil {
		return nil, err
	}

	return &startDataQualityRuleRecommendationRunOutput{RunID: run.RecommendationRunID}, nil
}

// startExportLabelsTaskRunInput holds input for StartExportLabelsTaskRun.
type startExportLabelsTaskRunInput struct {
	TransformID  string `json:"TransformId"`
	OutputS3Path string `json:"OutputS3Path,omitempty"`
}

// startExportLabelsTaskRunOutput holds the result for StartExportLabelsTaskRun.
type startExportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartExportLabelsTaskRun(
	_ context.Context,
	in *startExportLabelsTaskRunInput,
) (*startExportLabelsTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startExportLabelsTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartExportLabelsTaskRun(in.TransformID, in.OutputS3Path)
	if err != nil {
		return nil, err
	}

	return &startExportLabelsTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startImportLabelsTaskRunInput holds input for StartImportLabelsTaskRun.
type startImportLabelsTaskRunInput struct {
	TransformID string `json:"TransformId"`
	InputS3Path string `json:"InputS3Path,omitempty"`
}

// startImportLabelsTaskRunOutput holds the result for StartImportLabelsTaskRun.
type startImportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartImportLabelsTaskRun(
	_ context.Context,
	in *startImportLabelsTaskRunInput,
) (*startImportLabelsTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startImportLabelsTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartImportLabelsTaskRun(in.TransformID, in.InputS3Path)
	if err != nil {
		return nil, err
	}

	return &startImportLabelsTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startMLEvaluationTaskRunInput holds input for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunInput struct {
	TransformID string `json:"TransformId"`
}

// startMLEvaluationTaskRunOutput holds the result for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLEvaluationTaskRun(
	_ context.Context,
	in *startMLEvaluationTaskRunInput,
) (*startMLEvaluationTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startMLEvaluationTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartMLEvaluationTaskRun(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &startMLEvaluationTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startMLLabelingSetGenerationTaskRunInput holds input for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunInput struct {
	TransformID string `json:"TransformId"`
}

// startMLLabelingSetGenerationTaskRunOutput holds the result for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLLabelingSetGenerationTaskRun(
	_ context.Context,
	in *startMLLabelingSetGenerationTaskRunInput,
) (*startMLLabelingSetGenerationTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startMLLabelingSetGenerationTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartMLLabelingSetGenerationTaskRun(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &startMLLabelingSetGenerationTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startMaterializedViewRefreshTaskRunInput holds input for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// startMaterializedViewRefreshTaskRunOutput holds the result for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *startMaterializedViewRefreshTaskRunInput,
) (*startMaterializedViewRefreshTaskRunOutput, error) {
	run, err := h.Backend.StartMaterializedViewRefreshTaskRun(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &startMaterializedViewRefreshTaskRunOutput{RunID: run.TaskRunID}, nil
}

// startTriggerInput holds input for StartTrigger.
type startTriggerInput struct {
	Name string `json:"Name"`
}

// startTriggerOutput holds the result for StartTrigger.
type startTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartTrigger(
	_ context.Context,
	in *startTriggerInput,
) (*startTriggerOutput, error) {
	if err := h.Backend.StartTrigger(in.Name); err != nil {
		return nil, err
	}

	return &startTriggerOutput{Name: in.Name}, nil
}

// startWorkflowRunInput holds input for StartWorkflowRun.
type startWorkflowRunInput struct {
	Name string `json:"Name"`
}

// startWorkflowRunOutput holds the result for StartWorkflowRun.
type startWorkflowRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartWorkflowRun(
	_ context.Context,
	in *startWorkflowRunInput,
) (*startWorkflowRunOutput, error) {
	run, err := h.Backend.StartWorkflowRun(in.Name)
	if err != nil {
		return nil, err
	}

	return &startWorkflowRunOutput{RunID: run.RunID}, nil
}

// stopColumnStatisticsTaskRunInput holds input for StopColumnStatisticsTaskRun.
type stopColumnStatisticsTaskRunInput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

func (h *Handler) handleStopColumnStatisticsTaskRun(
	_ context.Context,
	in *stopColumnStatisticsTaskRunInput,
) (*emptyOutput, error) {
	if in.ColumnStatisticsTaskRunID == "" {
		return &emptyOutput{}, nil
	}

	return &emptyOutput{}, h.Backend.StopColumnStatisticsTaskRun(in.ColumnStatisticsTaskRunID)
}

// stopColumnStatisticsTaskRunScheduleInput holds input for StopColumnStatisticsTaskRunSchedule.
type stopColumnStatisticsTaskRunScheduleInput struct{}

func (h *Handler) handleStopColumnStatisticsTaskRunSchedule(
	_ context.Context,
	_ *stopColumnStatisticsTaskRunScheduleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// stopMaterializedViewRefreshTaskRunInput holds input for StopMaterializedViewRefreshTaskRun.
type stopMaterializedViewRefreshTaskRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStopMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *stopMaterializedViewRefreshTaskRunInput,
) (*emptyOutput, error) {
	if in.RunID == "" {
		return &emptyOutput{}, nil
	}

	return &emptyOutput{}, h.Backend.StopMaterializedViewRefreshTaskRun(in.RunID)
}

// stopSessionInput holds input for StopSession.
type stopSessionInput struct {
	ID string `json:"Id"`
}

// stopSessionOutput holds the result for StopSession.
type stopSessionOutput struct {
	ID string `json:"Id"`
}

func (h *Handler) handleStopSession(
	_ context.Context,
	in *stopSessionInput,
) (*stopSessionOutput, error) {
	if err := h.Backend.StopSession(in.ID); err != nil {
		return nil, err
	}

	return &stopSessionOutput{ID: in.ID}, nil
}

// stopTriggerInput holds input for StopTrigger.
type stopTriggerInput struct {
	Name string `json:"Name"`
}

// stopTriggerOutput holds the result for StopTrigger.
type stopTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopTrigger(
	_ context.Context,
	in *stopTriggerInput,
) (*stopTriggerOutput, error) {
	if err := h.Backend.StopTrigger(in.Name); err != nil {
		return nil, err
	}

	return &stopTriggerOutput{Name: in.Name}, nil
}

// stopWorkflowRunInput holds input for StopWorkflowRun.
type stopWorkflowRunInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

func (h *Handler) handleStopWorkflowRun(
	_ context.Context,
	in *stopWorkflowRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopWorkflowRun(in.Name, in.RunID)
}

// testConnectionInput holds input for TestConnection.
type testConnectionInput struct{}

// testConnectionOutput holds the result for TestConnection.
type testConnectionOutput struct {
	Status string `json:"Status"`
}

func (h *Handler) handleTestConnection(
	_ context.Context,
	_ *testConnectionInput,
) (*testConnectionOutput, error) {
	return &testConnectionOutput{Status: stateSucceeded}, nil
}

// updateBlueprintInput holds input for UpdateBlueprint.
type updateBlueprintInput struct {
	Name string `json:"Name"`
}

// updateBlueprintOutput holds the result for UpdateBlueprint.
type updateBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateBlueprint(
	_ context.Context,
	in *updateBlueprintInput,
) (*updateBlueprintOutput, error) {
	if _, err := h.Backend.UpdateBlueprint(in.Name); err != nil {
		return nil, err
	}

	return &updateBlueprintOutput{Name: in.Name}, nil
}

// updateCatalogInput holds input for UpdateCatalog.
type updateCatalogInput struct {
	CatalogInput struct {
		Parameters  map[string]string `json:"Parameters,omitzero"`
		Description string            `json:"Description,omitempty"`
	} `json:"CatalogInput"`
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleUpdateCatalog(
	_ context.Context,
	in *updateCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateCatalog(
		in.CatalogID,
		in.CatalogInput.Description,
		in.CatalogInput.Parameters,
	)
}

// updateClassifierInput holds input for UpdateClassifier.
type updateClassifierInput struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JSONClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}

func (h *Handler) handleUpdateClassifier(
	_ context.Context,
	in *updateClassifierInput,
) (*emptyOutput, error) {
	c := Classifier{
		GrokClassifier: in.GrokClassifier,
		XMLClassifier:  in.XMLClassifier,
		JSONClassifier: in.JSONClassifier,
		CsvClassifier:  in.CsvClassifier,
	}
	if err := h.Backend.UpdateClassifier(c); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// updateColumnStatisticsForPartitionInput holds input for UpdateColumnStatisticsForPartition.
type updateColumnStatisticsForPartitionInput struct {
	DatabaseName         string              `json:"DatabaseName"`
	TableName            string              `json:"TableName"`
	PartitionValues      []string            `json:"PartitionValues"`
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
}

// updateColumnStatisticsForPartitionOutput holds the result for UpdateColumnStatisticsForPartition.
type updateColumnStatisticsForPartitionOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForPartition(
	_ context.Context,
	in *updateColumnStatisticsForPartitionInput,
) (*updateColumnStatisticsForPartitionOutput, error) {
	err := h.Backend.UpdateColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnStatisticsList,
	)
	if err != nil {
		return nil, err
	}

	return &updateColumnStatisticsForPartitionOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsForTableInput holds input for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableInput struct {
	DatabaseName         string              `json:"DatabaseName"`
	TableName            string              `json:"TableName"`
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
}

// updateColumnStatisticsForTableOutput holds the result for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForTable(
	_ context.Context,
	in *updateColumnStatisticsForTableInput,
) (*updateColumnStatisticsForTableOutput, error) {
	err := h.Backend.UpdateColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnStatisticsList,
	)
	if err != nil {
		return nil, err
	}

	return &updateColumnStatisticsForTableOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsTaskSettingsInput holds input for UpdateColumnStatisticsTaskSettings.
type updateColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	RoleArn      string `json:"RoleArn,omitempty"`
}

func (h *Handler) handleUpdateColumnStatisticsTaskSettings(
	_ context.Context,
	in *updateColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateColumnStatisticsTaskSettings(
		in.DatabaseName, in.TableName, in.RoleArn,
	)
}

// updateConnectionInput holds input for UpdateConnection.
type updateConnectionInput struct {
	Name            string          `json:"Name"`
	ConnectionInput connectionInput `json:"ConnectionInput"`
}

func (h *Handler) handleUpdateConnection(
	_ context.Context,
	in *updateConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdateConnection(
		in.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// updateDevEndpointInput holds input for UpdateDevEndpoint.
type updateDevEndpointInput struct {
	AddArguments map[string]string `json:"AddArguments,omitempty"`
	EndpointName string            `json:"EndpointName"`
}

func (h *Handler) handleUpdateDevEndpoint(
	_ context.Context,
	in *updateDevEndpointInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateDevEndpoint(in.EndpointName, in.AddArguments)
}

// updateIdentityCenterConfigurationInput holds input for UpdateGlueIdentityCenterConfiguration.
type updateIdentityCenterConfigurationInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
}

func (h *Handler) handleUpdateGlueIdentityCenterConfiguration(
	_ context.Context,
	in *updateIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateGlueIdentityCenterConfiguration(in.InstanceArn)
}

// updateIntegrationResourcePropertyInput holds input for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyInput struct{}

// updateIntegrationResourcePropertyOutput holds the result for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyOutput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleUpdateIntegrationResourceProperty(
	_ context.Context,
	_ *updateIntegrationResourcePropertyInput,
) (*updateIntegrationResourcePropertyOutput, error) {
	return &updateIntegrationResourcePropertyOutput{}, nil
}

// updateIntegrationTablePropertiesInput holds input for UpdateIntegrationTableProperties.
type updateIntegrationTablePropertiesInput struct{}

func (h *Handler) handleUpdateIntegrationTableProperties(
	_ context.Context,
	_ *updateIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// updateJobFromSourceControlInput holds input for UpdateJobFromSourceControl.
type updateJobFromSourceControlInput struct {
	JobName string `json:"JobName"`
}

// updateJobFromSourceControlOutput holds the result for UpdateJobFromSourceControl.
type updateJobFromSourceControlOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleUpdateJobFromSourceControl(
	_ context.Context,
	in *updateJobFromSourceControlInput,
) (*updateJobFromSourceControlOutput, error) {
	return &updateJobFromSourceControlOutput{JobName: in.JobName}, nil
}

// updateMLTransformInput holds input for UpdateMLTransform.
type updateMLTransformInput struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	TransformID       string               `json:"TransformId"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	Name              string               `json:"Name,omitempty"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
}

// updateMLTransformOutput holds the result for UpdateMLTransform.
type updateMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleUpdateMLTransform(
	_ context.Context,
	in *updateMLTransformInput,
) (*updateMLTransformOutput, error) {
	update := MLTransform{
		Name:              in.Name,
		Description:       in.Description,
		Role:              in.Role,
		InputRecordTables: in.InputRecordTables,
		Parameters:        in.Parameters,
	}
	if err := h.Backend.UpdateMLTransform(in.TransformID, update); err != nil {
		return nil, err
	}

	return &updateMLTransformOutput{TransformID: in.TransformID}, nil
}

// updatePartitionInput holds input for UpdatePartition.
type updatePartitionInput struct {
	DatabaseName       string         `json:"DatabaseName"`
	TableName          string         `json:"TableName"`
	PartitionValueList []string       `json:"PartitionValueList"`
	PartitionInput     PartitionInput `json:"PartitionInput"`
}

func (h *Handler) handleUpdatePartition(
	_ context.Context,
	in *updatePartitionInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdatePartition(
		in.DatabaseName, in.TableName,
		in.PartitionValueList, in.PartitionInput,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// updateRegistryInput holds input for UpdateRegistry.
type updateRegistryInput struct {
	RegistryID  *registryIDInput `json:"RegistryId"`
	Description string           `json:"Description"`
}

// updateRegistryOutput holds the result for UpdateRegistry.
type updateRegistryOutput struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
}

func (h *Handler) handleUpdateRegistry(
	_ context.Context,
	in *updateRegistryInput,
) (*updateRegistryOutput, error) {
	name := ""
	if in.RegistryID != nil {
		name = in.RegistryID.RegistryName
	}

	if err := h.Backend.UpdateRegistry(name, in.Description); err != nil {
		return nil, err
	}

	return &updateRegistryOutput{RegistryName: name}, nil
}

// updateSchemaInput holds input for UpdateSchema.
type updateSchemaInput struct {
	SchemaID      *schemaIDInput `json:"SchemaId"`
	Compatibility string         `json:"Compatibility"`
	Description   string         `json:"Description"`
}

// updateSchemaOutput holds the result for UpdateSchema.
type updateSchemaOutput struct {
	SchemaArn    string `json:"SchemaArn"`
	SchemaName   string `json:"SchemaName"`
	RegistryName string `json:"RegistryName"`
}

func (h *Handler) handleUpdateSchema(
	_ context.Context,
	in *updateSchemaInput,
) (*updateSchemaOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	if err := h.Backend.UpdateSchema(registryName, schemaName, in.Compatibility, in.Description); err != nil {
		return nil, err
	}

	return &updateSchemaOutput{SchemaName: schemaName, RegistryName: registryName}, nil
}

// updateSourceControlFromJobInput holds input for UpdateSourceControlFromJob.
type updateSourceControlFromJobInput struct {
	JobName string `json:"JobName"`
}

// updateSourceControlFromJobOutput holds the result for UpdateSourceControlFromJob.
type updateSourceControlFromJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleUpdateSourceControlFromJob(
	_ context.Context,
	in *updateSourceControlFromJobInput,
) (*updateSourceControlFromJobOutput, error) {
	return &updateSourceControlFromJobOutput{JobName: in.JobName}, nil
}

// updateTableOptimizerInput holds input for UpdateTableOptimizer.
type updateTableOptimizerInput struct {
	CatalogID                   string                      `json:"CatalogId,omitempty"`
	DatabaseName                string                      `json:"DatabaseName"`
	TableName                   string                      `json:"TableName"`
	Type                        string                      `json:"Type"`
	TableOptimizerConfiguration TableOptimizerConfiguration `json:"TableOptimizerConfiguration"`
}

func (h *Handler) handleUpdateTableOptimizer(
	_ context.Context,
	in *updateTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateTableOptimizer(
		in.DatabaseName,
		in.TableName,
		in.Type,
		in.TableOptimizerConfiguration,
	)
}

// updateTriggerInput holds input for UpdateTrigger.
type updateTriggerInput struct {
	Name          string        `json:"Name"`
	TriggerUpdate triggerUpdate `json:"TriggerUpdate"`
}

// triggerUpdate holds the mutable fields for UpdateTrigger.
type triggerUpdate struct {
	Predicate *TriggerPredicate `json:"Predicate,omitempty"`
	Schedule  string            `json:"Schedule,omitempty"`
	Actions   []TriggerAction   `json:"Actions,omitempty"`
}

// updateTriggerOutput holds the result for UpdateTrigger.
type updateTriggerOutput struct {
	Trigger *Trigger `json:"Trigger"`
}

func (h *Handler) handleUpdateTrigger(
	_ context.Context,
	in *updateTriggerInput,
) (*updateTriggerOutput, error) {
	update := Trigger{
		Schedule:  in.TriggerUpdate.Schedule,
		Actions:   in.TriggerUpdate.Actions,
		Predicate: in.TriggerUpdate.Predicate,
	}
	if err := h.Backend.UpdateTrigger(in.Name, update); err != nil {
		return nil, err
	}

	t, err := h.Backend.GetTrigger(in.Name)
	if err != nil {
		return nil, err
	}

	return &updateTriggerOutput{Trigger: t}, nil
}

// updateUsageProfileInput holds input for UpdateUsageProfile.
type updateUsageProfileInput struct {
	Name string `json:"Name"`
}

// updateUsageProfileOutput holds the result for UpdateUsageProfile.
type updateUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateUsageProfile(
	_ context.Context,
	in *updateUsageProfileInput,
) (*updateUsageProfileOutput, error) {
	if _, err := h.Backend.UpdateUsageProfile(in.Name, ""); err != nil {
		return nil, err
	}

	return &updateUsageProfileOutput{Name: in.Name}, nil
}

// updateUserDefinedFunctionInput holds input for UpdateUserDefinedFunction.
type updateUserDefinedFunctionInput struct {
	DatabaseName  string              `json:"DatabaseName"`
	FunctionName  string              `json:"FunctionName"`
	FunctionInput UserDefinedFunction `json:"FunctionInput"`
}

func (h *Handler) handleUpdateUserDefinedFunction(
	_ context.Context,
	in *updateUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateUserDefinedFunction(
		in.DatabaseName,
		in.FunctionName,
		in.FunctionInput,
	)
}

// updateWorkflowInput holds input for UpdateWorkflow.
type updateWorkflowInput struct {
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
}

// updateWorkflowOutput holds the result for UpdateWorkflow.
type updateWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateWorkflow(
	_ context.Context,
	in *updateWorkflowInput,
) (*updateWorkflowOutput, error) {
	update := Workflow{
		Description:          in.Description,
		DefaultRunProperties: in.DefaultRunProperties,
	}
	if err := h.Backend.UpdateWorkflow(in.Name, update); err != nil {
		return nil, err
	}

	return &updateWorkflowOutput{Name: in.Name}, nil
}
