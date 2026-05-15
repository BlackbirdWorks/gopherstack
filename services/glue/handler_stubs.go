package glue

import (
	"context"
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
type batchGetTableOptimizerInput struct{}

// batchGetTableOptimizerOutput holds the result for BatchGetTableOptimizer.
type batchGetTableOptimizerOutput struct {
	TableOptimizers []any `json:"TableOptimizers"`
	Failures        []any `json:"Failures"`
}

func (h *Handler) handleBatchGetTableOptimizer(
	_ context.Context,
	_ *batchGetTableOptimizerInput,
) (*batchGetTableOptimizerOutput, error) {
	return &batchGetTableOptimizerOutput{TableOptimizers: []any{}, Failures: []any{}}, nil
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

// batchUpdatePartitionInput holds input for BatchUpdatePartition.
type batchUpdatePartitionInput struct{}

// batchUpdatePartitionOutput holds the result for BatchUpdatePartition.
type batchUpdatePartitionOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleBatchUpdatePartition(
	_ context.Context,
	_ *batchUpdatePartitionInput,
) (*batchUpdatePartitionOutput, error) {
	return &batchUpdatePartitionOutput{Errors: []any{}}, nil
}

// cancelDataQualityRuleRecommendationRunInput holds input for CancelDataQualityRuleRecommendationRun.
type cancelDataQualityRuleRecommendationRunInput struct{}

func (h *Handler) handleCancelDataQualityRuleRecommendationRun(
	_ context.Context,
	_ *cancelDataQualityRuleRecommendationRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// cancelMLTaskRunInput holds input for CancelMLTaskRun.
type cancelMLTaskRunInput struct{}

func (h *Handler) handleCancelMLTaskRun(
	_ context.Context,
	_ *cancelMLTaskRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// cancelStatementInput holds input for CancelStatement.
type cancelStatementInput struct{}

func (h *Handler) handleCancelStatement(
	_ context.Context,
	_ *cancelStatementInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &createBlueprintOutput{Name: in.Name}, nil
}

// createCatalogInput holds input for CreateCatalog.
type createCatalogInput struct{}

func (h *Handler) handleCreateCatalog(
	_ context.Context,
	_ *createCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type createColumnStatisticsTaskSettingsInput struct{}

func (h *Handler) handleCreateColumnStatisticsTaskSettings(
	_ context.Context,
	_ *createColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// createCustomEntityTypeInput holds input for CreateCustomEntityType.
type createCustomEntityTypeInput struct {
	Name string `json:"Name"`
}

// createCustomEntityTypeOutput holds the result for CreateCustomEntityType.
type createCustomEntityTypeOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateCustomEntityType(
	_ context.Context,
	in *createCustomEntityTypeInput,
) (*createCustomEntityTypeOutput, error) {
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

// createGlueIdentityCenterConfigurationInput holds input for CreateGlueIdentityCenterConfiguration.
type createGlueIdentityCenterConfigurationInput struct{}

func (h *Handler) handleCreateGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *createGlueIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// createIntegrationInput holds input for CreateIntegration.
type createIntegrationInput struct {
	IntegrationName string `json:"IntegrationName"`
}

// createIntegrationOutput holds the result for CreateIntegration.
type createIntegrationOutput struct {
	IntegrationName string `json:"IntegrationName"`
}

func (h *Handler) handleCreateIntegration(
	_ context.Context,
	in *createIntegrationInput,
) (*createIntegrationOutput, error) {
	return &createIntegrationOutput{IntegrationName: in.IntegrationName}, nil
}

// createIntegrationResourcePropertyInput holds input for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyInput struct{}

func (h *Handler) handleCreateIntegrationResourceProperty(
	_ context.Context,
	_ *createIntegrationResourcePropertyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// createIntegrationTablePropertiesInput holds input for CreateIntegrationTableProperties.
type createIntegrationTablePropertiesInput struct{}

func (h *Handler) handleCreateIntegrationTableProperties(
	_ context.Context,
	_ *createIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// createMLTransformInput holds input for CreateMLTransform.
type createMLTransformInput struct {
	Name string `json:"Name"`
}

// createMLTransformOutput holds the result for CreateMLTransform.
type createMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleCreateMLTransform(
	_ context.Context,
	_ *createMLTransformInput,
) (*createMLTransformOutput, error) {
	return &createMLTransformOutput{TransformID: "transform-stub"}, nil
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
type createPartitionIndexInput struct{}

func (h *Handler) handleCreatePartitionIndex(
	_ context.Context,
	_ *createPartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	Name string `json:"Name"`
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
	return &createSecurityConfigurationOutput{Name: in.Name}, nil
}

// createSessionInput holds input for CreateSession.
type createSessionInput struct {
	ID string `json:"Id"`
}

// createSessionOutput holds the result for CreateSession.
type createSessionOutput struct {
	Session any `json:"Session"`
}

func (h *Handler) handleCreateSession(
	_ context.Context,
	in *createSessionInput,
) (*createSessionOutput, error) {
	return &createSessionOutput{
		Session: map[string]string{"Id": in.ID, "Status": stateProvisioning},
	}, nil
}

// createTableOptimizerInput holds input for CreateTableOptimizer.
type createTableOptimizerInput struct{}

func (h *Handler) handleCreateTableOptimizer(
	_ context.Context,
	_ *createTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	Name string `json:"Name"`
}

// createUsageProfileOutput holds the result for CreateUsageProfile.
type createUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateUsageProfile(
	_ context.Context,
	in *createUsageProfileInput,
) (*createUsageProfileOutput, error) {
	return &createUsageProfileOutput{Name: in.Name}, nil
}

// createUserDefinedFunctionInput holds input for CreateUserDefinedFunction.
type createUserDefinedFunctionInput struct{}

func (h *Handler) handleCreateUserDefinedFunction(
	_ context.Context,
	_ *createUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &deleteBlueprintOutput{Name: in.Name}, nil
}

// deleteCatalogInput holds input for DeleteCatalog.
type deleteCatalogInput struct{}

func (h *Handler) handleDeleteCatalog(
	_ context.Context,
	_ *deleteCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type deleteColumnStatisticsForPartitionInput struct{}

func (h *Handler) handleDeleteColumnStatisticsForPartition(
	_ context.Context,
	_ *deleteColumnStatisticsForPartitionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteColumnStatisticsForTableInput holds input for DeleteColumnStatisticsForTable.
type deleteColumnStatisticsForTableInput struct{}

func (h *Handler) handleDeleteColumnStatisticsForTable(
	_ context.Context,
	_ *deleteColumnStatisticsForTableInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteColumnStatisticsTaskSettingsInput holds input for DeleteColumnStatisticsTaskSettings.
type deleteColumnStatisticsTaskSettingsInput struct{}

func (h *Handler) handleDeleteColumnStatisticsTaskSettings(
	_ context.Context,
	_ *deleteColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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

// deleteGlueIdentityCenterConfigurationInput holds input for DeleteGlueIdentityCenterConfiguration.
type deleteGlueIdentityCenterConfigurationInput struct{}

func (h *Handler) handleDeleteGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *deleteGlueIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type deletePartitionIndexInput struct{}

func (h *Handler) handleDeletePartitionIndex(
	_ context.Context,
	_ *deletePartitionIndexInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type deleteResourcePolicyInput struct{}

func (h *Handler) handleDeleteResourcePolicy(
	_ context.Context,
	_ *deleteResourcePolicyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type deleteSecurityConfigurationInput struct{}

func (h *Handler) handleDeleteSecurityConfiguration(
	_ context.Context,
	_ *deleteSecurityConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &deleteSessionOutput{ID: in.ID}, nil
}

// deleteTableOptimizerInput holds input for DeleteTableOptimizer.
type deleteTableOptimizerInput struct{}

func (h *Handler) handleDeleteTableOptimizer(
	_ context.Context,
	_ *deleteTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteTableVersionInput holds input for DeleteTableVersion.
type deleteTableVersionInput struct{}

func (h *Handler) handleDeleteTableVersion(
	_ context.Context,
	_ *deleteTableVersionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type deleteUsageProfileInput struct{}

func (h *Handler) handleDeleteUsageProfile(
	_ context.Context,
	_ *deleteUsageProfileInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// deleteUserDefinedFunctionInput holds input for DeleteUserDefinedFunction.
type deleteUserDefinedFunctionInput struct{}

func (h *Handler) handleDeleteUserDefinedFunction(
	_ context.Context,
	_ *deleteUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type describeConnectionTypeInput struct{}

// describeConnectionTypeOutput holds the result for DescribeConnectionType.
type describeConnectionTypeOutput struct {
	ConnectionType string `json:"ConnectionType"`
}

func (h *Handler) handleDescribeConnectionType(
	_ context.Context,
	_ *describeConnectionTypeInput,
) (*describeConnectionTypeOutput, error) {
	return &describeConnectionTypeOutput{}, nil
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
	return &describeInboundIntegrationsOutput{Integrations: []any{}}, nil
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
	return &describeIntegrationsOutput{Integrations: []any{}}, nil
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
	found, _ := h.Backend.BatchGetBlueprints([]string{in.Name})
	if len(found) > 0 {
		return &getBlueprintOutput{Blueprint: found[0]}, nil
	}

	return &getBlueprintOutput{}, nil
}

// getBlueprintRunInput holds input for GetBlueprintRun.
type getBlueprintRunInput struct{}

// getBlueprintRunOutput holds the result for GetBlueprintRun.
type getBlueprintRunOutput struct {
	Run any `json:"Run"`
}

func (h *Handler) handleGetBlueprintRun(
	_ context.Context,
	_ *getBlueprintRunInput,
) (*getBlueprintRunOutput, error) {
	return &getBlueprintRunOutput{}, nil
}

// getBlueprintRunsInput holds input for GetBlueprintRuns.
type getBlueprintRunsInput struct{}

// getBlueprintRunsOutput holds the result for GetBlueprintRuns.
type getBlueprintRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleGetBlueprintRuns(
	_ context.Context,
	_ *getBlueprintRunsInput,
) (*getBlueprintRunsOutput, error) {
	return &getBlueprintRunsOutput{Runs: []any{}}, nil
}

// getCatalogInput holds input for GetCatalog.
type getCatalogInput struct{}

// getCatalogOutput holds the result for GetCatalog.
type getCatalogOutput struct {
	Catalog any `json:"Catalog"`
}

func (h *Handler) handleGetCatalog(
	_ context.Context,
	_ *getCatalogInput,
) (*getCatalogOutput, error) {
	return &getCatalogOutput{}, nil
}

// getCatalogImportStatusInput holds input for GetCatalogImportStatus.
type getCatalogImportStatusInput struct{}

// getCatalogImportStatusOutput holds the result for GetCatalogImportStatus.
type getCatalogImportStatusOutput struct {
	ImportStatus any `json:"ImportStatus"`
}

func (h *Handler) handleGetCatalogImportStatus(
	_ context.Context,
	_ *getCatalogImportStatusInput,
) (*getCatalogImportStatusOutput, error) {
	return &getCatalogImportStatusOutput{}, nil
}

// getCatalogsInput holds input for GetCatalogs.
type getCatalogsInput struct{}

// getCatalogsOutput holds the result for GetCatalogs.
type getCatalogsOutput struct {
	CatalogList []any `json:"CatalogList"`
}

func (h *Handler) handleGetCatalogs(
	_ context.Context,
	_ *getCatalogsInput,
) (*getCatalogsOutput, error) {
	return &getCatalogsOutput{CatalogList: []any{}}, nil
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
type getColumnStatisticsForPartitionInput struct{}

// getColumnStatisticsForPartitionOutput holds the result for GetColumnStatisticsForPartition.
type getColumnStatisticsForPartitionOutput struct {
	ColumnStatisticsList []any `json:"ColumnStatisticsList"`
	Errors               []any `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForPartition(
	_ context.Context,
	_ *getColumnStatisticsForPartitionInput,
) (*getColumnStatisticsForPartitionOutput, error) {
	return &getColumnStatisticsForPartitionOutput{
		ColumnStatisticsList: []any{},
		Errors:               []any{},
	}, nil
}

// getColumnStatisticsForTableInput holds input for GetColumnStatisticsForTable.
type getColumnStatisticsForTableInput struct{}

// getColumnStatisticsForTableOutput holds the result for GetColumnStatisticsForTable.
type getColumnStatisticsForTableOutput struct {
	ColumnStatisticsList []any `json:"ColumnStatisticsList"`
	Errors               []any `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForTable(
	_ context.Context,
	_ *getColumnStatisticsForTableInput,
) (*getColumnStatisticsForTableOutput, error) {
	return &getColumnStatisticsForTableOutput{ColumnStatisticsList: []any{}, Errors: []any{}}, nil
}

// getColumnStatisticsTaskRunInput holds input for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunInput struct{}

// getColumnStatisticsTaskRunOutput holds the result for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRun any `json:"ColumnStatisticsTaskRun"`
}

func (h *Handler) handleGetColumnStatisticsTaskRun(
	_ context.Context,
	_ *getColumnStatisticsTaskRunInput,
) (*getColumnStatisticsTaskRunOutput, error) {
	return &getColumnStatisticsTaskRunOutput{}, nil
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
	return &getColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRuns: []any{}}, nil
}

// getColumnStatisticsTaskSettingsInput holds input for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsInput struct{}

// getColumnStatisticsTaskSettingsOutput holds the result for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsOutput struct {
	ColumnStatisticsTaskSettings any `json:"ColumnStatisticsTaskSettings"`
}

func (h *Handler) handleGetColumnStatisticsTaskSettings(
	_ context.Context,
	_ *getColumnStatisticsTaskSettingsInput,
) (*getColumnStatisticsTaskSettingsOutput, error) {
	return &getColumnStatisticsTaskSettingsOutput{}, nil
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
	found, _ := h.Backend.BatchGetCustomEntityTypes([]string{in.Name})
	if len(found) > 0 {
		return &getCustomEntityTypeOutput{
			Name:         found[0].Name,
			RegexString:  found[0].RegexString,
			ContextWords: found[0].ContextWords,
		}, nil
	}

	return &getCustomEntityTypeOutput{}, nil
}

// getDataCatalogEncryptionSettingsInput holds input for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsInput struct{}

// getDataCatalogEncryptionSettingsOutput holds the result for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsOutput struct {
	DataCatalogEncryptionSettings any `json:"DataCatalogEncryptionSettings"`
}

func (h *Handler) handleGetDataCatalogEncryptionSettings(
	_ context.Context,
	_ *getDataCatalogEncryptionSettingsInput,
) (*getDataCatalogEncryptionSettingsOutput, error) {
	return &getDataCatalogEncryptionSettingsOutput{
		DataCatalogEncryptionSettings: map[string]any{},
	}, nil
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
type getDataQualityModelResultInput struct{}

// getDataQualityModelResultOutput holds the result for GetDataQualityModelResult.
type getDataQualityModelResultOutput struct {
	CompletedOn float64 `json:"CompletedOn"`
}

func (h *Handler) handleGetDataQualityModelResult(
	_ context.Context,
	_ *getDataQualityModelResultInput,
) (*getDataQualityModelResultOutput, error) {
	return &getDataQualityModelResultOutput{}, nil
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
	found, _ := h.Backend.BatchGetDataQualityResult([]string{in.ResultID})
	if len(found) > 0 {
		return &getDataQualityResultOutput{ResultID: found[0].ResultID, Score: found[0].Score}, nil
	}

	return &getDataQualityResultOutput{ResultID: in.ResultID}, nil
}

// getDataQualityRuleRecommendationRunInput holds input for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunInput struct{}

// getDataQualityRuleRecommendationRunOutput holds the result for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetDataQualityRuleRecommendationRun(
	_ context.Context,
	_ *getDataQualityRuleRecommendationRunInput,
) (*getDataQualityRuleRecommendationRunOutput, error) {
	return &getDataQualityRuleRecommendationRunOutput{Status: stateSucceeded}, nil
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

// getGlueIdentityCenterConfigurationInput holds input for GetGlueIdentityCenterConfiguration.
type getGlueIdentityCenterConfigurationInput struct{}

// getGlueIdentityCenterConfigurationOutput holds the result for GetGlueIdentityCenterConfiguration.
type getGlueIdentityCenterConfigurationOutput struct {
	InstanceArn string `json:"InstanceArn"`
}

func (h *Handler) handleGetGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *getGlueIdentityCenterConfigurationInput,
) (*getGlueIdentityCenterConfigurationOutput, error) {
	return &getGlueIdentityCenterConfigurationOutput{}, nil
}

// getIntegrationResourcePropertyInput holds input for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyInput struct{}

// getIntegrationResourcePropertyOutput holds the result for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyOutput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetIntegrationResourceProperty(
	_ context.Context,
	_ *getIntegrationResourcePropertyInput,
) (*getIntegrationResourcePropertyOutput, error) {
	return &getIntegrationResourcePropertyOutput{}, nil
}

// getIntegrationTablePropertiesInput holds input for GetIntegrationTableProperties.
type getIntegrationTablePropertiesInput struct{}

// getIntegrationTablePropertiesOutput holds the result for GetIntegrationTableProperties.
type getIntegrationTablePropertiesOutput struct {
	ResourceArn string `json:"ResourceArn"`
	TableName   string `json:"TableName"`
}

func (h *Handler) handleGetIntegrationTableProperties(
	_ context.Context,
	_ *getIntegrationTablePropertiesInput,
) (*getIntegrationTablePropertiesOutput, error) {
	return &getIntegrationTablePropertiesOutput{}, nil
}

// getMLTaskRunInput holds input for GetMLTaskRun.
type getMLTaskRunInput struct{}

// getMLTaskRunOutput holds the result for GetMLTaskRun.
type getMLTaskRunOutput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
	Status      string `json:"Status"`
}

func (h *Handler) handleGetMLTaskRun(
	_ context.Context,
	_ *getMLTaskRunInput,
) (*getMLTaskRunOutput, error) {
	return &getMLTaskRunOutput{Status: stateSucceeded}, nil
}

// getMLTaskRunsInput holds input for GetMLTaskRuns.
type getMLTaskRunsInput struct{}

// getMLTaskRunsOutput holds the result for GetMLTaskRuns.
type getMLTaskRunsOutput struct {
	TaskRuns []any `json:"TaskRuns"`
}

func (h *Handler) handleGetMLTaskRuns(
	_ context.Context,
	_ *getMLTaskRunsInput,
) (*getMLTaskRunsOutput, error) {
	return &getMLTaskRunsOutput{TaskRuns: []any{}}, nil
}

// getMLTransformInput holds input for GetMLTransform.
type getMLTransformInput struct{}

// getMLTransformOutput holds the result for GetMLTransform.
type getMLTransformOutput struct {
	TransformID string `json:"TransformId"`
	Status      string `json:"Status"`
}

func (h *Handler) handleGetMLTransform(
	_ context.Context,
	_ *getMLTransformInput,
) (*getMLTransformOutput, error) {
	return &getMLTransformOutput{Status: stateReady}, nil
}

// getMLTransformsInput holds input for GetMLTransforms.
type getMLTransformsInput struct{}

// getMLTransformsOutput holds the result for GetMLTransforms.
type getMLTransformsOutput struct {
	Transforms []any `json:"Transforms"`
}

func (h *Handler) handleGetMLTransforms(
	_ context.Context,
	_ *getMLTransformsInput,
) (*getMLTransformsOutput, error) {
	return &getMLTransformsOutput{Transforms: []any{}}, nil
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
type getMaterializedViewRefreshTaskRunInput struct{}

// getMaterializedViewRefreshTaskRunOutput holds the result for GetMaterializedViewRefreshTaskRun.
type getMaterializedViewRefreshTaskRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetMaterializedViewRefreshTaskRun(
	_ context.Context,
	_ *getMaterializedViewRefreshTaskRunInput,
) (*getMaterializedViewRefreshTaskRunOutput, error) {
	return &getMaterializedViewRefreshTaskRunOutput{Status: stateSucceeded}, nil
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
	_ *getPartitionInput,
) (*getPartitionOutput, error) {
	return &getPartitionOutput{}, nil
}

// getPartitionIndexesInput holds input for GetPartitionIndexes.
type getPartitionIndexesInput struct{}

// getPartitionIndexesOutput holds the result for GetPartitionIndexes.
type getPartitionIndexesOutput struct {
	PartitionIndexDescriptorList []any `json:"PartitionIndexDescriptorList"`
}

func (h *Handler) handleGetPartitionIndexes(
	_ context.Context,
	_ *getPartitionIndexesInput,
) (*getPartitionIndexesOutput, error) {
	return &getPartitionIndexesOutput{PartitionIndexDescriptorList: []any{}}, nil
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
	_ *getPartitionsInput,
) (*getPartitionsOutput, error) {
	return &getPartitionsOutput{Partitions: []*Partition{}}, nil
}

// getPlanInput holds input for GetPlan.
type getPlanInput struct{}

// getPlanOutput holds the result for GetPlan.
type getPlanOutput struct {
	PythonScript string `json:"PythonScript"`
	ScalaCode    string `json:"ScalaCode"`
}

func (h *Handler) handleGetPlan(_ context.Context, _ *getPlanInput) (*getPlanOutput, error) {
	return &getPlanOutput{}, nil
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
type getResourcePolicyInput struct{}

// getResourcePolicyOutput holds the result for GetResourcePolicy.
type getResourcePolicyOutput struct {
	PolicyInJSON string `json:"PolicyInJson"`
}

func (h *Handler) handleGetResourcePolicy(
	_ context.Context,
	_ *getResourcePolicyInput,
) (*getResourcePolicyOutput, error) {
	return &getResourcePolicyOutput{}, nil
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
type getSchemaByDefinitionInput struct{}

// getSchemaByDefinitionOutput holds the result for GetSchemaByDefinition.
type getSchemaByDefinitionOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	DataFormat      string `json:"DataFormat"`
	Status          string `json:"Status"`
}

func (h *Handler) handleGetSchemaByDefinition(
	_ context.Context,
	_ *getSchemaByDefinitionInput,
) (*getSchemaByDefinitionOutput, error) {
	return &getSchemaByDefinitionOutput{Status: stateAvailable}, nil
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

// getSchemaVersionsDiffInput holds input for GetSchemaVersionsDiff.
type getSchemaVersionsDiffInput struct{}

// getSchemaVersionsDiffOutput holds the result for GetSchemaVersionsDiff.
type getSchemaVersionsDiffOutput struct {
	Diff string `json:"Diff"`
}

func (h *Handler) handleGetSchemaVersionsDiff(
	_ context.Context,
	_ *getSchemaVersionsDiffInput,
) (*getSchemaVersionsDiffOutput, error) {
	return &getSchemaVersionsDiffOutput{}, nil
}

// getSecurityConfigurationInput holds input for GetSecurityConfiguration.
type getSecurityConfigurationInput struct{}

// getSecurityConfigurationOutput holds the result for GetSecurityConfiguration.
type getSecurityConfigurationOutput struct {
	SecurityConfiguration any `json:"SecurityConfiguration"`
}

func (h *Handler) handleGetSecurityConfiguration(
	_ context.Context,
	_ *getSecurityConfigurationInput,
) (*getSecurityConfigurationOutput, error) {
	return &getSecurityConfigurationOutput{}, nil
}

// getSecurityConfigurationsInput holds input for GetSecurityConfigurations.
type getSecurityConfigurationsInput struct{}

// getSecurityConfigurationsOutput holds the result for GetSecurityConfigurations.
type getSecurityConfigurationsOutput struct {
	SecurityConfigurations []any `json:"SecurityConfigurations"`
}

func (h *Handler) handleGetSecurityConfigurations(
	_ context.Context,
	_ *getSecurityConfigurationsInput,
) (*getSecurityConfigurationsOutput, error) {
	return &getSecurityConfigurationsOutput{SecurityConfigurations: []any{}}, nil
}

// getSessionInput holds input for GetSession.
type getSessionInput struct {
	ID string `json:"Id"`
}

// getSessionOutput holds the result for GetSession.
type getSessionOutput struct {
	Session any `json:"Session"`
}

func (h *Handler) handleGetSession(
	_ context.Context,
	in *getSessionInput,
) (*getSessionOutput, error) {
	return &getSessionOutput{Session: map[string]string{"Id": in.ID, "Status": stateReady}}, nil
}

// getStatementInput holds input for GetStatement.
type getStatementInput struct{}

// getStatementOutput holds the result for GetStatement.
type getStatementOutput struct {
	Statement any `json:"Statement"`
}

func (h *Handler) handleGetStatement(
	_ context.Context,
	_ *getStatementInput,
) (*getStatementOutput, error) {
	return &getStatementOutput{}, nil
}

// getTableOptimizerInput holds input for GetTableOptimizer.
type getTableOptimizerInput struct{}

// getTableOptimizerOutput holds the result for GetTableOptimizer.
type getTableOptimizerOutput struct {
	TableOptimizer any `json:"TableOptimizer"`
}

func (h *Handler) handleGetTableOptimizer(
	_ context.Context,
	_ *getTableOptimizerInput,
) (*getTableOptimizerOutput, error) {
	return &getTableOptimizerOutput{}, nil
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
type getUnfilteredPartitionMetadataInput struct{}

// getUnfilteredPartitionMetadataOutput holds the result for GetUnfilteredPartitionMetadata.
type getUnfilteredPartitionMetadataOutput struct {
	Partition                     *Partition `json:"Partition"`
	AuthorizedColumns             []string   `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool       `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredPartitionMetadata(
	_ context.Context,
	_ *getUnfilteredPartitionMetadataInput,
) (*getUnfilteredPartitionMetadataOutput, error) {
	return &getUnfilteredPartitionMetadataOutput{AuthorizedColumns: []string{}}, nil
}

// getUnfilteredPartitionsMetadataInput holds input for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataInput struct{}

// getUnfilteredPartitionsMetadataOutput holds the result for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataOutput struct {
	UnfilteredPartitions []any `json:"UnfilteredPartitions"`
}

func (h *Handler) handleGetUnfilteredPartitionsMetadata(
	_ context.Context,
	_ *getUnfilteredPartitionsMetadataInput,
) (*getUnfilteredPartitionsMetadataOutput, error) {
	return &getUnfilteredPartitionsMetadataOutput{UnfilteredPartitions: []any{}}, nil
}

// getUnfilteredTableMetadataInput holds input for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataInput struct{}

// getUnfilteredTableMetadataOutput holds the result for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataOutput struct {
	Table                         *Table   `json:"Table"`
	AuthorizedColumns             []string `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool     `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredTableMetadata(
	_ context.Context,
	_ *getUnfilteredTableMetadataInput,
) (*getUnfilteredTableMetadataOutput, error) {
	return &getUnfilteredTableMetadataOutput{AuthorizedColumns: []string{}}, nil
}

// getUsageProfileInput holds input for GetUsageProfile.
type getUsageProfileInput struct {
	Name string `json:"Name"`
}

// getUsageProfileOutput holds the result for GetUsageProfile.
type getUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleGetUsageProfile(
	_ context.Context,
	in *getUsageProfileInput,
) (*getUsageProfileOutput, error) {
	return &getUsageProfileOutput{Name: in.Name}, nil
}

// getUserDefinedFunctionInput holds input for GetUserDefinedFunction.
type getUserDefinedFunctionInput struct{}

// getUserDefinedFunctionOutput holds the result for GetUserDefinedFunction.
type getUserDefinedFunctionOutput struct {
	UserDefinedFunction any `json:"UserDefinedFunction"`
}

func (h *Handler) handleGetUserDefinedFunction(
	_ context.Context,
	_ *getUserDefinedFunctionInput,
) (*getUserDefinedFunctionOutput, error) {
	return &getUserDefinedFunctionOutput{}, nil
}

// getUserDefinedFunctionsInput holds input for GetUserDefinedFunctions.
type getUserDefinedFunctionsInput struct{}

// getUserDefinedFunctionsOutput holds the result for GetUserDefinedFunctions.
type getUserDefinedFunctionsOutput struct {
	UserDefinedFunctions []any `json:"UserDefinedFunctions"`
}

func (h *Handler) handleGetUserDefinedFunctions(
	_ context.Context,
	_ *getUserDefinedFunctionsInput,
) (*getUserDefinedFunctionsOutput, error) {
	return &getUserDefinedFunctionsOutput{UserDefinedFunctions: []any{}}, nil
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
type getWorkflowRunPropertiesInput struct{}

// getWorkflowRunPropertiesOutput holds the result for GetWorkflowRunProperties.
type getWorkflowRunPropertiesOutput struct {
	RunProperties map[string]string `json:"RunProperties"`
}

func (h *Handler) handleGetWorkflowRunProperties(
	_ context.Context,
	_ *getWorkflowRunPropertiesInput,
) (*getWorkflowRunPropertiesOutput, error) {
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
type importCatalogToGlueInput struct{}

func (h *Handler) handleImportCatalogToGlue(
	_ context.Context,
	_ *importCatalogToGlueInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &listBlueprintsOutput{Blueprints: []string{}}, nil
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
	return &listColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRunIDs: []string{}}, nil
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
	return &listCustomEntityTypesOutput{CustomEntityTypes: []*CustomEntityType{}}, nil
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
	return &listDataQualityResultsOutput{Results: []any{}}, nil
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
	return &listDataQualityRuleRecommendationRunsOutput{Runs: []any{}}, nil
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
	return &listDataQualityRulesetEvaluationRunsOutput{Runs: []any{}}, nil
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
	return &listMLTransformsOutput{TransformIDs: []string{}}, nil
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
	return &listMaterializedViewRefreshTaskRunsOutput{Runs: []any{}}, nil
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
	IDs      []string `json:"Ids"`
	Sessions []any    `json:"Sessions"`
}

func (h *Handler) handleListSessions(
	_ context.Context,
	_ *listSessionsInput,
) (*listSessionsOutput, error) {
	return &listSessionsOutput{IDs: []string{}, Sessions: []any{}}, nil
}

// listStatementsInput holds input for ListStatements.
type listStatementsInput struct{}

// listStatementsOutput holds the result for ListStatements.
type listStatementsOutput struct {
	Statements []any `json:"Statements"`
}

func (h *Handler) handleListStatements(
	_ context.Context,
	_ *listStatementsInput,
) (*listStatementsOutput, error) {
	return &listStatementsOutput{Statements: []any{}}, nil
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
	return &listUsageProfilesOutput{Profiles: []any{}}, nil
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
	_ *modifyIntegrationInput,
) (*modifyIntegrationOutput, error) {
	return &modifyIntegrationOutput{Status: stateActive}, nil
}

// putDataCatalogEncryptionSettingsInput holds input for PutDataCatalogEncryptionSettings.
type putDataCatalogEncryptionSettingsInput struct{}

func (h *Handler) handlePutDataCatalogEncryptionSettings(
	_ context.Context,
	_ *putDataCatalogEncryptionSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type putResourcePolicyInput struct{}

// putResourcePolicyOutput holds the result for PutResourcePolicy.
type putResourcePolicyOutput struct {
	PolicyHash string `json:"PolicyHash"`
}

func (h *Handler) handlePutResourcePolicy(
	_ context.Context,
	_ *putResourcePolicyInput,
) (*putResourcePolicyOutput, error) {
	return &putResourcePolicyOutput{PolicyHash: "stub-hash"}, nil
}

// putSchemaVersionMetadataInput holds input for PutSchemaVersionMetadata.
type putSchemaVersionMetadataInput struct{}

// putSchemaVersionMetadataOutput holds the result for PutSchemaVersionMetadata.
type putSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	VersionNumber   int64  `json:"VersionNumber"`
	LatestVersion   bool   `json:"LatestVersion"`
}

func (h *Handler) handlePutSchemaVersionMetadata(
	_ context.Context,
	_ *putSchemaVersionMetadataInput,
) (*putSchemaVersionMetadataOutput, error) {
	return &putSchemaVersionMetadataOutput{}, nil
}

// putWorkflowRunPropertiesInput holds input for PutWorkflowRunProperties.
type putWorkflowRunPropertiesInput struct{}

func (h *Handler) handlePutWorkflowRunProperties(
	_ context.Context,
	_ *putWorkflowRunPropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// querySchemaVersionMetadataInput holds input for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataInput struct{}

// querySchemaVersionMetadataOutput holds the result for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataOutput struct {
	MetadataInfo    map[string]any `json:"MetadataInfo"`
	SchemaVersionID string         `json:"SchemaVersionId"`
}

func (h *Handler) handleQuerySchemaVersionMetadata(
	_ context.Context,
	_ *querySchemaVersionMetadataInput,
) (*querySchemaVersionMetadataOutput, error) {
	return &querySchemaVersionMetadataOutput{MetadataInfo: map[string]any{}}, nil
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
type removeSchemaVersionMetadataInput struct{}

// removeSchemaVersionMetadataOutput holds the result for RemoveSchemaVersionMetadata.
type removeSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	VersionNumber   int64  `json:"VersionNumber"`
	LatestVersion   bool   `json:"LatestVersion"`
}

func (h *Handler) handleRemoveSchemaVersionMetadata(
	_ context.Context,
	_ *removeSchemaVersionMetadataInput,
) (*removeSchemaVersionMetadataOutput, error) {
	return &removeSchemaVersionMetadataOutput{}, nil
}

// resumeWorkflowRunInput holds input for ResumeWorkflowRun.
type resumeWorkflowRunInput struct{}

// resumeWorkflowRunOutput holds the result for ResumeWorkflowRun.
type resumeWorkflowRunOutput struct {
	RunID   string   `json:"RunId"`
	NodeIDs []string `json:"NodeIds"`
}

func (h *Handler) handleResumeWorkflowRun(
	_ context.Context,
	_ *resumeWorkflowRunInput,
) (*resumeWorkflowRunOutput, error) {
	return &resumeWorkflowRunOutput{NodeIDs: []string{}}, nil
}

// runStatementInput holds input for RunStatement.
type runStatementInput struct{}

// runStatementOutput holds the result for RunStatement.
type runStatementOutput struct {
	ID int32 `json:"Id"`
}

func (h *Handler) handleRunStatement(
	_ context.Context,
	_ *runStatementInput,
) (*runStatementOutput, error) {
	return &runStatementOutput{ID: 1}, nil
}

// searchTablesInput holds input for SearchTables.
type searchTablesInput struct{}

// searchTablesOutput holds the result for SearchTables.
type searchTablesOutput struct {
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleSearchTables(
	_ context.Context,
	_ *searchTablesInput,
) (*searchTablesOutput, error) {
	return &searchTablesOutput{TableList: []*Table{}}, nil
}

// startBlueprintRunInput holds input for StartBlueprintRun.
type startBlueprintRunInput struct{}

// startBlueprintRunOutput holds the result for StartBlueprintRun.
type startBlueprintRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartBlueprintRun(
	_ context.Context,
	_ *startBlueprintRunInput,
) (*startBlueprintRunOutput, error) {
	return &startBlueprintRunOutput{RunID: "blueprint-run-stub"}, nil
}

// startColumnStatisticsTaskRunInput holds input for StartColumnStatisticsTaskRun.
type startColumnStatisticsTaskRunInput struct{}

// startColumnStatisticsTaskRunOutput holds the result for StartColumnStatisticsTaskRun.
type startColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

func (h *Handler) handleStartColumnStatisticsTaskRun(
	_ context.Context,
	_ *startColumnStatisticsTaskRunInput,
) (*startColumnStatisticsTaskRunOutput, error) {
	return &startColumnStatisticsTaskRunOutput{ColumnStatisticsTaskRunID: "col-stats-run-stub"}, nil
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
type startDataQualityRuleRecommendationRunInput struct{}

// startDataQualityRuleRecommendationRunOutput holds the result for StartDataQualityRuleRecommendationRun.
type startDataQualityRuleRecommendationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRuleRecommendationRun(
	_ context.Context,
	_ *startDataQualityRuleRecommendationRunInput,
) (*startDataQualityRuleRecommendationRunOutput, error) {
	return &startDataQualityRuleRecommendationRunOutput{RunID: "dq-rec-run-stub"}, nil
}

// startExportLabelsTaskRunInput holds input for StartExportLabelsTaskRun.
type startExportLabelsTaskRunInput struct{}

// startExportLabelsTaskRunOutput holds the result for StartExportLabelsTaskRun.
type startExportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartExportLabelsTaskRun(
	_ context.Context,
	_ *startExportLabelsTaskRunInput,
) (*startExportLabelsTaskRunOutput, error) {
	return &startExportLabelsTaskRunOutput{TaskRunID: "export-labels-stub"}, nil
}

// startImportLabelsTaskRunInput holds input for StartImportLabelsTaskRun.
type startImportLabelsTaskRunInput struct{}

// startImportLabelsTaskRunOutput holds the result for StartImportLabelsTaskRun.
type startImportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartImportLabelsTaskRun(
	_ context.Context,
	_ *startImportLabelsTaskRunInput,
) (*startImportLabelsTaskRunOutput, error) {
	return &startImportLabelsTaskRunOutput{TaskRunID: "import-labels-stub"}, nil
}

// startMLEvaluationTaskRunInput holds input for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunInput struct{}

// startMLEvaluationTaskRunOutput holds the result for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLEvaluationTaskRun(
	_ context.Context,
	_ *startMLEvaluationTaskRunInput,
) (*startMLEvaluationTaskRunOutput, error) {
	return &startMLEvaluationTaskRunOutput{TaskRunID: "ml-eval-stub"}, nil
}

// startMLLabelingSetGenerationTaskRunInput holds input for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunInput struct{}

// startMLLabelingSetGenerationTaskRunOutput holds the result for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLLabelingSetGenerationTaskRun(
	_ context.Context,
	_ *startMLLabelingSetGenerationTaskRunInput,
) (*startMLLabelingSetGenerationTaskRunOutput, error) {
	return &startMLLabelingSetGenerationTaskRunOutput{TaskRunID: "ml-label-stub"}, nil
}

// startMaterializedViewRefreshTaskRunInput holds input for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunInput struct{}

// startMaterializedViewRefreshTaskRunOutput holds the result for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartMaterializedViewRefreshTaskRun(
	_ context.Context,
	_ *startMaterializedViewRefreshTaskRunInput,
) (*startMaterializedViewRefreshTaskRunOutput, error) {
	return &startMaterializedViewRefreshTaskRunOutput{RunID: "mat-view-refresh-stub"}, nil
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
type stopColumnStatisticsTaskRunInput struct{}

func (h *Handler) handleStopColumnStatisticsTaskRun(
	_ context.Context,
	_ *stopColumnStatisticsTaskRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type stopMaterializedViewRefreshTaskRunInput struct{}

func (h *Handler) handleStopMaterializedViewRefreshTaskRun(
	_ context.Context,
	_ *stopMaterializedViewRefreshTaskRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type stopWorkflowRunInput struct{}

func (h *Handler) handleStopWorkflowRun(
	_ context.Context,
	_ *stopWorkflowRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &updateBlueprintOutput{Name: in.Name}, nil
}

// updateCatalogInput holds input for UpdateCatalog.
type updateCatalogInput struct{}

func (h *Handler) handleUpdateCatalog(
	_ context.Context,
	_ *updateCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
type updateColumnStatisticsForPartitionInput struct{}

// updateColumnStatisticsForPartitionOutput holds the result for UpdateColumnStatisticsForPartition.
type updateColumnStatisticsForPartitionOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForPartition(
	_ context.Context,
	_ *updateColumnStatisticsForPartitionInput,
) (*updateColumnStatisticsForPartitionOutput, error) {
	return &updateColumnStatisticsForPartitionOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsForTableInput holds input for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableInput struct{}

// updateColumnStatisticsForTableOutput holds the result for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForTable(
	_ context.Context,
	_ *updateColumnStatisticsForTableInput,
) (*updateColumnStatisticsForTableOutput, error) {
	return &updateColumnStatisticsForTableOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsTaskSettingsInput holds input for UpdateColumnStatisticsTaskSettings.
type updateColumnStatisticsTaskSettingsInput struct{}

func (h *Handler) handleUpdateColumnStatisticsTaskSettings(
	_ context.Context,
	_ *updateColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// updateConnectionInput holds input for UpdateConnection.
type updateConnectionInput struct {
	Name            string          `json:"Name"`
	ConnectionInput connectionInput `json:"ConnectionInput"`
}

func (h *Handler) handleUpdateConnection(
	_ context.Context,
	_ *updateConnectionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// updateDevEndpointInput holds input for UpdateDevEndpoint.
type updateDevEndpointInput struct{}

func (h *Handler) handleUpdateDevEndpoint(
	_ context.Context,
	_ *updateDevEndpointInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// updateGlueIdentityCenterConfigurationInput holds input for UpdateGlueIdentityCenterConfiguration.
type updateGlueIdentityCenterConfigurationInput struct{}

func (h *Handler) handleUpdateGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *updateGlueIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	TransformID string `json:"TransformId"`
}

// updateMLTransformOutput holds the result for UpdateMLTransform.
type updateMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleUpdateMLTransform(
	_ context.Context,
	in *updateMLTransformInput,
) (*updateMLTransformOutput, error) {
	return &updateMLTransformOutput{TransformID: in.TransformID}, nil
}

// updatePartitionInput holds input for UpdatePartition.
type updatePartitionInput struct{}

func (h *Handler) handleUpdatePartition(
	_ context.Context,
	_ *updatePartitionInput,
) (*emptyOutput, error) {
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
type updateTableOptimizerInput struct{}

func (h *Handler) handleUpdateTableOptimizer(
	_ context.Context,
	_ *updateTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
	return &updateUsageProfileOutput{Name: in.Name}, nil
}

// updateUserDefinedFunctionInput holds input for UpdateUserDefinedFunction.
type updateUserDefinedFunctionInput struct{}

func (h *Handler) handleUpdateUserDefinedFunction(
	_ context.Context,
	_ *updateUserDefinedFunctionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, nil
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
