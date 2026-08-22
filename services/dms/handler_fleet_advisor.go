package dms

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createFleetAdvisorCollectorInput struct {
	CollectorName        *string `json:"CollectorName"`
	Description          *string `json:"Description"`
	ServiceAccessRoleArn *string `json:"ServiceAccessRoleArn"`
	S3BucketName         *string `json:"S3BucketName"`
}

type createFleetAdvisorCollectorOutput struct {
	CollectorName         string `json:"CollectorName"`
	CollectorReferencedID string `json:"CollectorReferencedId"`
	Description           string `json:"Description,omitempty"`
	ServiceAccessRoleArn  string `json:"ServiceAccessRoleArn"`
	S3BucketName          string `json:"S3BucketName"`
}

func (h *Handler) handleCreateFleetAdvisorCollector(
	ctx context.Context, in *createFleetAdvisorCollectorInput,
) (*createFleetAdvisorCollectorOutput, error) {
	name := ptrconv.String(in.CollectorName)
	if name == "" {
		return nil, fmt.Errorf("%w: CollectorName is required", ErrValidation)
	}

	col, err := h.Backend.CreateFleetAdvisorCollector(
		ctx,
		name,
		ptrconv.String(in.Description),
		ptrconv.String(in.ServiceAccessRoleArn),
		ptrconv.String(in.S3BucketName),
	)
	if err != nil {
		return nil, err
	}

	return &createFleetAdvisorCollectorOutput{
		CollectorName:         col.CollectorName,
		CollectorReferencedID: col.CollectorReferencedID,
		Description:           col.Description,
		ServiceAccessRoleArn:  col.ServiceAccessRoleArn,
		S3BucketName:          col.S3BucketName,
	}, nil
}

type deleteFleetAdvisorCollectorInput struct {
	CollectorReferencedID *string `json:"CollectorReferencedId"`
}

type deleteFleetAdvisorCollectorOutput struct{}

func (h *Handler) handleDeleteFleetAdvisorCollector(
	ctx context.Context, in *deleteFleetAdvisorCollectorInput,
) (*deleteFleetAdvisorCollectorOutput, error) {
	if err := h.Backend.DeleteFleetAdvisorCollector(ctx, ptrconv.String(in.CollectorReferencedID)); err != nil {
		return nil, err
	}

	return &deleteFleetAdvisorCollectorOutput{}, nil
}

type deleteFleetAdvisorDatabasesInput struct {
	DatabaseIDs []string `json:"DatabaseIds"`
}

type deleteFleetAdvisorDatabasesOutput struct {
	DatabaseIDs []string `json:"DatabaseIds"`
}

func (h *Handler) handleDeleteFleetAdvisorDatabases(
	ctx context.Context, in *deleteFleetAdvisorDatabasesInput,
) (*deleteFleetAdvisorDatabasesOutput, error) {
	deleted, err := h.Backend.DeleteFleetAdvisorDatabases(ctx, in.DatabaseIDs)
	if err != nil {
		return nil, err
	}

	return &deleteFleetAdvisorDatabasesOutput{DatabaseIDs: deleted}, nil
}

type describeFleetAdvisorCollectorsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type fleetAdvisorCollectorJSON struct {
	CollectorName         string `json:"CollectorName"`
	CollectorReferencedID string `json:"CollectorReferencedId"`
	CollectorVersion      string `json:"CollectorVersion"`
	Description           string `json:"Description,omitempty"`
	ServiceAccessRoleArn  string `json:"ServiceAccessRoleArn"`
	S3BucketName          string `json:"S3BucketName"`
	CollectorHealthCheck  string `json:"CollectorHealthCheck"`
}

type describeFleetAdvisorCollectorsOutput struct {
	Collectors []fleetAdvisorCollectorJSON `json:"Collectors"`
}

func (h *Handler) handleDescribeFleetAdvisorCollectors(
	ctx context.Context, _ *describeFleetAdvisorCollectorsInput,
) (*describeFleetAdvisorCollectorsOutput, error) {
	list, err := h.Backend.DescribeFleetAdvisorCollectors(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]fleetAdvisorCollectorJSON, 0, len(list))
	for _, col := range list {
		result = append(result, fleetAdvisorCollectorJSON{
			CollectorName:         col.CollectorName,
			CollectorReferencedID: col.CollectorReferencedID,
			CollectorVersion:      col.CollectorVersion,
			Description:           col.Description,
			ServiceAccessRoleArn:  col.ServiceAccessRoleArn,
			S3BucketName:          col.S3BucketName,
			CollectorHealthCheck:  col.CollectorHealthCheck,
		})
	}

	return &describeFleetAdvisorCollectorsOutput{Collectors: result}, nil
}

type describeFleetAdvisorDatabasesInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorDatabasesOutput struct {
	NextToken *string          `json:"NextToken,omitempty"`
	Databases []map[string]any `json:"Databases"`
}

func (h *Handler) handleDescribeFleetAdvisorDatabases(
	ctx context.Context, _ *describeFleetAdvisorDatabasesInput,
) (*describeFleetAdvisorDatabasesOutput, error) {
	list, err := h.Backend.DescribeFleetAdvisorDatabases(ctx)
	if err != nil {
		return nil, err
	}

	dbs := make([]map[string]any, 0, len(list))
	for _, db := range list {
		dbs = append(dbs, map[string]any{
			"DatabaseId":   db.DatabaseID,
			"DatabaseName": db.DatabaseName,
			"IpAddress":    db.IPAddress,
			"SoftwareDetails": map[string]any{
				"Engine": db.EngineName,
			},
			"Collectors": []map[string]any{
				{"CollectorReferencedId": db.CollectorReferencedID},
			},
		})
	}

	return &describeFleetAdvisorDatabasesOutput{Databases: dbs}, nil
}

type describeFleetAdvisorLsaAnalysisInput struct {
	NextToken  *string `json:"NextToken"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeFleetAdvisorLsaAnalysisOutput struct {
	NextToken *string          `json:"NextToken,omitempty"`
	Analysis  []map[string]any `json:"Analysis"`
}

func (h *Handler) handleDescribeFleetAdvisorLsaAnalysis(
	_ context.Context, _ *describeFleetAdvisorLsaAnalysisInput,
) (*describeFleetAdvisorLsaAnalysisOutput, error) {
	return &describeFleetAdvisorLsaAnalysisOutput{Analysis: []map[string]any{}}, nil
}

type describeFleetAdvisorSchemaObjectSummaryInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorSchemaObjectSummaryOutput struct {
	NextToken                 *string          `json:"NextToken,omitempty"`
	FleetAdvisorSchemaObjects []map[string]any `json:"FleetAdvisorSchemaObjects"`
}

func (h *Handler) handleDescribeFleetAdvisorSchemaObjectSummary(
	_ context.Context, _ *describeFleetAdvisorSchemaObjectSummaryInput,
) (*describeFleetAdvisorSchemaObjectSummaryOutput, error) {
	return &describeFleetAdvisorSchemaObjectSummaryOutput{
		FleetAdvisorSchemaObjects: []map[string]any{},
	}, nil
}

type describeFleetAdvisorSchemasInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorSchemasOutput struct {
	NextToken           *string          `json:"NextToken,omitempty"`
	FleetAdvisorSchemas []map[string]any `json:"FleetAdvisorSchemas"`
}

func (h *Handler) handleDescribeFleetAdvisorSchemas(
	_ context.Context, _ *describeFleetAdvisorSchemasInput,
) (*describeFleetAdvisorSchemasOutput, error) {
	return &describeFleetAdvisorSchemasOutput{FleetAdvisorSchemas: []map[string]any{}}, nil
}

type runFleetAdvisorLsaAnalysisInput struct{}

type runFleetAdvisorLsaAnalysisOutput struct {
	LsaAnalysisID string `json:"LsaAnalysisId"`
	Status        string `json:"Status"`
}

func (h *Handler) handleRunFleetAdvisorLsaAnalysis(
	_ context.Context, _ *runFleetAdvisorLsaAnalysisInput,
) (*runFleetAdvisorLsaAnalysisOutput, error) {
	return &runFleetAdvisorLsaAnalysisOutput{
		LsaAnalysisID: uuid.NewString(),
		Status:        "RUNNING",
	}, nil
}

// opsFleetAdvisor returns the dispatch-table entries for the fleet_advisor operation family.
func (h *Handler) opsFleetAdvisor() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateFleetAdvisorCollector: service.WrapOp(
			h.handleCreateFleetAdvisorCollector,
		),
		opDeleteFleetAdvisorCollector: service.WrapOp(
			h.handleDeleteFleetAdvisorCollector,
		),
		opDeleteFleetAdvisorDatabases: service.WrapOp(
			h.handleDeleteFleetAdvisorDatabases,
		),
		opDescribeFleetAdvisorCollectors: service.WrapOp(
			h.handleDescribeFleetAdvisorCollectors,
		),
		opDescribeFleetAdvisorDatabases: service.WrapOp(
			h.handleDescribeFleetAdvisorDatabases,
		),
		opDescribeFleetAdvisorLsaAnalysis: service.WrapOp(
			h.handleDescribeFleetAdvisorLsaAnalysis,
		),
		opDescribeFleetAdvisorSchemaObjectSummary: service.WrapOp(
			h.handleDescribeFleetAdvisorSchemaObjectSummary,
		),
		opDescribeFleetAdvisorSchemas: service.WrapOp(
			h.handleDescribeFleetAdvisorSchemas,
		),
		opRunFleetAdvisorLsaAnalysis: service.WrapOp(
			h.handleRunFleetAdvisorLsaAnalysis,
		),
	}
}
