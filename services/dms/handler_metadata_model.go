package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/google/uuid"
)

// listMetadataModelRequests retrieves metadata model requests of a given type and paginates them.
func listMetadataModelRequests(
	ctx context.Context,
	h *Handler,
	projectID, reqType string,
	marker *string,
	maxRecords *int32,
) ([]map[string]any, *string, error) {
	list, err := h.Backend.ListMetadataModelRequests(ctx, projectID, reqType)
	if err != nil {
		return nil, nil, err
	}

	all := make([]map[string]any, 0, len(list))
	for _, req := range list {
		all = append(all, map[string]any{
			"RequestIdentifier":          req.RequestIdentifier,
			"MigrationProjectIdentifier": req.MigrationProjectIdentifier,
			"Status":                     req.Status,
			"SelectionRules":             req.SelectionRules,
		})
	}

	data, nextMarker := dmsPaginate(all, marker, maxRecords)

	return data, nextMarker, nil
}

type cancelMetadataModelConversionInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelConversionOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleCancelMetadataModelConversion(
	ctx context.Context, in *cancelMetadataModelConversionInput,
) (*cancelMetadataModelConversionOutput, error) {
	reqID, err := h.Backend.CancelMetadataModelConversion(
		ctx,
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelConversionOutput{RequestIdentifier: reqID}, nil
}

type cancelMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelCreationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleCancelMetadataModelCreation(
	ctx context.Context, in *cancelMetadataModelCreationInput,
) (*cancelMetadataModelCreationOutput, error) {
	reqID, err := h.Backend.CancelMetadataModelCreation(
		ctx,
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelCreationOutput{RequestIdentifier: reqID}, nil
}

type describeConversionConfigurationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
}

type describeConversionConfigurationOutput struct {
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    string `json:"ConversionConfiguration"`
}

func (h *Handler) handleDescribeConversionConfiguration(
	_ context.Context, in *describeConversionConfigurationInput,
) (*describeConversionConfigurationOutput, error) {
	return &describeConversionConfigurationOutput{
		MigrationProjectIdentifier: ptrconv.String(in.MigrationProjectIdentifier),
		ConversionConfiguration:    "{}",
	}, nil
}

type describeExtensionPackAssociationsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeExtensionPackAssociationsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeExtensionPackAssociations(
	_ context.Context, _ *describeExtensionPackAssociationsInput,
) (*describeExtensionPackAssociationsOutput, error) {
	return &describeExtensionPackAssociationsOutput{Requests: []map[string]any{}}, nil
}

type describeMetadataModelInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type describeMetadataModelOutput struct{}

func (h *Handler) handleDescribeMetadataModel(
	ctx context.Context, in *describeMetadataModelInput,
) (*describeMetadataModelOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	// Validate project exists via a read of its request store; returns empty list if none started.
	if _, err := h.Backend.ListMetadataModelRequests(ctx, projectID, "assessment"); err != nil {
		return nil, err
	}

	return &describeMetadataModelOutput{}, nil
}

type describeMetadataModelAssessmentsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelAssessmentsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelAssessments(
	ctx context.Context, in *describeMetadataModelAssessmentsInput,
) (*describeMetadataModelAssessmentsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "assessment", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelAssessmentsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelChildrenInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelChildrenOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Items  []map[string]any `json:"Items"`
}

func (h *Handler) handleDescribeMetadataModelChildren(
	ctx context.Context, in *describeMetadataModelChildrenInput,
) (*describeMetadataModelChildrenOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "children", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelChildrenOutput{Items: reqs, Marker: marker}, nil
}

type describeMetadataModelConversionsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelConversionsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelConversions(
	ctx context.Context, in *describeMetadataModelConversionsInput,
) (*describeMetadataModelConversionsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "conversion", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelConversionsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelCreationsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelCreationsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelCreations(
	ctx context.Context, in *describeMetadataModelCreationsInput,
) (*describeMetadataModelCreationsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "creation", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelCreationsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelExportsAsScriptInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelExportsAsScriptOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsAsScript(
	ctx context.Context, in *describeMetadataModelExportsAsScriptInput,
) (*describeMetadataModelExportsAsScriptOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "export-as-script", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelExportsAsScriptOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelExportsToTargetInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelExportsToTargetOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsToTarget(
	ctx context.Context, in *describeMetadataModelExportsToTargetInput,
) (*describeMetadataModelExportsToTargetOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "export-to-target", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelExportsToTargetOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelImportsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelImportsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelImports(
	ctx context.Context, in *describeMetadataModelImportsInput,
) (*describeMetadataModelImportsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "import", in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelImportsOutput{Requests: reqs, Marker: marker}, nil
}

type exportMetadataModelAssessmentInput struct {
	MigrationProjectIdentifier *string  `json:"MigrationProjectIdentifier"`
	SelectionRules             *string  `json:"SelectionRules"`
	FileName                   *string  `json:"FileName"`
	AssessmentReportTypes      []string `json:"AssessmentReportTypes"`
}

type s3ObjectKeyJSON struct {
	S3ObjectKey string `json:"S3ObjectKey"`
}

type exportMetadataModelAssessmentOutput struct {
	PdfReport s3ObjectKeyJSON `json:"PdfReport"`
	CsvReport s3ObjectKeyJSON `json:"CsvReport"`
}

func (h *Handler) handleExportMetadataModelAssessment(
	_ context.Context, _ *exportMetadataModelAssessmentInput,
) (*exportMetadataModelAssessmentOutput, error) {
	return &exportMetadataModelAssessmentOutput{
		PdfReport: s3ObjectKeyJSON{S3ObjectKey: ""},
		CsvReport: s3ObjectKeyJSON{S3ObjectKey: ""},
	}, nil
}

type getTargetSelectionRulesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type getTargetSelectionRulesOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Rules  []map[string]any `json:"Rules"`
}

func (h *Handler) handleGetTargetSelectionRules(
	_ context.Context, _ *getTargetSelectionRulesInput,
) (*getTargetSelectionRulesOutput, error) {
	return &getTargetSelectionRulesOutput{Rules: []map[string]any{}}, nil
}

type modifyConversionConfigurationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    *string `json:"ConversionConfiguration"`
}

type modifyConversionConfigurationOutput struct {
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    string `json:"ConversionConfiguration"`
}

func (h *Handler) handleModifyConversionConfiguration(
	_ context.Context, in *modifyConversionConfigurationInput,
) (*modifyConversionConfigurationOutput, error) {
	return &modifyConversionConfigurationOutput{
		MigrationProjectIdentifier: ptrconv.String(in.MigrationProjectIdentifier),
		ConversionConfiguration:    ptrconv.String(in.ConversionConfiguration),
	}, nil
}

type startExtensionPackAssociationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
}

type startExtensionPackAssociationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartExtensionPackAssociation(
	_ context.Context, _ *startExtensionPackAssociationInput,
) (*startExtensionPackAssociationOutput, error) {
	return &startExtensionPackAssociationOutput{RequestIdentifier: uuid.NewString()}, nil
}

type startMetadataModelAssessmentInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelAssessmentOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelAssessment(
	ctx context.Context, in *startMetadataModelAssessmentInput,
) (*startMetadataModelAssessmentOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "assessment", ptrconv.String(in.SelectionRules))
	if err != nil {
		return nil, err
	}

	return &startMetadataModelAssessmentOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelConversionInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelConversionOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelConversion(
	ctx context.Context, in *startMetadataModelConversionInput,
) (*startMetadataModelConversionOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "conversion", ptrconv.String(in.SelectionRules))
	if err != nil {
		return nil, err
	}

	return &startMetadataModelConversionOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelCreationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelCreation(
	ctx context.Context, in *startMetadataModelCreationInput,
) (*startMetadataModelCreationOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "creation", ptrconv.String(in.SelectionRules))
	if err != nil {
		return nil, err
	}

	return &startMetadataModelCreationOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelExportAsScriptInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	FileName                   *string `json:"FileName"`
	Origin                     *string `json:"Origin"`
}

type startMetadataModelExportAsScriptOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelExportAsScript(
	ctx context.Context, in *startMetadataModelExportAsScriptInput,
) (*startMetadataModelExportAsScriptOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(
		ctx, projectID, "export-as-script", ptrconv.String(in.SelectionRules),
	)
	if err != nil {
		return nil, err
	}

	return &startMetadataModelExportAsScriptOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelExportToTargetInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	OverwriteExtensionPack     *bool   `json:"OverwriteExtensionPack"`
}

type startMetadataModelExportToTargetOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelExportToTarget(
	ctx context.Context, in *startMetadataModelExportToTargetInput,
) (*startMetadataModelExportToTargetOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(
		ctx, projectID, "export-to-target", ptrconv.String(in.SelectionRules),
	)
	if err != nil {
		return nil, err
	}

	return &startMetadataModelExportToTargetOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelImportInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	Origin                     *string `json:"Origin"`
	Refresh                    *bool   `json:"Refresh"`
}

type startMetadataModelImportOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelImport(
	ctx context.Context, in *startMetadataModelImportInput,
) (*startMetadataModelImportOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "import", ptrconv.String(in.SelectionRules))
	if err != nil {
		return nil, err
	}

	return &startMetadataModelImportOutput{RequestIdentifier: reqID}, nil
}

// opsMetadataModel returns the dispatch-table entries for the metadata_model operation family.
func (h *Handler) opsMetadataModel() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CancelMetadataModelConversion": service.WrapOp(
			h.handleCancelMetadataModelConversion,
		),
		"CancelMetadataModelCreation": service.WrapOp(
			h.handleCancelMetadataModelCreation,
		),
		opDescribeConversionConfiguration: service.WrapOp(
			h.handleDescribeConversionConfiguration,
		),
		opDescribeExtensionPackAssociations: service.WrapOp(
			h.handleDescribeExtensionPackAssociations,
		),
		opDescribeMetadataModel: service.WrapOp(
			h.handleDescribeMetadataModel,
		),
		opDescribeMetadataModelAssessments: service.WrapOp(
			h.handleDescribeMetadataModelAssessments,
		),
		opDescribeMetadataModelChildren: service.WrapOp(
			h.handleDescribeMetadataModelChildren,
		),
		opDescribeMetadataModelConversions: service.WrapOp(
			h.handleDescribeMetadataModelConversions,
		),
		opDescribeMetadataModelCreations: service.WrapOp(
			h.handleDescribeMetadataModelCreations,
		),
		opDescribeMetadataModelExportsAsScript: service.WrapOp(
			h.handleDescribeMetadataModelExportsAsScript,
		),
		opDescribeMetadataModelExportsToTarget: service.WrapOp(
			h.handleDescribeMetadataModelExportsToTarget,
		),
		opDescribeMetadataModelImports: service.WrapOp(
			h.handleDescribeMetadataModelImports,
		),
		opExportMetadataModelAssessment: service.WrapOp(
			h.handleExportMetadataModelAssessment,
		),
		opGetTargetSelectionRules: service.WrapOp(
			h.handleGetTargetSelectionRules,
		),
		opModifyConversionConfiguration: service.WrapOp(
			h.handleModifyConversionConfiguration,
		),
		opStartExtensionPackAssociation: service.WrapOp(
			h.handleStartExtensionPackAssociation,
		),
		opStartMetadataModelAssessment: service.WrapOp(
			h.handleStartMetadataModelAssessment,
		),
		opStartMetadataModelConversion: service.WrapOp(
			h.handleStartMetadataModelConversion,
		),
		opStartMetadataModelCreation: service.WrapOp(
			h.handleStartMetadataModelCreation,
		),
		opStartMetadataModelExportAsScript: service.WrapOp(
			h.handleStartMetadataModelExportAsScript,
		),
		opStartMetadataModelExportToTarget: service.WrapOp(
			h.handleStartMetadataModelExportToTarget,
		),
		opStartMetadataModelImport: service.WrapOp(
			h.handleStartMetadataModelImport,
		),
	}
}
