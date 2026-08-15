package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// schemaConversionRequestJSON mirrors types.SchemaConversionRequest -- the
// wire shape used both by the Describe* list operations ("Requests" field)
// and by the Cancel* operations ("Request" field). Error/ExportSqlDetails/
// Progress are optional pointer fields on the real struct that gopherstack
// does not model; omitting them matches a request that never encountered an
// error or produced export details.
type schemaConversionRequestJSON struct {
	RequestIdentifier   string `json:"RequestIdentifier,omitempty"`
	MigrationProjectArn string `json:"MigrationProjectArn,omitempty"`
	Status              string `json:"Status,omitempty"`
}

func reqToSchemaConversionJSON(req *MetadataModelRequest) schemaConversionRequestJSON {
	return schemaConversionRequestJSON{
		RequestIdentifier:   req.RequestIdentifier,
		MigrationProjectArn: req.MigrationProjectIdentifier,
		Status:              req.Status,
	}
}

// listMetadataModelRequests retrieves metadata model requests of a given type,
// applies the request-id/status filters documented on every Describe* schema
// conversion operation's Filters member, and paginates them.
func listMetadataModelRequests(
	ctx context.Context,
	h *Handler,
	projectID, reqType string,
	filters []filterEntry,
	marker *string,
	maxRecords *int32,
) ([]schemaConversionRequestJSON, *string, error) {
	list, err := h.Backend.ListMetadataModelRequests(ctx, projectID, reqType)
	if err != nil {
		return nil, nil, err
	}

	requestIDFilter := extractFilterValue(filters, "request-id")
	statusFilter := extractFilterValue(filters, "status")

	all := make([]schemaConversionRequestJSON, 0, len(list))
	for _, req := range list {
		if requestIDFilter != "" && req.RequestIdentifier != requestIDFilter {
			continue
		}

		if statusFilter != "" && req.Status != statusFilter {
			continue
		}

		all = append(all, reqToSchemaConversionJSON(req))
	}

	data, nextMarker := dmsPaginate(all, marker, maxRecords)

	return data, nextMarker, nil
}

type cancelMetadataModelConversionInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelConversionOutput struct {
	Request schemaConversionRequestJSON `json:"Request"`
}

func (h *Handler) handleCancelMetadataModelConversion(
	ctx context.Context, in *cancelMetadataModelConversionInput,
) (*cancelMetadataModelConversionOutput, error) {
	req, err := h.Backend.CancelMetadataModelConversion(
		ctx,
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelConversionOutput{Request: reqToSchemaConversionJSON(req)}, nil
}

type cancelMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelCreationOutput struct {
	Request schemaConversionRequestJSON `json:"Request"`
}

func (h *Handler) handleCancelMetadataModelCreation(
	ctx context.Context, in *cancelMetadataModelCreationInput,
) (*cancelMetadataModelCreationOutput, error) {
	req, err := h.Backend.CancelMetadataModelCreation(
		ctx,
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelCreationOutput{Request: reqToSchemaConversionJSON(req)}, nil
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
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeExtensionPackAssociationsOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeExtensionPackAssociations(
	ctx context.Context, in *describeExtensionPackAssociationsInput,
) (*describeExtensionPackAssociationsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(
		ctx, h, projectID, "extension-pack", in.Filters, in.Marker, in.MaxRecords,
	)
	if err != nil {
		return nil, err
	}

	return &describeExtensionPackAssociationsOutput{Requests: reqs, Marker: marker}, nil
}

// isValidOrigin validates the Origin enum (types.OriginTypeValue): SOURCE or
// TARGET (uppercase, unlike the lowercase EndpointType enum).
func isValidOrigin(s string) bool {
	return s == "SOURCE" || s == "TARGET"
}

// metadataModelReferenceJSON mirrors types.MetadataModelReference, used both
// for DescribeMetadataModelChildren's list and DescribeMetadataModelOutput's
// TargetMetadataModels.
type metadataModelReferenceJSON struct {
	MetadataModelName string `json:"MetadataModelName,omitempty"`
	SelectionRules    string `json:"SelectionRules,omitempty"`
}

type describeMetadataModelInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Origin                     *string `json:"Origin"`
	SelectionRules             *string `json:"SelectionRules"`
}

type describeMetadataModelOutput struct {
	Definition           string                       `json:"Definition,omitempty"`
	MetadataModelName    string                       `json:"MetadataModelName,omitempty"`
	MetadataModelType    string                       `json:"MetadataModelType,omitempty"`
	TargetMetadataModels []metadataModelReferenceJSON `json:"TargetMetadataModels"`
}

// validateMetadataModelLookup checks the three fields required by every
// DMS Schema Conversion "read a metadata model" operation
// (MigrationProjectIdentifier, Origin, SelectionRules).
func validateMetadataModelLookup(projectID, origin, selectionRules string) error {
	if projectID == "" {
		return fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if origin == "" {
		return fmt.Errorf("%w: Origin is required", ErrValidation)
	}

	if !isValidOrigin(origin) {
		return fmt.Errorf("%w: invalid Origin %q; valid: SOURCE, TARGET", ErrValidation, origin)
	}

	if selectionRules == "" {
		return fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	return nil
}

func (h *Handler) handleDescribeMetadataModel(
	_ context.Context, in *describeMetadataModelInput,
) (*describeMetadataModelOutput, error) {
	err := validateMetadataModelLookup(
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.Origin),
		ptrconv.String(in.SelectionRules),
	)
	if err != nil {
		return nil, err
	}

	// No schema-conversion engine models real converted metadata in this
	// emulation; Definition/MetadataModelName/MetadataModelType are
	// legitimately absent per the SDK doc ("might not be populated for some
	// metadata models"), and TargetMetadataModels is empty since nothing has
	// ever been converted to a target tree.
	return &describeMetadataModelOutput{TargetMetadataModels: []metadataModelReferenceJSON{}}, nil
}

type describeMetadataModelAssessmentsInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelAssessmentsOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelAssessments(
	ctx context.Context, in *describeMetadataModelAssessmentsInput,
) (*describeMetadataModelAssessmentsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(
		ctx,
		h,
		projectID,
		"assessment",
		in.Filters,
		in.Marker,
		in.MaxRecords,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelAssessmentsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelChildrenInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Origin                     *string `json:"Origin"`
	SelectionRules             *string `json:"SelectionRules"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelChildrenOutput struct {
	Marker                *string                      `json:"Marker,omitempty"`
	MetadataModelChildren []metadataModelReferenceJSON `json:"MetadataModelChildren"`
}

func (h *Handler) handleDescribeMetadataModelChildren(
	_ context.Context, in *describeMetadataModelChildrenInput,
) (*describeMetadataModelChildrenOutput, error) {
	err := validateMetadataModelLookup(
		ptrconv.String(in.MigrationProjectIdentifier),
		ptrconv.String(in.Origin),
		ptrconv.String(in.SelectionRules),
	)
	if err != nil {
		return nil, err
	}

	// No schema-conversion engine has ever produced child metadata models in
	// this emulation; an empty list is the correct response for a project
	// whose selected object has no children.
	return &describeMetadataModelChildrenOutput{MetadataModelChildren: []metadataModelReferenceJSON{}}, nil
}

type describeMetadataModelConversionsInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelConversionsOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelConversions(
	ctx context.Context, in *describeMetadataModelConversionsInput,
) (*describeMetadataModelConversionsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(
		ctx,
		h,
		projectID,
		"conversion",
		in.Filters,
		in.Marker,
		in.MaxRecords,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelConversionsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelCreationsInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelCreationsOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelCreations(
	ctx context.Context, in *describeMetadataModelCreationsInput,
) (*describeMetadataModelCreationsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "creation", in.Filters, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelCreationsOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelExportsAsScriptInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelExportsAsScriptOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsAsScript(
	ctx context.Context, in *describeMetadataModelExportsAsScriptInput,
) (*describeMetadataModelExportsAsScriptOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(
		ctx, h, projectID, "export-as-script", in.Filters, in.Marker, in.MaxRecords,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelExportsAsScriptOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelExportsToTargetInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelExportsToTargetOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsToTarget(
	ctx context.Context, in *describeMetadataModelExportsToTargetInput,
) (*describeMetadataModelExportsToTargetOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(
		ctx, h, projectID, "export-to-target", in.Filters, in.Marker, in.MaxRecords,
	)
	if err != nil {
		return nil, err
	}

	return &describeMetadataModelExportsToTargetOutput{Requests: reqs, Marker: marker}, nil
}

type describeMetadataModelImportsInput struct {
	MigrationProjectIdentifier *string       `json:"MigrationProjectIdentifier"`
	Marker                     *string       `json:"Marker"`
	MaxRecords                 *int32        `json:"MaxRecords"`
	Filters                    []filterEntry `json:"Filters"`
}

type describeMetadataModelImportsOutput struct {
	Marker   *string                       `json:"Marker,omitempty"`
	Requests []schemaConversionRequestJSON `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelImports(
	ctx context.Context, in *describeMetadataModelImportsInput,
) (*describeMetadataModelImportsOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqs, marker, err := listMetadataModelRequests(ctx, h, projectID, "import", in.Filters, in.Marker, in.MaxRecords)
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

// exportResultEntryJSON mirrors types.ExportMetadataModelAssessmentResultEntry.
type exportResultEntryJSON struct {
	ObjectURL   string `json:"ObjectURL,omitempty"`
	S3ObjectKey string `json:"S3ObjectKey,omitempty"`
}

type exportMetadataModelAssessmentOutput struct {
	PdfReport exportResultEntryJSON `json:"PdfReport"`
	CsvReport exportResultEntryJSON `json:"CsvReport"`
}

func (h *Handler) handleExportMetadataModelAssessment(
	_ context.Context, in *exportMetadataModelAssessmentInput,
) (*exportMetadataModelAssessmentOutput, error) {
	if ptrconv.String(in.MigrationProjectIdentifier) == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if ptrconv.String(in.SelectionRules) == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	// No schema-conversion engine or S3 integration exists in this emulation
	// to produce a real exported report; ObjectURL/S3ObjectKey are
	// legitimately absent (both are optional pointer fields on the real
	// struct) rather than fabricated.
	return &exportMetadataModelAssessmentOutput{}, nil
}

type getTargetSelectionRulesInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type getTargetSelectionRulesOutput struct {
	TargetSelectionRules string `json:"TargetSelectionRules"`
}

func (h *Handler) handleGetTargetSelectionRules(
	_ context.Context, in *getTargetSelectionRulesInput,
) (*getTargetSelectionRulesOutput, error) {
	if ptrconv.String(in.MigrationProjectIdentifier) == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	selectionRules := ptrconv.String(in.SelectionRules)
	if selectionRules == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	// No schema-conversion engine exists to compute the real target
	// counterpart selection rules; echo the source rules unchanged as a
	// best-effort identity mapping, preserving the documented single-string
	// wire shape (TargetSelectionRules is a scalar, not a list).
	return &getTargetSelectionRulesOutput{TargetSelectionRules: selectionRules}, nil
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
	ctx context.Context, in *startExtensionPackAssociationInput,
) (*startExtensionPackAssociationOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "extension-pack", "", "")
	if err != nil {
		return nil, err
	}

	return &startExtensionPackAssociationOutput{RequestIdentifier: reqID}, nil
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
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	selectionRules := ptrconv.String(in.SelectionRules)
	if selectionRules == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "assessment", selectionRules, "")
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
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	selectionRules := ptrconv.String(in.SelectionRules)
	if selectionRules == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "conversion", selectionRules, "")
	if err != nil {
		return nil, err
	}

	return &startMetadataModelConversionOutput{RequestIdentifier: reqID}, nil
}

// statementPropertiesJSON mirrors types.StatementProperties (types.go:5207).
type statementPropertiesJSON struct {
	Definition *string `json:"Definition"`
}

// metadataModelPropertiesJSON mirrors the types.MetadataModelProperties union
// (types.go:1872), which currently has a single member. JSON-RPC 1.1 unions
// serialize as a single-key object naming the active member (verified
// against awsAwsjson11_serializeDocumentMetadataModelProperties,
// serializers.go), so it decodes the same way here.
type metadataModelPropertiesJSON struct {
	StatementProperties *statementPropertiesJSON `json:"StatementProperties,omitempty"`
}

type startMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string                      `json:"MigrationProjectIdentifier"`
	MetadataModelName          *string                      `json:"MetadataModelName"`
	SelectionRules             *string                      `json:"SelectionRules"`
	Properties                 *metadataModelPropertiesJSON `json:"Properties"`
}

type startMetadataModelCreationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelCreation(
	ctx context.Context, in *startMetadataModelCreationInput,
) (*startMetadataModelCreationOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if ptrconv.String(in.MetadataModelName) == "" {
		return nil, fmt.Errorf("%w: MetadataModelName is required", ErrValidation)
	}

	selectionRules := ptrconv.String(in.SelectionRules)
	if selectionRules == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	// Properties is a required StartMetadataModelCreationInput member
	// (verified against validateOpStartMetadataModelCreationInput,
	// validators.go) that the pre-fix request never read at all.
	if in.Properties == nil {
		return nil, fmt.Errorf("%w: Properties is required", ErrValidation)
	}

	var definition string

	if sp := in.Properties.StatementProperties; sp != nil {
		definition = ptrconv.String(sp.Definition)
		if definition == "" {
			return nil, fmt.Errorf("%w: Properties.StatementProperties.Definition is required", ErrValidation)
		}
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "creation", selectionRules, definition)
	if err != nil {
		return nil, err
	}

	return &startMetadataModelCreationOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelExportAsScriptInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Origin                     *string `json:"Origin"`
	SelectionRules             *string `json:"SelectionRules"`
	FileName                   *string `json:"FileName"`
}

type startMetadataModelExportAsScriptOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelExportAsScript(
	ctx context.Context, in *startMetadataModelExportAsScriptInput,
) (*startMetadataModelExportAsScriptOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	selectionRules := ptrconv.String(in.SelectionRules)

	err := validateMetadataModelLookup(projectID, ptrconv.String(in.Origin), selectionRules)
	if err != nil {
		return nil, err
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "export-as-script", selectionRules, "")
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
	if projectID == "" {
		return nil, fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	selectionRules := ptrconv.String(in.SelectionRules)
	if selectionRules == "" {
		return nil, fmt.Errorf("%w: SelectionRules is required", ErrValidation)
	}

	reqID, err := h.Backend.StartMetadataModelRequest(
		ctx, projectID, "export-to-target", selectionRules, "",
	)
	if err != nil {
		return nil, err
	}

	return &startMetadataModelExportToTargetOutput{RequestIdentifier: reqID}, nil
}

type startMetadataModelImportInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Origin                     *string `json:"Origin"`
	SelectionRules             *string `json:"SelectionRules"`
	Refresh                    *bool   `json:"Refresh"`
}

type startMetadataModelImportOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelImport(
	ctx context.Context, in *startMetadataModelImportInput,
) (*startMetadataModelImportOutput, error) {
	projectID := ptrconv.String(in.MigrationProjectIdentifier)
	selectionRules := ptrconv.String(in.SelectionRules)

	err := validateMetadataModelLookup(projectID, ptrconv.String(in.Origin), selectionRules)
	if err != nil {
		return nil, err
	}

	reqID, err := h.Backend.StartMetadataModelRequest(ctx, projectID, "import", selectionRules, "")
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
