package glue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// validateSchemaDefinition checks a schema definition string against its DataFormat.
// Returns (true, "") when valid; (false, errMsg) otherwise.
func validateSchemaDefinition(dataFormat, definition string) (bool, string) {
	switch strings.ToUpper(dataFormat) {
	case "AVRO":
		return validateAvroSchema(definition)
	case "JSON":
		return validateJSONSchema(definition)
	case "PROTOBUF":
		return validateProtobufSchema(definition)
	default:
		return false, "unsupported DataFormat: " + dataFormat
	}
}

func validateAvroSchema(def string) (bool, string) {
	var v map[string]any
	if err := json.Unmarshal([]byte(def), &v); err != nil {
		return false, "schema is not valid JSON: " + err.Error()
	}

	if _, ok := v["type"]; !ok {
		return false, "AVRO schema must have a 'type' field"
	}

	return true, ""
}

func validateJSONSchema(def string) (bool, string) {
	var v any
	if err := json.Unmarshal([]byte(def), &v); err != nil {
		return false, "schema is not valid JSON: " + err.Error()
	}

	return true, ""
}

func validateProtobufSchema(def string) (bool, string) {
	if !strings.Contains(def, "syntax") {
		return false, "PROTOBUF schema must contain a 'syntax' declaration"
	}

	if !strings.Contains(def, "message") {
		return false, "PROTOBUF schema must contain at least one 'message' declaration"
	}

	return true, ""
}

// parseVersionRanges parses an AWS-style version range string (e.g. "1-3,5,7-9")
// into a sorted list of individual version numbers.
func parseVersionRanges(versions string) ([]int64, error) {
	if versions == "" {
		return nil, nil
	}

	var nums []int64

	for part := range strings.SplitSeq(versions, ",") {
		part = strings.TrimSpace(part)

		lo, hi, hasRange := strings.Cut(part, "-")

		if !hasRange {
			v, err := strconv.ParseInt(lo, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid version number %q", ErrValidation, part)
			}

			nums = append(nums, v)

			continue
		}

		start, err := strconv.ParseInt(strings.TrimSpace(lo), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid version range %q", ErrValidation, part)
		}

		end, err := strconv.ParseInt(strings.TrimSpace(hi), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid version range %q", ErrValidation, part)
		}

		if start > end {
			return nil, fmt.Errorf(
				"%w: version range start %d > end %d",
				ErrValidation,
				start,
				end,
			)
		}

		for v := start; v <= end; v++ {
			nums = append(nums, v)
		}
	}

	return nums, nil
}

// checkSchemaVersionValidityInput holds input for CheckSchemaVersionValidity.
type checkSchemaVersionValidityInput struct {
	DataFormat       string `json:"DataFormat"`
	SchemaDefinition string `json:"SchemaDefinition"`
}

// checkSchemaVersionValidityOutput holds the result for CheckSchemaVersionValidity.
type checkSchemaVersionValidityOutput struct {
	Error string `json:"Error,omitempty"`
	Valid bool   `json:"Valid"`
}

func (h *Handler) handleCheckSchemaVersionValidity(
	_ context.Context,
	in *checkSchemaVersionValidityInput,
) (*checkSchemaVersionValidityOutput, error) {
	if in.DataFormat == "" {
		return nil, fmt.Errorf("%w: DataFormat is required", ErrValidation)
	}

	if in.SchemaDefinition == "" {
		return nil, fmt.Errorf("%w: SchemaDefinition is required", ErrValidation)
	}

	valid, errMsg := validateSchemaDefinition(in.DataFormat, in.SchemaDefinition)

	return &checkSchemaVersionValidityOutput{Valid: valid, Error: errMsg}, nil
}

// createRegistryInput holds input for CreateRegistry.
type createRegistryInput struct {
	Tags         map[string]string `json:"Tags"`
	RegistryName string            `json:"RegistryName"`
	Description  string            `json:"Description"`
}

// createRegistryOutput holds the result for CreateRegistry. Real
// CreateRegistryOutput (glue@v1.152.0 api_op_CreateRegistry.go) carries
// Description, not Status -- Status does not exist anywhere on this op's
// response.
type createRegistryOutput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryName string            `json:"RegistryName"`
	RegistryArn  string            `json:"RegistryArn"`
	Description  string            `json:"Description,omitempty"`
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
		Description:  reg.Description,
		Tags:         reg.Tags,
	}, nil
}

// createSchemaInput holds input for CreateSchema.
type createSchemaInput struct {
	RegistryID       *registryIDInput  `json:"RegistryId"`
	Tags             map[string]string `json:"Tags"`
	SchemaName       string            `json:"SchemaName"`
	DataFormat       string            `json:"DataFormat"`
	Compatibility    string            `json:"Compatibility"`
	Description      string            `json:"Description"`
	SchemaDefinition string            `json:"SchemaDefinition"`
}

// registryIDInput holds registry identification fields.
type registryIDInput struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
}

// createSchemaOutput holds the result for CreateSchema. LatestSchemaVersion,
// NextSchemaVersion, SchemaCheckpoint, SchemaVersionId and SchemaVersionStatus
// mirror the real CreateSchemaOutput (aws-sdk-go-v2/service/glue@v1.152.0
// api_op_CreateSchema.go:116-166); SchemaVersionId/Status are only populated
// when SchemaDefinition was supplied, matching AWS's *string/absent-when-unset
// fields.
type createSchemaOutput struct {
	Tags                map[string]string `json:"Tags,omitempty"`
	RegistryName        string            `json:"RegistryName"`
	RegistryArn         string            `json:"RegistryArn"`
	SchemaName          string            `json:"SchemaName"`
	SchemaArn           string            `json:"SchemaArn"`
	DataFormat          string            `json:"DataFormat"`
	Compatibility       string            `json:"Compatibility"`
	SchemaStatus        string            `json:"SchemaStatus"`
	SchemaVersionID     string            `json:"SchemaVersionId,omitempty"`
	SchemaVersionStatus string            `json:"SchemaVersionStatus,omitempty"`
	LatestSchemaVersion int64             `json:"LatestSchemaVersion"`
	NextSchemaVersion   int64             `json:"NextSchemaVersion"`
	SchemaCheckpoint    int64             `json:"SchemaCheckpoint"`
}

func (h *Handler) handleCreateSchema(
	_ context.Context,
	in *createSchemaInput,
) (*createSchemaOutput, error) {
	registryName := ""
	if in.RegistryID != nil {
		registryName = in.RegistryID.RegistryName
	}

	s, sv, err := h.Backend.CreateSchema(
		registryName,
		in.SchemaName,
		in.DataFormat,
		in.Compatibility,
		in.Description,
		in.SchemaDefinition,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	out := &createSchemaOutput{
		RegistryName:        s.RegistryName,
		RegistryArn:         s.RegistryARN,
		SchemaName:          s.SchemaName,
		SchemaArn:           s.SchemaARN,
		DataFormat:          s.DataFormat,
		Compatibility:       s.Compatibility,
		SchemaStatus:        s.SchemaStatus,
		Tags:                s.Tags,
		LatestSchemaVersion: s.LatestSchemaVersion,
		NextSchemaVersion:   s.NextSchemaVersion,
		SchemaCheckpoint:    s.CheckpointVersion,
	}

	if sv != nil {
		out.SchemaVersionID = sv.SchemaVersionID
		out.SchemaVersionStatus = sv.Status
	}

	return out, nil
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

	reg, err := h.Backend.DeleteRegistry(name)
	if err != nil {
		return nil, err
	}

	return &deleteRegistryOutput{RegistryName: name, RegistryArn: reg.ARN, Status: stateDeleting}, nil
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

	s, err := h.Backend.DeleteSchema(registryName, schemaName)
	if err != nil {
		return nil, err
	}

	return &deleteSchemaOutput{SchemaName: schemaName, SchemaArn: s.SchemaARN, Status: stateDeleting}, nil
}

// deleteSchemaVersionsInput holds input for DeleteSchemaVersions.
type deleteSchemaVersionsInput struct {
	SchemaID *schemaIDInput `json:"SchemaId"`
	Versions string         `json:"Versions"`
}

// schemaVersionError is a per-version error in a DeleteSchemaVersions response.
type schemaVersionError struct {
	ErrorDetails  ErrorDetail `json:"ErrorDetails"`
	VersionNumber int64       `json:"VersionNumber"`
}

// deleteSchemaVersionsOutput holds the result for DeleteSchemaVersions.
type deleteSchemaVersionsOutput struct {
	SchemaVersionErrors []schemaVersionError `json:"SchemaVersionErrors"`
}

func (h *Handler) handleDeleteSchemaVersions(
	_ context.Context,
	in *deleteSchemaVersionsInput,
) (*deleteSchemaVersionsOutput, error) {
	registryName, schemaName := "", ""
	if in.SchemaID != nil {
		registryName = in.SchemaID.RegistryName
		schemaName = in.SchemaID.SchemaName
	}

	versions, err := parseVersionRanges(in.Versions)
	if err != nil {
		return nil, err
	}

	var errs []schemaVersionError

	for _, v := range versions {
		if delErr := h.Backend.DeleteSchemaVersion(registryName, schemaName, v); delErr != nil {
			errs = append(errs, schemaVersionError{
				VersionNumber: v,
				ErrorDetails: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: delErr.Error(),
				},
			})
		}
	}

	if errs == nil {
		errs = []schemaVersionError{}
	}

	return &deleteSchemaVersionsOutput{SchemaVersionErrors: errs}, nil
}

// getRegistryInput holds input for GetRegistry.
type getRegistryInput struct {
	RegistryID *registryIDInput `json:"RegistryId"`
}

// getRegistryOutput holds the result for GetRegistry. No Tags -- that member
// exists on CreateRegistryOutput (api_op_CreateRegistry.go) but not here
// (api_op_GetRegistry.go).
//
// CreatedTime/UpdatedTime are *string on the real type (glue@v1.152.0
// deserializers.go's GetRegistryOutput switch: "expected CreatedTimestamp to
// be of type string"), the same Schema Registry exception ListRegistries was
// fixed for.
type getRegistryOutput struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
	Description  string `json:"Description,omitempty"`
	Status       string `json:"Status"`
	CreatedTime  string `json:"CreatedTime,omitempty"`
	UpdatedTime  string `json:"UpdatedTime,omitempty"`
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
		CreatedTime:  formatGlueTimestampString(reg.CreatedTime),
		UpdatedTime:  formatGlueTimestampString(reg.UpdatedTime),
	}, nil
}

// getSchemaInput holds input for GetSchema.
type getSchemaInput struct {
	SchemaID *schemaIDInput `json:"SchemaId"`
}

// getSchemaOutput holds the result for GetSchema. LatestSchemaVersion,
// NextSchemaVersion and SchemaCheckpoint are real members
// (api_op_GetSchema.go) this handler previously dropped even though the
// backend's Schema model already tracks them (used by CreateSchema's output).
//
// CreatedTime/UpdatedTime are *string on the real type (glue@v1.152.0
// deserializers.go's GetSchemaOutput switch: "expected CreatedTimestamp to
// be of type string"), the same Schema Registry exception ListRegistries was
// fixed for.
type getSchemaOutput struct {
	RegistryName        string `json:"RegistryName"`
	RegistryArn         string `json:"RegistryArn"`
	SchemaName          string `json:"SchemaName"`
	SchemaArn           string `json:"SchemaArn"`
	DataFormat          string `json:"DataFormat"`
	Compatibility       string `json:"Compatibility"`
	SchemaStatus        string `json:"SchemaStatus"`
	Description         string `json:"Description,omitempty"`
	CreatedTime         string `json:"CreatedTime,omitempty"`
	UpdatedTime         string `json:"UpdatedTime,omitempty"`
	LatestSchemaVersion int64  `json:"LatestSchemaVersion"`
	NextSchemaVersion   int64  `json:"NextSchemaVersion"`
	SchemaCheckpoint    int64  `json:"SchemaCheckpoint"`
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
		RegistryName:        s.RegistryName,
		RegistryArn:         s.RegistryARN,
		SchemaName:          s.SchemaName,
		SchemaArn:           s.SchemaARN,
		DataFormat:          s.DataFormat,
		Compatibility:       s.Compatibility,
		SchemaStatus:        s.SchemaStatus,
		Description:         s.Description,
		CreatedTime:         formatGlueTimestampString(s.CreatedTime),
		UpdatedTime:         formatGlueTimestampString(s.UpdatedTime),
		LatestSchemaVersion: s.LatestSchemaVersion,
		NextSchemaVersion:   s.NextSchemaVersion,
		SchemaCheckpoint:    s.CheckpointVersion,
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

// getSchemaVersionOutput holds the result for GetSchemaVersion. CreatedTime
// is *string on the real type (glue@v1.152.0 deserializers.go's
// GetSchemaVersionOutput switch: "expected CreatedTimestamp to be of type
// string"), the same Schema Registry exception ListRegistries was fixed for.
type getSchemaVersionOutput struct {
	SchemaVersionID  string `json:"SchemaVersionId"`
	SchemaArn        string `json:"SchemaArn"`
	SchemaDefinition string `json:"SchemaDefinition,omitempty"`
	DataFormat       string `json:"DataFormat,omitempty"`
	Status           string `json:"Status"`
	CreatedTime      string `json:"CreatedTime,omitempty"`
	VersionNumber    int64  `json:"VersionNumber"`
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
		CreatedTime:      formatGlueTimestampString(sv.CreatedTime),
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

	diff, err := h.Backend.GetSchemaVersionsDiff(
		in.SchemaID.RegistryName,
		in.SchemaID.SchemaName,
		v1,
		v2,
	)
	if err != nil {
		return nil, err
	}

	return &getSchemaVersionsDiffOutput{Diff: diff}, nil
}

// defaultListRegistriesLimit is used when ListRegistriesInput.MaxResults is
// unset, matching the real API's documented default (api_op_ListRegistries.go:
// "If the value is not supplied, this will be defaulted to 25 per page.").
const defaultListRegistriesLimit = 25

// listRegistriesInput holds input for ListRegistries.
type listRegistriesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// registryListItem mirrors types.RegistryListItem: RegistryName, RegistryArn,
// Description, Status, CreatedTime, UpdatedTime. No Tags — that member exists
// only on CreateRegistryOutput, not here or on GetRegistryOutput.
//
// CreatedTime/UpdatedTime are *string on the real type (glue@v1.152.0
// types/types.go), not the usual unixTimestamp float -- Glue Schema
// Registry's timestamps are a documented exception to the rest of this
// service's wire shapes. A real client rejects a JSON number here
// ("expected CreatedTimestamp to be of type string"). GetRegistry/GetSchema/
// ListSchemas/ListSchemaVersions/GetSchemaVersion share this same fix
// (gopherstack-7f5k).
type registryListItem struct {
	RegistryName string `json:"RegistryName"`
	RegistryArn  string `json:"RegistryArn"`
	Description  string `json:"Description,omitempty"`
	Status       string `json:"Status"`
	CreatedTime  string `json:"CreatedTime,omitempty"`
	UpdatedTime  string `json:"UpdatedTime,omitempty"`
}

// formatGlueTimestampString renders an epoch-seconds float as the RFC3339
// string real Glue Schema Registry timestamp fields use on the wire.
func formatGlueTimestampString(epochSeconds float64) string {
	if epochSeconds == 0 {
		return ""
	}

	return time.Unix(int64(epochSeconds), 0).UTC().Format("2006-01-02T15:04:05Z")
}

// listRegistriesOutput holds the result for ListRegistries.
type listRegistriesOutput struct {
	NextToken  string              `json:"NextToken,omitempty"`
	Registries []*registryListItem `json:"Registries"`
}

func (h *Handler) handleListRegistries(
	_ context.Context,
	in *listRegistriesInput,
) (*listRegistriesOutput, error) {
	regs := h.Backend.ListRegistries()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListRegistriesLimit
	}

	page, next := paginateSlice(regs, in.NextToken, limit)

	items := make([]*registryListItem, 0, len(page))
	for _, r := range page {
		items = append(items, &registryListItem{
			RegistryName: r.Name,
			RegistryArn:  r.ARN,
			Description:  r.Description,
			Status:       r.Status,
			CreatedTime:  formatGlueTimestampString(r.CreatedTime),
			UpdatedTime:  formatGlueTimestampString(r.UpdatedTime),
		})
	}

	return &listRegistriesOutput{Registries: items, NextToken: next}, nil
}

// defaultListSchemaVersionsLimit is used when ListSchemaVersionsInput.MaxResults
// is unset, matching the real API's documented default (api_op_ListSchemaVersions.go:
// "If the value is not supplied, this will be defaulted to 25 per page.").
const defaultListSchemaVersionsLimit = 25

// listSchemaVersionsInput holds input for ListSchemaVersions.
type listSchemaVersionsInput struct {
	SchemaID   *schemaIDInput `json:"SchemaId"`
	NextToken  string         `json:"NextToken,omitempty"`
	MaxResults int32          `json:"MaxResults,omitempty"`
}

// schemaVersionListItem mirrors types.SchemaVersionListItem: SchemaVersionId,
// SchemaArn, Status, VersionNumber, CreatedTime. No SchemaDefinition or
// DataFormat — those live only on GetSchemaVersion's output.
//
// CreatedTime is *string on the real type (glue@v1.152.0 deserializers.go's
// SchemaVersionListItem switch: "expected CreatedTimestamp to be of type
// string"), the same Schema Registry exception ListRegistries was fixed for.
type schemaVersionListItem struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	Status          string `json:"Status"`
	CreatedTime     string `json:"CreatedTime,omitempty"`
	VersionNumber   int64  `json:"VersionNumber"`
}

// listSchemaVersionsOutput holds the result for ListSchemaVersions. The real
// field is Schemas, not SchemaVersions (api_op_ListSchemaVersions.go); the
// wrong key meant a real client silently decoded to an empty slice.
type listSchemaVersionsOutput struct {
	NextToken string                   `json:"NextToken,omitempty"`
	Schemas   []*schemaVersionListItem `json:"Schemas"`
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

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListSchemaVersionsLimit
	}

	page, next := paginateSlice(versions, in.NextToken, limit)

	items := make([]*schemaVersionListItem, 0, len(page))
	for _, v := range page {
		items = append(items, &schemaVersionListItem{
			SchemaVersionID: v.SchemaVersionID,
			SchemaArn:       v.SchemaARN,
			Status:          v.Status,
			VersionNumber:   v.VersionNumber,
			CreatedTime:     formatGlueTimestampString(v.CreatedTime),
		})
	}

	return &listSchemaVersionsOutput{Schemas: items, NextToken: next}, nil
}

// defaultListSchemasLimit is used when ListSchemasInput.MaxResults is unset,
// matching the real API's documented default (api_op_ListSchemas.go: "If the
// value is not supplied, this will be defaulted to 25 per page.").
const defaultListSchemasLimit = 25

// listSchemasInput holds input for ListSchemas.
type listSchemasInput struct {
	RegistryID *registryIDInput `json:"RegistryId"`
	NextToken  string           `json:"NextToken,omitempty"`
	MaxResults int32            `json:"MaxResults,omitempty"`
}

// schemaListItem mirrors types.SchemaListItem: SchemaName, SchemaArn,
// RegistryName, SchemaStatus, Description, CreatedTime, UpdatedTime. No
// RegistryArn, DataFormat, Compatibility, LatestSchemaVersion,
// NextSchemaVersion, CheckpointVersion or Tags — those live only on
// GetSchema/CreateSchema's output.
//
// CreatedTime/UpdatedTime are *string on the real type (glue@v1.152.0
// deserializers.go's SchemaListItem switch: "expected CreatedTimestamp to be
// of type string"), the same Schema Registry exception ListRegistries was
// fixed for.
type schemaListItem struct {
	SchemaName   string `json:"SchemaName"`
	SchemaArn    string `json:"SchemaArn"`
	RegistryName string `json:"RegistryName"`
	SchemaStatus string `json:"SchemaStatus"`
	Description  string `json:"Description,omitempty"`
	CreatedTime  string `json:"CreatedTime,omitempty"`
	UpdatedTime  string `json:"UpdatedTime,omitempty"`
}

// listSchemasOutput holds the result for ListSchemas.
type listSchemasOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Schemas   []*schemaListItem `json:"Schemas"`
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

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListSchemasLimit
	}

	page, next := paginateSlice(schemas, in.NextToken, limit)

	items := make([]*schemaListItem, 0, len(page))
	for _, s := range page {
		items = append(items, &schemaListItem{
			SchemaName:   s.SchemaName,
			SchemaArn:    s.SchemaARN,
			RegistryName: s.RegistryName,
			SchemaStatus: s.SchemaStatus,
			Description:  s.Description,
			CreatedTime:  formatGlueTimestampString(s.CreatedTime),
			UpdatedTime:  formatGlueTimestampString(s.UpdatedTime),
		})
	}

	return &listSchemasOutput{Schemas: items, NextToken: next}, nil
}

// putSchemaVersionMetadataInput holds input for PutSchemaVersionMetadata.
type putSchemaVersionMetadataInput struct {
	MetadataKeyValue *struct {
		MetadataKey   string `json:"MetadataKey"`
		MetadataValue string `json:"MetadataValue"`
	} `json:"MetadataKeyValue"`
	SchemaVersionID string `json:"SchemaVersionId"`
}

// putSchemaVersionMetadataOutput holds the result for
// PutSchemaVersionMetadata. Real PutSchemaVersionMetadataOutput
// (glue@v1.152.0 api_op_PutSchemaVersionMetadata.go) also carries
// MetadataKey/MetadataValue (trivially available from the request) and
// RegistryName/SchemaName (derived below via FindSchemaVersionByID) --
// previously dropped entirely.
type putSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	RegistryName    string `json:"RegistryName,omitempty"`
	SchemaName      string `json:"SchemaName,omitempty"`
	MetadataKey     string `json:"MetadataKey,omitempty"`
	MetadataValue   string `json:"MetadataValue,omitempty"`
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

	out := &putSchemaVersionMetadataOutput{
		SchemaVersionID: in.SchemaVersionID,
		MetadataKey:     key,
		MetadataValue:   value,
	}

	if sv, s, ok := h.Backend.FindSchemaVersionByID(in.SchemaVersionID); ok {
		out.SchemaArn = sv.SchemaARN
		out.VersionNumber = sv.VersionNumber
		out.LatestVersion = sv.VersionNumber == s.LatestSchemaVersion
		out.RegistryName = s.RegistryName
		out.SchemaName = s.SchemaName
	}

	return out, nil
}

// querySchemaVersionMetadataInput holds input for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataInput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
}

// querySchemaVersionMetadataOutput holds the result for QuerySchemaVersionMetadata.
type querySchemaVersionMetadataOutput struct {
	MetadataInfo    map[string]any `json:"MetadataInfoMap"`
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

// removeSchemaVersionMetadataOutput holds the result for
// RemoveSchemaVersionMetadata. Real RemoveSchemaVersionMetadataOutput
// (glue@v1.152.0 api_op_RemoveSchemaVersionMetadata.go) also carries
// MetadataKey/MetadataValue and RegistryName/SchemaName -- previously
// dropped entirely, same as PutSchemaVersionMetadata.
type removeSchemaVersionMetadataOutput struct {
	SchemaVersionID string `json:"SchemaVersionId"`
	SchemaArn       string `json:"SchemaArn"`
	RegistryName    string `json:"RegistryName,omitempty"`
	SchemaName      string `json:"SchemaName,omitempty"`
	MetadataKey     string `json:"MetadataKey,omitempty"`
	MetadataValue   string `json:"MetadataValue,omitempty"`
	VersionNumber   int64  `json:"VersionNumber"`
	LatestVersion   bool   `json:"LatestVersion"`
}

func (h *Handler) handleRemoveSchemaVersionMetadata(
	_ context.Context,
	in *removeSchemaVersionMetadataInput,
) (*removeSchemaVersionMetadataOutput, error) {
	key, value := "", ""
	if in.MetadataKeyValue != nil {
		key = in.MetadataKeyValue.MetadataKey
		value = in.MetadataKeyValue.MetadataValue
	}

	if err := h.Backend.RemoveSchemaVersionMetadata(in.SchemaVersionID, key); err != nil {
		return nil, err
	}

	out := &removeSchemaVersionMetadataOutput{
		SchemaVersionID: in.SchemaVersionID,
		MetadataKey:     key,
		MetadataValue:   value,
	}

	if sv, s, ok := h.Backend.FindSchemaVersionByID(in.SchemaVersionID); ok {
		out.SchemaArn = sv.SchemaARN
		out.VersionNumber = sv.VersionNumber
		out.LatestVersion = sv.VersionNumber == s.LatestSchemaVersion
		out.RegistryName = s.RegistryName
		out.SchemaName = s.SchemaName
	}

	return out, nil
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

	reg, err := h.Backend.UpdateRegistry(name, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateRegistryOutput{RegistryName: name, RegistryArn: reg.ARN}, nil
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

	s, err := h.Backend.UpdateSchema(registryName, schemaName, in.Compatibility, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateSchemaOutput{SchemaName: schemaName, RegistryName: registryName, SchemaArn: s.SchemaARN}, nil
}
