package forecast

import (
	"fmt"
	"regexp"
)

// This file implements two real Amazon Forecast validation behaviors that
// were previously missing entirely (the emulator accepted any string):
//
//  1. Enum-field validation on Create* requests. Real Amazon Forecast
//     rejects an unrecognized Domain/DatasetType/ImportMode with
//     InvalidInputException; the client-side smithy validators
//     (aws-sdk-go-v2/service/forecast/validators.go) additionally require
//     Domain and DatasetType to be present at all.
//  2. Cross-resource FK existence validation on Create* requests that carry
//     an ARN reference to another Forecast resource (e.g. CreateForecast's
//     PredictorArn, CreateDatasetImportJob's DatasetArn). Real Amazon
//     Forecast returns ResourceNotFoundException for a dangling reference;
//     the emulator previously stored the reference verbatim without ever
//     resolving it.
//
// Scope note: only the *top-level* required ARN-reference fields named in
// the Amazon Forecast API models are validated here (see fkFieldSpec table
// below). Nested config blocks that also carry a resource reference --
// CreatePredictorInput.InputDataConfig.DatasetGroupArn and
// CreateAutoPredictorInput.DataConfig.DatasetGroupArn -- are intentionally
// out of scope for this pass (see PARITY.md); flagged there as a residual
// item rather than silently ignored.

// validDomains enumerates types.Domain (aws-sdk-go-v2/service/forecast/types/enums.go).
//
//nolint:gochecknoglobals // static enum lookup table, mirrors validImportModes/validDatasetTypes below
var validDomains = map[string]bool{
	"RETAIL":             true,
	"CUSTOM":             true,
	"INVENTORY_PLANNING": true,
	"EC2_CAPACITY":       true,
	"WORK_FORCE":         true,
	"WEB_TRAFFIC":        true,
	"METRICS":            true,
}

// validDatasetTypes enumerates types.DatasetType.
//
//nolint:gochecknoglobals // static enum lookup table
var validDatasetTypes = map[string]bool{
	"TARGET_TIME_SERIES":  true,
	"RELATED_TIME_SERIES": true,
	"ITEM_METADATA":       true,
}

// validImportModes enumerates types.ImportMode.
//
//nolint:gochecknoglobals // static enum lookup table
var validImportModes = map[string]bool{
	"FULL":        true,
	"INCREMENTAL": true,
}

// dataFrequencyPattern matches Amazon Forecast's documented DataFrequency
// format: an optional 1-2 digit interval (no leading zero) followed by one
// of Y/M/W/D/H/min (CreateDatasetInput.DataFrequency's doc comment: "Valid
// intervals are an integer followed by Y (Year), M (Month), W (Week), D
// (Day), H (Hour), and min (Minute)."). Unlike Domain/DatasetType/ImportMode,
// DataFrequency has no corresponding types.X enum in aws-sdk-go-v2/service/
// forecast/types -- it's server-validated free text, not client-side
// smithy-validated -- so this is a format check, not an enum-membership
// check. The interval is optional (matching the widely-used bare-letter form
// "D"/"W"/... AWS's own examples use) even though the doc text technically
// always shows one.
var dataFrequencyPattern = regexp.MustCompile(`^(?:[1-9][0-9]?)?(?:Y|M|W|D|H|min)$`)

// validateDataFrequencyField validates the optional DataFrequency field's
// format when present; an absent field is not an error (it is only
// documented as required for RELATED_TIME_SERIES datasets, and even then
// only in prose, not a smithy-enforced constraint).
func validateDataFrequencyField(data map[string]any) error {
	value := stringValue(data["DataFrequency"])
	if value == "" {
		return nil
	}

	if !dataFrequencyPattern.MatchString(value) {
		return fmt.Errorf(
			"%w: DataFrequency %q must be an optional interval followed by Y, M, W, D, H, or min",
			ErrValidation, value,
		)
	}

	return nil
}

// fkFieldSpec declares one Create* request's required ARN-reference field:
// the top-level input field name, and which resource kind(s) it must
// resolve to via the backend's arnIndex. targetKinds has more than one
// element only for CreateExplainability's ResourceArn, which real Amazon
// Forecast accepts as either a predictor ARN or a forecast ARN.
type fkFieldSpec struct {
	field       string
	targetKinds []resourceKind
	isList      bool
}

// createFKSpecs maps each resource kind's Create* operation to the FK field
// it must validate, built directly from the "This member is required" ARN
// fields in aws-sdk-go-v2/service/forecast's validators.go. kindDatasetGroup,
// kindDataset, and kindPredictor are deliberately absent: DatasetGroup has no
// required ARN-reference field, Dataset has none either (Domain/DatasetType
// are enums, not FKs), and Predictor's DatasetGroupArn reference is nested
// (see scope note above).
//
//nolint:gochecknoglobals,exhaustive // static declarative table, absent kinds documented above
var createFKSpecs = map[resourceKind]fkFieldSpec{
	kindDatasetImportJob:        {field: "DatasetArn", targetKinds: []resourceKind{kindDataset}},
	kindPredictorBacktestExport: {field: fieldPredictorArn, targetKinds: []resourceKind{kindPredictor}},
	kindForecast:                {field: fieldPredictorArn, targetKinds: []resourceKind{kindPredictor}},
	kindForecastExport:          {field: "ForecastArn", targetKinds: []resourceKind{kindForecast}},
	kindExplainabilityExport:    {field: "ExplainabilityArn", targetKinds: []resourceKind{kindExplainability}},
	kindExplainability:          {field: "ResourceArn", targetKinds: []resourceKind{kindPredictor, kindForecast}},
	kindMonitor:                 {field: "ResourceArn", targetKinds: []resourceKind{kindPredictor}},
	kindWhatIfAnalysis:          {field: "ForecastArn", targetKinds: []resourceKind{kindForecast}},
	kindWhatIfForecast:          {field: "WhatIfAnalysisArn", targetKinds: []resourceKind{kindWhatIfAnalysis}},
	kindWhatIfForecastExport: {
		field: "WhatIfForecastArns", targetKinds: []resourceKind{kindWhatIfForecast}, isList: true,
	},
}

// validateCreateFieldsLocked validates enum fields and FK references on a
// Create* request. It must be called with b.mu already held (for write, from
// within create): FK resolution reads b.arnIndex/b.resources via
// lookupLocked, which assumes the caller holds the lock.
func (b *InMemoryBackend) validateCreateFieldsLocked(kind resourceKind, data map[string]any) error {
	if err := validateEnumFields(kind, data); err != nil {
		return err
	}

	spec, ok := createFKSpecs[kind]
	if !ok {
		return nil
	}

	return b.validateFKRefLocked(spec, data)
}

func validateEnumFields(kind resourceKind, data map[string]any) error {
	switch kind {
	case kindDatasetGroup:
		return validateEnumField("Domain", data, validDomains)
	case kindDataset:
		if err := validateEnumField("Domain", data, validDomains); err != nil {
			return err
		}
		if err := validateEnumField("DatasetType", data, validDatasetTypes); err != nil {
			return err
		}
		if data["Schema"] == nil {
			return fmt.Errorf("%w: Schema is required", ErrValidation)
		}

		return validateDataFrequencyField(data)
	case kindDatasetImportJob:
		return validateOptionalEnumField("ImportMode", data, validImportModes)
	default:
		return nil
	}
}

// validateEnumField requires field to be present in data and to be one of
// allowed's keys.
func validateEnumField(field string, data map[string]any, allowed map[string]bool) error {
	value := stringValue(data[field])
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	}

	if !allowed[value] {
		return fmt.Errorf("%w: %s %q is not a recognized Amazon Forecast value", ErrValidation, field, value)
	}

	return nil
}

// validateOptionalEnumField validates field against allowed only when
// present; an absent field is not an error (matches ImportMode, which
// defaults to FULL when omitted).
func validateOptionalEnumField(field string, data map[string]any, allowed map[string]bool) error {
	value := stringValue(data[field])
	if value == "" {
		return nil
	}

	if !allowed[value] {
		return fmt.Errorf("%w: %s %q is not a recognized Amazon Forecast value", ErrValidation, field, value)
	}

	return nil
}

// validateFKRefLocked validates that spec.field on data resolves to an
// existing resource of one of spec.targetKinds. Must be called with b.mu
// held (see validateCreateFieldsLocked).
func (b *InMemoryBackend) validateFKRefLocked(spec fkFieldSpec, data map[string]any) error {
	if spec.isList {
		return b.validateFKListLocked(spec, data)
	}

	value := stringValue(data[spec.field])
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, spec.field)
	}

	if !b.fkExistsLocked(spec.targetKinds, value) {
		return fmt.Errorf("%w: %s %q", ErrNotFound, spec.field, value)
	}

	return nil
}

func (b *InMemoryBackend) validateFKListLocked(spec fkFieldSpec, data map[string]any) error {
	raw, ok := data[spec.field].([]any)
	if !ok || len(raw) == 0 {
		return fmt.Errorf("%w: %s is required", ErrValidation, spec.field)
	}

	for _, entry := range raw {
		value, isStr := entry.(string)
		if !isStr || value == "" {
			return fmt.Errorf("%w: %s entries must be non-empty strings", ErrValidation, spec.field)
		}

		if !b.fkExistsLocked(spec.targetKinds, value) {
			return fmt.Errorf("%w: %s %q", ErrNotFound, spec.field, value)
		}
	}

	return nil
}

// fkExistsLocked reports whether value identifies (by name or ARN, matching
// the rest of the backend's identifier semantics) an existing resource of
// any of the given kinds.
func (b *InMemoryBackend) fkExistsLocked(kinds []resourceKind, value string) bool {
	for _, kind := range kinds {
		if _, ok := b.lookupLocked(kind, value); ok {
			return true
		}
	}

	return false
}

// deletableStatuses lists Amazon Forecast's documented per-kind Delete*
// preconditions (e.g. DeletePredictor: "You can delete only predictor that
// have a status of ACTIVE or CREATE_FAILED", DeleteMonitor: "ACTIVE,
// ACTIVE_STOPPED, CREATE_FAILED, or CREATE_STOPPED"). A kind absent from
// this map has no documented status precondition (PredictorBacktestExportJob
// and ExplainabilityExport) and is always deletable. Every kind present here
// allows the emulator's own STOPPED convention (see UpdateResourceStatus) in
// addition to ACTIVE/CREATE_FAILED, since stopping a resource is what makes
// it eligible for deletion under real AWS's ACTIVE_STOPPED/CREATE_STOPPED
// states.
//
//nolint:gochecknoglobals,exhaustive // static declarative table, absent kinds documented above
var deletableStatuses = map[resourceKind]map[string]bool{
	kindDatasetGroup:         {statusActive: true, statusCreateFailed: true, "UPDATE_FAILED": true},
	kindDataset:              {statusActive: true, statusCreateFailed: true},
	kindDatasetImportJob:     {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindPredictor:            {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindForecast:             {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindForecastExport:       {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindExplainability:       {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindWhatIfAnalysis:       {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindWhatIfForecast:       {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindWhatIfForecastExport: {statusActive: true, statusCreateFailed: true, statusStopped: true},
	kindMonitor:              {statusActive: true, statusCreateFailed: true, statusStopped: true},
}

// validateDeletableLocked returns ErrResourceInUse when resource's kind has a
// documented Delete* status precondition and its current status does not
// satisfy it (e.g. still CREATE_PENDING, the emulator's stand-in for
// CREATE_IN_PROGRESS). Must be called with b.mu held.
func validateDeletableLocked(resource *Resource) error {
	allowed, ok := deletableStatuses[resource.Kind]
	if !ok {
		return nil
	}

	if allowed[resource.Status] {
		return nil
	}

	return fmt.Errorf(
		"%w: %s %q has status %s and cannot be deleted yet",
		ErrResourceInUse, resource.Kind, resource.Name, resource.Status,
	)
}
