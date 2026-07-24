package neptune

import "fmt"

// This file backs the "parameter value store" gap identified in PARITY.md:
// ModifyDBParameterGroup/ModifyDBClusterParameterGroup/
// ResetDBParameterGroup/ResetDBClusterParameterGroup used to validate that
// the named group existed and then silently discard every parameter change,
// which made DescribeDBParameters/DescribeDBClusterParameters (and
// DescribeEngineDefaultParameters/DescribeEngineDefaultClusterParameters)
// always return an empty list regardless of what a caller "set". AWS's exact
// default parameter catalog is server-side data, not part of the SDK, so
// neptuneParameterCatalog models a representative subset of real, documented
// Neptune engine parameters rather than an exhaustive mirror -- but every
// value a caller writes through Modify/Reset now genuinely persists and is
// genuinely reflected back by Describe, which is the behavior that was
// actually broken.

const (
	applyMethodImmediate     = "immediate"
	applyMethodPendingReboot = "pending-reboot"
	applyTypeStatic          = "static"
	applyTypeDynamic         = "dynamic"
	parameterSourceUser      = "user"
	parameterSourceEngine    = "engine-default"
	maxModifiableParameters  = 20
	paramDataTypeString      = "string"
	paramDataTypeInteger     = "integer"
	paramDataTypeBoolean     = "boolean"
)

// neptuneParameterCatalog returns the canonical set of Neptune engine
// parameters modeled by this backend. The same catalog backs both DB
// parameter groups (instance-level) and DB cluster parameter groups
// (cluster-level): real Neptune parameter names are shared across both scopes
// (e.g. neptune_query_timeout, neptune_streams), so a single catalog avoids
// duplicating near-identical data while still exercising real static/dynamic
// ApplyMethod validation and a non-modifiable system parameter.
func neptuneParameterCatalog() []EngineParameter {
	return []EngineParameter{
		{
			ParameterName:  "neptune_query_timeout",
			ParameterValue: "120000",
			Description:    "Sets the query timeout, in milliseconds.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeInteger,
			AllowedValues:  "0-2147483647",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_enable_audit_log",
			ParameterValue: "0",
			Description:    "Enables audit logging.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  "0,1",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_streams",
			ParameterValue: "0",
			Description:    "Enables the Neptune Streams change-data-capture feature.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  "0,1",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_result_cache",
			ParameterValue: "DISABLED",
			Description:    "Controls the Neptune query result cache.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeString,
			AllowedValues:  "ENABLED,DISABLED",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_dfe_query_engine",
			ParameterValue: "viaQueryHint",
			Description:    "Controls whether the degree-fused-encoding query engine is used.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeString,
			AllowedValues:  "disabled,viaQueryHint,enabled",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_ml_iam_role",
			ParameterValue: "",
			Description:    "The IAM role ARN Neptune ML uses to access SageMaker and S3.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_lab_mode",
			ParameterValue: "",
			Description:    "Enables lab/experimental features, as a comma-separated list.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:        "neptune_shard_hash_partitions",
			ParameterValue:       "6",
			Description:          "The number of hash partitions used internally by the storage layer.",
			Source:               parameterSourceEngine,
			ApplyType:            applyTypeStatic,
			DataType:             paramDataTypeInteger,
			AllowedValues:        "1-24",
			MinimumEngineVersion: engineVersion1200,
			IsModifiable:         false,
		},
	}
}

// neptuneParameterCatalogIndex returns neptuneParameterCatalog keyed by
// ParameterName for O(1) validation lookups.
func neptuneParameterCatalogIndex() map[string]EngineParameter {
	catalog := neptuneParameterCatalog()
	idx := make(map[string]EngineParameter, len(catalog))
	for _, p := range catalog {
		idx[p.ParameterName] = p
	}

	return idx
}

// validateApplyMethod rejects any ApplyMethod other than the two AWS defines.
func validateApplyMethod(m string) error {
	if m != applyMethodImmediate && m != applyMethodPendingReboot {
		return fmt.Errorf(
			"%w: ApplyMethod must be one of immediate, pending-reboot",
			ErrInvalidParameter,
		)
	}

	return nil
}

// applyParameterInputs validates and applies a batch of parameter overrides
// against the canonical Neptune parameter catalog into store (a per-group
// override map from parameter_groups.go/cluster_parameter_groups.go),
// mirroring real AWS's per-request cap of 20 modified parameters and its
// static-parameter/pending-reboot ApplyMethod compatibility rule. Callers
// must hold the backend write lock.
func applyParameterInputs(store map[string]ParameterValue, params []ParameterInput) error {
	if len(params) > maxModifiableParameters {
		return fmt.Errorf(
			"%w: a maximum of %d parameters can be modified in a single request",
			ErrInvalidParameter, maxModifiableParameters,
		)
	}
	catalog := neptuneParameterCatalogIndex()
	for _, p := range params {
		if p.ParameterName == "" {
			return fmt.Errorf("%w: ParameterName is required", ErrInvalidParameter)
		}
		def, ok := catalog[p.ParameterName]
		if !ok {
			return fmt.Errorf(
				"%w: parameter %q is not a recognized Neptune parameter",
				ErrInvalidParameter, p.ParameterName,
			)
		}
		if !def.IsModifiable {
			return fmt.Errorf("%w: parameter %q is not modifiable", ErrInvalidParameter, p.ParameterName)
		}
		if err := validateApplyMethod(p.ApplyMethod); err != nil {
			return err
		}
		if def.ApplyType == applyTypeStatic && p.ApplyMethod != applyMethodPendingReboot {
			return fmt.Errorf(
				"%w: static parameter %q requires ApplyMethod pending-reboot",
				ErrInvalidParameter, p.ParameterName,
			)
		}
		store[p.ParameterName] = ParameterValue{ParameterValue: p.ParameterValue, ApplyMethod: p.ApplyMethod}
	}

	return nil
}

// resetParameterInputs validates and removes overrides from store, reverting
// the affected parameters to their engine-default values. resetAll clears
// every override (ResetAllParameters=true); otherwise only the named
// parameters are reset. Callers must hold the backend write lock.
func resetParameterInputs(store map[string]ParameterValue, resetAll bool, params []ParameterInput) error {
	if resetAll {
		for k := range store {
			delete(store, k)
		}

		return nil
	}
	if len(params) > maxModifiableParameters {
		return fmt.Errorf(
			"%w: a maximum of %d parameters can be modified in a single request",
			ErrInvalidParameter, maxModifiableParameters,
		)
	}
	catalog := neptuneParameterCatalogIndex()
	for _, p := range params {
		if p.ParameterName == "" {
			return fmt.Errorf("%w: ParameterName is required", ErrInvalidParameter)
		}
		if _, ok := catalog[p.ParameterName]; !ok {
			return fmt.Errorf(
				"%w: parameter %q is not a recognized Neptune parameter",
				ErrInvalidParameter, p.ParameterName,
			)
		}
		delete(store, p.ParameterName)
	}

	return nil
}

// describeParameters merges the static catalog with store's per-group
// overrides, producing the list DescribeDBParameters/
// DescribeDBClusterParameters render. Read-only; callers must hold at least
// a read lock.
func describeParameters(store map[string]ParameterValue) []EngineParameter {
	catalog := neptuneParameterCatalog()
	result := make([]EngineParameter, 0, len(catalog))
	for _, def := range catalog {
		p := def
		if ov, ok := store[def.ParameterName]; ok {
			p.ParameterValue = ov.ParameterValue
			p.ApplyMethod = ov.ApplyMethod
			p.Source = parameterSourceUser
		}
		result = append(result, p)
	}

	return result
}
