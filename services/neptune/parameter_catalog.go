package neptune

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// This file backs the "parameter value store" gap identified in PARITY.md:
// ModifyDBParameterGroup/ModifyDBClusterParameterGroup/
// ResetDBParameterGroup/ResetDBClusterParameterGroup used to validate that
// the named group existed and then silently discard every parameter change,
// which made DescribeDBParameters/DescribeDBClusterParameters (and
// DescribeEngineDefaultParameters/DescribeEngineDefaultClusterParameters)
// always return an empty list regardless of what a caller "set".
//
// Unlike the DB-instance/DB-cluster resource shapes, Neptune's exact default
// parameter *values* are server-side data with no SDK-side source of truth --
// but the parameter *catalog itself* (names, allowed values, apply type,
// data type, description, cluster-vs-instance scope) is fully documented:
// https://docs.aws.amazon.com/neptune/latest/userguide/parameters.html
// (fetched 2026-09-11). neptuneClusterParameterCatalog/
// neptuneInstanceParameterCatalog below transcribe that page's "Cluster-level
// parameters" and "Instance-level parameters" lists verbatim, including the
// one deprecated cluster parameter it still documents. Apply-type defaults
// come from the sibling page's blanket rule ("Currently all parameters are
// static except for ... neptune_enable_slow_query_log ...
// neptune_slow_query_log_threshold" and "Currently, all the ... instance-level
// parameters ... are static"):
// https://docs.aws.amazon.com/neptune/latest/userguide/parameter-groups.html
// (fetched 2026-09-11).
//
// The wire shape (Parameter's 10 members) is verified against
// neptune@v1.48.4 types/types.go:1320 and its deserializer,
// awsAwsquery_deserializeDocumentParameter (deserializers.go:20346) -- both
// list exactly AllowedValues/ApplyMethod/ApplyType/DataType/Description/
// IsModifiable/MinimumEngineVersion/ParameterName/ParameterValue/Source, all
// modeled here as EngineParameter.
//
// One documented dependency this backend does NOT model: neptune_lookup_cache
// defaults to "1" instead of "0" whenever an R5d instance is created in the
// cluster (per the same parameters.html page) -- this backend's parameter
// override store isn't keyed by instance class, so that default-flip is
// disclosed in PARITY.md rather than fabricated.

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
	allowedBoolean           = "0,1"
	rangeParts               = 2
)

// neptuneClusterParameterCatalog returns the documented Neptune DB
// cluster-parameter-group catalog (parameters.html "Cluster-level
// parameters", plus the one parameter still documented under "Deprecated
// parameters": neptune_enforce_ssl is cluster-level per its own heading).
func neptuneClusterParameterCatalog() []EngineParameter {
	return append(neptuneClusterParameterCatalogLogging(), neptuneClusterParameterCatalogFeatures()...)
}

// neptuneClusterParameterCatalogLogging holds the logging/timeout/streams
// half of the cluster catalog (split out of neptuneClusterParameterCatalog
// to stay under this repo's funlen limit -- decomposition, not a nolint).
func neptuneClusterParameterCatalogLogging() []EngineParameter {
	return []EngineParameter{
		{
			ParameterName:  "neptune_enable_audit_log",
			ParameterValue: "0",
			Description:    "This parameter toggles audit logging for Neptune.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  allowedBoolean,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_enable_slow_query_log",
			ParameterValue: "disabled",
			Description:    "Enables or disables Neptune's slow-query logging feature.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeDynamic,
			DataType:       paramDataTypeString,
			AllowedValues:  "info,debug,disabled",
			IsModifiable:   true,
		},
		{
			ParameterName: "neptune_slow_query_log_threshold",
			// Doc gives only the default (5000ms); no allowed-value
			// range is documented, so AllowedValues is left empty
			// rather than fabricated.
			ParameterValue: "5000",
			Description: "The execution time threshold, in milliseconds, after which a query " +
				"is considered a slow query.",
			Source:       parameterSourceEngine,
			ApplyType:    applyTypeDynamic,
			DataType:     paramDataTypeInteger,
			IsModifiable: true,
		},
		{
			ParameterName:  "neptune_lab_mode",
			ParameterValue: "",
			Description:    "Enables specific experimental (lab mode) Neptune features.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_query_timeout",
			ParameterValue: "120000",
			Description:    "Specifies a timeout duration for graph queries, in milliseconds, for the cluster.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeInteger,
			AllowedValues:  "10-2147483647",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_streams",
			ParameterValue: "0",
			Description:    "Enables or disables Neptune Streams.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  allowedBoolean,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_streams_expiry_days",
			ParameterValue: "7",
			Description:    "How many days elapse before the server deletes Neptune Streams records.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeInteger,
			AllowedValues:  "1-90",
			// "This parameter was introduced in engine version 1.2.0.0."
			MinimumEngineVersion: engineVersion1200,
			IsModifiable:         true,
		},
	}
}

// neptuneClusterParameterCatalogFeatures holds the feature-toggle/ML/legacy
// half of the cluster catalog (see neptuneClusterParameterCatalogLogging).
func neptuneClusterParameterCatalogFeatures() []EngineParameter {
	return []EngineParameter{
		{
			ParameterName:  "neptune_lookup_cache",
			ParameterValue: "0",
			Description: "Disables or re-enables the Neptune lookup cache on R5d instances " +
				"(auto-enabled by real Neptune when an R5d instance joins the cluster; not " +
				"modeled by this backend -- see file header).",
			Source:        parameterSourceEngine,
			ApplyType:     applyTypeStatic,
			DataType:      paramDataTypeBoolean,
			AllowedValues: allowedBoolean,
			IsModifiable:  true,
		},
		{
			ParameterName:  "neptune_autoscaling_config",
			ParameterValue: "",
			Description:    "JSON configuration for the read-replica instances Neptune auto-scaling creates and manages.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_ml_iam_role",
			ParameterValue: "",
			Description:    "The IAM role ARN Neptune ML uses for machine learning on graphs.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_ml_endpoint",
			ParameterValue: "",
			Description:    "The SageMaker AI endpoint name Neptune ML uses for machine learning on graphs.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeString,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_enable_inline_server_generated_edge_id",
			ParameterValue: "0",
			Description:    "Enables or disables the Neptune inline server-generated Edge ID feature.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  allowedBoolean,
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_enforce_ssl",
			ParameterValue: "1",
			Description: "(Deprecated) Used to force HTTPS-only connections; no longer relevant " +
				"since Neptune now accepts only HTTPS connections in every region.",
			Source:        parameterSourceEngine,
			ApplyType:     applyTypeStatic,
			DataType:      paramDataTypeBoolean,
			AllowedValues: allowedBoolean,
			IsModifiable:  true,
		},
	}
}

// neptuneInstanceParameterCatalog returns the documented Neptune
// DB-parameter-group (instance-level) catalog (parameters.html
// "Instance-level parameters"). Real Neptune shares the neptune_query_timeout
// name across both cluster and instance scope with an identical definition
// (parameters.html documents it twice, once per scope); this backend keeps
// the two catalogs independent so an instance-level override never leaks
// into a cluster-level Describe or vice versa.
func neptuneInstanceParameterCatalog() []EngineParameter {
	return []EngineParameter{
		{
			ParameterName:  "neptune_dfe_query_engine",
			ParameterValue: "viaQueryHint",
			Description:    "Controls how the DFE (degree-fused-encoding) query engine is used.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeString,
			AllowedValues:  "enabled,viaQueryHint",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_query_timeout",
			ParameterValue: "120000",
			Description:    "Specifies a timeout duration for graph queries, in milliseconds, for one instance.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeInteger,
			AllowedValues:  "10-2147483647",
			IsModifiable:   true,
		},
		{
			ParameterName:  "neptune_result_cache",
			ParameterValue: "0",
			Description:    "Enables or disables caching of query results.",
			Source:         parameterSourceEngine,
			ApplyType:      applyTypeStatic,
			DataType:       paramDataTypeBoolean,
			AllowedValues:  allowedBoolean,
			IsModifiable:   true,
		},
		{
			ParameterName:  "UndoLogPurgeConfig",
			ParameterValue: "default",
			Description:    "Enables or disables aggressive UndoLog purging in Neptune.",
			Source:         parameterSourceEngine,
			// "Currently, all the Neptune instance-level parameters ... are static"
			// (parameter-groups.html); this parameter's own entry doesn't repeat the
			// static/dynamic sentence, so the family-wide rule applies.
			ApplyType:     applyTypeStatic,
			DataType:      paramDataTypeString,
			AllowedValues: "default,aggressive",
			IsModifiable:  true,
		},
	}
}

// catalogIndex keys a parameter catalog by ParameterName for O(1) validation lookups.
func catalogIndex(catalog []EngineParameter) map[string]EngineParameter {
	idx := make(map[string]EngineParameter, len(catalog))
	for _, p := range catalog {
		idx[p.ParameterName] = p
	}

	return idx
}

func neptuneClusterParameterCatalogIndex() map[string]EngineParameter {
	return catalogIndex(neptuneClusterParameterCatalog())
}

func neptuneInstanceParameterCatalogIndex() map[string]EngineParameter {
	return catalogIndex(neptuneInstanceParameterCatalog())
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

// validateParameterValue rejects a value outside def's documented
// AllowedValues. AllowedValues is either a comma-separated enum (e.g.
// "0,1", "info,debug,disabled") or a numeric "min-max" range (e.g.
// "10-2147483647"); an empty AllowedValues (free-form parameters like
// neptune_ml_iam_role) accepts any value.
func validateParameterValue(def EngineParameter, value string) error {
	if def.AllowedValues == "" {
		return nil
	}
	if lo, hi, ok := parseAllowedRange(def.AllowedValues); ok {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < lo || n > hi {
			return fmt.Errorf(
				"%w: parameter %q value %q is not within the allowed range %s",
				ErrInvalidParameter, def.ParameterName, value, def.AllowedValues,
			)
		}

		return nil
	}
	if slices.Contains(strings.Split(def.AllowedValues, ","), value) {
		return nil
	}

	return fmt.Errorf(
		"%w: parameter %q value %q is not one of the allowed values %s",
		ErrInvalidParameter, def.ParameterName, value, def.AllowedValues,
	)
}

// parseAllowedRange parses an AllowedValues string of the form "min-max" into
// its numeric bounds. Returns ok=false for anything else (a comma-separated
// enum), so callers fall back to exact-match validation.
func parseAllowedRange(allowed string) (int64, int64, bool) {
	parts := strings.SplitN(allowed, "-", rangeParts)
	if len(parts) != rangeParts {
		return 0, 0, false
	}
	lo, errLo := strconv.ParseInt(parts[0], 10, 64)
	hi, errHi := strconv.ParseInt(parts[1], 10, 64)
	if errLo != nil || errHi != nil {
		return 0, 0, false
	}

	return lo, hi, true
}

// applyParameterInputs validates and applies a batch of parameter overrides
// against catalog into store (a per-group override map from
// parameter_groups.go/cluster_parameter_groups.go), mirroring real AWS's
// per-request cap of 20 modified parameters, its AllowedValues constraint,
// and its static-parameter/pending-reboot ApplyMethod compatibility rule.
// Callers must hold the backend write lock.
func applyParameterInputs(
	catalog map[string]EngineParameter, store map[string]ParameterValue, params []ParameterInput,
) error {
	if len(params) > maxModifiableParameters {
		return fmt.Errorf(
			"%w: a maximum of %d parameters can be modified in a single request",
			ErrInvalidParameter, maxModifiableParameters,
		)
	}
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
		if err := validateParameterValue(def, p.ParameterValue); err != nil {
			return err
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
func resetParameterInputs(
	catalog map[string]EngineParameter, store map[string]ParameterValue, resetAll bool, params []ParameterInput,
) error {
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

// describeParameters merges catalog with store's per-group overrides,
// producing the list DescribeDBParameters/DescribeDBClusterParameters
// render. Read-only; callers must hold at least a read lock.
func describeParameters(catalog []EngineParameter, store map[string]ParameterValue) []EngineParameter {
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
