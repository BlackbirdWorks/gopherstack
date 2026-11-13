package azurearm

import (
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	policyCodeMissingParam = "MissingPolicyParameter"
	policyCodeInUse        = "PolicyDefinitionInUse"
	policyCodeSetInUse     = "PolicySetDefinitionInUse"
	policyParamType        = "type"
	policyParamDefault     = "defaultValue"
	policyParamAllowed     = "allowedValues"
)

// checkAssignmentParameters validates assignment parameter values against the parameters the definition declares.
func checkAssignmentParameters(defID string, defs, given map[string]any) error {
	for name := range given {
		if _, ok := paramDef(defs, name); !ok {
			return authBad(
				"The policy assignment parameter '%s' is not defined in policy definition '%s'.",
				name,
				defID,
			)
		}
	}

	for _, name := range sortedKeys(defs) {
		def := mapOf(defs[name])
		val, provided := paramValue(given, name)

		if !provided {
			if _, hasDefault := def[policyParamDefault]; !hasDefault {
				return authErr(http.StatusBadRequest, policyCodeMissingParam,
					"The policy parameter '%s' of policy definition '%s' has no default value and must be provided "+
						"in the policy assignment.", name, defID)
			}

			continue
		}

		if err := checkParamValue(name, def, val); err != nil {
			return err
		}
	}

	return nil
}

func paramDef(defs map[string]any, name string) (map[string]any, bool) {
	for k, v := range defs {
		if strings.EqualFold(k, name) {
			return mapOf(v), true
		}
	}

	return nil, false
}

func paramValue(given map[string]any, name string) (any, bool) {
	for k, v := range given {
		if strings.EqualFold(k, name) {
			vm := mapOf(v)
			if vm == nil {
				return nil, false
			}

			val, ok := vm[fieldValue]

			return val, ok
		}
	}

	return nil, false
}

func checkParamValue(name string, def map[string]any, val any) error {
	typ := strOf(def, policyParamType)

	if !paramTypeMatches(typ, val) {
		return authBad("The policy parameter '%s' value is not of the declared type '%s'.", name, typ)
	}

	allowed, restricted := def[policyParamAllowed].([]any)
	if restricted && !slices.ContainsFunc(allowed, func(a any) bool { return reflect.DeepEqual(a, val) }) {
		return authBad("The policy parameter '%s' value is not one of the allowed values.", name)
	}

	return nil
}

func paramTypeMatches(typ string, val any) bool {
	switch strings.ToLower(typ) {
	case logicVarString:
		_, ok := val.(string)

		return ok
	case "array":
		_, ok := val.([]any)

		return ok
	case "object":
		_, ok := val.(map[string]any)

		return ok
	case "boolean":
		_, ok := val.(bool)

		return ok
	case "integer":
		f, ok := val.(float64)

		return ok && f == float64(int64(f))
	case "float":
		_, ok := val.(float64)

		return ok
	case "datetime":
		s, ok := val.(string)
		if !ok {
			return false
		}

		_, err := time.Parse(time.RFC3339, s)

		return err == nil
	}

	return true
}

// checkPolicyInUse refuses to delete a definition that an assignment or an initiative still references.
func (p *AuthorizationProvider) checkPolicyInUse(k authPolKind, id string) error {
	for _, a := range p.policyAssigns {
		if strings.EqualFold(strOf(a.Props, authFieldPolicyDefinitionID), id) {
			return inUseErr(k, id, "policy assignment '"+a.Name+"'")
		}
	}

	if k != authPolKindDef {
		return nil
	}

	for _, set := range p.storeFor(authPolKindSet) {
		for _, r := range anyList(set.Props["policyDefinitions"]) {
			if strings.EqualFold(strOf(mapOf(r), authFieldPolicyDefinitionID), id) {
				return inUseErr(k, id, "policy set definition '"+set.Name+"'")
			}
		}
	}

	return nil
}

func inUseErr(k authPolKind, id, by string) error {
	code := policyCodeInUse
	if k == authPolKindSet {
		code = policyCodeSetInUse
	}

	return authErr(http.StatusConflict, code, "The policy definition '%s' cannot be deleted because it is "+
		"referenced by %s.", id, by)
}
