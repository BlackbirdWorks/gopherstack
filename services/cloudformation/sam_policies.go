package cloudformation

import (
	_ "embed"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// The policy templates and the AWS managed policy name table are the SAM translator's own data
// (aws/serverless-application-model, samtranslator/policy_templates_data/policy_templates.json and
// samtranslator/internal/data/aws_managed_policies.json), Apache-2.0, Copyright Amazon.com, Inc. or its
// affiliates. The managed table keeps the "aws" partition, each name mapped to its path after
// "arn:aws:iam::aws:policy/".

//go:embed sam_policy_templates.json
var samPolicyTemplatesJSON []byte

//go:embed sam_managed_policies.json
var samManagedPoliciesJSON []byte

const (
	samPolicyParamPrefix = "___SAM_POLICY_PARAMETER_"
	samManagedARNPrefix  = "arn:${AWS::Partition}:iam::aws:policy/"
)

type samPolicyTemplate struct {
	Definition map[string]any            `json:"Definition"`
	Parameters map[string]map[string]any `json:"Parameters"`
}

//nolint:gochecknoglobals // lazily decoded embedded tables
var (
	samPolicyTemplates = sync.OnceValue(func() map[string]samPolicyTemplate {
		var out map[string]samPolicyTemplate
		if err := json.Unmarshal(samPolicyTemplatesJSON, &out, jsonx.V1()); err != nil {
			panic("cloudformation: invalid embedded sam_policy_templates.json: " + err.Error())
		}

		return out
	})
	samManagedPolicies = sync.OnceValue(func() map[string]string {
		var out map[string]string
		if err := json.Unmarshal(samManagedPoliciesJSON, &out, jsonx.V1()); err != nil {
			panic("cloudformation: invalid embedded sam_managed_policies.json: " + err.Error())
		}

		return out
	})
)

// samManagedPolicyARN resolves an AWS managed policy name to its ARN; an ARN or unknown name is kept as is.
func samManagedPolicyARN(name string) string {
	if path, ok := samManagedPolicies()[name]; ok {
		return samManagedARNPrefix + path
	}

	return name
}

// samRolePolicies is what a generated execution role carries beyond its fixed managed policies.
type samRolePolicies struct {
	managed []any
	inline  []any
}

func (r *samRolePolicies) addManaged(policy any) {
	if !slices.ContainsFunc(r.managed, func(p any) bool { return sameJSON(p, policy) }) {
		r.managed = append(r.managed, policy)
	}
}

// addInline appends a named policy unless one with the same document is already present.
func (r *samRolePolicies) addInline(policy map[string]any) {
	if policy == nil {
		return
	}
	doc := policy[samKeyPolicyDoc]
	if !slices.ContainsFunc(r.inline, func(p any) bool { return sameJSON(asMap(p)[samKeyPolicyDoc], doc) }) {
		r.inline = append(r.inline, policy)
	}
}

type samPolicyKind int

const (
	samPolicyUnknown samPolicyKind = iota
	samPolicyManaged
	samPolicyStatement
	samPolicyTemplateKind
)

// policyKind classifies one Policies entry like SAM's ResourcePolicies; an Fn::If takes the kind of its branches.
func policyKind(policy any) (samPolicyKind, error) {
	switch p := policy.(type) {
	case string:
		return samPolicyManaged, nil
	case map[string]any:
		if cond, ok := p[fnIf].([]any); ok && len(p) == 1 {
			return ifPolicyKind(cond)
		}
		if isSAMIntrinsic(p) {
			return samPolicyManaged, nil
		}
		if _, has := p[samKeyStatement]; has {
			return samPolicyStatement, nil
		}
		if len(p) == 1 {
			for name := range p {
				if _, known := samPolicyTemplates()[name]; known {
					return samPolicyTemplateKind, nil
				}
			}
		}
	}

	return samPolicyUnknown, nil
}

func ifPolicyKind(cond []any) (samPolicyKind, error) {
	const ifItems = 3
	if len(cond) != ifItems {
		return samPolicyUnknown, fmt.Errorf("%w: Fn::If requires 3 arguments", ErrSAMTransform)
	}
	yes, no := cond[1], cond[2]
	yesKind, err := policyKind(yes)
	if err != nil {
		return samPolicyUnknown, err
	}
	noKind, err := policyKind(no)
	if err != nil {
		return samPolicyUnknown, err
	}
	switch {
	case yesKind == noKind:
		return yesKind, nil
	case isNoValue(yes):
		return noKind, nil
	case isNoValue(no):
		return yesKind, nil
	}

	return samPolicyUnknown, fmt.Errorf("%w: Different policy types within the same Fn::If statement is "+
		"unsupported. Separate different policy types into different Fn::If statements", ErrSAMTransform)
}

// applyPolicies adds a resource's Policies property to role the way SAM's construct_role_for_resource does.
func (t *samTranslator) applyPolicies(resID, roleID string, policies any, role *samRolePolicies) error {
	for i, policy := range asList(policies) {
		kind, err := policyKind(policy)
		if err != nil {
			return samErr(resID, "%s", strings.TrimPrefix(err.Error(), ErrSAMTransform.Error()+": "))
		}
		if kind == samPolicyTemplateKind || isIfOf(policy, samPolicyTemplateKind) {
			if policy, err = convertPolicyTemplates(resID, policy); err != nil {
				return err
			}
			kind = samPolicyStatement
		}
		switch kind {
		case samPolicyStatement:
			role.addInline(namedPolicy(roleID, i, policy))
		case samPolicyManaged:
			role.addManaged(managedPolicyValue(policy))
		case samPolicyUnknown, samPolicyTemplateKind:
			return samErr(resID, "Policy at index %d in the 'Policies' property is not valid", i)
		}
	}

	return nil
}

func isIfOf(policy any, want samPolicyKind) bool {
	m, ok := policy.(map[string]any)
	if !ok {
		return false
	}
	cond, isIf := m[fnIf].([]any)
	if !isIf || len(m) != 1 {
		return false
	}
	kind, err := ifPolicyKind(cond)

	return err == nil && kind == want
}

// namedPolicy wraps a policy document as <Role>Policy<index>; an Fn::If wraps each real branch.
func namedPolicy(roleID string, index int, policy any) map[string]any {
	name := fmt.Sprintf("%sPolicy%d", roleID, index)
	if m, ok := policy.(map[string]any); ok {
		if cond, isIf := m[fnIf].([]any); isIf && len(m) == 1 {
			branches := make([]any, len(cond))
			copy(branches, cond)
			for _, b := range []int{1, 2} {
				if !isNoValue(branches[b]) {
					branches[b] = map[string]any{samKeyPolicyName: name, samKeyPolicyDoc: branches[b]}
				}
			}

			return map[string]any{fnIf: branches}
		}
	}

	return map[string]any{samKeyPolicyName: name, samKeyPolicyDoc: policy}
}

// managedPolicyValue maps managed policy names to ARNs, also inside an Fn::If.
func managedPolicyValue(policy any) any {
	switch p := policy.(type) {
	case string:
		return samManagedPolicyARN(p)
	case map[string]any:
		if cond, ok := p[fnIf].([]any); ok && len(p) == 1 && len(cond) == 3 {
			out := slices.Clone(cond)
			for _, b := range []int{1, 2} {
				if s, isStr := out[b].(string); isStr {
					out[b] = samManagedPolicyARN(s)
				}
			}

			return map[string]any{fnIf: out}
		}
	}

	return policy
}

// convertPolicyTemplates expands a policy template, or each template branch of an Fn::If.
func convertPolicyTemplates(resID string, policy any) (any, error) {
	m := asMap(policy)
	if cond, ok := m[fnIf].([]any); ok && len(m) == 1 {
		out := slices.Clone(cond)
		for _, b := range []int{1, 2} {
			if isNoValue(out[b]) {
				continue
			}
			conv, err := convertPolicyTemplates(resID, out[b])
			if err != nil {
				return nil, err
			}
			out[b] = conv
		}

		return map[string]any{fnIf: out}, nil
	}
	for name, params := range m {
		return expandPolicyTemplate(resID, name, params)
	}

	return policy, nil
}

func expandPolicyTemplate(resID, name string, params any) (any, error) {
	tmpl := samPolicyTemplates()[name]
	values, ok := params.(map[string]any)
	if !ok {
		return nil, samErr(resID, "Must specify valid parameter values for policy template '%s'", name)
	}
	var missing []string
	for p := range tmpl.Parameters {
		if _, has := values[p]; !has {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)

		return nil, samErr(resID, "Following required parameters of template '%s' don't have values: %v",
			name, missing)
	}
	needed := map[string]any{}
	for p := range tmpl.Parameters {
		needed[p] = values[p]
	}

	return substitutePolicyParams(tmpl.Definition, needed), nil
}

// substitutePolicyParams replaces {"Ref": <template parameter>} nodes by the supplied value; nothing else
// is resolved, so a caller's own Ref of the same name is left alone.
func substitutePolicyParams(node any, values map[string]any) any {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n[samKeyRef].(string); ok && len(n) == 1 {
			if v, found := values[ref]; found {
				return v
			}
		}
		out := make(map[string]any, len(n))
		for k, v := range n {
			out[k] = substitutePolicyParams(v, values)
		}

		return out
	case []any:
		out := make([]any, len(n))
		for i, v := range n {
			out[i] = substitutePolicyParams(v, values)
		}

		return out
	}

	return node
}

var _ = maps.Clone[map[string]any]
