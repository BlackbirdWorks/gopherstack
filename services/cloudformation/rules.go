package cloudformation

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// RuleParamSource resolves AWS-specific parameter types for Rules functions.
type RuleParamSource interface {
	// IDs returns every ID of paramType (e.g. AWS::EC2::VPC::Id) in the account and Region.
	IDs(paramType string) []string
	// Attribute returns the documented attribute (e.g. VpcId, Tags.Department) of one ID.
	Attribute(paramType, id, attr string) (string, bool)
}

const (
	paramTypeVPC    = "AWS::EC2::VPC::Id"
	paramTypeSubnet = "AWS::EC2::Subnet::Id"
	paramTypeSG     = "AWS::EC2::SecurityGroup::Id"
	tagAttrPrefix   = "Tags."
	ec2DefaultName  = "default"
)

type ec2RuleSource struct{ b ec2.Backend }

func newEC2RuleSource(b ec2.Backend) RuleParamSource {
	if b == nil {
		return nil
	}

	return ec2RuleSource{b: b}
}

func (s ec2RuleSource) IDs(paramType string) []string {
	var ids []string

	switch paramType {
	case paramTypeVPC:
		for _, v := range s.b.DescribeVpcs(nil) {
			ids = append(ids, v.ID)
		}
	case paramTypeSubnet:
		for _, v := range s.b.DescribeSubnets(nil) {
			ids = append(ids, v.ID)
		}
	case paramTypeSG:
		for _, v := range s.b.DescribeSecurityGroups(nil) {
			ids = append(ids, v.ID)
		}
	}

	slices.Sort(ids)

	return ids
}

func (s ec2RuleSource) Attribute(paramType, id, attr string) (string, bool) {
	if key, ok := strings.CutPrefix(attr, tagAttrPrefix); ok {
		v, found := s.b.TagsForResource(id)[key]

		return v, found
	}

	switch paramType {
	case paramTypeVPC:
		return s.vpcAttribute(id, attr)
	case paramTypeSubnet:
		for _, sn := range s.b.DescribeSubnets([]string{id}) {
			switch attr {
			case "AvailabilityZone":
				return sn.AvailabilityZone, true
			case "VpcId":
				return sn.VPCID, true
			}
		}
	}

	return "", false
}

func (s ec2RuleSource) vpcAttribute(vpcID, attr string) (string, bool) {
	switch attr {
	case "DefaultSecurityGroup":
		for _, sg := range s.b.DescribeSecurityGroups(nil) {
			if sg.VPCID == vpcID && sg.Name == ec2DefaultName {
				return sg.ID, true
			}
		}
	case "DefaultNetworkAcl":
		for _, acl := range s.b.DescribeNetworkAclsFiltered([]string{vpcID}) {
			if acl.IsDefault {
				return acl.ID, true
			}
		}
	}

	return "", false
}

// ruleEnv is what a Rules expression is evaluated against.
type ruleEnv struct {
	tmpl     *Template
	resolved map[string]string
	src      RuleParamSource
}

// evaluateRules checks Rules assertions against the resolved parameter values.
func evaluateRules(tmpl *Template, resolved map[string]string, src RuleParamSource) error {
	env := &ruleEnv{tmpl: tmpl, resolved: resolved, src: src}

	for _, name := range sortedKeys(tmpl.Rules) {
		rule, _ := tmpl.Rules[name].(map[string]any)

		if cond, ok := rule["RuleCondition"]; ok && !env.eval(cond) {
			continue
		}

		asserts, _ := rule["Assertions"].([]any)
		for _, a := range asserts {
			am, _ := a.(map[string]any)

			if env.eval(am["Assert"]) {
				continue
			}

			desc, _ := am["AssertDescription"].(string)

			return awsErrorf(ErrRuleAssertion, "Rule '%s' assertion failed: %s", name, desc)
		}
	}

	return nil
}

func isListParamType(t string) bool {
	return t == "CommaDelimitedList" || strings.HasPrefix(t, "List<")
}

func baseParamType(t string) string {
	if inner, ok := strings.CutPrefix(t, "List<"); ok {
		return strings.TrimSuffix(inner, ">")
	}

	return t
}

func splitList(raw string) []string {
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	return parts
}

func (e *ruleEnv) refValues(name string) []string {
	raw := e.resolved[name]
	if isListParamType(e.tmpl.Parameters[name].Type) {
		return splitList(raw)
	}

	return []string{raw}
}

func (e *ruleEnv) allIDs(paramType string) []string {
	if e.src == nil {
		return nil
	}

	return e.src.IDs(paramType)
}

func (e *ruleEnv) attrOf(paramType string, ids []string, attr string) []string {
	if e.src == nil {
		return nil
	}

	var out []string

	for _, id := range ids {
		if v, ok := e.src.Attribute(paramType, id, attr); ok {
			out = append(out, v)
		}
	}

	return out
}

func twoStrings(v any) (string, string, bool) {
	list, ok := v.([]any)
	if !ok || len(list) != ruleBinary {
		return "", "", false
	}

	a, aok := list[0].(string)
	b, bok := list[1].(string)

	return a, b, aok && bok
}

// values flattens a rule operand (literal, Ref, Fn::RefAll, Fn::ValueOf,
// Fn::ValueOfAll, or a list of those) into its string members.
func (e *ruleEnv) values(v any) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []any:
		var out []string
		for _, item := range val {
			out = append(out, e.values(item)...)
		}

		return out
	case map[string]any:
		return e.fnValues(val)
	}

	return []string{fmt.Sprint(v)}
}

func (e *ruleEnv) fnValues(m map[string]any) []string {
	if ref, ok := m["Ref"].(string); ok {
		return e.refValues(ref)
	}

	if t, ok := m["Fn::RefAll"].(string); ok {
		return e.allIDs(t)
	}

	if param, attr, ok := twoStrings(m["Fn::ValueOf"]); ok {
		p := e.tmpl.Parameters[param]

		return e.attrOf(baseParamType(p.Type), e.refValues(param), attr)
	}

	if t, attr, ok := twoStrings(m["Fn::ValueOfAll"]); ok {
		return e.attrOf(t, e.allIDs(t), attr)
	}

	return []string{fmt.Sprint(m)}
}

func (e *ruleEnv) scalar(v any) string { return strings.Join(e.values(v), ",") }

// eval evaluates a rule condition; an unsupported function counts as
// satisfied rather than guessed.
func (e *ruleEnv) eval(expr any) bool {
	m, ok := expr.(map[string]any)
	if !ok {
		return true
	}

	for _, fn := range ruleFns() {
		if args, is := m[fn.name].([]any); is && fn.arity(len(args)) {
			return fn.eval(e, args)
		}
	}

	return true
}

const (
	ruleUnary  = 1
	ruleBinary = 2
)

type ruleFn struct {
	arity func(n int) bool
	eval  func(e *ruleEnv, args []any) bool
	name  string
}

func arityIs(want int) func(int) bool { return func(n int) bool { return n == want } }

func arityAny(int) bool { return true }

func ruleFns() []ruleFn {
	return []ruleFn{
		{name: "Fn::Equals", arity: arityIs(ruleBinary), eval: ruleEquals},
		{name: "Fn::Contains", arity: arityIs(ruleBinary), eval: ruleContains},
		{name: "Fn::EachMemberEquals", arity: arityIs(ruleBinary), eval: ruleEachMemberEquals},
		{name: "Fn::EachMemberIn", arity: arityIs(ruleBinary), eval: ruleEachMemberIn},
		{name: "Fn::And", arity: arityAny, eval: ruleAnd},
		{name: "Fn::Or", arity: arityAny, eval: ruleOr},
		{name: "Fn::Not", arity: arityIs(ruleUnary), eval: ruleNot},
	}
}

func ruleEquals(e *ruleEnv, args []any) bool {
	return e.scalar(args[0]) == e.scalar(args[1])
}

func ruleContains(e *ruleEnv, args []any) bool {
	return slices.Contains(e.values(args[0]), e.scalar(args[1]))
}

func ruleEachMemberEquals(e *ruleEnv, args []any) bool {
	want := e.scalar(args[1])

	return !slices.ContainsFunc(e.values(args[0]), func(s string) bool { return s != want })
}

func ruleEachMemberIn(e *ruleEnv, args []any) bool {
	allowed := e.values(args[1])

	return !slices.ContainsFunc(e.values(args[0]), func(s string) bool { return !slices.Contains(allowed, s) })
}

func ruleAnd(e *ruleEnv, args []any) bool {
	return !slices.ContainsFunc(args, func(a any) bool { return !e.eval(a) })
}

func ruleOr(e *ruleEnv, args []any) bool {
	return slices.ContainsFunc(args, func(a any) bool { return e.eval(a) })
}

func ruleNot(e *ruleEnv, args []any) bool {
	return !e.eval(args[0])
}

func (b *InMemoryBackend) ruleSource() RuleParamSource {
	if b.creator == nil || b.creator.backends == nil || b.creator.backends.EC2 == nil {
		return nil
	}

	return newEC2RuleSource(b.creator.backends.EC2.Backend)
}
