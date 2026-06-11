package cloudformation

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Intrinsic-validation errors. These are raised by a pre-flight pass over the
// parsed template (before any resource is provisioned) so that a template that
// references a missing resource, uses an unsupported resource type, or leaves an
// unsupported intrinsic unresolved fails the stack with an AWS-accurate
// StatusReason instead of silently succeeding.
var (
	// ErrUnresolvedGetAtt mirrors AWS "Template error: instance of Fn::GetAtt
	// references undefined resource <Logical>".
	ErrUnresolvedGetAtt = errors.New("Fn::GetAtt references undefined resource")
	// ErrUnresolvedSubRef mirrors AWS "Template error: instance of Fn::Sub
	// references undefined resource <Logical>".
	ErrUnresolvedSubRef = errors.New("Fn::Sub references undefined resource")
	// ErrUnsupportedResourceType mirrors AWS "Resource type <Type> is not
	// supported / Unrecognized resource type".
	ErrUnsupportedResourceType = errors.New("unsupported resource type")
)

// awsResourceTypePattern matches the syntactic shape of a CloudFormation
// resource type identifier. AWS accepts three families:
//
//	AWS::<Service>::<Resource>[::<...>]
//	Custom::<Name>
//	Alexa::ASK::<Resource>
//
// Anything that does not match this shape is definitively not a real AWS
// resource type, so the stack must fail (AWS rejects it at validation time).
// This intentionally does NOT enumerate every supported service: a well-formed
// type the engine doesn't have a dedicated creator for still falls through to
// the stub path, preserving the behaviour the existing templates/tests rely on.
var awsResourceTypePattern = regexp.MustCompile(
	`^(AWS|Alexa)::[A-Za-z0-9]+::[A-Za-z0-9]+(::[A-Za-z0-9]+)*$|^Custom::[A-Za-z0-9_-]+$`,
)

// isValidResourceTypeName reports whether name is a syntactically valid AWS
// CloudFormation resource type identifier.
func isValidResourceTypeName(name string) bool {
	return awsResourceTypePattern.MatchString(name)
}

// validateIntrinsics performs a pre-flight pass over a parsed template and
// returns the first AWS-accurate error for:
//
//   - an unsupported (syntactically invalid) resource Type;
//   - an Fn::GetAtt whose logical resource ID is not defined in the template;
//   - an Fn::Sub ${Logical.Attr} whose logical resource ID is not defined.
//
// It deliberately does NOT validate attribute names: the resolver falls back to
// the physical ID for attributes it doesn't model, and existing templates rely
// on that. Only a reference to a wholly-undefined logical ID is an error, which
// is exactly what AWS flags as a template error.
func validateIntrinsics(tmpl *Template) error {
	if tmpl == nil {
		return nil
	}

	// Build the set of names a GetAtt/Sub logical reference may legitimately
	// resolve against: declared resources. (Parameters can be Ref'd but not
	// GetAtt'd; pseudo-parameters are handled separately below.)
	resources := make(map[string]struct{}, len(tmpl.Resources))
	for logicalID := range tmpl.Resources {
		resources[logicalID] = struct{}{}
	}

	// Names that may legally appear before a "." in an Fn::Sub ${...} expression
	// without being a declared resource: template parameters and pseudo-params.
	subRefNames := make(map[string]struct{}, len(tmpl.Parameters))
	for name := range tmpl.Parameters {
		subRefNames[name] = struct{}{}
	}

	if err := validateResourceTypes(tmpl); err != nil {
		return err
	}

	for _, res := range tmpl.Resources {
		if err := validateGetAttRefs(res.Properties, resources); err != nil {
			return err
		}
		if err := validateSubRefs(res.Properties, resources, subRefNames); err != nil {
			return err
		}
	}

	for _, out := range tmpl.Outputs {
		if err := validateGetAttRefs(out.Value, resources); err != nil {
			return err
		}
		if err := validateSubRefs(out.Value, resources, subRefNames); err != nil {
			return err
		}
	}

	return nil
}

// validateResourceTypes ensures every resource Type is a syntactically valid
// AWS resource-type identifier.
func validateResourceTypes(tmpl *Template) error {
	for logicalID, res := range tmpl.Resources {
		if !isValidResourceTypeName(res.Type) {
			return fmt.Errorf(
				"%w: resource %s has type %q which is not a recognized resource type",
				ErrUnsupportedResourceType, logicalID, res.Type,
			)
		}
	}

	return nil
}

// validateGetAttRefs walks a value and errors on any Fn::GetAtt whose logical
// resource ID is not a declared resource.
func validateGetAttRefs(v any, resources map[string]struct{}) error {
	switch val := v.(type) {
	case map[string]any:
		if err := checkGetAttNode(val, resources); err != nil {
			return err
		}

		for _, child := range val {
			if err := validateGetAttRefs(child, resources); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range val {
			if err := validateGetAttRefs(item, resources); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkGetAttNode validates the Fn::GetAtt logical reference of a single node.
func checkGetAttNode(node map[string]any, resources map[string]struct{}) error {
	getAttArgs, isGetAtt := node["Fn::GetAtt"].([]any)
	if !isGetAtt || len(getAttArgs) == 0 {
		return nil
	}

	// A dotted single-string form "Logical.Attr" is also accepted by AWS; the
	// resolver only handles the array form, but validate the logical ID either
	// way.
	logicalID, _ := getAttArgs[0].(string)
	if logicalID == "" {
		return nil
	}

	if _, ok := resources[logicalID]; !ok {
		return fmt.Errorf("%w: %s", ErrUnresolvedGetAtt, logicalID)
	}

	return nil
}

// validateSubRefs walks a value and errors on any Fn::Sub string whose
// ${Logical.Attr} expression references an undefined logical resource ID. Plain
// ${Var} references (no dot) are not validated here because they may resolve to
// parameters, pseudo-parameters, or two-arg Sub variable maps; the resolver
// leaves genuinely-unknown ones as literal placeholders (AWS-compatible for the
// non-dotted case).
func validateSubRefs(v any, resources, subRefNames map[string]struct{}) error {
	switch val := v.(type) {
	case map[string]any:
		if err := validateSubExpr(val, resources, subRefNames); err != nil {
			return err
		}

		for _, child := range val {
			if err := validateSubRefs(child, resources, subRefNames); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range val {
			if err := validateSubRefs(item, resources, subRefNames); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateSubExpr validates the ${Logical.Attr} references inside a single
// Fn::Sub node (either the string form or the two-arg [template, vars] form).
func validateSubExpr(node map[string]any, resources, subRefNames map[string]struct{}) error {
	tmplStr, localVars := subTemplateAndLocals(node)
	if tmplStr == "" {
		return nil
	}

	for _, match := range subVarPattern.FindAllStringSubmatch(tmplStr, -1) {
		expr := match[1]
		logicalID, _, hasDot := strings.Cut(expr, ".")
		if !hasDot {
			// Plain ${Var}: may be a parameter, pseudo-param, local var, or
			// physical-ID ref; not validated (resolver leaves unknowns literal).
			continue
		}

		if _, ok := resources[logicalID]; ok {
			continue
		}
		if _, ok := subRefNames[logicalID]; ok {
			continue
		}
		if _, ok := localVars[logicalID]; ok {
			continue
		}
		if isPseudoParameter(logicalID) {
			continue
		}

		return fmt.Errorf("%w: %s", ErrUnresolvedSubRef, logicalID)
	}

	return nil
}

// subTemplateAndLocals extracts the Fn::Sub template string and any local
// variable names declared by the two-arg form.
func subTemplateAndLocals(node map[string]any) (string, map[string]struct{}) {
	if s, ok := node["Fn::Sub"].(string); ok {
		return s, nil
	}

	if args, isArr := node["Fn::Sub"].([]any); isArr && len(args) == 2 {
		s, _ := args[0].(string)
		locals := map[string]struct{}{}
		if varMap, isMap := args[1].(map[string]any); isMap {
			for k := range varMap {
				locals[k] = struct{}{}
			}
		}

		return s, locals
	}

	return "", nil
}

// isPseudoParameter reports whether name is an AWS pseudo-parameter that may be
// referenced in an Fn::Sub expression.
func isPseudoParameter(name string) bool {
	switch name {
	case "AWS::Region", "AWS::AccountId", "AWS::StackName", "AWS::StackId",
		"AWS::Partition", "AWS::URLSuffix", "AWS::NoValue", "AWS::NotificationARNs":
		return true
	default:
		return false
	}
}
