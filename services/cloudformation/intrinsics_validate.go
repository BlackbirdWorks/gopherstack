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
	// ErrUnsupportedGetAttAttribute mirrors AWS "Template error: resource <X>
	// does not support attribute type <Y> in Fn::GetAtt".
	ErrUnsupportedGetAttAttribute = errors.New("does not support attribute type")
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
//   - an Fn::Sub ${Logical.Attr} whose logical resource ID is not defined;
//   - an Fn::GetAtt/Fn::Sub attribute name a resource type is known (via the
//     CloudFormation resource spec, cfnResourceAttributes) not to support.
//
// A resource type absent from cfnResourceAttributes is treated as unknown to
// the spec, not as having no attributes: the resolver falls back to the
// physical ID for such types' attributes, and existing templates rely on
// that (see checkGetAttNode/validateGetAttAttribute).
func validateIntrinsics(tmpl *Template) error {
	if tmpl == nil {
		return nil
	}

	// Build the map a GetAtt/Sub logical reference may legitimately resolve
	// against: declared resources, keyed by their CFN Type. (Parameters can
	// be Ref'd but not GetAtt'd; pseudo-parameters are handled separately
	// below.)
	resources := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resources[logicalID] = res.Type
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

// resourceTypeAllowed reports whether resourceType matches one of the
// documented ResourceTypes wildcard patterns (CreateStackInput.ResourceTypes,
// api_op_CreateStack.go): an exact match, "AWS::*"/"Custom::*" (all
// resources of that family), or "AWS::Service::*" (all resources of one
// service).
func resourceTypeAllowed(resourceType string, allowed []string) bool {
	for _, pattern := range allowed {
		if pattern == resourceType {
			return true
		}

		if prefix, ok := strings.CutSuffix(pattern, "*"); ok && strings.HasPrefix(resourceType, prefix) {
			return true
		}
	}

	return false
}

// preflightGetAttAttributeErr parses templateBody and runs the same
// Fn::GetAtt/Fn::Sub attribute check validateIntrinsics uses, but surfaces
// only ErrUnsupportedGetAttAttribute -- an undefined logical ID or
// unsupported resource type still fails the stack asynchronously via
// validateIntrinsics's later call, matching this repo's existing behaviour
// for those. Real CreateStack/UpdateStack reject an undocumented Fn::GetAtt
// attribute synchronously with a ValidationError (gopherstack-p7pvq), so this
// is called before the stack is even created/mutated.
func preflightGetAttAttributeErr(templateBody string) error {
	if templateBody == "" {
		return nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		// A malformed template is reported by the normal parse-error path
		// (createStackFromTemplate/parseAndValidateUpdateTemplate) instead.
		return nil //nolint:nilerr // intentional: this preflight only cares about attribute validation
	}

	if intErr := validateIntrinsics(tmpl); intErr != nil && errors.Is(intErr, ErrUnsupportedGetAttAttribute) {
		return intErr
	}

	return nil
}

// validateResourceTypesAllowed enforces CreateStack/UpdateStack/
// CreateChangeSet's optional ResourceTypes allowlist: when non-empty, every
// resource Type in the template must match one of its documented wildcard
// patterns, or the operation fails (real doc: "If the list of resource
// types doesn't include a resource type that you're updating, the stack
// update fails"). An empty allowlist imposes no constraint (the documented
// default: any resource type is permitted).
func validateResourceTypesAllowed(tmpl *Template, allowed []string) error {
	if len(allowed) == 0 || tmpl == nil {
		return nil
	}

	for logicalID, res := range tmpl.Resources {
		if !resourceTypeAllowed(res.Type, allowed) {
			return fmt.Errorf(
				"%w: resource %s has type %q",
				ErrResourceTypeNotAllowed, logicalID, res.Type,
			)
		}
	}

	return nil
}

// validateGetAttRefs walks a value and errors on any Fn::GetAtt whose logical
// resource ID is not a declared resource, or whose attribute a known
// resource type doesn't support.
func validateGetAttRefs(v any, resources map[string]string) error {
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

// checkGetAttNode validates the Fn::GetAtt logical reference (and, when the
// resource type is known, its attribute) of a single node.
func checkGetAttNode(node map[string]any, resources map[string]string) error {
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

	resType, ok := resources[logicalID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnresolvedGetAtt, logicalID)
	}

	const getAttArgsWithAttr = 2
	if len(getAttArgs) < getAttArgsWithAttr {
		return nil
	}

	attrName, _ := getAttArgs[1].(string)

	return validateGetAttAttribute(logicalID, resType, attrName)
}

// validateGetAttAttribute rejects attrName only when resType is a resource
// type the CloudFormation spec documents (cfnResourceAttributes, generated
// by cmd/cfnattrgen) AND attrName isn't in its documented attribute set. A
// resType absent from the table falls back to today's behaviour -- it may be
// a legitimate, simply unmodelled attribute (gopherstack-p7pvq).
func validateGetAttAttribute(logicalID, resType, attrName string) error {
	if attrName == "" {
		return nil
	}

	attrs, known := cfnResourceAttributes[resType]
	if !known {
		return nil
	}

	if _, ok := attrs[attrName]; ok {
		return nil
	}

	return fmt.Errorf(
		"%w: Template error: resource %s does not support attribute type %s in Fn::GetAtt",
		ErrUnsupportedGetAttAttribute, logicalID, attrName,
	)
}

// validateSubRefs walks a value and errors on any Fn::Sub string whose
// ${Logical.Attr} expression references an undefined logical resource ID. Plain
// ${Var} references (no dot) are not validated here because they may resolve to
// parameters, pseudo-parameters, or two-arg Sub variable maps; the resolver
// leaves genuinely-unknown ones as literal placeholders (AWS-compatible for the
// non-dotted case).
func validateSubRefs(v any, resources map[string]string, subRefNames map[string]struct{}) error {
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
// Fn::Sub node (either the string form or the two-arg [template, vars] form),
// including the attribute check ${Logical.Attr} shares with Fn::GetAtt (see
// validateGetAttAttribute).
func validateSubExpr(node map[string]any, resources map[string]string, subRefNames map[string]struct{}) error {
	tmplStr, localVars := subTemplateAndLocals(node)
	if tmplStr == "" {
		return nil
	}

	for _, match := range subVarPattern.FindAllStringSubmatch(tmplStr, -1) {
		expr := match[1]
		logicalID, attrName, hasDot := strings.Cut(expr, ".")
		if !hasDot {
			// Plain ${Var}: may be a parameter, pseudo-param, local var, or
			// physical-ID ref; not validated (resolver leaves unknowns literal).
			continue
		}

		if resType, ok := resources[logicalID]; ok {
			if err := validateGetAttAttribute(logicalID, resType, attrName); err != nil {
				return err
			}

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
