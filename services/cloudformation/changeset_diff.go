package cloudformation

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// Replacement / recreation constants used by change-set previews.
const (
	recreateAlways        = "Always"
	recreateConditionally = "Conditionally"
	recreateNever         = "Never"

	replacementTrue          = "True"
	replacementFalse         = "False"
	replacementConditionally = "Conditionally"

	scopeProperties = "Properties"
	scopeTags       = "Tags"

	evalStatic  = "Static"
	evalDynamic = "Dynamic"

	changeSourceDirect    = "DirectModification"
	changeSourceParamRef  = "ParameterReference"
	changeSourceResAttr   = "ResourceAttribute"
	changeSourceAutomatic = "Automatic"

	changeTypeResource = "Resource"

	// Difference types for drift property comparisons.
	diffAdd      = "ADD"
	diffRemove   = "REMOVE"
	diffNotEqual = "NOT_EQUAL"
)

// requiresRecreation returns the RequiresRecreation classification for a change
// to the named property of the given resource type. Only the most common
// immutable (replacement-forcing) properties are modelled; unlisted properties
// default to recreateNever (an in-place Modify).
func requiresRecreation(resType, propName string) string {
	switch resType {
	case resTypeS3Bucket:
		return alwaysIfOneOf(propName, "BucketName")
	case resTypeDynamoDBTable:
		switch propName {
		case "TableName", "KeySchema":
			return recreateAlways
		case "AttributeDefinitions":
			return recreateConditionally
		}
	case resTypeSQSQueue:
		return alwaysIfOneOf(propName, "QueueName", "FifoQueue")
	case resTypeSNSTopic:
		return alwaysIfOneOf(propName, "TopicName", "FifoTopic")
	case "AWS::EC2::Instance":
		return alwaysIfOneOf(propName, "ImageId", "AvailabilityZone", "SubnetId", "KeyName")
	case "AWS::RDS::DBInstance":
		switch propName {
		case "DBInstanceIdentifier":
			return recreateAlways
		case "Engine", "AvailabilityZone":
			return recreateConditionally
		}
	case resTypeLambdaFunction:
		return alwaysIfOneOf(propName, "FunctionName")
	case "AWS::IAM::Role":
		return alwaysIfOneOf(propName, "RoleName", "Path")
	case resTypeKMSKey:
		return alwaysIfOneOf(propName, "KeySpec", "KeyUsage")
	case "AWS::Logs::LogGroup":
		return alwaysIfOneOf(propName, "LogGroupName")
	}

	return recreateNever
}

// alwaysIfOneOf returns recreateAlways when propName is one of the listed
// replacement-forcing property names, else recreateNever.
func alwaysIfOneOf(propName string, names ...string) string {
	if slices.Contains(names, propName) {
		return recreateAlways
	}

	return recreateNever
}

// addChangesForTemplate returns an Add change for every resource in a template,
// used for CREATE change sets where no prior stack exists.
func addChangesForTemplate(tmpl *Template) []Change {
	changes := make([]Change, 0, len(tmpl.Resources))
	for _, logicalID := range collections.SortedKeys(tmpl.Resources) {
		res := tmpl.Resources[logicalID]
		changes = append(changes, Change{
			Type: changeTypeResource,
			ResourceChange: ResourceChange{
				Action:       "Add",
				LogicalID:    logicalID,
				ResourceType: res.Type,
			},
		})
	}

	return changes
}

// diffTemplates computes the change-set diff between the currently-deployed
// template (oldTmpl, may be nil) and the proposed template (newTmpl). deployed
// carries the live resources so physical IDs can be reported for Modify/Remove.
func diffTemplates(
	oldTmpl, newTmpl *Template,
	deployed map[string]*StackResource,
) []Change {
	oldRes := map[string]TemplateResource{}
	if oldTmpl != nil {
		oldRes = oldTmpl.Resources
	} else {
		// Without a parseable prior template, treat every deployed resource as
		// the prior state (properties unknown → forces a Modify when present).
		for id, r := range deployed {
			oldRes[id] = TemplateResource{Type: r.Type, Properties: r.Properties}
		}
	}

	ids := unionKeys(oldRes, newTmpl.Resources)
	changes := make([]Change, 0, len(ids))

	for _, logicalID := range ids {
		oldR, inOld := oldRes[logicalID]
		newR, inNew := newTmpl.Resources[logicalID]

		var physID string
		if dep, ok := deployed[logicalID]; ok {
			physID = dep.PhysicalID
		}

		switch {
		case inNew && !inOld:
			changes = append(changes, addChange(logicalID, newR))
		case inOld && !inNew:
			changes = append(changes, removeChange(logicalID, oldR, physID))
		default:
			if ch, changed := modifyChange(logicalID, oldR, newR, physID); changed {
				changes = append(changes, ch)
			}
		}
	}

	return changes
}

func addChange(logicalID string, newR TemplateResource) Change {
	return Change{
		Type: changeTypeResource,
		ResourceChange: ResourceChange{
			Action:       "Add",
			LogicalID:    logicalID,
			ResourceType: newR.Type,
		},
	}
}

func removeChange(logicalID string, oldR TemplateResource, physID string) Change {
	return Change{
		Type: changeTypeResource,
		ResourceChange: ResourceChange{
			Action:       "Remove",
			LogicalID:    logicalID,
			PhysicalID:   physID,
			ResourceType: oldR.Type,
		},
	}
}

// modifyChange returns the Modify change for a resource present in both
// templates, or changed=false when nothing actually changed (so unchanged
// resources are omitted from the change set, matching AWS).
func modifyChange(
	logicalID string,
	oldR, newR TemplateResource,
	physID string,
) (Change, bool) {
	typeChanged := oldR.Type != newR.Type
	changedKeys := changedPropertyKeys(oldR.Properties, newR.Properties)

	if !typeChanged && len(changedKeys) == 0 {
		return Change{}, false
	}

	details, scope := buildDetails(newR, changedKeys)
	replacement := overallReplacement(typeChanged, details)

	if typeChanged {
		// A resource-type change is always a full replacement.
		details = append([]ResourceChangeDetail{{
			Target: &ResourceTargetDefinition{
				Attribute:          scopeProperties,
				RequiresRecreation: recreateAlways,
			},
			Evaluation:   evalStatic,
			ChangeSource: changeSourceDirect,
		}}, details...)
		if !slices.Contains(scope, scopeProperties) {
			scope = append(scope, scopeProperties)
		}
	}

	return Change{
		Type: changeTypeResource,
		ResourceChange: ResourceChange{
			Action:       "Modify",
			LogicalID:    logicalID,
			PhysicalID:   physID,
			ResourceType: newR.Type,
			Replacement:  replacement,
			Scope:        scope,
			Details:      details,
		},
	}, true
}

// buildDetails constructs the per-property change details and the change scope
// for the changed property keys of a resource.
func buildDetails(newR TemplateResource, changedKeys []string) ([]ResourceChangeDetail, []string) {
	details := make([]ResourceChangeDetail, 0, len(changedKeys))
	scopeSet := map[string]bool{}

	for _, key := range changedKeys {
		rr := requiresRecreation(newR.Type, key)
		details = append(details, ResourceChangeDetail{
			Target: &ResourceTargetDefinition{
				Attribute:          scopeProperties,
				Name:               key,
				RequiresRecreation: rr,
			},
			Evaluation:   evaluationFor(newR.Properties[key]),
			ChangeSource: changeSourceFor(newR.Properties[key]),
		})
		if key == scopeTags {
			scopeSet[scopeTags] = true
		} else {
			scopeSet[scopeProperties] = true
		}
	}

	scope := make([]string, 0, len(scopeSet))
	for s := range scopeSet {
		scope = append(scope, s)
	}
	sort.Strings(scope)

	return details, scope
}

// overallReplacement folds the per-property recreation classifications into the
// resource-level Replacement flag AWS reports on a ResourceChange.
func overallReplacement(typeChanged bool, details []ResourceChangeDetail) string {
	if typeChanged {
		return replacementTrue
	}

	anyConditional := false
	for _, d := range details {
		if d.Target == nil {
			continue
		}
		switch d.Target.RequiresRecreation {
		case recreateAlways:
			return replacementTrue
		case recreateConditionally:
			anyConditional = true
		}
	}
	if anyConditional {
		return replacementConditionally
	}

	return replacementFalse
}

// evaluationFor reports whether a property value can be evaluated statically at
// change-set time (Static) or depends on runtime values such as Fn::GetAtt
// (Dynamic).
func evaluationFor(v any) string {
	if referencesRuntimeValue(v) {
		return evalDynamic
	}

	return evalStatic
}

// changeSourceFor classifies where a property change originates.
func changeSourceFor(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return changeSourceDirect
	}
	if _, has := m[fnGetAtt]; has {
		return changeSourceResAttr
	}
	if ref, has := m["Ref"].(string); has {
		// Pseudo parameters (AWS::*) are automatic; others are parameter refs.
		if len(ref) > 5 && ref[:5] == "AWS::" {
			return changeSourceAutomatic
		}

		return changeSourceParamRef
	}
	if _, has := m["Fn::Sub"]; has {
		return changeSourceParamRef
	}

	return changeSourceDirect
}

// referencesRuntimeValue reports whether a value (recursively) contains an
// intrinsic whose result is only known at deploy time.
func referencesRuntimeValue(v any) bool {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			switch k {
			case fnGetAtt, "Ref", "Fn::Sub", "Fn::ImportValue", "Fn::Select", "Fn::Join":
				return true
			}
			if referencesRuntimeValue(child) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(val, referencesRuntimeValue)
	}

	return false
}

// changedPropertyKeys returns the sorted set of top-level property keys that
// were added, removed, or modified between two property maps.
func changedPropertyKeys(oldProps, newProps map[string]any) []string {
	keys := unionMapKeys(oldProps, newProps)
	changed := make([]string, 0, len(keys))
	for _, k := range keys {
		if !jsonEqual(oldProps[k], newProps[k]) {
			changed = append(changed, k)
		}
	}

	return changed
}

// computePropertyDifferences produces the property-level drift differences
// between the expected (template) and actual (live) property maps.
func computePropertyDifferences(expected, actual map[string]any) []PropertyDifference {
	keys := unionMapKeys(expected, actual)
	diffs := make([]PropertyDifference, 0)

	for _, k := range keys {
		expVal, hasExp := expected[k]
		actVal, hasAct := actual[k]

		switch {
		case hasExp && !hasAct:
			diffs = append(diffs, PropertyDifference{
				PropertyPath:   "/" + k,
				ExpectedValue:  toJSONString(expVal),
				ActualValue:    "",
				DifferenceType: diffRemove,
			})
		case !hasExp && hasAct:
			diffs = append(diffs, PropertyDifference{
				PropertyPath:   "/" + k,
				ExpectedValue:  "",
				ActualValue:    toJSONString(actVal),
				DifferenceType: diffAdd,
			})
		default:
			if !jsonEqual(expVal, actVal) {
				diffs = append(diffs, PropertyDifference{
					PropertyPath:   "/" + k,
					ExpectedValue:  toJSONString(expVal),
					ActualValue:    toJSONString(actVal),
					DifferenceType: diffNotEqual,
				})
			}
		}
	}

	sort.Slice(diffs, func(i, j int) bool { return diffs[i].PropertyPath < diffs[j].PropertyPath })

	return diffs
}

// --- small helpers -------------------------------------------------------

func unionKeys(a map[string]TemplateResource, b map[string]TemplateResource) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

func unionMapKeys(a, b map[string]any) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

func jsonEqual(a, b any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}

	return string(ab) == string(bb)
}

func toJSONString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}

	return string(b)
}
