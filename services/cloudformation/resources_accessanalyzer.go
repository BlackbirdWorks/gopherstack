package cloudformation

import (
	"fmt"

	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

const (
	resTypeAccessAnalyzerAnalyzer    = "AWS::AccessAnalyzer::Analyzer"
	resTypeAccessAnalyzerArchiveRule = "AWS::AccessAnalyzer::ArchiveRule"
)

// createAccessAnalyzerResource handles the AccessAnalyzer resource types
// listed above. ArchiveRule deletes through deleteNewestPropsBasedResource
// since DeleteArchiveRule needs the owning AnalyzerName, which arrives as an
// ARN (Analyzer's own Ref) rather than a bare name.
func (rc *ResourceCreator) createAccessAnalyzerResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAccessAnalyzerAnalyzer:
		id, err := rc.createAccessAnalyzerAnalyzer(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAccessAnalyzerArchiveRule:
		id, err := rc.createAccessAnalyzerArchiveRule(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAccessAnalyzerResource handles AWS::AccessAnalyzer::Analyzer
// deletion only; ArchiveRule is handled by deleteNewestPropsBasedResource
// (see the doc comment above).
func (rc *ResourceCreator) deleteAccessAnalyzerResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeAccessAnalyzerAnalyzer {
		return false, nil
	}

	if rc.backends.AccessAnalyzer == nil {
		return true, nil
	}

	return true, rc.backends.AccessAnalyzer.Backend.DeleteAnalyzer(resourceNameFromARN(physicalID))
}

// ---- AWS::AccessAnalyzer::Analyzer ----
// Ref returns the analyzer ARN (documented).

func (rc *ResourceCreator) createAccessAnalyzerAnalyzer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AccessAnalyzer == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "AnalyzerName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	analyzerType := strProp(props, "Type", params, physicalIDs)
	if analyzerType == "" {
		analyzerType = "ACCOUNT"
	}

	a, err := rc.backends.AccessAnalyzer.Backend.CreateAnalyzer(
		name, accessanalyzerbackend.AnalyzerType(analyzerType), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AccessAnalyzer analyzer %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = a.Arn

	return a.Arn, nil
}

// ---- AWS::AccessAnalyzer::ArchiveRule ----
// Ref is undocumented; the rule name (its natural single-value identifier)
// is used. No Fn::GetAtt attribute is implemented: the doc page lists an
// "Arn" attribute, but ArchiveRule has no backend-modeled ARN (it is keyed
// by AnalyzerName+RuleName, not a resource of its own), so fabricating one
// would violate the no-stub rule.

func (rc *ResourceCreator) createAccessAnalyzerArchiveRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AccessAnalyzer == nil {
		return logicalID + "-stub", nil
	}

	analyzerName := resourceNameFromARN(strProp(props, "AnalyzerName", params, physicalIDs))
	ruleName := strProp(props, "RuleName", params, physicalIDs)
	if ruleName == "" {
		ruleName = logicalID
	}

	filter := accessAnalyzerFilter(props["Filter"], params, physicalIDs)

	rule, err := rc.backends.AccessAnalyzer.Backend.CreateArchiveRule(analyzerName, ruleName, filter)
	if err != nil {
		return "", fmt.Errorf("create AccessAnalyzer archive rule %s/%s: %w", analyzerName, ruleName, err)
	}

	return rule.RuleName, nil
}

// deleteAccessAnalyzerArchiveRule deletes an archive rule using the sibling
// AnalyzerName/RuleName properties (both declared template properties, so
// unlike GuardDuty IPSet/AppConfig Environment this needs no physicalID).
func (rc *ResourceCreator) deleteAccessAnalyzerArchiveRule(
	props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.AccessAnalyzer == nil {
		return nil
	}

	analyzerName := resourceNameFromARN(strProp(props, "AnalyzerName", nil, stackPhysicalIDs))
	ruleName := strProp(props, "RuleName", nil, stackPhysicalIDs)

	return rc.backends.AccessAnalyzer.Backend.DeleteArchiveRule(analyzerName, ruleName)
}

// accessAnalyzerFilter converts a CFN Filter property (a list of
// {Property, Eq, Neq, Contains, Exists} maps) into the backend's
// map[string]FilterCriterion keyed by Property.
func accessAnalyzerFilter(
	v any, params, physicalIDs map[string]string,
) map[string]accessanalyzerbackend.FilterCriterion {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make(map[string]accessanalyzerbackend.FilterCriterion, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		property := strProp(m, "Property", params, physicalIDs)
		if property == "" {
			continue
		}

		out[property] = accessanalyzerbackend.FilterCriterion{
			Contains: strSliceProp(m["Contains"], params, physicalIDs),
			Eq:       strSliceProp(m["Eq"], params, physicalIDs),
			Neq:      strSliceProp(m["Neq"], params, physicalIDs),
		}
	}

	return out
}
