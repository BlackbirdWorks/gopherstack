package cloudformation

import "fmt"

// ResilienceHubResourceMapping is one AWS::ResilienceHub::App ResourceMapping
// entry, using CloudFormation's own property names (see
// aws-properties-resiliencehub-app-resourcemapping.html), not
// resiliencehub's wire names. Declared here rather than imported from
// services/resiliencehub because that package already imports
// cloudformation (for its own CfnStack resource-mapping resolution) --
// importing it back would cycle.
type ResilienceHubResourceMapping struct {
	MappingType          string
	PhysicalResourceID   string
	PhysicalResourceType string
	ResourceName         string
	LogicalStackName     string
	TerraformSourceName  string
	EksSourceName        string
}

// ResilienceHubFailurePolicy is one AWS::ResilienceHub::ResiliencyPolicy
// PolicyMap entry's FailurePolicy value (RTO/RPO in seconds).
type ResilienceHubFailurePolicy struct {
	RtoInSecs int32
	RpoInSecs int32
}

// ResilienceHubBackend is the subset of the ResilienceHub service backend
// that AWS::ResilienceHub::App/ResiliencyPolicy provisioning needs. Declared
// locally instead of depending on *resiliencehub.Handler directly, for the
// same import-cycle reason as the types above; services/resiliencehub's
// Handler implements this interface structurally (see
// services/resiliencehub/handler_cfn_provision.go).
type ResilienceHubBackend interface {
	CreateResilienceHubApp(
		name, description, assessmentSchedule, policyArn string, tags map[string]string,
	) (string, error)
	SetResilienceHubAppTemplate(appArn, templateBody string) error
	AddResilienceHubAppResourceMappings(appArn string, mappings []ResilienceHubResourceMapping) error
	DeleteResilienceHubApp(appArn string, forceDelete bool) error
	CreateResilienceHubResiliencyPolicy(
		name, tier, description, dataLocationConstraint string,
		policy map[string]ResilienceHubFailurePolicy,
		tags map[string]string,
	) (string, error)
	DeleteResilienceHubResiliencyPolicy(policyArn string) error
}

// createResilienceHubSupplementalResource handles AWS::ResilienceHub::App and
// AWS::ResilienceHub::ResiliencyPolicy resource creation.
func (rc *ResourceCreator) createResilienceHubSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ResilienceHub::App":
		id, err := rc.createResilienceHubApp(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ResilienceHub::ResiliencyPolicy":
		id, err := rc.createResilienceHubResiliencyPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteResilienceHubSupplementalResource handles deletion for
// AWS::ResilienceHub::App and AWS::ResilienceHub::ResiliencyPolicy.
func (rc *ResourceCreator) deleteResilienceHubSupplementalResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::ResilienceHub::App":
		return true, rc.deleteResilienceHubApp(physicalID)
	case "AWS::ResilienceHub::ResiliencyPolicy":
		return true, rc.deleteResilienceHubResiliencyPolicy(physicalID)
	default:
		return false, nil
	}
}

// createResilienceHubApp provisions an AWS::ResilienceHub::App. Ref/physical
// ID is the App ARN, matching the resource type's documented Ref return
// value. AppTemplateBody and ResourceMappings are both CloudFormation-required
// properties but map to separate ResilienceHub API calls
// (PutDraftAppVersionTemplate, AddDraftAppVersionResourceMappings) rather than
// CreateApp fields, since the CreateApp operation itself has no template or
// mapping fields.
func (rc *ResourceCreator) createResilienceHubApp(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ResilienceHub == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	appArn, err := rc.backends.ResilienceHub.CreateResilienceHubApp(
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "AppAssessmentSchedule", params, physicalIDs),
		strProp(props, "ResiliencyPolicyArn", params, physicalIDs),
		strMapProp(props, "Tags", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Resilience Hub app %s: %w", name, err)
	}

	if templateBody := strProp(props, "AppTemplateBody", params, physicalIDs); templateBody != "" {
		if putErr := rc.backends.ResilienceHub.SetResilienceHubAppTemplate(appArn, templateBody); putErr != nil {
			return "", fmt.Errorf("put Resilience Hub app template for %s: %w", appArn, putErr)
		}
	}

	if mappings := resilienceHubResourceMappingsProp(props, params, physicalIDs); len(mappings) > 0 {
		if mapErr := rc.backends.ResilienceHub.AddResilienceHubAppResourceMappings(appArn, mappings); mapErr != nil {
			return "", fmt.Errorf("add Resilience Hub app resource mappings for %s: %w", appArn, mapErr)
		}
	}

	return appArn, nil
}

func (rc *ResourceCreator) deleteResilienceHubApp(physicalID string) error {
	if rc.backends.ResilienceHub == nil {
		return nil
	}

	return rc.backends.ResilienceHub.DeleteResilienceHubApp(physicalID, false)
}

// createResilienceHubResiliencyPolicy provisions an
// AWS::ResilienceHub::ResiliencyPolicy. Ref/physical ID is the policy ARN,
// matching the resource type's documented Ref return value.
func (rc *ResourceCreator) createResilienceHubResiliencyPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ResilienceHub == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "PolicyName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	policyArn, err := rc.backends.ResilienceHub.CreateResilienceHubResiliencyPolicy(
		name,
		strProp(props, "Tier", params, physicalIDs),
		strProp(props, "PolicyDescription", params, physicalIDs),
		strProp(props, "DataLocationConstraint", params, physicalIDs),
		resilienceHubPolicyMapProp(props),
		strMapProp(props, "Tags", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Resilience Hub resiliency policy %s: %w", name, err)
	}

	return policyArn, nil
}

func (rc *ResourceCreator) deleteResilienceHubResiliencyPolicy(physicalID string) error {
	if rc.backends.ResilienceHub == nil {
		return nil
	}

	return rc.backends.ResilienceHub.DeleteResilienceHubResiliencyPolicy(physicalID)
}

// strMapProp decodes an object-of-string CloudFormation property (e.g. Tags
// on AWS::ResilienceHub::App/ResiliencyPolicy, which -- unlike most AWS
// resource types' array-of-{Key,Value} Tags -- is a plain JSON object; see
// cfn-resiliencehub-app-tags's "Type: Object of String").
func strMapProp(props map[string]any, key string, params, physicalIDs map[string]string) map[string]string {
	raw, ok := props[key].(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(raw))
	for k, v := range raw {
		out[k] = resolve(v, params, physicalIDs)
	}

	return out
}

// resilienceHubResourceMappingsProp decodes AWS::ResilienceHub::App's
// ResourceMappings property (array of ResourceMapping objects; see
// aws-properties-resiliencehub-app-resourcemapping.html).
func resilienceHubResourceMappingsProp(
	props map[string]any, params, physicalIDs map[string]string,
) []ResilienceHubResourceMapping {
	raw, ok := props["ResourceMappings"].([]any)
	if !ok {
		return nil
	}

	out := make([]ResilienceHubResourceMapping, 0, len(raw))

	for _, item := range raw {
		m, itemOK := item.(map[string]any)
		if !itemOK {
			continue
		}

		rm := ResilienceHubResourceMapping{
			MappingType:         strProp(m, "MappingType", params, physicalIDs),
			ResourceName:        strProp(m, "ResourceName", params, physicalIDs),
			LogicalStackName:    strProp(m, "LogicalStackName", params, physicalIDs),
			TerraformSourceName: strProp(m, "TerraformSourceName", params, physicalIDs),
			EksSourceName:       strProp(m, "EksSourceName", params, physicalIDs),
		}

		if pri, priOK := m["PhysicalResourceId"].(map[string]any); priOK {
			rm.PhysicalResourceID = strProp(pri, "Identifier", params, physicalIDs)
			rm.PhysicalResourceType = strProp(pri, "Type", params, physicalIDs)
		}

		out = append(out, rm)
	}

	return out
}

// resilienceHubPolicyMapProp decodes AWS::ResilienceHub::ResiliencyPolicy's
// Policy property (PolicyMap: a map keyed by disruption type -- Software,
// Hardware, AZ, Region -- of FailurePolicy{RtoInSecs,RpoInSecs}; see
// aws-properties-resiliencehub-resiliencypolicy-policymap.html).
func resilienceHubPolicyMapProp(props map[string]any) map[string]ResilienceHubFailurePolicy {
	raw, ok := props["Policy"].(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]ResilienceHubFailurePolicy, len(raw))

	for k, v := range raw {
		fp, fpOK := v.(map[string]any)
		if !fpOK {
			continue
		}

		out[k] = ResilienceHubFailurePolicy{
			RtoInSecs: int32(intProp(fp, "RtoInSecs")), // #nosec G115 -- seconds count, not attacker-controlled range
			RpoInSecs: int32(intProp(fp, "RpoInSecs")), // #nosec G115 -- seconds count, not attacker-controlled range
		}
	}

	return out
}
