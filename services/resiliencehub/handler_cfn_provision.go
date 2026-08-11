package resiliencehub

import (
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// This file implements cfnbackend.ResilienceHubBackend on *Handler, letting
// AWS::ResilienceHub::App/ResiliencyPolicy CloudFormation resource types
// (services/cloudformation/resources_resiliencehub.go) provision real Apps
// and ResiliencyPolicies. cloudformation cannot import this package directly
// (cross_service.go already imports cloudformation, for CfnStack
// resource-mapping resolution -- importing it back would cycle), so
// cloudformation declares the interface and its two small parameter types
// locally, and this file satisfies it structurally.

// CreateResilienceHubApp creates an App from AWS::ResilienceHub::App's
// CloudFormation properties and returns its ARN.
func (h *Handler) CreateResilienceHubApp(
	name, description, assessmentSchedule, policyArn string, tags map[string]string,
) (string, error) {
	a, err := h.Backend.CreateApp(&createAppRequest{
		Name:               name,
		Description:        description,
		AssessmentSchedule: assessmentSchedule,
		PolicyArn:          policyArn,
		Tags:               tags,
	})
	if err != nil {
		return "", err
	}

	return a.ARN, nil
}

// SetResilienceHubAppTemplate sets appArn's draft AppTemplateBody.
func (h *Handler) SetResilienceHubAppTemplate(appArn, templateBody string) error {
	_, err := h.Backend.PutDraftAppVersionTemplate(appArn, templateBody)

	return err
}

// AddResilienceHubAppResourceMappings appends CloudFormation-shaped resource
// mappings to appArn's draft AppVersion.
func (h *Handler) AddResilienceHubAppResourceMappings(
	appArn string, mappings []cfnbackend.ResilienceHubResourceMapping,
) error {
	wire := make([]resourceMappingWire, 0, len(mappings))

	for _, m := range mappings {
		w := resourceMappingWire{
			MappingType:         m.MappingType,
			ResourceName:        m.ResourceName,
			LogicalStackName:    m.LogicalStackName,
			TerraformSourceName: m.TerraformSourceName,
			EksSourceName:       m.EksSourceName,
		}
		if m.PhysicalResourceID != "" {
			w.PhysicalResourceID = &physicalResourceIDWire{
				Identifier: m.PhysicalResourceID,
				Type:       m.PhysicalResourceType,
			}
		}

		wire = append(wire, w)
	}

	_, _, err := h.Backend.AddDraftAppVersionResourceMappings(appArn, wire)

	return err
}

// DeleteResilienceHubApp deletes the App identified by appArn.
func (h *Handler) DeleteResilienceHubApp(appArn string, forceDelete bool) error {
	return h.Backend.DeleteApp(appArn, forceDelete)
}

// CreateResilienceHubResiliencyPolicy creates a ResiliencyPolicy from
// AWS::ResilienceHub::ResiliencyPolicy's CloudFormation properties and
// returns its ARN.
func (h *Handler) CreateResilienceHubResiliencyPolicy(
	name, tier, description, dataLocationConstraint string,
	policy map[string]cfnbackend.ResilienceHubFailurePolicy,
	tags map[string]string,
) (string, error) {
	wirePolicy := make(map[string]failurePolicyWire, len(policy))
	for k, v := range policy {
		wirePolicy[k] = failurePolicyWire{RtoInSecs: v.RtoInSecs, RpoInSecs: v.RpoInSecs}
	}

	p, err := h.Backend.CreateResiliencyPolicy(&createResiliencyPolicyRequest{
		PolicyName:             name,
		Tier:                   tier,
		PolicyDescription:      description,
		DataLocationConstraint: dataLocationConstraint,
		Policy:                 wirePolicy,
		Tags:                   tags,
	})
	if err != nil {
		return "", err
	}

	return p.ARN, nil
}

// DeleteResilienceHubResiliencyPolicy deletes the policy identified by policyArn.
func (h *Handler) DeleteResilienceHubResiliencyPolicy(policyArn string) error {
	return h.Backend.DeleteResiliencyPolicy(policyArn)
}
