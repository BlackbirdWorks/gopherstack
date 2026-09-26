package cloudformation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

const (
	resTypeIoTThingType            = "AWS::IoT::ThingType"
	resTypeIoTThingGroup           = "AWS::IoT::ThingGroup"
	resTypeIoTPolicy               = "AWS::IoT::Policy"
	resTypeIoTTopicRuleDestination = "AWS::IoT::TopicRuleDestination"
	resTypeIoTRoleAlias            = "AWS::IoT::RoleAlias"
	resTypeIoTCertificate          = "AWS::IoT::Certificate"
	resTypeIoTProvisioningTemplate = "AWS::IoT::ProvisioningTemplate"
	resTypeIoTAuthorizer           = "AWS::IoT::Authorizer"
	resTypeIoTDomainConfiguration  = "AWS::IoT::DomainConfiguration"
	resTypeIoTJobTemplate          = "AWS::IoT::JobTemplate"
	resTypeIoTDimension            = "AWS::IoT::Dimension"
	resTypeIoTSecurityProfile      = "AWS::IoT::SecurityProfile"
	resTypeIoTCustomMetric         = "AWS::IoT::CustomMetric"
	resTypeIoTFleetMetric          = "AWS::IoT::FleetMetric"
	resTypeIoTBillingGroup         = "AWS::IoT::BillingGroup"
	resTypeIoTMitigationAction     = "AWS::IoT::MitigationAction"
	resTypeIoTScheduledAudit       = "AWS::IoT::ScheduledAudit"
)

// createIoTMoreResource handles the IoT resource types beyond Thing/TopicRule
// (resources_iot.go). Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createIoTMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createIoTIdentityResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	return rc.createIoTFleetResource(logicalID, resourceType, props, params, physicalIDs)
}

// iotIdentityCreators maps the identity/provisioning-family IoT types
// (ThingType, ThingGroup, Policy, TopicRuleDestination, RoleAlias,
// Certificate, ProvisioningTemplate, Authorizer) to their create method.
//
//nolint:gochecknoglobals // static dispatch table, analogous to yamlShortFormTags (template.go)
var iotIdentityCreators = map[string]resourceCreatorFunc{
	resTypeIoTThingType:            (*ResourceCreator).createIoTThingType,
	resTypeIoTThingGroup:           (*ResourceCreator).createIoTThingGroup,
	resTypeIoTPolicy:               (*ResourceCreator).createIoTPolicy,
	resTypeIoTTopicRuleDestination: (*ResourceCreator).createIoTTopicRuleDestination,
	resTypeIoTRoleAlias:            (*ResourceCreator).createIoTRoleAlias,
	resTypeIoTCertificate:          (*ResourceCreator).createIoTCertificate,
	resTypeIoTProvisioningTemplate: (*ResourceCreator).createIoTProvisioningTemplate,
	resTypeIoTAuthorizer:           (*ResourceCreator).createIoTAuthorizer,
}

// createIoTIdentityResource handles the types listed on iotIdentityCreators.
func (rc *ResourceCreator) createIoTIdentityResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	return dispatchCreate(rc, iotIdentityCreators, resourceType, logicalID, props, params, physicalIDs)
}

// createIoTFleetResource handles the fleet-management-family IoT types:
// DomainConfiguration, JobTemplate, Dimension, SecurityProfile, CustomMetric,
// FleetMetric, BillingGroup, MitigationAction, ScheduledAudit.
func (rc *ResourceCreator) createIoTFleetResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeIoTDomainConfiguration:
		id, err := rc.createIoTDomainConfiguration(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTJobTemplate:
		id, err := rc.createIoTJobTemplate(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTDimension:
		id, err := rc.createIoTDimension(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTSecurityProfile:
		id, err := rc.createIoTSecurityProfile(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTCustomMetric:
		id, err := rc.createIoTCustomMetric(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTFleetMetric:
		id, err := rc.createIoTFleetMetric(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTBillingGroup:
		id, err := rc.createIoTBillingGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTMitigationAction:
		id, err := rc.createIoTMitigationAction(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIoTScheduledAudit:
		id, err := rc.createIoTScheduledAudit(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteIoTMoreResource handles deletion for the types described in
// createIoTMoreResource.
func (rc *ResourceCreator) deleteIoTMoreResource(resourceType, physicalID string) (bool, error) {
	if handled, err := rc.deleteIoTIdentityResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteIoTFleetResource(resourceType, physicalID)
}

// deleteIoTIdentityResource handles deletion for the types described in
// createIoTIdentityResource.
func (rc *ResourceCreator) deleteIoTIdentityResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeIoTThingType:
		return true, rc.deleteIoTThingType(physicalID)
	case resTypeIoTThingGroup:
		return true, rc.deleteIoTThingGroup(physicalID)
	case resTypeIoTPolicy:
		return true, rc.deleteIoTPolicy(physicalID)
	case resTypeIoTTopicRuleDestination:
		return true, rc.deleteIoTTopicRuleDestination(physicalID)
	case resTypeIoTRoleAlias:
		return true, rc.deleteIoTRoleAlias(physicalID)
	case resTypeIoTCertificate:
		return true, rc.deleteIoTCertificate(physicalID)
	case resTypeIoTProvisioningTemplate:
		return true, rc.deleteIoTProvisioningTemplate(physicalID)
	case resTypeIoTAuthorizer:
		return true, rc.deleteIoTAuthorizer(physicalID)
	default:
		return false, nil
	}
}

// deleteIoTFleetResource handles deletion for the types described in
// createIoTFleetResource.
func (rc *ResourceCreator) deleteIoTFleetResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeIoTDomainConfiguration:
		return true, rc.deleteIoTDomainConfiguration(physicalID)
	case resTypeIoTJobTemplate:
		return true, rc.deleteIoTJobTemplate(physicalID)
	case resTypeIoTDimension:
		return true, rc.deleteIoTDimension(physicalID)
	case resTypeIoTSecurityProfile:
		return true, rc.deleteIoTSecurityProfile(physicalID)
	case resTypeIoTCustomMetric:
		return true, rc.deleteIoTCustomMetric(physicalID)
	case resTypeIoTFleetMetric:
		return true, rc.deleteIoTFleetMetric(physicalID)
	case resTypeIoTBillingGroup:
		return true, rc.deleteIoTBillingGroup(physicalID)
	case resTypeIoTMitigationAction:
		return true, rc.deleteIoTMitigationAction(physicalID)
	case resTypeIoTScheduledAudit:
		return true, rc.deleteIoTScheduledAudit(physicalID)
	default:
		return false, nil
	}
}

// ---- IoT ThingType ----
// Ref returns ThingTypeId; ThingTypeArn is not derivable from the ID alone
// (it embeds the name), so it's stashed for Fn::GetAtt.

func (rc *ResourceCreator) createIoTThingType(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ThingTypeName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	ttProps, _ := props["ThingTypeProperties"].(map[string]any)

	out, err := rc.backends.IoT.Backend.CreateThingType(&iotbackend.CreateThingTypeInput{
		ThingTypeName:        name,
		Description:          strProp(ttProps, "ThingTypeDescription", params, physicalIDs),
		SearchableAttributes: strSliceProp(ttProps["SearchableAttributes"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT thing type %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = out.ThingTypeARN

	if boolProp(props, "DeprecateThingType") {
		deprecateErr := rc.backends.IoT.Backend.DeprecateThingType(&iotbackend.DeprecateThingTypeInput{
			ThingTypeName: name,
		})
		if deprecateErr != nil {
			return "", fmt.Errorf("deprecate IoT thing type %s: %w", name, deprecateErr)
		}
	}

	return out.ThingTypeID, nil
}

// deleteIoTThingType looks the thing type up by ID (physicalID) via
// ListThingTypes, since DeleteThingType takes the name, not the ID; real AWS
// also requires a thing type to be deprecated before it can be deleted.
func (rc *ResourceCreator) deleteIoTThingType(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	name := ""

	for _, tt := range rc.backends.IoT.Backend.ListThingTypes() {
		if tt.ThingTypeID == physicalID {
			name = tt.ThingTypeName

			break
		}
	}

	if name == "" {
		return nil
	}

	if err := rc.backends.IoT.Backend.DeprecateThingType(&iotbackend.DeprecateThingTypeInput{
		ThingTypeName: name,
	}); err != nil {
		return fmt.Errorf("deprecate IoT thing type %s: %w", name, err)
	}

	return rc.backends.IoT.Backend.DeleteThingType(name)
}

// ---- IoT ThingGroup ----
// Ref returns ThingGroupId; ThingGroupArn is stashed for the same reason as ThingType.

func (rc *ResourceCreator) createIoTThingGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ThingGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	tgProps, _ := props["ThingGroupProperties"].(map[string]any)
	attrPayload, _ := tgProps["AttributePayload"].(map[string]any)

	input := &iotbackend.CreateThingGroupInput{
		ThingGroupName:  name,
		ParentGroupName: strProp(props, "ParentGroupName", params, physicalIDs),
		Description:     strProp(tgProps, "ThingGroupDescription", params, physicalIDs),
		QueryString:     strProp(props, "QueryString", params, physicalIDs),
		Attributes:      stringMapProp(attrPayload["Attributes"], params, physicalIDs),
	}

	var (
		out *iotbackend.ThingGroup
		err error
	)

	if input.QueryString != "" {
		out, err = rc.backends.IoT.Backend.CreateDynamicThingGroup(input)
	} else {
		out, err = rc.backends.IoT.Backend.CreateThingGroup(input)
	}

	if err != nil {
		return "", fmt.Errorf("create IoT thing group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = out.ThingGroupARN

	return out.ThingGroupID, nil
}

// deleteIoTThingGroup looks the group up by ID via ListThingGroups, same
// reason as deleteIoTThingType.
func (rc *ResourceCreator) deleteIoTThingGroup(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	for _, tg := range rc.backends.IoT.Backend.ListThingGroups() {
		if tg.ThingGroupID != physicalID {
			continue
		}

		if tg.IsDynamic {
			return rc.backends.IoT.Backend.DeleteDynamicThingGroup(tg.ThingGroupName, 0)
		}

		return rc.backends.IoT.Backend.DeleteThingGroup(tg.ThingGroupName, 0)
	}

	return nil
}

// ---- IoT Policy ----
// Ref returns the policy name; Arn is deterministic ("policy/"+name).

func (rc *ResourceCreator) createIoTPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "PolicyName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreatePolicy(&iotbackend.CreatePolicyInput{
		PolicyName:     name,
		PolicyDocument: jsonProp(props, "PolicyDocument"),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT policy %s: %w", name, err)
	}

	return out.PolicyName, nil
}

func (rc *ResourceCreator) deleteIoTPolicy(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	err := rc.backends.IoT.Backend.DeletePolicy(physicalID)
	if errors.Is(err, iotbackend.ErrPolicyNotFound) {
		return nil
	}

	return err
}

// ---- IoT TopicRuleDestination ----
// Ref returns the destination ARN, and physicalID IS that ARN, so Arn/Id
// resolve to physID via the default fallback; only StatusReason (never set by
// this backend) needs a special case.

func (rc *ResourceCreator) createIoTTopicRuleDestination(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	cfg := &iotbackend.TopicRuleDestinationConfiguration{}

	if httpProps, ok := props["HttpUrlProperties"].(map[string]any); ok {
		cfg.HTTPURLConfiguration = &iotbackend.HTTPURLDestinationConfiguration{
			ConfirmationURL: strProp(httpProps, "ConfirmationUrl", params, physicalIDs),
		}
	}

	if vpcProps, ok := props["VpcProperties"].(map[string]any); ok {
		cfg.VPCConfiguration = &iotbackend.VPCDestinationConfiguration{
			RoleARN:        strProp(vpcProps, "RoleArn", params, physicalIDs),
			VpcID:          strProp(vpcProps, "VpcId", params, physicalIDs),
			SecurityGroups: strSliceProp(vpcProps["SecurityGroups"], params, physicalIDs),
			SubnetIDs:      strSliceProp(vpcProps["SubnetIds"], params, physicalIDs),
		}
	}

	out, err := rc.backends.IoT.Backend.CreateTopicRuleDestination(
		&iotbackend.CreateTopicRuleDestinationInput{DestinationConfiguration: cfg},
	)
	if err != nil {
		return "", fmt.Errorf("create IoT topic rule destination for %s: %w", logicalID, err)
	}

	return out.ARN, nil
}

func (rc *ResourceCreator) deleteIoTTopicRuleDestination(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteTopicRuleDestination(physicalID)
}

// ---- IoT RoleAlias ----
// Ref returns the role alias name; RoleAliasArn is deterministic.

func (rc *ResourceCreator) createIoTRoleAlias(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	alias := strProp(props, "RoleAlias", params, physicalIDs)
	if alias == "" {
		alias = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateRoleAlias(&iotbackend.CreateRoleAliasInput{
		RoleAlias:                 alias,
		RoleARN:                   strProp(props, "RoleArn", params, physicalIDs),
		CredentialDurationSeconds: int(int32Prop(props, "CredentialDurationSeconds", params, physicalIDs)),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT role alias %s: %w", alias, err)
	}

	return out.RoleAlias, nil
}

func (rc *ResourceCreator) deleteIoTRoleAlias(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteRoleAlias(physicalID)
}

// ---- IoT Certificate ----
// Ref returns the certificate ID; Arn is deterministic ("cert/"+id).

func (rc *ResourceCreator) createIoTCertificate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	status := strProp(props, "Status", params, physicalIDs)

	var (
		cert *iotbackend.Certificate
		err  error
	)

	switch {
	case strProp(props, "CertificateSigningRequest", params, physicalIDs) != "":
		cert, err = rc.backends.IoT.Backend.CreateCertificateFromCsr(&iotbackend.CreateCertificateFromCsrInput{
			CertificateSigningRequest: strProp(props, "CertificateSigningRequest", params, physicalIDs),
			SetAsActive:               status == statusActive,
		})
	case strProp(props, "CertificateMode", params, physicalIDs) == "SNI_ONLY":
		cert, err = rc.backends.IoT.Backend.RegisterCertificateWithoutCA(&iotbackend.RegisterCertificateInput{
			CertificatePem: strProp(props, "CertificatePem", params, physicalIDs),
			Status:         status,
		})
	default:
		cert, err = rc.backends.IoT.Backend.RegisterCertificate(&iotbackend.RegisterCertificateInput{
			CertificatePem: strProp(props, "CertificatePem", params, physicalIDs),
			Status:         status,
		})
	}

	if err != nil {
		return "", fmt.Errorf("create IoT certificate for %s: %w", logicalID, err)
	}

	return cert.CertificateID, nil
}

// deleteIoTCertificate deactivates the certificate before deleting -- real
// AWS IoT (and this backend) rejects deleting an ACTIVE certificate.
func (rc *ResourceCreator) deleteIoTCertificate(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	_ = rc.backends.IoT.Backend.UpdateCertificate(&iotbackend.UpdateCertificateInput{
		CertificateID: physicalID,
		NewStatus:     "INACTIVE",
	})

	err := rc.backends.IoT.Backend.DeleteCertificate(physicalID)
	if errors.Is(err, iotbackend.ErrCertificateNotFound) {
		return nil
	}

	return err
}

// ---- IoT ProvisioningTemplate ----
// Ref returns the template name; TemplateArn is deterministic.

func (rc *ResourceCreator) createIoTProvisioningTemplate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "TemplateName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateProvisioningTemplate(&iotbackend.CreateProvisioningTemplateInput{
		TemplateName:        name,
		Description:         strProp(props, "Description", params, physicalIDs),
		TemplateBody:        strProp(props, "TemplateBody", params, physicalIDs),
		ProvisioningRoleARN: strProp(props, "ProvisioningRoleArn", params, physicalIDs),
		Type:                strProp(props, "TemplateType", params, physicalIDs),
		Enabled:             boolProp(props, "Enabled"),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT provisioning template %s: %w", name, err)
	}

	return out.TemplateName, nil
}

func (rc *ResourceCreator) deleteIoTProvisioningTemplate(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteProvisioningTemplate(physicalID)
}

// ---- IoT Authorizer ----
// Ref returns the authorizer name; Arn is deterministic.

func (rc *ResourceCreator) createIoTAuthorizer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "AuthorizerName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateAuthorizer(&iotbackend.CreateAuthorizerInput{
		AuthorizerName:        name,
		AuthorizerFunctionARN: strProp(props, "AuthorizerFunctionArn", params, physicalIDs),
		TokenKeyName:          strProp(props, "TokenKeyName", params, physicalIDs),
		Status:                strProp(props, "Status", params, physicalIDs),
		SigningDisabled:       boolProp(props, "SigningDisabled"),
		EnableCachingForHTTP:  boolProp(props, "EnableCachingForHttp"),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT authorizer %s: %w", name, err)
	}

	return out.AuthorizerName, nil
}

func (rc *ResourceCreator) deleteIoTAuthorizer(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteAuthorizer(physicalID)
}

// ---- IoT DomainConfiguration ----
// Ref returns the name; Arn is deterministic. DomainType/ServerCertificates
// aren't tracked on the backend struct, so they're stashed from the input
// props (echoing what AWS itself would report back for a declared config).

func (rc *ResourceCreator) createIoTDomainConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DomainConfigurationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateDomainConfiguration(&iotbackend.CreateDomainConfigurationInput{
		DomainConfigurationName: name,
		DomainName:              strProp(props, "DomainName", params, physicalIDs),
		ServiceType:             strProp(props, "ServiceType", params, physicalIDs),
		ApplicationProtocol:     strProp(props, "ApplicationProtocol", params, physicalIDs),
		AuthenticationType:      strProp(props, "AuthenticationType", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT domain configuration %s: %w", name, err)
	}

	domainType := "AWS_MANAGED"
	if out.DomainName != "" {
		domainType = "CUSTOMER_MANAGED"
	}

	physicalIDs[logicalID+"/DomainType"] = domainType
	physicalIDs[logicalID+"/ServerCertificates"] = strings.Join(
		strSliceProp(props["ServerCertificateArns"], params, physicalIDs), ",",
	)

	return out.DomainConfigurationName, nil
}

func (rc *ResourceCreator) deleteIoTDomainConfiguration(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteDomainConfiguration(physicalID)
}

// ---- IoT JobTemplate ----
// Ref returns JobTemplateId (a required CFN property, so it's a stable
// physical ID); Arn is deterministic ("jobtemplate/"+id).

func (rc *ResourceCreator) createIoTJobTemplate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "JobTemplateId", params, physicalIDs)
	if id == "" {
		id = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateJobTemplate(&iotbackend.CreateJobTemplateInput{
		JobTemplateID:  id,
		Description:    strProp(props, "Description", params, physicalIDs),
		Document:       strProp(props, "Document", params, physicalIDs),
		DocumentSource: strProp(props, "DocumentSource", params, physicalIDs),
		JobARN:         strProp(props, "JobArn", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT job template %s: %w", id, err)
	}

	return out.JobTemplateID, nil
}

func (rc *ResourceCreator) deleteIoTJobTemplate(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteJobTemplate(physicalID)
}

// ---- IoT Dimension ----
// Ref returns the name; Arn is deterministic.

func (rc *ResourceCreator) createIoTDimension(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateDimension(&iotbackend.CreateDimensionInput{
		Name:         name,
		Type:         strProp(props, "Type", params, physicalIDs),
		StringValues: strSliceProp(props["StringValues"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT dimension %s: %w", name, err)
	}

	return out.Name, nil
}

func (rc *ResourceCreator) deleteIoTDimension(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteDimension(physicalID)
}

// ---- IoT SecurityProfile ----
// Ref returns the security profile name; SecurityProfileArn is deterministic
// via the backend's own SecurityProfileARN builder.

func (rc *ResourceCreator) createIoTSecurityProfile(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "SecurityProfileName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateSecurityProfile(&iotbackend.CreateSecurityProfileInput{
		SecurityProfileName:        name,
		SecurityProfileDescription: strProp(props, "SecurityProfileDescription", params, physicalIDs),
		AdditionalMetricsToRetain:  strSliceProp(props["AdditionalMetricsToRetain"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT security profile %s: %w", name, err)
	}

	return out.SecurityProfileName, nil
}

func (rc *ResourceCreator) deleteIoTSecurityProfile(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteSecurityProfile(physicalID, 0)
}

// ---- IoT CustomMetric ----
// Ref returns the metric name; MetricArn is deterministic.

func (rc *ResourceCreator) createIoTCustomMetric(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "MetricName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateCustomMetric(&iotbackend.CreateCustomMetricInput{
		MetricName:  name,
		MetricType:  strProp(props, "MetricType", params, physicalIDs),
		DisplayName: strProp(props, "DisplayName", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT custom metric %s: %w", name, err)
	}

	return out.MetricName, nil
}

func (rc *ResourceCreator) deleteIoTCustomMetric(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteCustomMetric(physicalID)
}

// ---- IoT FleetMetric ----
// Ref returns the metric name; MetricArn is deterministic. CreationDate/
// LastModifiedDate/Version are backend-generated and stashed.

func (rc *ResourceCreator) createIoTFleetMetric(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "MetricName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var aggType *iotbackend.AggregationType
	if at, ok := props["AggregationType"].(map[string]any); ok {
		aggType = &iotbackend.AggregationType{
			Name:   strProp(at, "Name", params, physicalIDs),
			Values: strSliceProp(at["Values"], params, physicalIDs),
		}
	}

	out, err := rc.backends.IoT.Backend.CreateFleetMetric(&iotbackend.CreateFleetMetricInput{
		MetricName:       name,
		QueryString:      strProp(props, "QueryString", params, physicalIDs),
		IndexName:        strProp(props, "IndexName", params, physicalIDs),
		Description:      strProp(props, "Description", params, physicalIDs),
		AggregationField: strProp(props, "AggregationField", params, physicalIDs),
		AggregationType:  aggType,
		Unit:             strProp(props, "Unit", params, physicalIDs),
		Period:           int32Prop(props, "Period", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT fleet metric %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CreationDate"] = strconv.FormatFloat(out.CreationDate, 'f', -1, 64)
	physicalIDs[logicalID+"/LastModifiedDate"] = strconv.FormatFloat(out.LastModified, 'f', -1, 64)
	physicalIDs[logicalID+"/Version"] = strconv.FormatInt(out.Version, 10)

	return out.MetricName, nil
}

func (rc *ResourceCreator) deleteIoTFleetMetric(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteFleetMetric(physicalID, 0)
}

// ---- IoT BillingGroup ----
// Ref returns BillingGroupId; Arn is stashed (embeds the name, not the ID).

func (rc *ResourceCreator) createIoTBillingGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "BillingGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	bgProps, _ := props["BillingGroupProperties"].(map[string]any)

	out, err := rc.backends.IoT.Backend.CreateBillingGroup(&iotbackend.CreateBillingGroupInput{
		BillingGroupName: name,
		BillingGroupProperties: iotbackend.BillingGroupProperties{
			BillingGroupDescription: strProp(bgProps, "BillingGroupDescription", params, physicalIDs),
		},
	})
	if err != nil {
		return "", fmt.Errorf("create IoT billing group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = out.BillingGroupARN

	return out.BillingGroupID, nil
}

// deleteIoTBillingGroup looks the group up by ID via ListBillingGroups, same
// reason as deleteIoTThingType.
func (rc *ResourceCreator) deleteIoTBillingGroup(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	for _, bg := range rc.backends.IoT.Backend.ListBillingGroups() {
		if bg.BillingGroupID == physicalID {
			return rc.backends.IoT.Backend.DeleteBillingGroup(bg.BillingGroupName, 0)
		}
	}

	return nil
}

// ---- IoT MitigationAction ----
// Ref returns the action name; MitigationActionArn is deterministic.
// MitigationActionId is backend-generated and stashed.

func (rc *ResourceCreator) createIoTMitigationAction(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ActionName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	actionParams, _ := props["ActionParams"].(map[string]any)

	out, err := rc.backends.IoT.Backend.CreateMitigationAction(&iotbackend.CreateMitigationActionInput{
		ActionName:   name,
		RoleARN:      strProp(props, "RoleArn", params, physicalIDs),
		ActionParams: actionParams,
	})
	if err != nil {
		return "", fmt.Errorf("create IoT mitigation action %s: %w", name, err)
	}

	physicalIDs[logicalID+"/MitigationActionId"] = out.ActionID

	return out.ActionName, nil
}

func (rc *ResourceCreator) deleteIoTMitigationAction(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteMitigationAction(physicalID)
}

// ---- IoT ScheduledAudit ----
// Ref returns the name; ScheduledAuditArn is deterministic.

func (rc *ResourceCreator) createIoTScheduledAudit(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ScheduledAuditName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateScheduledAudit(&iotbackend.CreateScheduledAuditInput{
		ScheduledAuditName: name,
		Frequency:          strProp(props, "Frequency", params, physicalIDs),
		DayOfMonth:         strProp(props, "DayOfMonth", params, physicalIDs),
		DayOfWeek:          strProp(props, "DayOfWeek", params, physicalIDs),
		TargetCheckNames:   strSliceProp(props["TargetCheckNames"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create IoT scheduled audit %s: %w", name, err)
	}

	return out.ScheduledAuditName, nil
}

func (rc *ResourceCreator) deleteIoTScheduledAudit(physicalID string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	return rc.backends.IoT.Backend.DeleteScheduledAudit(physicalID)
}

// iotDeterministicArn maps a resType to the GetAtt attribute name whose value
// is a deterministic function of the resource's name (physID) -- the ARN
// suffix a resource's own backend ARN-builder uses (e.g. roleAliasARN,
// authorizerARN). Every other attribute on these types falls back to physID.
//
//nolint:gochecknoglobals // static lookup table, analogous to yamlShortFormTags (template.go)
var iotDeterministicArn = map[string]struct{ attrName, arnInfix string }{
	resTypeIoTPolicy:               {attrNameArn, "policy/"},
	resTypeIoTRoleAlias:            {"RoleAliasArn", "rolealias/"},
	resTypeIoTCertificate:          {attrNameArn, "cert/"},
	resTypeIoTProvisioningTemplate: {"TemplateArn", "provisioningtemplate/"},
	resTypeIoTAuthorizer:           {attrNameArn, "authorizer/"},
	resTypeIoTDomainConfiguration:  {attrNameArn, "domainconfiguration/"},
	resTypeIoTJobTemplate:          {attrNameArn, "jobtemplate/"},
	resTypeIoTDimension:            {attrNameArn, "dimension/"},
	resTypeIoTSecurityProfile:      {"SecurityProfileArn", "securityprofile/"},
	resTypeIoTCustomMetric:         {"MetricArn", "custommetric/"},
	resTypeIoTFleetMetric:          {"MetricArn", "fleetmetric/"},
	resTypeIoTMitigationAction:     {"MitigationActionArn", "mitigationaction/"},
	resTypeIoTScheduledAudit:       {"ScheduledAuditArn", "scheduledaudit/"},
}

// getIoTMoreAttribute derives Fn::GetAtt values for the IoT types handled by
// this file. ARNs that are a deterministic function of the resource's name
// are computed directly (matching the backend's own ARN builders); values
// that aren't derivable from physID alone were stashed at create time.
func getIoTMoreAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	if resType == resTypeIoTTopicRuleDestination {
		if attrName == "StatusReason" {
			return "", true
		}

		return physID, true
	}

	cfg, ok := iotDeterministicArn[resType]
	if !ok {
		return "", false
	}

	if attrName == cfg.attrName {
		return arn.Build("iot", region, accountID, cfg.arnInfix+physID), true
	}

	return physID, true
}

// stringMapProp reads a property that is a map of string keys to
// string-or-Ref values, resolving each value.
func stringMapProp(v any, params, physicalIDs map[string]string) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = resolve(val, params, physicalIDs)
	}

	return out
}
