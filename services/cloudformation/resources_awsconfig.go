package cloudformation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
)

const (
	resTypeConfigConfigRule               = "AWS::Config::ConfigRule"
	resTypeConfigConfigurationRecorder    = "AWS::Config::ConfigurationRecorder"
	resTypeConfigDeliveryChannel          = "AWS::Config::DeliveryChannel"
	resTypeConfigConfigurationAggregator  = "AWS::Config::ConfigurationAggregator"
	resTypeConfigAggregationAuthorization = "AWS::Config::AggregationAuthorization"
	resTypeConfigConformancePack          = "AWS::Config::ConformancePack"
	resTypeConfigRemediationConfiguration = "AWS::Config::RemediationConfiguration"
	resTypeConfigStoredQuery              = "AWS::Config::StoredQuery"
)

// errStoredQueryMissingAfterCreate indicates PutStoredQuery succeeded but a
// follow-up GetStoredQuery (needed to read back the generated QueryId) found
// nothing -- should never happen outside a race with a concurrent delete.
var errStoredQueryMissingAfterCreate = errors.New("stored query not found after create")

// awsConfigCreators maps each AWS::Config::* resource type to its create method.
//
//nolint:gochecknoglobals // static dispatch table, analogous to yamlShortFormTags (template.go)
var awsConfigCreators = map[string]resourceCreatorFunc{
	resTypeConfigConfigRule:               (*ResourceCreator).createConfigConfigRule,
	resTypeConfigConfigurationRecorder:    (*ResourceCreator).createConfigConfigurationRecorder,
	resTypeConfigDeliveryChannel:          (*ResourceCreator).createConfigDeliveryChannel,
	resTypeConfigConfigurationAggregator:  (*ResourceCreator).createConfigConfigurationAggregator,
	resTypeConfigAggregationAuthorization: (*ResourceCreator).createConfigAggregationAuthorization,
	resTypeConfigConformancePack:          (*ResourceCreator).createConfigConformancePack,
	resTypeConfigRemediationConfiguration: (*ResourceCreator).createConfigRemediationConfiguration,
	resTypeConfigStoredQuery:              (*ResourceCreator).createConfigStoredQuery,
}

// createAWSConfigResource handles the types listed on awsConfigCreators.
func (rc *ResourceCreator) createAWSConfigResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	return dispatchCreate(rc, awsConfigCreators, resourceType, logicalID, props, params, physicalIDs)
}

// deleteAWSConfigResource handles deletion for the types described in
// createAWSConfigResource.
func (rc *ResourceCreator) deleteAWSConfigResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeConfigConfigRule:
		return true, rc.deleteConfigConfigRule(physicalID)
	case resTypeConfigConfigurationRecorder:
		return true, rc.deleteConfigConfigurationRecorder(physicalID)
	case resTypeConfigDeliveryChannel:
		return true, rc.deleteConfigDeliveryChannel(physicalID)
	case resTypeConfigConfigurationAggregator:
		return true, rc.deleteConfigConfigurationAggregator(physicalID)
	case resTypeConfigAggregationAuthorization:
		return true, rc.deleteConfigAggregationAuthorization(physicalID)
	case resTypeConfigConformancePack:
		return true, rc.deleteConfigConformancePack(physicalID)
	case resTypeConfigRemediationConfiguration:
		return true, rc.deleteConfigRemediationConfiguration(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Config::ConfigRule ----
// Ref returns the rule name; Arn/ConfigRuleId are backend-generated and stashed.

func (rc *ResourceCreator) createConfigConfigRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ConfigRuleName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	rule := &awsconfigbackend.ConfigRule{
		ConfigRuleName:            name,
		Description:               strProp(props, "Description", params, physicalIDs),
		InputParameters:           jsonProp(props, "InputParameters"),
		MaximumExecutionFrequency: strProp(props, "MaximumExecutionFrequency", params, physicalIDs),
	}

	if src, ok := props["Source"].(map[string]any); ok {
		rule.Source = &awsconfigbackend.ConfigRuleSource{
			Owner:            strProp(src, "Owner", params, physicalIDs),
			SourceIdentifier: strProp(src, "SourceIdentifier", params, physicalIDs),
		}
	}

	if scope, ok := props["Scope"].(map[string]any); ok {
		rule.Scope = &awsconfigbackend.ConfigRuleScope{
			ComplianceResourceID:    strProp(scope, "ComplianceResourceId", params, physicalIDs),
			TagKey:                  strProp(scope, "TagKey", params, physicalIDs),
			TagValue:                strProp(scope, "TagValue", params, physicalIDs),
			ComplianceResourceTypes: strSliceProp(scope["ComplianceResourceTypes"], params, physicalIDs),
		}
	}

	if err := rc.backends.AWSConfig.Backend.PutConfigRule(rule); err != nil {
		return "", fmt.Errorf("put config rule %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = rule.ConfigRuleArn
	physicalIDs[logicalID+"/ConfigRuleId"] = rule.ConfigRuleID

	return name, nil
}

func (rc *ResourceCreator) deleteConfigConfigRule(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteConfigRule(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNoSuchConfigRule) {
		return nil
	}

	return err
}

// ---- AWS::Config::ConfigurationRecorder ----
// Ref returns the recorder name; no documented Fn::GetAtt attributes.

func (rc *ResourceCreator) createConfigConfigurationRecorder(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var group *awsconfigbackend.RecordingGroup
	if rg, ok := props["RecordingGroup"].(map[string]any); ok {
		group = &awsconfigbackend.RecordingGroup{
			ResourceTypes:              strSliceProp(rg["ResourceTypes"], params, physicalIDs),
			AllSupported:               boolProp(rg, "AllSupported"),
			IncludeGlobalResourceTypes: boolProp(rg, "IncludeGlobalResourceTypes"),
		}
	}

	err := rc.backends.AWSConfig.Backend.PutConfigurationRecorder(
		name, strProp(props, "RoleARN", params, physicalIDs), group,
	)
	if err != nil {
		return "", fmt.Errorf("put configuration recorder %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteConfigConfigurationRecorder(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteConfigurationRecorder(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Config::DeliveryChannel ----
// Ref returns the channel name; no documented Fn::GetAtt attributes.

func (rc *ResourceCreator) createConfigDeliveryChannel(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var snapshotProps *awsconfigbackend.DeliverySnapshotProperties
	if sp, ok := props["ConfigSnapshotDeliveryProperties"].(map[string]any); ok {
		snapshotProps = &awsconfigbackend.DeliverySnapshotProperties{
			DeliveryFrequency: strProp(sp, "DeliveryFrequency", params, physicalIDs),
		}
	}

	err := rc.backends.AWSConfig.Backend.PutDeliveryChannel(
		name,
		strProp(props, "S3BucketName", params, physicalIDs),
		strProp(props, "SnsTopicARN", params, physicalIDs),
		strProp(props, "S3KeyPrefix", params, physicalIDs),
		snapshotProps,
	)
	if err != nil {
		return "", fmt.Errorf("put delivery channel %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteConfigDeliveryChannel(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteDeliveryChannel(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNoSuchDeliveryChannel) {
		return nil
	}

	return err
}

// ---- AWS::Config::ConfigurationAggregator ----
// Ref returns the aggregator name; ConfigurationAggregatorArn is stashed.

func (rc *ResourceCreator) createConfigConfigurationAggregator(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ConfigurationAggregatorName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var sources []awsconfigbackend.AccountAggregationSource

	if raw, ok := props["AccountAggregationSources"].([]any); ok {
		for _, item := range raw {
			m, isMap := item.(map[string]any)
			if !isMap {
				continue
			}

			sources = append(sources, awsconfigbackend.AccountAggregationSource{
				AccountIDs:    strSliceProp(m["AccountIds"], params, physicalIDs),
				AwsRegions:    strSliceProp(m["AwsRegions"], params, physicalIDs),
				AllAwsRegions: boolProp(m, "AllAwsRegions"),
			})
		}
	}

	var orgSource *awsconfigbackend.OrganizationAggregationSource
	if os, ok := props["OrganizationAggregationSource"].(map[string]any); ok {
		orgSource = &awsconfigbackend.OrganizationAggregationSource{
			RoleArn:       strProp(os, "RoleArn", params, physicalIDs),
			AwsRegions:    strSliceProp(os["AwsRegions"], params, physicalIDs),
			AllAwsRegions: boolProp(os, "AllAwsRegions"),
		}
	}

	err := rc.backends.AWSConfig.Backend.PutConfigurationAggregator(name, sources, orgSource, nil)
	if err != nil {
		return "", fmt.Errorf("put configuration aggregator %s: %w", name, err)
	}

	agg, ok := rc.backends.AWSConfig.Backend.GetConfigurationAggregator(name)
	if ok {
		physicalIDs[logicalID+"/ConfigurationAggregatorArn"] = agg.ConfigurationAggregatorArn
	}

	return name, nil
}

func (rc *ResourceCreator) deleteConfigConfigurationAggregator(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteConfigurationAggregator(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNoSuchAggregator) {
		return nil
	}

	return err
}

// ---- AWS::Config::AggregationAuthorization ----
// Ref returns the authorization's ARN, which physicalID already is; delete
// parses AuthorizedAccountId/AuthorizedAwsRegion back out of it (the ARN's
// own suffix, matching how PutAggregationAuthorization built it).

func (rc *ResourceCreator) createConfigAggregationAuthorization(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	accountID := strProp(props, "AuthorizedAccountId", params, physicalIDs)
	region := strProp(props, "AuthorizedAwsRegion", params, physicalIDs)

	err := rc.backends.AWSConfig.Backend.PutAggregationAuthorization(accountID, region, nil)
	if err != nil {
		return "", fmt.Errorf("put aggregation authorization for %s/%s: %w", accountID, region, err)
	}

	authARN := arn.Build(
		"config", rc.backends.Region, rc.backends.AccountID,
		"aggregation-authorization/"+accountID+"/"+region,
	)

	return authARN, nil
}

func (rc *ResourceCreator) deleteConfigAggregationAuthorization(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	accountID, region, ok := parseAggregationAuthARN(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.AWSConfig.Backend.DeleteAggregationAuthorization(accountID, region)
}

const aggregationAuthARNInfix = "aggregation-authorization/"

// parseAggregationAuthARN extracts AuthorizedAccountId/AuthorizedAwsRegion
// from an "arn:...:aggregation-authorization/<accountId>/<region>" ARN.
func parseAggregationAuthARN(authARN string) (string, string, bool) {
	_, suffix, found := strings.Cut(authARN, aggregationAuthARNInfix)
	if !found {
		return "", "", false
	}

	accountID, region, found := strings.Cut(suffix, "/")
	if !found {
		return "", "", false
	}

	return accountID, region, true
}

// ---- AWS::Config::ConformancePack ----
// Ref returns the pack name; ConformancePackArn is stashed.

func (rc *ResourceCreator) createConfigConformancePack(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ConformancePackName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	err := rc.backends.AWSConfig.Backend.PutConformancePack(
		name,
		strProp(props, "DeliveryS3Bucket", params, physicalIDs),
		strProp(props, "DeliveryS3KeyPrefix", params, physicalIDs),
		strProp(props, "TemplateBody", params, physicalIDs),
		strProp(props, "TemplateS3Uri", params, physicalIDs),
		"",
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("put conformance pack %s: %w", name, err)
	}

	for _, pack := range rc.backends.AWSConfig.Backend.DescribeConformancePacks() {
		if pack.ConformancePackName == name {
			physicalIDs[logicalID+"/ConformancePackArn"] = pack.ConformancePackArn

			break
		}
	}

	return name, nil
}

func (rc *ResourceCreator) deleteConfigConformancePack(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteConformancePack(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNoSuchConformancePack) {
		return nil
	}

	return err
}

// ---- AWS::Config::RemediationConfiguration ----
// Ref returns the associated config rule name (this backend keys remediation
// configurations by ConfigRuleName, one per rule, matching real AWS Config).

func (rc *ResourceCreator) createConfigRemediationConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	ruleName := strProp(props, "ConfigRuleName", params, physicalIDs)
	if ruleName == "" {
		ruleName = logicalID
	}

	cfg := awsconfigbackend.RemediationConfiguration{
		ConfigRuleName:           ruleName,
		TargetType:               strProp(props, "TargetType", params, physicalIDs),
		TargetID:                 strProp(props, "TargetId", params, physicalIDs),
		TargetVersion:            strProp(props, "TargetVersion", params, physicalIDs),
		ResourceType:             strProp(props, "ResourceType", params, physicalIDs),
		Automatic:                boolProp(props, "Automatic"),
		MaximumAutomaticAttempts: int32Prop(props, "MaximumAutomaticAttempts", params, physicalIDs),
		RetryAttemptSeconds:      int64(int32Prop(props, "RetryAttemptSeconds", params, physicalIDs)),
		Parameters:               remediationParametersProp(props["Parameters"], params, physicalIDs),
	}

	if err := rc.backends.AWSConfig.Backend.PutRemediationConfigurations(
		[]awsconfigbackend.RemediationConfiguration{cfg},
	); err != nil {
		return "", fmt.Errorf("put remediation configuration for %s: %w", ruleName, err)
	}

	return ruleName, nil
}

func (rc *ResourceCreator) deleteConfigRemediationConfiguration(physicalID string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	err := rc.backends.AWSConfig.Backend.DeleteRemediationConfiguration(physicalID)
	if errors.Is(err, awsconfigbackend.ErrNoSuchRemediationConfiguration) {
		return nil
	}

	return err
}

// remediationParametersProp parses the CFN "Parameters" Json property (a map
// of parameter name to {StaticValue: {Values: [...]}} or
// {ResourceValue: {Value: "..."}}) into RemediationParameterValue.
func remediationParametersProp(
	v any, params, physicalIDs map[string]string,
) map[string]awsconfigbackend.RemediationParameterValue {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]awsconfigbackend.RemediationParameterValue, len(m))

	for name, raw := range m {
		entry, isMap := raw.(map[string]any)
		if !isMap {
			continue
		}

		var pv awsconfigbackend.RemediationParameterValue

		if sv, isStatic := entry["StaticValue"].(map[string]any); isStatic {
			pv.StaticValue = &awsconfigbackend.RemediationStaticValue{
				Values: strSliceProp(sv["Values"], params, physicalIDs),
			}
		}

		if rv, isResource := entry["ResourceValue"].(map[string]any); isResource {
			pv.ResourceValue = &awsconfigbackend.RemediationResourceValue{
				Value: strProp(rv, "Value", params, physicalIDs),
			}
		}

		out[name] = pv
	}

	return out
}

// ---- AWS::Config::StoredQuery ----
// Ref is undocumented in the CFN reference; the resource type schema's
// primary identifier is QueryId, so Ref returns that. QueryName (required,
// Replacement-triggering) is re-read from props at delete time since QueryId
// doesn't embed it.

func (rc *ResourceCreator) createConfigStoredQuery(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AWSConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "QueryName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	queryARN, err := rc.backends.AWSConfig.Backend.PutStoredQuery(
		name,
		strProp(props, "QueryDescription", params, physicalIDs),
		strProp(props, "QueryExpression", params, physicalIDs),
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("put stored query %s: %w", name, err)
	}

	physicalIDs[logicalID+"/QueryArn"] = queryARN

	q := rc.backends.AWSConfig.Backend.GetStoredQuery(name)
	if q == nil {
		return "", fmt.Errorf("%w: %s", errStoredQueryMissingAfterCreate, name)
	}

	return q.QueryID, nil
}

// deleteConfigStoredQuery is wired via deletePropsBasedResource (props.go)
// since it needs QueryName, not derivable from the QueryId physicalID.
func (rc *ResourceCreator) deleteConfigStoredQuery(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.AWSConfig == nil {
		return nil
	}

	name := strProp(props, "QueryName", nil, stackPhysicalIDs)
	if name == "" {
		return nil
	}

	return rc.backends.AWSConfig.Backend.DeleteStoredQuery(name)
}

// getAWSConfigAttribute derives Fn::GetAtt values for AWS::Config::* types.
// ConfigurationAggregator/ConformancePack ARNs are backend-generated (a
// counter, not a function of the name) and are resolved via the create-time
// physicalIDs stash (see the resolveGetAtt gate in template.go) rather than
// here; this fallback only covers the remaining always-physID attributes.
func getAWSConfigAttribute(resType, physID, _, _, _ string) (string, bool) {
	switch resType {
	case resTypeConfigConfigurationAggregator, resTypeConfigConformancePack,
		resTypeConfigAggregationAuthorization, resTypeConfigStoredQuery:
		return physID, true
	}

	return "", false
}

// getIoTOrConfigAttribute merges the IoT and AWS::Config GetAtt resolvers
// into a single chain link, keeping getExtraResourceAttribute's own
// cyclomatic complexity down.
func getIoTOrConfigAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	if v, ok := getIoTMoreAttribute(resType, physID, attrName, accountID, region); ok {
		return v, true
	}

	return getAWSConfigAttribute(resType, physID, attrName, accountID, region)
}
