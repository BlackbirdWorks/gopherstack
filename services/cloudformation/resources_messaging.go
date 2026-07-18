package cloudformation

import (
	"context"
	"fmt"
	"strings"

	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func (rc *ResourceCreator) createExtraMessagingResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SNS::TopicPolicy":
		id, err := rc.createSNSTopicPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEventsConnection:
		id, err := rc.createEventsConnection(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Events::Archive":
		id, err := rc.createEventsArchive(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeStepFunctionsActivity:
		id, err := rc.createStepFunctionsActivity(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteExtraMessagingResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::SNS::TopicPolicy":

		return true, nil // topic policy is an attribute on the topic; removed with the topic
	case resTypeEventsConnection:

		return true, rc.deleteEventsConnection(ctx, physicalID)
	case "AWS::Events::Archive":

		return true, rc.deleteEventsArchive(ctx, physicalID)
	case resTypeStepFunctionsActivity:

		return true, rc.deleteStepFunctionsActivity(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createSNSTopicPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}

	policyDoc := strProp(props, "PolicyDocument", params, physicalIDs)
	topicArns := strSliceProp(props["Topics"], params, physicalIDs)

	for _, topicArn := range topicArns {
		if topicArn == "" {
			continue
		}
		if err := rc.backends.SNS.Backend.SetTopicAttributes(topicArn, "Policy", policyDoc); err != nil {
			return "", fmt.Errorf("set SNS topic policy on %s: %w", topicArn, err)
		}
	}

	return logicalID, nil
}

func (rc *ResourceCreator) createEventsConnection(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	authType := strProp(props, "AuthorizationType", params, physicalIDs)
	if authType == "" {
		authType = cfnKeyTypeAPIKey
	}

	conn, err := rc.backends.EventBridge.Backend.CreateConnection(ctx, ebbackend.CreateConnectionInput{
		Name:              name,
		AuthorizationType: authType,
		Description:       strProp(props, "Description", params, physicalIDs),
		AuthParameters:    defaultConnectionAuthParameters(authType),
	})
	if err != nil {
		return "", fmt.Errorf("create EventBridge connection %s: %w", name, err)
	}

	return conn.Name, nil
}

func defaultConnectionAuthParameters(authType string) *ebbackend.ConnectionAuthParameters {
	if authType == cfnKeyTypeAPIKey {
		return &ebbackend.ConnectionAuthParameters{
			APIKeyAuthParameters: &ebbackend.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "cfn-managed",
			},
		}
	}

	return nil
}

func (rc *ResourceCreator) deleteEventsConnection(ctx context.Context, name string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	return rc.backends.EventBridge.Backend.DeleteConnection(ctx, name)
}

func (rc *ResourceCreator) createEventsArchive(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ArchiveName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	arch, err := rc.backends.EventBridge.Backend.CreateArchive(ctx, ebbackend.CreateArchiveInput{
		ArchiveName:    name,
		EventSourceArn: strProp(props, "SourceArn", params, physicalIDs),
		Description:    strProp(props, "Description", params, physicalIDs),
		EventPattern:   strProp(props, "EventPattern", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create EventBridge archive %s: %w", name, err)
	}

	return arch.ArchiveName, nil
}

func (rc *ResourceCreator) deleteEventsArchive(ctx context.Context, name string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	return rc.backends.EventBridge.Backend.DeleteArchive(ctx, name)
}

func (rc *ResourceCreator) createStepFunctionsActivity(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.StepFunctions == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	act, err := rc.backends.StepFunctions.Backend.CreateActivity(ctx, name)
	if err != nil {
		return "", fmt.Errorf("create Step Functions activity %s: %w", name, err)
	}

	return act.ActivityArn, nil
}

func (rc *ResourceCreator) deleteStepFunctionsActivity(activityArn string) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	return rc.backends.StepFunctions.Backend.DeleteActivity(activityArn)
}

// ---- Events supplemental ----

// createEventsSupplementalResource handles Events ApiDestination and EventBusPolicy
// resource creation.
func (rc *ResourceCreator) createEventsSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Events::ApiDestination":
		id, err := rc.createEventsAPIDestination(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Events::EventBusPolicy":
		id, err := rc.createEventsEventBusPolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEventsSupplementalResource handles Events ApiDestination and EventBusPolicy
// resource deletion.
func (rc *ResourceCreator) deleteEventsSupplementalResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::Events::ApiDestination":
		return true, rc.deleteEventsAPIDestination(ctx, physicalID)
	case "AWS::Events::EventBusPolicy":
		return true, nil // EventBusPolicy is additive; deletion is a no-op
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createEventsAPIDestination(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	httpMethod := strProp(props, "HttpMethod", params, physicalIDs)
	if httpMethod == "" {
		httpMethod = "POST"
	}
	dest, err := rc.backends.EventBridge.Backend.CreateAPIDestination(
		ctx,
		ebbackend.CreateAPIDestinationInput{
			Name:               name,
			ConnectionArn:      strProp(props, "ConnectionArn", params, physicalIDs),
			HTTPMethod:         httpMethod,
			InvocationEndpoint: strProp(props, "InvocationEndpoint", params, physicalIDs),
			Description:        strProp(props, "Description", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create Events API destination %s: %w", name, err)
	}

	return dest.APIDestinationArn, nil
}

func (rc *ResourceCreator) deleteEventsAPIDestination(
	ctx context.Context,
	physicalID string,
) error {
	if rc.backends.EventBridge == nil {
		return nil
	}
	// physicalID is the ARN; extract the name from the trailing segment.
	name := physicalID
	if idx := strings.LastIndex(physicalID, "/"); idx >= 0 {
		name = physicalID[idx+1:]
	}

	return rc.backends.EventBridge.Backend.DeleteAPIDestination(ctx, name)
}

func (rc *ResourceCreator) createEventsEventBusPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}
	eventBusName := strProp(props, "EventBusName", params, physicalIDs)
	if eventBusName == "" {
		eventBusName = "default"
	}
	statementID := strProp(props, "StatementId", params, physicalIDs)
	statement := strProp(props, "Statement", params, physicalIDs)
	policy := statement
	if policy == "" && statementID != "" {
		policy = fmt.Sprintf(
			`{"Version":"2012-10-17","Statement":[{"Sid":%q,`+
				`"Effect":"Allow","Principal":{"AWS":"*"},`+
				`"Action":"events:PutEvents","Resource":"*"}]}`,
			statementID,
		)
	}
	if err := rc.backends.EventBridge.Backend.PutEventBusPolicy(ctx, ebbackend.PutEventBusPolicyInput{
		EventBusName: eventBusName,
		Policy:       policy,
	}); err != nil {
		return "", fmt.Errorf("create Events event bus policy on %s: %w", eventBusName, err)
	}

	return eventBusName + ":" + statementID, nil
}
