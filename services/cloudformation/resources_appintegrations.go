package cloudformation

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	aibackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
)

const (
	resTypeAIApplication     = "AWS::AppIntegrations::Application"
	resTypeAIDataIntegration = "AWS::AppIntegrations::DataIntegration"
	resTypeAIEventIntegation = "AWS::AppIntegrations::EventIntegration"

	aiServiceName = "app-integrations"
)

func (rc *ResourceCreator) createAppIntegrationsResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	var create func(context.Context, string, map[string]any, map[string]string, map[string]string) (string, error)

	switch resourceType {
	case resTypeAIApplication:
		create = rc.createAIApplication
	case resTypeAIDataIntegration:
		create = rc.createAIDataIntegration
	case resTypeAIEventIntegation:
		create = rc.createAIEventIntegration
	default:
		return "", false, nil
	}

	id, err := create(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

func (rc *ResourceCreator) deleteAppIntegrationsResource(resourceType, physicalID string) (bool, error) {
	var del func(*aibackend.InMemoryBackend, string) error

	switch resourceType {
	case resTypeAIApplication:
		del = func(be *aibackend.InMemoryBackend, id string) error { return be.DeleteApplication(id, true) }
	case resTypeAIDataIntegration:
		del = (*aibackend.InMemoryBackend).DeleteDataIntegration
	case resTypeAIEventIntegation:
		del = (*aibackend.InMemoryBackend).DeleteEventIntegration
	default:
		return false, nil
	}

	if rc.backends.AppIntegrations == nil {
		return true, nil
	}

	return true, ignoreRESTNotFound(del(rc.backends.AppIntegrations.Backend, physicalID))
}

// createAIApplication provisions an AWS::AppIntegrations::Application. Ref and GetAtt ApplicationArn return the
// ARN; GetAtt Id is the ARN's trailing UUID.
func (rc *ResourceCreator) createAIApplication(
	ctx context.Context, logicalID string, props map[string]any, params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppIntegrations == nil {
		return logicalID + "-stub", nil
	}

	var in aibackend.CreateApplicationInput
	if err := decodeCFNProps(&in, props, params, physicalIDs); err != nil {
		return "", fmt.Errorf("create AppIntegrations application %s: %w", logicalID, err)
	}

	app, err := rc.backends.AppIntegrations.Backend.CreateApplication(ctx, &in)
	if err != nil {
		return "", fmt.Errorf("create AppIntegrations application %s: %w", in.Name, err)
	}

	return app.Arn, nil
}

// createAIDataIntegration provisions an AWS::AppIntegrations::DataIntegration. Ref and GetAtt Id return the ID.
func (rc *ResourceCreator) createAIDataIntegration(
	ctx context.Context, logicalID string, props map[string]any, params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppIntegrations == nil {
		return logicalID + "-stub", nil
	}

	var in aibackend.CreateDataIntegrationInput
	if err := decodeCFNProps(&in, props, params, physicalIDs, "ScheduleConfig"); err != nil {
		return "", fmt.Errorf("create AppIntegrations data integration %s: %w", logicalID, err)
	}

	if sched := asMap(props["ScheduleConfig"]); sched != nil {
		in.ScheduleConfig = &aibackend.ScheduleConfiguration{}
		if err := decodeCFNProps(in.ScheduleConfig, sched, params, physicalIDs); err != nil {
			return "", fmt.Errorf("create AppIntegrations data integration %s: %w", logicalID, err)
		}
	}

	d, err := rc.backends.AppIntegrations.Backend.CreateDataIntegration(ctx, &in)
	if err != nil {
		return "", fmt.Errorf("create AppIntegrations data integration %s: %w", in.Name, err)
	}

	return d.ID, nil
}

// createAIEventIntegration provisions an AWS::AppIntegrations::EventIntegration. Ref returns the Name.
func (rc *ResourceCreator) createAIEventIntegration(
	ctx context.Context, logicalID string, props map[string]any, params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppIntegrations == nil {
		return logicalID + "-stub", nil
	}

	var in aibackend.CreateEventIntegrationInput
	if err := decodeCFNProps(&in, props, params, physicalIDs); err != nil {
		return "", fmt.Errorf("create AppIntegrations event integration %s: %w", logicalID, err)
	}

	e, err := rc.backends.AppIntegrations.Backend.CreateEventIntegration(ctx, &in)
	if err != nil {
		return "", fmt.Errorf("create AppIntegrations event integration %s: %w", in.Name, err)
	}

	return e.Name, nil
}

func (rc *ResourceCreator) updateAppIntegrationsResource(
	resourceType, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.AppIntegrations == nil {
		return nil
	}

	be := rc.backends.AppIntegrations.Backend

	switch resourceType {
	case resTypeAIApplication:
		return updateAIApplication(be, physicalID, props, params, physicalIDs)
	case resTypeAIDataIntegration:
		return updateAIDataIntegration(be, physicalID, props, params, physicalIDs)
	default:
		return updateAIEventIntegration(be, physicalID, props, params, physicalIDs)
	}
}

func updateAIApplication(
	be *aibackend.InMemoryBackend, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	var in aibackend.UpdateApplicationInput
	if err := decodeCFNProps(&in, props, params, physicalIDs, "Namespace", "Tags"); err != nil {
		return fmt.Errorf("update AppIntegrations application %s: %w", physicalID, err)
	}

	in.Arn = physicalID
	in.Description = new(strProp(props, "Description", params, physicalIDs))

	namespace := strProp(props, "Namespace", params, physicalIDs)
	if err := be.CFNUpdateApplication(&in, namespace); err != nil {
		return fmt.Errorf("update AppIntegrations application %s: %w", physicalID, err)
	}

	return syncAITags(be, physicalID, tagListProp(props, params, physicalIDs))
}

func updateAIDataIntegration(
	be *aibackend.InMemoryBackend, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	var in aibackend.CreateDataIntegrationInput
	if err := decodeCFNProps(&in, props, params, physicalIDs, "ScheduleConfig"); err != nil {
		return fmt.Errorf("update AppIntegrations data integration %s: %w", physicalID, err)
	}

	err := be.CFNUpdateDataIntegration(
		physicalID,
		new(in.Name),
		new(in.Description),
		in.FileConfiguration,
		in.ObjectConfiguration,
	)
	if err != nil {
		return fmt.Errorf("update AppIntegrations data integration %s: %w", physicalID, err)
	}

	d, err := be.GetDataIntegration(physicalID)
	if err != nil {
		return err
	}

	return syncAITags(be, d.Arn, tagListProp(props, params, physicalIDs))
}

func updateAIEventIntegration(
	be *aibackend.InMemoryBackend, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	description := strProp(props, "Description", params, physicalIDs)
	if err := be.UpdateEventIntegration(physicalID, &description); err != nil {
		return fmt.Errorf("update AppIntegrations event integration %s: %w", physicalID, err)
	}

	e, err := be.GetEventIntegration(physicalID)
	if err != nil {
		return err
	}

	return syncAITags(be, e.EventIntegrationArn, tagListProp(props, params, physicalIDs))
}

func syncAITags(be *aibackend.InMemoryBackend, resourceARN string, want map[string]string) error {
	current, err := be.ListTagsForResource(resourceARN)
	if err != nil {
		return err
	}

	return syncTagSet(
		current,
		want,
		func(m map[string]string) error { return be.TagResource(resourceARN, m) },
		func(keys []string) error { return be.UntagResource(resourceARN, keys) },
	)
}

func appIntegrationsRecreation(resourceType, propName string) string {
	switch resourceType {
	case resTypeAIDataIntegration:
		return alwaysIfOneOf(propName, "ScheduleConfig", "SourceURI", "KmsKey")
	case resTypeAIEventIntegation:
		return alwaysIfOneOf(propName, "Name", "EventBridgeBus", "EventFilter")
	default:
		return recreateNever
	}
}

// getAppIntegrationsAttribute derives the GetAtt values from the physical ID (ARN, ID or Name).
func getAppIntegrationsAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeAIApplication:
		if attrName == "Id" {
			return lastPathSegment(physID), true
		}

		return physID, true
	case resTypeAIDataIntegration:
		if attrName == "DataIntegrationArn" {
			return arn.Build(aiServiceName, region, accountID, "data-integration/"+physID), true
		}

		return physID, true
	case resTypeAIEventIntegation:
		return arn.Build(aiServiceName, region, accountID, "event-integration/"+physID), true
	default:
		return "", false
	}
}

func lastPathSegment(s string) string {
	if _, after, ok := strings.CutLast(s, "/"); ok {
		return after
	}

	return s
}
