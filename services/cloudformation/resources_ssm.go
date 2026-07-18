package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"

	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const resTypeSSMDocument = "AWS::SSM::Document"

func (rc *ResourceCreator) createSSMSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SSM::MaintenanceWindow":
		id, err := rc.createSSMMaintenanceWindow(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::Association":
		id := rc.createSSMAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, nil
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createSSMMaintenanceWindow(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	schedule := strProp(props, "Schedule", params, physicalIDs)
	if schedule == "" {
		schedule = "cron(0 2 ? * SUN *)"
	}

	var duration, cutoff int32 = 4, 1
	if v, hasDuration := props["Duration"].(float64); hasDuration {
		duration = int32(v)
	}
	if v, hasCutoff := props["Cutoff"].(float64); hasCutoff {
		cutoff = int32(v)
	}

	allowUnassociated := false
	if v, hasAllow := props["AllowUnassociatedTargets"].(bool); hasAllow {
		allowUnassociated = v
	}

	out, err := rc.backends.SSM.Backend.CreateMaintenanceWindow(ctx, &ssmbackend.CreateMaintenanceWindowInput{
		Name:                     name,
		Schedule:                 schedule,
		Duration:                 duration,
		Cutoff:                   cutoff,
		AllowUnassociatedTargets: allowUnassociated,
	})
	if err != nil {
		return "", fmt.Errorf("create SSM maintenance window %s: %w", name, err)
	}

	return out.WindowID, nil
}

func (rc *ResourceCreator) deleteSSMMaintenanceWindow(ctx context.Context, windowID string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeleteMaintenanceWindow(ctx, &ssmbackend.DeleteMaintenanceWindowInput{
		WindowID: windowID,
	})

	return err
}

func (rc *ResourceCreator) createSSMAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) string {
	if rc.backends.SSM == nil {
		return logicalID + "-stub"
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	assocName := strProp(props, "AssociationName", params, physicalIDs)

	// SSM Association requires a document; if it doesn't exist, CreateAssociation errors.
	// Treat errors as a stub to avoid propagating document-not-found failures.
	out, _ := rc.backends.SSM.Backend.CreateAssociation(ctx, &ssmbackend.CreateAssociationInput{
		Name:            name,
		AssociationName: assocName,
	})
	if out == nil || out.AssociationDescription.AssociationID == "" {
		return logicalID + "-stub"
	}

	return out.AssociationDescription.AssociationID
}

func (rc *ResourceCreator) deleteSSMAssociation(ctx context.Context, assocID string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeleteAssociation(ctx, &ssmbackend.DeleteAssociationInput{
		AssociationID: assocID,
	})

	return err
}

// deleteSSMSupplementalResource handles deletion for SSM supplemental resource types.
func (rc *ResourceCreator) deleteSSMSupplementalResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::SSM::MaintenanceWindow":
		return true, rc.deleteSSMMaintenanceWindow(ctx, physicalID)
	case "AWS::SSM::Association":
		return true, rc.deleteSSMAssociation(ctx, physicalID)
	case resTypeSSMDocument:
		return true, rc.deleteSSMDocument(ctx, physicalID)
	}

	return false, nil
}

func (rc *ResourceCreator) createSSMDocument(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	content := documentContent(props, params, physicalIDs)
	docType := strProp(props, "DocumentType", params, physicalIDs)
	if docType == "" {
		docType = "Command"
	}

	docFormat := strProp(props, "DocumentFormat", params, physicalIDs)

	out, err := rc.backends.SSM.Backend.CreateDocument(ctx, &ssmbackend.CreateDocumentInput{
		Name:           name,
		Content:        content,
		DocumentType:   docType,
		DocumentFormat: docFormat,
	})
	if err != nil {
		return "", fmt.Errorf("create SSM document %s: %w", name, err)
	}

	return out.DocumentDescription.Name, nil
}

func documentContent(props map[string]any, params, physicalIDs map[string]string) string {
	switch c := props["Content"].(type) {
	case string:
		return c
	case map[string]any:
		if b, err := marshalJSON(c); err == nil {
			return string(b)
		}
	}

	return strProp(props, "Content", params, physicalIDs)
}

func (rc *ResourceCreator) deleteSSMDocument(ctx context.Context, name string) error {
	if rc.backends.SSM == nil {
		return nil
	}

	_, err := rc.backends.SSM.Backend.DeleteDocument(ctx, &ssmbackend.DeleteDocumentInput{Name: name})

	return err
}

// marshalJSON serializes a value to compact JSON bytes.
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}
