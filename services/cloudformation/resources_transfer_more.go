package cloudformation

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

const (
	resTypeTransferProfile  = "AWS::Transfer::Profile"
	resTypeTransferWorkflow = "AWS::Transfer::Workflow"
)

// createTransferMoreResource handles the Transfer resource types listed
// above. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createTransferMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeTransferProfile:
		id, err := rc.createTransferProfile(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeTransferWorkflow:
		id, err := rc.createTransferWorkflow(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteTransferMoreResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteTransferMoreResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.Transfer == nil {
		switch resourceType {
		case resTypeTransferProfile, resTypeTransferWorkflow:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeTransferProfile:
		return true, rc.backends.Transfer.Backend.DeleteProfile(resourceNameFromARN(physicalID))
	case resTypeTransferWorkflow:
		return true, rc.backends.Transfer.Backend.DeleteWorkflow(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Transfer::Profile ----
// Ref returns the profile ARN (documented).

func (rc *ResourceCreator) createTransferProfile(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Transfer == nil {
		return logicalID + "-stub", nil
	}

	p, err := rc.backends.Transfer.Backend.CreateProfile(
		strProp(props, "ProfileType", params, physicalIDs),
		strProp(props, "As2Id", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Transfer profile: %w", err)
	}

	profileArn := transferProfileARN(rc.backends.Region, rc.backends.AccountID, p.ProfileID)
	physicalIDs[logicalID+"/Arn"] = profileArn
	physicalIDs[logicalID+"/ProfileId"] = p.ProfileID

	return profileArn, nil
}

// ---- AWS::Transfer::Workflow ----
// Ref is undocumented; the workflow ID (its only documented Fn::GetAtt
// attribute, and its natural primary identifier) is used. Steps is a
// required CFN property but this backend does not validate step count, so
// an empty Steps list (a template with no natural step to model) still
// creates a real, deletable workflow rather than being rejected as a stub.

func (rc *ResourceCreator) createTransferWorkflow(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Transfer == nil {
		return logicalID + "-stub", nil
	}

	steps := transferWorkflowSteps(props["Steps"], params, physicalIDs)
	onException := transferWorkflowSteps(props["OnExceptionSteps"], params, physicalIDs)

	w, err := rc.backends.Transfer.Backend.CreateWorkflow(
		strProp(props, "Description", params, physicalIDs),
		steps,
		onException,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Transfer workflow: %w", err)
	}

	physicalIDs[logicalID+"/WorkflowId"] = w.WorkflowID

	return w.WorkflowID, nil
}

// transferWorkflowSteps converts a CFN Steps/OnExceptionSteps property
// (a list of {Type, ...StepDetails} maps) into []transferbackend.WorkflowStep.
// Only Type is read; the various *StepDetails sub-objects are template
// authoring detail this backend's validateWorkflowSteps does not require.
func transferWorkflowSteps(v any, params, physicalIDs map[string]string) []transferbackend.WorkflowStep {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]transferbackend.WorkflowStep, 0, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, transferbackend.WorkflowStep{Type: strProp(m, "Type", params, physicalIDs)})
	}

	return out
}

// transferProfileARN mirrors the transfer package's unexported profileARN.
func transferProfileARN(region, accountID, profileID string) string {
	return arn.Build("transfer", region, accountID, "profile/"+profileID)
}
