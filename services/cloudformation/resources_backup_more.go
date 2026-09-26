package cloudformation

import (
	"errors"
	"fmt"
	"time"

	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
)

const (
	resTypeBackupFramework  = "AWS::Backup::Framework"
	resTypeBackupReportPlan = "AWS::Backup::ReportPlan"
)

func (rc *ResourceCreator) createBackupMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeBackupFramework:
		id, err := rc.createBackupFramework(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeBackupReportPlan:
		id, err := rc.createBackupReportPlan(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteBackupMoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeBackupFramework:
		return true, rc.deleteBackupFramework(physicalID)
	case resTypeBackupReportPlan:
		return true, rc.deleteBackupReportPlan(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Backup::Framework ----
// Ref returns the framework ARN; Fn::GetAtt CreationTime/DeploymentStatus/
// FrameworkArn/FrameworkStatus are stashed. DeleteFramework is name-keyed,
// so delete extracts the name back out of the ARN (matching Backup Vault's
// own precedent, resources_backup.go).

func (rc *ResourceCreator) createBackupFramework(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Backup == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "FrameworkName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	controls := backupFrameworkControlsProp(props["FrameworkControls"], params, physicalIDs)

	fw, err := rc.backends.Backup.Backend.CreateFramework(
		name, strProp(props, "FrameworkDescription", params, physicalIDs), controls,
	)
	if err != nil {
		return "", fmt.Errorf("create Backup framework %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CreationTime"] = fw.CreationTime.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/DeploymentStatus"] = fw.DeploymentStatus
	physicalIDs[logicalID+"/FrameworkArn"] = fw.FrameworkArn
	physicalIDs[logicalID+"/FrameworkStatus"] = fw.FrameworkStatus

	return fw.FrameworkArn, nil
}

func (rc *ResourceCreator) deleteBackupFramework(arn string) error {
	if rc.backends.Backup == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	err := rc.backends.Backup.Backend.DeleteFramework(name)
	if errors.Is(err, backupbackend.ErrNotFound) {
		return nil
	}

	return err
}

// backupFrameworkControlsProp decodes props["FrameworkControls"] (a list of
// {ControlName, ControlInputParameters, ControlScope} maps).
func backupFrameworkControlsProp(
	v any, params, physicalIDs map[string]string,
) []backupbackend.FrameworkControl {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]backupbackend.FrameworkControl, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		fc := backupbackend.FrameworkControl{
			ControlName: strProp(m, "ControlName", params, physicalIDs),
		}

		if params2, hasParams := m["ControlInputParameters"].([]any); hasParams {
			for _, p := range params2 {
				pm, isPMap := p.(map[string]any)
				if !isPMap {
					continue
				}

				fc.ControlInputParameters = append(fc.ControlInputParameters, backupbackend.ControlInputParameter{
					ParameterName:  strProp(pm, "ParameterName", params, physicalIDs),
					ParameterValue: strProp(pm, "ParameterValue", params, physicalIDs),
				})
			}
		}

		if scope, hasScope := m["ControlScope"].(map[string]any); hasScope {
			fc.ControlScope = &backupbackend.ControlScope{
				ComplianceResourceIDs:   strSliceProp(scope["ComplianceResourceIds"], params, physicalIDs),
				ComplianceResourceTypes: strSliceProp(scope["ComplianceResourceTypes"], params, physicalIDs),
				Tags:                    tagListFromSlice(asAnySlice(scope["Tags"]), params, physicalIDs),
			}
		}

		out = append(out, fc)
	}

	return out
}

// ---- AWS::Backup::ReportPlan ----
// Ref returns the resource name (ReportPlanName); Fn::GetAtt ReportPlanArn
// is stashed.

func (rc *ResourceCreator) createBackupReportPlan(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Backup == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ReportPlanName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var deliveryChannel *backupbackend.ReportDeliveryChannel
	if dc, ok := props["ReportDeliveryChannel"].(map[string]any); ok {
		deliveryChannel = &backupbackend.ReportDeliveryChannel{
			S3BucketName: strProp(dc, "S3BucketName", params, physicalIDs),
			S3KeyPrefix:  strProp(dc, "S3KeyPrefix", params, physicalIDs),
			Formats:      strSliceProp(dc["Formats"], params, physicalIDs),
		}
	}

	var setting *backupbackend.ReportSetting
	if rs, ok := props["ReportSetting"].(map[string]any); ok {
		setting = &backupbackend.ReportSetting{
			ReportTemplate:    strProp(rs, "ReportTemplate", params, physicalIDs),
			FrameworkArns:     strSliceProp(rs["FrameworkArns"], params, physicalIDs),
			Accounts:          strSliceProp(rs["Accounts"], params, physicalIDs),
			OrganizationUnits: strSliceProp(rs["OrganizationUnits"], params, physicalIDs),
			Regions:           strSliceProp(rs["Regions"], params, physicalIDs),
		}
	}

	rp, err := rc.backends.Backup.Backend.CreateReportPlan(
		name, strProp(props, "ReportPlanDescription", params, physicalIDs), deliveryChannel, setting,
	)
	if err != nil {
		return "", fmt.Errorf("create Backup report plan %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ReportPlanArn"] = rp.ReportPlanArn

	return rp.ReportPlanName, nil
}

func (rc *ResourceCreator) deleteBackupReportPlan(name string) error {
	if rc.backends.Backup == nil {
		return nil
	}

	err := rc.backends.Backup.Backend.DeleteReportPlan(name)
	if errors.Is(err, backupbackend.ErrNotFound) {
		return nil
	}

	return err
}

// asAnySlice normalizes a nil-safe cast of v to []any.
func asAnySlice(v any) []any {
	s, _ := v.([]any)

	return s
}
