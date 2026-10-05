package guardduty

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// dataSources is the deprecated DataSourceConfigurations request shape; each toggle maps onto a feature.
type dataSources struct {
	S3Logs     *enableToggle `json:"s3Logs"`
	Kubernetes *struct {
		AuditLogs *enableToggle `json:"auditLogs"`
	} `json:"kubernetes"`
	MalwareProtection *struct {
		ScanEc2InstanceWithFindings *struct {
			EbsVolumes *bool `json:"ebsVolumes"`
		} `json:"scanEc2InstanceWithFindings"`
	} `json:"malwareProtection"`
}

type enableToggle struct {
	Enable *bool `json:"enable"`
}

func featureStatus(enable *bool) string {
	if enable != nil && *enable {
		return statusEnabled
	}

	return statusDisabled
}

// features returns the DetectorFeature entries the legacy dataSources block implies.
func (d *dataSources) features() []DetectorFeature {
	if d == nil {
		return nil
	}

	var out []DetectorFeature

	if d.S3Logs != nil {
		out = append(out, DetectorFeature{Name: featureS3DataEvents, Status: featureStatus(d.S3Logs.Enable)})
	}

	if d.Kubernetes != nil && d.Kubernetes.AuditLogs != nil {
		out = append(out, DetectorFeature{
			Name: featureEKSAuditLogs, Status: featureStatus(d.Kubernetes.AuditLogs.Enable),
		})
	}

	if m := d.MalwareProtection; m != nil && m.ScanEc2InstanceWithFindings != nil {
		out = append(out, DetectorFeature{
			Name: featureEBSMalware, Status: featureStatus(m.ScanEc2InstanceWithFindings.EbsVolumes),
		})
	}

	return out
}

// mergeFeatures upserts updates into base by feature name, keeping base order.
func mergeFeatures(base, updates []DetectorFeature) []DetectorFeature {
	if len(updates) == 0 {
		return base
	}

	out := append([]DetectorFeature(nil), base...)

	for _, u := range updates {
		replaced := false

		for i := range out {
			if out[i].Name == u.Name {
				out[i] = u
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, u)
		}
	}

	return out
}

// validateMemberFeatures rejects names and statuses outside the OrgFeature and FeatureStatus enums.
func validateMemberFeatures(features []DetectorFeature) error {
	for _, f := range features {
		if !validDetectorFeatureNames[f.Name] || f.Name == featureAIAnalyst || !validFeatureStatuses[f.Status] {
			return ErrValidation
		}

		for _, a := range f.AdditionalConfiguration {
			if !validMemberAdditionalNames[a.Name] || !validFeatureStatuses[a.Status] {
				return ErrValidation
			}
		}
	}

	return nil
}

//nolint:gochecknoglobals // static lookup table
var validMemberAdditionalNames = map[string]bool{
	"EKS_ADDON_MANAGEMENT":         true,
	"ECS_FARGATE_AGENT_MANAGEMENT": true,
	"EC2_AGENT_MANAGEMENT":         true,
}

// applyMemberFeatures upserts updates by feature name, stamping updatedAt.
func applyMemberFeatures(base []MemberFeature, updates []DetectorFeature, now time.Time) []MemberFeature {
	out := append([]MemberFeature(nil), base...)

	for _, u := range updates {
		mf := MemberFeature{Name: u.Name, Status: u.Status, UpdatedAt: now}
		for _, a := range u.AdditionalConfiguration {
			mf.AdditionalConfiguration = append(mf.AdditionalConfiguration,
				MemberAdditional{Name: a.Name, Status: a.Status, UpdatedAt: now})
		}

		if i := slices.IndexFunc(out, func(e MemberFeature) bool { return e.Name == u.Name }); i >= 0 {
			out[i] = mf
		} else {
			out = append(out, mf)
		}
	}

	return out
}

func memberFeaturesWire(features []MemberFeature) []map[string]any {
	out := make([]map[string]any, 0, len(features))

	for _, f := range features {
		entry := map[string]any{keyName: f.Name, "status": f.Status, keyUpdatedAt: awstime.Epoch(f.UpdatedAt)}

		if len(f.AdditionalConfiguration) > 0 {
			extras := make([]map[string]any, 0, len(f.AdditionalConfiguration))
			for _, a := range f.AdditionalConfiguration {
				extras = append(extras, map[string]any{
					keyName: a.Name, "status": a.Status, keyUpdatedAt: awstime.Epoch(a.UpdatedAt),
				})
			}

			entry["additionalConfiguration"] = extras
		}

		out = append(out, entry)
	}

	return out
}

type orgDataSources struct {
	S3Logs     *orgAutoEnable `json:"s3Logs"`
	Kubernetes *struct {
		AuditLogs *orgAutoEnable `json:"auditLogs"`
	} `json:"kubernetes"`
	MalwareProtection *struct {
		ScanEc2InstanceWithFindings *struct {
			EbsVolumes *orgAutoEnable `json:"ebsVolumes"`
		} `json:"scanEc2InstanceWithFindings"`
	} `json:"malwareProtection"`
}

type orgAutoEnable struct {
	AutoEnable *bool `json:"autoEnable"`
}

func orgStatus(t *orgAutoEnable) string {
	if t != nil && t.AutoEnable != nil && *t.AutoEnable {
		return "NEW"
	}

	return "NONE"
}

// features maps the deprecated org dataSources block onto features: autoEnable true is NEW, false NONE.
func (d *orgDataSources) features() []OrgFeature {
	if d == nil {
		return nil
	}

	var out []OrgFeature

	if d.S3Logs != nil {
		out = append(out, OrgFeature{Name: featureS3DataEvents, AutoEnable: orgStatus(d.S3Logs)})
	}

	if d.Kubernetes != nil && d.Kubernetes.AuditLogs != nil {
		out = append(out, OrgFeature{Name: featureEKSAuditLogs, AutoEnable: orgStatus(d.Kubernetes.AuditLogs)})
	}

	if m := d.MalwareProtection; m != nil && m.ScanEc2InstanceWithFindings != nil &&
		m.ScanEc2InstanceWithFindings.EbsVolumes != nil {
		out = append(out, OrgFeature{
			Name: featureEBSMalware, AutoEnable: orgStatus(m.ScanEc2InstanceWithFindings.EbsVolumes),
		})
	}

	return out
}

//nolint:gochecknoglobals // static lookup table
var validOrgAutoEnable = map[string]bool{"": true, "NEW": true, "ALL": true, "NONE": true}

func validateOrgUpdate(members string, features []OrgFeature) error {
	if !validOrgAutoEnable[members] {
		return ErrValidation
	}

	for _, f := range features {
		if !validDetectorFeatureNames[f.Name] || f.Name == featureAIAnalyst || !validOrgAutoEnable[f.AutoEnable] {
			return ErrValidation
		}

		for _, a := range f.AdditionalConfiguration {
			if !validMemberAdditionalNames[a.Name] || !validOrgAutoEnable[a.AutoEnable] {
				return ErrValidation
			}
		}
	}

	return nil
}

// mergeOrgFeatures upserts updates into base by feature name.
func mergeOrgFeatures(base, updates []OrgFeature) []OrgFeature {
	out := make([]OrgFeature, 0, len(base)+len(updates))
	out = append(out, base...)

	for _, u := range updates {
		if i := slices.IndexFunc(out, func(e OrgFeature) bool { return e.Name == u.Name }); i >= 0 {
			out[i] = u
		} else {
			out = append(out, u)
		}
	}

	return out
}
