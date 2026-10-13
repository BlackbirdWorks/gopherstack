package ssm

import "slices"

const (
	patchOSUbuntu     = "UBUNTU"
	patchFamilyUbuntu = "Ubuntu"

	patchOwnerAWS  = "AWS"
	patchOwnerSelf = "Self"
	patchOwnerAll  = "All"
)

type managedBaseline struct {
	os, name, label string
}

func managedBaselineDefs() []managedBaseline {
	return []managedBaseline{
		{"AMAZON_LINUX", "AWS-AmazonLinuxDefaultPatchBaseline", "Amazon Linux"},
		{"AMAZON_LINUX_2", "AWS-AmazonLinux2DefaultPatchBaseline", "Amazon Linux 2"},
		{"AMAZON_LINUX_2022", "AWS-AmazonLinux2022DefaultPatchBaseline", "Amazon Linux 2022"},
		{"AMAZON_LINUX_2023", "AWS-AmazonLinux2023DefaultPatchBaseline", "Amazon Linux 2023"},
		{"CENTOS", "AWS-CentOSDefaultPatchBaseline", "CentOS"},
		{"DEBIAN", "AWS-DebianDefaultPatchBaseline", "Debian"},
		{"MACOS", "AWS-MacOSDefaultPatchBaseline", "macOS"},
		{"ORACLE_LINUX", "AWS-OracleLinuxDefaultPatchBaseline", "Oracle Linux"},
		{"RASPBIAN", "AWS-RaspbianDefaultPatchBaseline", "Raspbian"},
		{"REDHAT_ENTERPRISE_LINUX", "AWS-RedHatDefaultPatchBaseline", "Red Hat Enterprise Linux"},
		{"ROCKY_LINUX", "AWS-RockyLinuxDefaultPatchBaseline", "Rocky Linux"},
		{"SUSE", "AWS-SuseDefaultPatchBaseline", "SUSE Linux Enterprise Server"},
		{patchOSUbuntu, "AWS-UbuntuDefaultPatchBaseline", patchFamilyUbuntu},
		{"ALMA_LINUX", "AWS-AlmaLinuxDefaultPatchBaseline", "Alma Linux"},
		{patchOSWindows, "AWS-DefaultPatchBaseline", "Windows"},
	}
}

// awsManagedPatchBaselines are the per-OS baselines AWS provides in every
// account; they are derived, not stored, so they survive Reset.
func awsManagedPatchBaselines() []PatchBaseline {
	out := make([]PatchBaseline, 0, len(managedBaselineDefs()))

	for _, d := range managedBaselineDefs() {
		out = append(out, PatchBaseline{
			BaselineID:      defaultBaselineID(d.os),
			Name:            d.name,
			OperatingSystem: d.os,
			Description:     "Default Patch Baseline for " + d.label + " Provided by AWS.",
		})
	}

	return out
}

func awsManagedPatchBaseline(id string) (PatchBaseline, bool) {
	for _, bl := range awsManagedPatchBaselines() {
		if bl.BaselineID == id {
			return bl, true
		}
	}

	return PatchBaseline{}, false
}

// patchBaselineOwnerAllowed applies the OWNER filter ("AWS", "Self", "All").
func patchBaselineOwnerAllowed(filters []PatchBaselineFilter, owner string) bool {
	for _, f := range filters {
		if f.Key != "OWNER" || slices.Contains(f.Values, patchOwnerAll) {
			continue
		}

		if !slices.Contains(f.Values, owner) {
			return false
		}
	}

	return true
}
