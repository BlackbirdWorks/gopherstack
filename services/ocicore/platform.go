package ocicore

import (
	"slices"
	"strings"
)

const (
	platAmdVM        = "AMD_VM"
	platIntelVM      = "INTEL_VM"
	platAmdMilanBM   = "AMD_MILAN_BM"
	platAmdRomeBM    = "AMD_ROME_BM"
	platAmdMilanGPU  = "AMD_MILAN_BM_GPU"
	platAmdRomeGPU   = "AMD_ROME_BM_GPU"
	platIcelakeBM    = "INTEL_ICELAKE_BM"
	platSkylakeBM    = "INTEL_SKYLAKE_BM"
	platGenericBM    = "GENERIC_BM"
	preemptTerminate = "TERMINATE"
)

// PlatformConfig is the platformConfig of an instance; the type discriminates the permitted fields.
type PlatformConfig struct {
	IsSecureBootEnabled                      *bool             `json:"isSecureBootEnabled,omitempty"`
	IsTrustedPlatformModuleEnabled           *bool             `json:"isTrustedPlatformModuleEnabled,omitempty"`
	IsMeasuredBootEnabled                    *bool             `json:"isMeasuredBootEnabled,omitempty"`
	IsMemoryEncryptionEnabled                *bool             `json:"isMemoryEncryptionEnabled,omitempty"`
	IsSymmetricMultiThreadingEnabled         *bool             `json:"isSymmetricMultiThreadingEnabled,omitempty"`
	IsAccessControlServiceEnabled            *bool             `json:"isAccessControlServiceEnabled,omitempty"`
	AreVirtualInstructionsEnabled            *bool             `json:"areVirtualInstructionsEnabled,omitempty"`
	IsInputOutputMemoryManagementUnitEnabled *bool             `json:"isInputOutputMemoryManagementUnitEnabled,omitempty"`
	PercentageOfCoresEnabled                 *int              `json:"percentageOfCoresEnabled,omitempty"`
	ConfigMap                                map[string]string `json:"configMap,omitempty"`
	Type                                     string            `json:"type"`
	NumaNodesPerSocket                       string            `json:"numaNodesPerSocket,omitempty"`
}

// PreemptibleIn is the preemptibleInstanceConfig of LaunchInstance.
type PreemptibleIn struct {
	PreemptionAction struct {
		PreserveBootVolume *bool  `json:"preserveBootVolume,omitempty"`
		Type               string `json:"type"`
	} `json:"preemptionAction"`
}

// platformTypeFor is the platformConfig type a shape accepts; shapes without one (Ampere) accept none.
func platformTypeFor(s shapeDef) string {
	proc := strings.ToLower(s.processor)

	switch {
	case s.arch == archArm:
		return ""
	case !s.bareMetal && strings.Contains(proc, "amd"):
		return platAmdVM
	case !s.bareMetal:
		return platIntelVM
	case strings.Contains(s.name, "GPU4"):
		return platAmdRomeGPU
	case strings.Contains(s.name, "A100") || strings.Contains(s.name, "MI300"):
		return platAmdMilanGPU
	case strings.Contains(proc, "milan"):
		return platAmdMilanBM
	case strings.Contains(proc, "rome") || strings.Contains(proc, "epyc 7551"):
		return platAmdRomeBM
	case strings.Contains(proc, "ice lake") || strings.Contains(s.name, "Standard3"):
		return platIcelakeBM
	case strings.Contains(proc, "8167m"):
		return platSkylakeBM
	}

	return platGenericBM
}

func (c PlatformConfig) bmOnly() bool {
	return c.IsAccessControlServiceEnabled != nil || c.AreVirtualInstructionsEnabled != nil ||
		c.IsInputOutputMemoryManagementUnitEnabled != nil || c.PercentageOfCoresEnabled != nil ||
		c.ConfigMap != nil || c.NumaNodesPerSocket != ""
}

func checkPlatform(sh shapeDef, c *PlatformConfig) *APIError {
	if c == nil {
		return nil
	}

	want := platformTypeFor(sh)

	switch {
	case want == "":
		return errParam("Shape " + sh.name + " does not accept a platformConfig")
	case c.Type != want:
		return errParam("platformConfig.type must be " + want + " for shape " + sh.name)
	case !sh.bareMetal && c.bmOnly():
		return errParam("platformConfig.type " + c.Type + " only accepts the secure boot, measured boot, TPM, " +
			"memory encryption and symmetric multithreading settings")
	case c.NumaNodesPerSocket != "" &&
		!slices.Contains([]string{"NPS0", "NPS1", "NPS2", "NPS4", "NPS6"}, c.NumaNodesPerSocket):
		return errParam("numaNodesPerSocket must be NPS0, NPS1, NPS2, NPS4 or NPS6")
	case c.PercentageOfCoresEnabled != nil &&
		(*c.PercentageOfCoresEnabled < 1 || *c.PercentageOfCoresEnabled > percentMax):
		return errParam("percentageOfCoresEnabled must be between 1 and 100")
	}

	return nil
}

const percentMax = 100

func checkPreemptible(p *PreemptibleIn) *APIError {
	if p != nil && p.PreemptionAction.Type != preemptTerminate {
		return errParam("preemptibleInstanceConfig.preemptionAction.type must be TERMINATE")
	}

	return nil
}
