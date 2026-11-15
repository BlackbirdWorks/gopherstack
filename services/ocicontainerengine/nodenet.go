package ocicontainerengine

import (
	"slices"
	"strings"
)

func launchTypes() []string { return []string{"VFIO", "E1000", "PARAVIRTUALIZED"} }

// SecondaryVnic is one entry of secondaryVnics.
type SecondaryVnic struct {
	CreateVnicDetails map[string]any `json:"createVnicDetails"`
	DisplayName       *string        `json:"displayName,omitempty"`
	NicIndex          *int           `json:"nicIndex,omitempty"`
}

// checkNodeNet validates the per-node network settings of a node pool.
func (b *InMemoryBackend) checkNodeNet(sec []SecondaryVnic, launch, vcnID string) *APIError {
	if launch != "" && !slices.Contains(launchTypes(), launch) {
		return errParam("networkLaunchType must be one of " + strings.Join(launchTypes(), ", "))
	}

	for _, v := range sec {
		id, _ := v.CreateVnicDetails["subnetId"].(string)
		if v.CreateVnicDetails == nil || id == "" {
			return errMissing("secondaryVnics.createVnicDetails.subnetId is required")
		}

		if _, aerr := b.checkSubnet("secondaryVnics.createVnicDetails.subnetId", id, vcnID); aerr != nil {
			return aerr
		}
	}

	return nil
}

func checkComputeCluster(id *string) *APIError {
	if id != nil && !strings.HasPrefix(*id, "ocid1.computecluster.") {
		return errParam("nodeConfigDetails.computeClusterId must be a compute cluster OCID")
	}

	return nil
}
