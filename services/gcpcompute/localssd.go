package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	diskScratch     = "SCRATCH"
	localSSDSizeGb  = 375
	localSSDTypeTag = "local-ssd"
	ifaceNVMe       = "NVME"
	ifaceSCSI       = "SCSI"
)

// n2Tiers are the N2 Local SSD counts by vCPU ceiling from the Local SSD documentation.
var n2Tiers = []struct { //nolint:gochecknoglobals // read-only lookup table
	counts  []int
	maxCPUs int64
}{
	{counts: []int{1, 2, 4, 8, 16, 24}, maxCPUs: 10},
	{counts: []int{2, 4, 8, 16, 24}, maxCPUs: 20},
	{counts: []int{4, 8, 16, 24}, maxCPUs: 40},
	{counts: []int{8, 16, 24}, maxCPUs: 80},
}

// localSSDCounts lists the Local SSD counts the Compute Engine Local SSD documentation allows; nil means the
// machine series has no Local SSD support.
func localSSDCounts(machine string, cpus int64) []int {
	family, _, _ := strings.Cut(machine, "-")
	if strings.HasPrefix(machine, "custom-") {
		family = "n1"
	}

	all := []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}

	switch family {
	case "n1":
		return all
	case "n2":
		for _, t := range n2Tiers {
			if cpus <= t.maxCPUs {
				return t.counts
			}
		}

		return []int{16, 24}
	}

	return nil
}

func machineCPUs(name string) int64 {
	if m, ok := findMachine(name); ok {
		return m.cpus
	}

	c, _, _ := customMachine(name)

	return c
}

func scratchDisk(idx, ordinal int, ad *computeapi.AttachedDisk) (*computeapi.AttachedDisk, *APIError) {
	field := "resource.disks[" + strconv.Itoa(idx) + "]"

	if ad.Boot {
		return nil, errField(field+".boot", "true", "A Local SSD disk cannot be the boot disk.")
	}

	if ad.Source != "" {
		return nil, errField(field+".source", ad.Source, "A SCRATCH disk takes no source.")
	}

	if ad.Mode != "" && ad.Mode != modeReadWrite {
		return nil, errField(field+".mode", ad.Mode, "A Local SSD disk is READ_WRITE.")
	}

	if ad.Interface != "" && ad.Interface != ifaceNVMe && ad.Interface != ifaceSCSI {
		return nil, errField(field+".interface", ad.Interface, "Must be NVME or SCSI.")
	}

	if p := ad.InitializeParams; p != nil {
		if p.DiskType != "" && lastSegment(p.DiskType) != localSSDTypeTag {
			return nil, errField(field+".initializeParams.diskType", p.DiskType, "A SCRATCH disk is of type local-ssd.")
		}

		if p.DiskSizeGb != 0 && p.DiskSizeGb != localSSDSizeGb {
			return nil, errField(field+".initializeParams.diskSizeGb", strconv.FormatInt(p.DiskSizeGb, 10),
				"Local SSD disks are 375 GB.")
		}
	}

	dev := ad.DeviceName
	if dev == "" {
		dev = "local-ssd-" + strconv.Itoa(ordinal)
	}

	return &computeapi.AttachedDisk{
		Kind: kindAttachedDisk, Type: diskScratch, Mode: modeReadWrite, Index: int64(idx), DeviceName: dev,
		AutoDelete: true, DiskSizeGb: localSSDSizeGb, Interface: cmpOr(ad.Interface, ifaceSCSI),
	}, nil
}

func checkLocalSSDCount(machine string, n int) *APIError {
	if n == 0 {
		return nil
	}

	name := lastSegment(machine)

	allowed := localSSDCounts(name, machineCPUs(name))
	if allowed == nil {
		return errInvalid(
			"Invalid value for field 'resource.disks': machine type '%s' does not support Local SSD.",
			name,
		)
	}

	if !slices.Contains(allowed, n) {
		return errInvalid("Invalid value for field 'resource.disks': %d Local SSD disks are not allowed for machine "+
			"type '%s'; allowed counts are %v.", n, name, allowed)
	}

	return nil
}
