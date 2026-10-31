package azurearm

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type vmSizeDef struct {
	name       string
	family     string
	cores      int
	memoryMB   int
	maxDisks   int
	maxNICs    int
	premium    bool
	resourceMB int
}

// computeVMSizes is a representative subset of the sizes Azure documents (B, D, E, F and legacy DSv2 series). Every
// size is offered in every location and zone.
//
//nolint:gochecknoglobals // static read-only catalog
var computeVMSizes = []vmSizeDef{
	{"Standard_B1s", familyBS, 1, 1024, 2, 2, true, 4096},
	{"Standard_B1ms", familyBS, 1, 2048, 2, 2, true, 4096},
	{"Standard_B2s", familyBS, 2, 4096, 4, 3, true, 8192},
	{"Standard_B2ms", familyBS, 2, 8192, 4, 3, true, 16384},
	{"Standard_B4ms", familyBS, 4, 16384, 8, 4, true, 32768},
	{"Standard_D2s_v3", familyDSv3, 2, 8192, 4, 2, true, 16384},
	{"Standard_D4s_v3", familyDSv3, 4, 16384, 8, 2, true, 32768},
	{"Standard_D8s_v3", familyDSv3, 8, 32768, 16, 4, true, 65536},
	{"Standard_D2s_v5", familyDSv5, 2, 8192, 4, 2, true, 0},
	{"Standard_D4s_v5", familyDSv5, 4, 16384, 8, 2, true, 0},
	{"Standard_D8s_v5", familyDSv5, 8, 32768, 16, 4, true, 0},
	{"Standard_D2as_v5", "standardDASv5Family", 2, 8192, 4, 2, true, 0},
	{"Standard_D4as_v5", "standardDASv5Family", 4, 16384, 8, 2, true, 0},
	{"Standard_D2_v5", "standardDv5Family", 2, 8192, 4, 2, false, 0},
	{"Standard_D4_v5", "standardDv5Family", 4, 16384, 8, 2, false, 0},
	{"Standard_E2s_v5", familyESv5, 2, 16384, 4, 2, true, 0},
	{"Standard_E4s_v5", familyESv5, 4, 32768, 8, 2, true, 0},
	{"Standard_E8s_v5", familyESv5, 8, 65536, 16, 4, true, 0},
	{"Standard_F2s_v2", "standardFSv2Family", 2, 4096, 4, 2, true, 16384},
	{"Standard_F4s_v2", "standardFSv2Family", 4, 8192, 8, 2, true, 32768},
	{"Standard_DS1_v2", familyDSv2, 1, 3584, 4, 2, true, 7168},
	{"Standard_DS2_v2", familyDSv2, 2, 7168, 8, 2, true, 14336},
	{"Standard_DS3_v2", familyDSv2, 4, 14336, 16, 4, true, 28672},
	{"Standard_A1_v2", "standardAv2Family", 1, 2048, 2, 2, false, 10240},
	{"Standard_A2_v2", "standardAv2Family", 2, 4096, 4, 2, false, 20480},
}

const (
	cmpOSDiskLimitMB = 1047552
	mbPerGB          = 1024
	cmpMaxNICsPerVM  = 8

	familyBS   = "standardBSFamily"
	familyDSv3 = "standardDSv3Family"
	familyDSv5 = "standardDSv5Family"
	familyESv5 = "standardESv5Family"
	familyDSv2 = "standardDSv2Family"
)

func findVMSize(name string) (vmSizeDef, bool) {
	for _, s := range computeVMSizes {
		if strings.EqualFold(s.name, name) {
			return s, true
		}
	}

	return vmSizeDef{}, false
}

func vmSizesBody() []any {
	out := make([]any, 0, len(computeVMSizes))

	for _, s := range computeVMSizes {
		out = append(out, map[string]any{
			fieldName: s.name, "numberOfCores": s.cores, "osDiskSizeInMB": cmpOSDiskLimitMB,
			"resourceDiskSizeInMB": s.resourceMB, "memoryInMB": s.memoryMB, "maxDataDiskCount": s.maxDisks,
		})
	}

	return out
}

//nolint:gochecknoglobals // static read-only catalog
var cmpDiskSKUs = strings.Fields(
	"Standard_LRS Premium_LRS StandardSSD_LRS UltraSSD_LRS Premium_ZRS StandardSSD_ZRS PremiumV2_LRS")

//nolint:gochecknoglobals // static read-only catalog
var cmpSnapshotSKUs = strings.Fields("Standard_LRS Premium_LRS Standard_ZRS")

func defaultDiskSKU() string { return cmpDiskSKUs[0] }

func canonicalIn(list []string, v string) (string, bool) {
	i := slices.IndexFunc(list, func(s string) bool { return strings.EqualFold(s, v) })
	if i < 0 {
		return "", false
	}

	return list[i], true
}

func diskTier(sku string) string {
	switch {
	case strings.HasPrefix(sku, "Premium"):
		return "Premium"
	case strings.HasPrefix(sku, "Ultra"):
		return "Ultra"
	}

	return skuTierStandard
}

// resourceSKUsBody renders the Microsoft.Compute/skus listing for location (all locations when empty).
func resourceSKUsBody(location string) []any {
	loc := normLocation(location)
	locs := []any{loc}

	if loc == "" {
		locs = []any{}
	}

	out := make([]any, 0, len(computeVMSizes)+len(cmpDiskSKUs))

	for _, s := range computeVMSizes {
		info := []any{}
		if loc != "" {
			info = append(info, map[string]any{fieldLocation: loc, "zones": []any{"1", "2", "3"}})
		}

		out = append(out, map[string]any{
			cmpKeyResourceType: "virtualMachines",
			fieldName:          s.name,
			cmpKeyTier:         skuTierStandard,
			"size":             strings.TrimPrefix(s.name, "Standard_"),
			cmpKeyFamily:       s.family,
			cmpKeyLocations:    locs,
			"locationInfo":     info,
			"restrictions":     []any{},
			cmpKeyCaps: []any{
				skuCap(
					"vCPUs",
					strconv.Itoa(s.cores),
				),
				skuCap("MemoryGB", fmt.Sprintf("%g", float64(s.memoryMB)/mbPerGB)),
				skuCap(
					"MaxDataDiskCount",
					strconv.Itoa(s.maxDisks),
				),
				skuCap("MaxNetworkInterfaces", strconv.Itoa(s.maxNICs)),
				skuCap("PremiumIO", strconv.FormatBool(s.premium)),
			},
		})
	}

	for _, d := range cmpDiskSKUs {
		out = append(out, map[string]any{
			cmpKeyResourceType: "disks",
			fieldName:          d,
			cmpKeyTier:         diskTier(d),
			cmpKeyLocations:    locs,
			"restrictions":     []any{},
			cmpKeyCaps:         []any{},
		})
	}

	return out
}

func skuCap(name, value string) map[string]any {
	return map[string]any{fieldName: name, fieldValue: value}
}
