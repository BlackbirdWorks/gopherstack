package outposts

import (
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxNameLen        = 1000
	maxDescriptionLen = 1000
	maxNotesLen       = 2000
)

var rackEnums = map[string][]string{ //nolint:gochecknoglobals // fixed enum tables from outposts@v1.66.1 types/enums.go
	"FiberOpticCableType":       {"SINGLE_MODE", "MULTI_MODE"},
	"MaximumSupportedWeightLbs": {"NO_LIMIT", "MAX_1400_LBS", "MAX_1600_LBS", "MAX_1800_LBS", "MAX_2000_LBS"},
	"OpticalStandard": {
		"OPTIC_10GBASE_SR", "OPTIC_10GBASE_IR", "OPTIC_10GBASE_LR", "OPTIC_40GBASE_SR", "OPTIC_40GBASE_ESR",
		"OPTIC_40GBASE_IR4_LR4L", "OPTIC_40GBASE_LR4", "OPTIC_100GBASE_SR4", "OPTIC_100GBASE_CWDM4",
		"OPTIC_100GBASE_LR4", "OPTIC_100G_PSM4_MSA", "OPTIC_1000BASE_LX", "OPTIC_1000BASE_SX",
	},
	"PowerConnector": {"L6_30P", "IEC309", "AH530P7W", "AH532P6W", "CS8365C"},
	"PowerDrawKva":   {"POWER_5_KVA", "POWER_10_KVA", "POWER_15_KVA", "POWER_30_KVA"},
	"PowerFeedDrop":  {"ABOVE_RACK", "BELOW_RACK"},
	"PowerPhase":     {"SINGLE_PHASE", "THREE_PHASE"},
	"UplinkCount": {
		"UPLINK_COUNT_1", "UPLINK_COUNT_2", "UPLINK_COUNT_3", "UPLINK_COUNT_4", "UPLINK_COUNT_5",
		"UPLINK_COUNT_6", "UPLINK_COUNT_7", "UPLINK_COUNT_8", "UPLINK_COUNT_12", "UPLINK_COUNT_16",
	},
	"UplinkGbps": {"UPLINK_1G", "UPLINK_10G", "UPLINK_40G", "UPLINK_100G"},
}

func validateText(field, value string, maxLen int) error {
	if utf8.RuneCountInString(value) > maxLen {
		return validationError(field + " must be at most " + strconv.Itoa(maxLen) + " characters")
	}

	return nil
}

func validateRackProperties(p *RackPhysicalProperties) error {
	if p == nil {
		return nil
	}

	values := map[string]string{
		"FiberOpticCableType": p.FiberOpticCableType, "MaximumSupportedWeightLbs": p.MaximumSupportedWeightLbs,
		"OpticalStandard": p.OpticalStandard, "PowerConnector": p.PowerConnector, "PowerDrawKva": p.PowerDrawKva,
		"PowerFeedDrop": p.PowerFeedDrop, "PowerPhase": p.PowerPhase, "UplinkCount": p.UplinkCount,
		"UplinkGbps": p.UplinkGbps,
	}

	for field, value := range values {
		if value == "" {
			continue
		}

		if !slices.Contains(rackEnums[field], value) {
			return validationError("invalid " + field + ": " + value)
		}
	}

	return nil
}

func validateSiteFields(name, description, notes string) error {
	if err := validateText("Name", name, maxNameLen); err != nil {
		return err
	}

	if strings.TrimSpace(name) == "" && name != "" {
		return validationError("Name must not be blank")
	}

	if err := validateText("Description", description, maxDescriptionLen); err != nil {
		return err
	}

	return validateText("Notes", notes, maxNotesLen)
}
