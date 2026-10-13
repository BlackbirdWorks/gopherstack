package autoscaling

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	azNameMinParts = 3
	maxZoneLetters = 26
)

func azDirectionAbbrev(p string) string {
	switch p {
	case "northeast":
		return "ne"
	case "northwest":
		return "nw"
	case "southeast":
		return "se"
	case "southwest":
		return "sw"
	}

	return p[:1]
}

// availabilityZoneID maps a zone name like "us-east-1a" to the zone ID the EC2
// emulator reports for it ("use1-az1"); names outside that form yield "".
func availabilityZoneID(az string) string {
	if len(az) < len("x-y-1a") {
		return ""
	}

	letter := az[len(az)-1]
	region := az[:len(az)-1]

	if letter < 'a' || letter > 'z' || region[len(region)-1] < '0' || region[len(region)-1] > '9' {
		return ""
	}

	parts := strings.Split(region, "-")
	if len(parts) < azNameMinParts {
		return ""
	}

	var sb strings.Builder

	sb.WriteString(parts[0])

	for _, p := range parts[1 : len(parts)-1] {
		if p != "" {
			sb.WriteString(azDirectionAbbrev(p))
		}
	}

	sb.WriteString(parts[len(parts)-1])
	sb.WriteString("-az")
	sb.WriteString(strconv.Itoa(int(letter-'a') + 1))

	return sb.String()
}

// zoneNameFromID is the inverse of availabilityZoneID within region.
func zoneNameFromID(region, id string) (string, bool) {
	for i := range maxZoneLetters {
		name := region + string(rune('a'+i))
		if availabilityZoneID(name) == id {
			return name, true
		}
	}

	return "", false
}

// zoneNamesFromIDs resolves zone IDs to names in region, rejecting unknown IDs.
func zoneNamesFromIDs(region string, ids []string) ([]string, error) {
	names := make([]string, 0, len(ids))

	for _, id := range ids {
		name, ok := zoneNameFromID(region, id)
		if !ok {
			return nil, fmt.Errorf("%w: invalid Availability Zone ID %q", ErrInvalidParameter, id)
		}

		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}

	return names, nil
}

// zoneIDList projects zone names to the AvailabilityZoneIds response list;
// nil when no name maps to an ID.
func zoneIDList(zones []string) *xmlStringValueList {
	members := make([]xmlStringValue, 0, len(zones))

	for _, z := range zones {
		if id := availabilityZoneID(z); id != "" {
			members = append(members, xmlStringValue{Value: id})
		}
	}

	if len(members) == 0 {
		return nil
	}

	return &xmlStringValueList{Members: members}
}

// zonesFromRequest returns the zone names for a Create/UpdateAutoScalingGroup
// request that carries AvailabilityZones or AvailabilityZoneIds, never both.
func (b *InMemoryBackend) zonesFromRequest(names, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return names, nil
	}

	if len(names) > 0 {
		return nil, fmt.Errorf(
			"%w: AvailabilityZones and AvailabilityZoneIds cannot both be specified",
			ErrInvalidParameter,
		)
	}

	return zoneNamesFromIDs(b.region, ids)
}
