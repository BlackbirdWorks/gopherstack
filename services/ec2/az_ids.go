package ec2

import (
	"strconv"
	"strings"
)

const azNameMinParts = 3

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

// availabilityZoneID maps a zone name like "us-east-1a" to a stable zone ID
// like "use1-az1"; names outside the region-plus-letter form yield "".
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

// regionOptInStatus reports "opted-in" for regions AWS disables by default
// (every region is usable here) and "opt-in-not-required" for the rest.
func regionOptInStatus(region string) string {
	switch region {
	case "af-south-1", "ap-east-1", "ap-east-2", "ap-south-2", "ap-southeast-3", "ap-southeast-4",
		"ap-southeast-5", "ap-southeast-6", "ap-southeast-7", "ca-west-1", "eu-central-2",
		"eu-south-1", "eu-south-2", "il-central-1", "me-central-1", "me-south-1", "mx-central-1":
		return "opted-in"
	}

	return "opt-in-not-required"
}
