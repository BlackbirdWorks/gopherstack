package ec2

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
)

var (
	ErrInvalidVpcRange     = errors.New("InvalidVpcRange")
	ErrMalformedVPCID      = errors.New("InvalidVpcID.Malformed")
	ErrMalformedInstanceID = errors.New("InvalidInstanceID.Malformed")
	ErrMalformedAMIID      = errors.New("InvalidAMIID.Malformed")
)

const (
	volTypeStandard = "standard"
	minVPCPrefixLen = 16
	maxVPCPrefixLen = 28
)

var instanceTypeRe = regexp.MustCompile(`^[a-z][a-z0-9-]*[0-9][a-z0-9-]*\.[a-z0-9-]+$`)

// validateVpcCIDR enforces the documented /16../28 IPv4 range for CreateVpc.
func validateVpcCIDR(cidr string) error {
	pfx, err := netip.ParsePrefix(cidr)
	if err != nil || !pfx.Addr().Is4() {
		return fmt.Errorf("%w: invalid CIDR block %q", ErrInvalidParameter, cidr)
	}

	if pfx.Bits() < minVPCPrefixLen || pfx.Bits() > maxVPCPrefixLen {
		return fmt.Errorf("%w: the CIDR %q is invalid; the block size must be between /%d and /%d",
			ErrInvalidVpcRange, cidr, minVPCPrefixLen, maxVPCPrefixLen)
	}

	return nil
}

// validateSubnetPrefixLen enforces the documented /16../28 subnet block size.
func validateSubnetPrefixLen(cidr string) error {
	pfx, err := netip.ParsePrefix(cidr)
	if err == nil && pfx.Addr().Is4() && (pfx.Bits() < minVPCPrefixLen || pfx.Bits() > maxVPCPrefixLen) {
		return fmt.Errorf("%w: the CIDR %q is invalid; the block size must be between /%d and /%d",
			ErrSubnetRange, cidr, minVPCPrefixLen, maxVPCPrefixLen)
	}

	return nil
}

func validateVpcTenancy(tenancy string) error {
	if tenancy == vpcTenancyDefault || tenancy == "dedicated" || tenancy == "host" {
		return nil
	}

	return fmt.Errorf("%w: invalid value %q for InstanceTenancy", ErrInvalidParameter, tenancy)
}

func requireIDPrefix(ids []string, prefix string, malformed error) error {
	for _, id := range ids {
		if !strings.HasPrefix(id, prefix) {
			return fmt.Errorf("%w: invalid id: %q (expecting %q...)", malformed, id, prefix)
		}
	}

	return nil
}

func validateInstanceType(instanceType string) error {
	if instanceType == "" || instanceTypeRe.MatchString(instanceType) {
		return nil
	}

	return fmt.Errorf("%w: invalid value %q for InstanceType", ErrInvalidParameter, instanceType)
}

//nolint:gochecknoglobals // static enum
var volumeSizeBounds = map[string][2]int{
	volTypeStandard: {1, 1024},
	"gp2":           {1, 16384},
	"gp3":           {1, 16384},
	"io1":           {4, 16384},
	"io2":           {4, 65536},
	"st1":           {125, 16384},
	"sc1":           {125, 16384},
}

// validateVolumeSizeAndType checks the VolumeType enum and the per-type size range; size 0 means unset.
func validateVolumeSizeAndType(volType string, size int, sizeGiven bool) error {
	if volType == "" {
		volType = volTypeDefaultGP2
	}

	bounds, ok := volumeSizeBounds[volType]
	if !ok {
		return fmt.Errorf("%w: invalid value %q for VolumeType", ErrInvalidParameter, volType)
	}

	if !sizeGiven {
		return nil
	}

	if size < bounds[0] || size > bounds[1] {
		return fmt.Errorf("%w: volume size %d is invalid for %s volumes; the allowed range is %d-%d GiB",
			ErrInvalidParameter, size, volType, bounds[0], bounds[1])
	}

	return nil
}

const maxSecurityGroupTextLen = 255

var securityGroupTextRe = regexp.MustCompile(`^[a-zA-Z0-9. _\-:/()#,@\[\]+=&;{}!$*]*$`)

// validateSecurityGroupText applies the documented GroupName/GroupDescription limits.
func validateSecurityGroupText(name, desc string) error {
	if strings.HasPrefix(name, "sg-") {
		return fmt.Errorf("%w: group names may not be in the format sg-*", ErrInvalidParameter)
	}

	for field, v := range map[string]string{"GroupName": name, "GroupDescription": desc} {
		if len(v) > maxSecurityGroupTextLen {
			return fmt.Errorf(
				"%w: %s is longer than %d characters", ErrInvalidParameter, field, maxSecurityGroupTextLen,
			)
		}

		if !securityGroupTextRe.MatchString(v) {
			return fmt.Errorf("%w: invalid characters in %s", ErrInvalidParameter, field)
		}
	}

	return nil
}

var notFoundCodeRe = regexp.MustCompile(`^Invalid([A-Za-z]+?)(?:ID|Id)\.NotFound$`)

// errorMessage strips the duplicated "<code>: " prefix sentinel wrapping puts in
// err.Error() and words a bare-ID not-found detail the way EC2 does.
func errorMessage(code, msg string) string {
	detail := strings.TrimPrefix(msg, code+": ")
	if detail == code {
		return msg
	}

	if m := notFoundCodeRe.FindStringSubmatch(code); m != nil && detail != "" && !strings.ContainsAny(detail, " :") {
		kind := m[1]

		return fmt.Sprintf("The %s ID '%s' does not exist", strings.ToLower(kind[:1])+kind[1:], detail)
	}

	return detail
}

var ErrInvalidZone = errors.New("InvalidZone.NotFound")

var zoneNameRe = regexp.MustCompile(`^[a-z]{2,}(-[a-z]+)+-[0-9]+[a-z0-9-]*$`)

// validateZoneName rejects names that are not shaped like an availability or local zone.
// The region is not compared: root wiring and in-repo callers mix regions per backend.
func (b *InMemoryBackend) validateZoneName(az string) error {
	if zoneNameRe.MatchString(az) {
		return nil
	}

	return fmt.Errorf("%w: the zone %q does not exist", ErrInvalidZone, az)
}

const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagsPerCall = 50
)

var ErrTagLimitExceeded = errors.New("TagLimitExceeded")

// validateTags applies the documented tag limits to a CreateTags request.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerCall {
		return fmt.Errorf("%w: more than %d tags specified", ErrTagLimitExceeded, maxTagsPerCall)
	}

	for k, v := range tags {
		switch {
		case k == "" || len(k) > maxTagKeyLen:
			return fmt.Errorf("%w: tag keys must be 1-%d characters", ErrInvalidParameter, maxTagKeyLen)
		case strings.HasPrefix(strings.ToLower(k), "aws:"):
			return fmt.Errorf("%w: tag keys starting with 'aws:' are reserved for internal use", ErrInvalidParameter)
		case len(v) > maxTagValueLen:
			return fmt.Errorf("%w: tag values must be at most %d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

func validateAddressDomain(domain string) error {
	if domain == "" || domain == resourceTypeVPC || domain == volTypeStandard {
		return nil
	}

	return fmt.Errorf("%w: invalid value %q for Domain", ErrInvalidParameter, domain)
}

// validateRunInstancesInput checks the RunInstances members that need no backend state.
func (h *Handler) validateRunInstancesInput(imageID, instanceType, keyName, userData string) error {
	if err := validateUserData(userData); err != nil {
		return err
	}

	if err := validateInstanceType(instanceType); err != nil {
		return err
	}

	if imageID != "" {
		if err := requireIDPrefix([]string{imageID}, "ami-", ErrMalformedAMIID); err != nil {
			return err
		}
	}

	if keyName != "" && len(h.Backend.DescribeKeyPairs([]string{keyName})) == 0 {
		return fmt.Errorf("%w: the key pair '%s' does not exist", ErrKeyPairNotFound, keyName)
	}

	return nil
}

// requireInstancesExist fails with InvalidInstanceID.NotFound/Malformed for explicit ids.
func (h *Handler) requireInstancesExist(ids []string) error {
	if err := requireIDPrefix(ids, "i-", ErrMalformedInstanceID); err != nil || len(ids) == 0 {
		return err
	}

	return requireAllIDsPresent(
		ids, h.Backend.DescribeInstances(ids, ""), func(i *Instance) string { return i.ID }, ErrInstanceNotFound,
	)
}
