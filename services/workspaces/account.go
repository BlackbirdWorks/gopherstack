package workspaces

import (
	"encoding/binary"
	"net/netip"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// DescribeAccount returns account configuration.
func (b *InMemoryBackend) DescribeAccount() storedAccountConfig {
	b.mu.RLock("DescribeAccount")
	defer b.mu.RUnlock()

	cfg := b.accountConfig
	if cfg.DedicatedTenancySupport == "" {
		cfg.DedicatedTenancySupport = "ENABLED"
	}

	return cfg
}

// ModifyAccount updates account configuration and records the change as a
// completed AccountModification, matching real AWS's DescribeAccountModifications
// (which surfaces the history of BYOL configuration changes). This backend
// applies the change synchronously, so ModificationState is always COMPLETED
// -- there is no PENDING window to model.
func (b *InMemoryBackend) ModifyAccount(
	dedicatedTenancyCidr, dedicatedTenancySupport string,
) error {
	b.mu.Lock("ModifyAccount")
	defer b.mu.Unlock()

	if dedicatedTenancyCidr != "" {
		b.accountConfig.ManagementCidrRange = dedicatedTenancyCidr
	}

	if dedicatedTenancySupport != "" {
		b.accountConfig.DedicatedTenancySupport = dedicatedTenancySupport
	}

	b.accountModifications = append(b.accountModifications, AccountModification{
		ModificationState:                   "COMPLETED",
		DedicatedTenancySupport:             dedicatedTenancySupport,
		DedicatedTenancyManagementCidrRange: dedicatedTenancyCidr,
		StartTime:                           time.Now().UTC(),
	})

	return nil
}

// DescribeAccountModifications returns the history of BYOL configuration
// changes recorded by ModifyAccount, most recent first (matching real AWS
// ordering), paginated. The real DescribeAccountModificationsInput has no
// MaxResults field (only NextToken), unlike most other paginated ops in this
// service -- so this backend applies its own fixed internal page size.
func (b *InMemoryBackend) DescribeAccountModifications(
	nextToken string,
) ([]AccountModification, string, error) {
	b.mu.RLock("DescribeAccountModifications")
	defer b.mu.RUnlock()

	reversed := make([]AccountModification, len(b.accountModifications))
	for i, m := range b.accountModifications {
		reversed[len(b.accountModifications)-1-i] = m
	}

	pg := page.New(reversed, nextToken, 0, describeAccountModificationsPageSize)

	return pg.Data, pg.Next, nil
}

// managementCidrSubnetPrefixLen is the granularity real AWS returns available
// BYOL management CIDR ranges in (/26 sub-blocks carved from the caller's
// constraint). ipv4Bits is the bit width of an IPv4 address, used to compute
// the /26 subnet step size.
const (
	managementCidrSubnetPrefixLen = 26
	ipv4Bits                      = 32
)

// maxManagementCidrRangesGenerated caps how many /26 sub-ranges are derived
// from a single constraint. This backend has no "already allocated" tracking
// (real AWS excludes ranges already in use elsewhere in the account), so the
// same constraint always deterministically yields the same set.
const maxManagementCidrRangesGenerated = 8

// describeAccountModificationsPageSize and cidrRangesPageSize are this
// backend's default page sizes; real AWS doesn't document exact defaults for
// either operation, so these are chosen generously (larger than
// maxManagementCidrRangesGenerated / any realistic modification history)
// so pagination only activates when a caller explicitly requests a smaller
// MaxResults.
const (
	describeAccountModificationsPageSize = 100
	cidrRangesPageSize                   = 100
)

// ListAvailableManagementCidrRanges returns candidate BYOL management network
// CIDR ranges contained within constraint, paginated. ManagementCidrRangeConstraint
// is a required field on the real API (smithy `required` trait) and must be a
// syntactically valid IPv4 CIDR block; both are validated here.
func (b *InMemoryBackend) ListAvailableManagementCidrRanges(
	constraint string, maxResults int32, nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListAvailableManagementCidrRanges")
	defer b.mu.RUnlock()

	if constraint == "" {
		return nil, "", awserr.New(
			"ManagementCidrRangeConstraint is required", awserr.ErrInvalidParameter)
	}

	prefix, err := netip.ParsePrefix(constraint)
	if err != nil || !prefix.Addr().Is4() {
		return nil, "", awserr.Newf(
			"ManagementCidrRangeConstraint %q is not a valid IPv4 CIDR block",
			awserr.ErrInvalidParameter, constraint)
	}

	ranges := deriveManagementCidrRanges(prefix)
	pg := page.New(ranges, nextToken, int(maxResults), cidrRangesPageSize)

	return pg.Data, pg.Next, nil
}

// deriveManagementCidrRanges subdivides constraint into up to
// maxManagementCidrRangesGenerated contiguous /26 sub-ranges. When constraint
// is already /26 or smaller (a higher prefix length), it is returned as a
// single-element result unchanged, since it can't be subdivided further.
func deriveManagementCidrRanges(constraint netip.Prefix) []string {
	constraint = constraint.Masked()
	bits := constraint.Bits()

	if bits >= managementCidrSubnetPrefixLen {
		return []string{constraint.String()}
	}

	base := constraint.Addr().As4()
	baseInt := binary.BigEndian.Uint32(base[:])
	step := uint32(1) << (ipv4Bits - managementCidrSubnetPrefixLen)

	ranges := make([]string, 0, maxManagementCidrRangesGenerated)

	for i := range maxManagementCidrRangesGenerated {
		var next [4]byte
		binary.BigEndian.PutUint32(next[:], baseInt+uint32(i)*step)

		sub := netip.PrefixFrom(netip.AddrFrom4(next), managementCidrSubnetPrefixLen)
		if !constraint.Contains(sub.Addr()) {
			break
		}

		ranges = append(ranges, sub.String())
	}

	return ranges
}
