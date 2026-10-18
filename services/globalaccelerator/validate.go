package globalaccelerator

import (
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxNameLen          = 64
	maxTags             = 50
	maxTagKeyLen        = 128
	maxTagValueLen      = 256
	maxPortRanges       = 10
	maxPort             = 65535
	maxEndpoints        = 10
	maxWeight           = 255
	defaultWeight       = 128
	maxAccelerators     = 20
	maxListeners        = 10
	maxThreshold        = 10
	maxTrafficDial      = 100
	defaultThreshold    = 3
	defaultHealthSecs   = 30
	fastHealthSecs      = 10
	maxPortOverrides    = 10
	maxTrafficRules     = 500
	maxCIDRv4Bits       = 24
	minCIDRv4Bits       = 8
	maxCIDRv6Bits       = 48
	minCIDRv6Bits       = 16
	accountIDLen        = 12
	maxAttachments      = 20
	maxResourcesPerAtt  = 100
	maxPrincipalsPerAtt = 100
)

var (
	nameRE      = regexp.MustCompile(`^[A-Za-z0-9_]([A-Za-z0-9_-]*[A-Za-z0-9_])?$`)
	regionRE    = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d$`)
	accountIDRE = regexp.MustCompile(`^\d{12}$`)
	principalRE = regexp.MustCompile(`^arn:aws[a-z-]*:organizations::\d{12}:(organization|ou)/.+$`)
)

func validateName(name string) error {
	if len(name) == 0 || len(name) > maxNameLen || !nameRE.MatchString(name) {
		return fail(
			ErrInvalidArgument,
			"name must be 1-%d characters of letters, digits, hyphens or underscores and must not begin or end with a hyphen",
			maxNameLen,
		)
	}

	return nil
}

func validateRegion(region string) error {
	if !regionRE.MatchString(region) {
		return fail(ErrInvalidArgument, "%q is not a valid AWS Region", region)
	}

	return nil
}

func validateIPAddressType(t string, custom bool) error {
	switch {
	case t == ipTypeV4:
		return nil
	case t == ipTypeDualStack && !custom:
		return nil
	default:
		return fail(ErrInvalidArgument, "ipAddressType %q is not supported", t)
	}
}

func validateTags(list []tags.KV) (map[string]string, error) {
	if len(list) > maxTags {
		return nil, fail(ErrLimitExceeded, "at most %d tags are allowed", maxTags)
	}

	out := make(map[string]string, len(list))

	for _, t := range list {
		if t.Key == "" || len(t.Key) > maxTagKeyLen || len(t.Value) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return nil, fail(ErrInvalidArgument, "tag %q is invalid", t.Key)
		}

		out[t.Key] = t.Value
	}

	return out, nil
}

func validatePortRanges(ranges []PortRange) error {
	if len(ranges) == 0 || len(ranges) > maxPortRanges {
		return fail(ErrInvalidPortRange, "a listener needs between 1 and %d port ranges", maxPortRanges)
	}

	for _, r := range ranges {
		if r.FromPort < 1 || r.ToPort > maxPort || r.FromPort > r.ToPort {
			return fail(ErrInvalidPortRange, "port range %d-%d is invalid", r.FromPort, r.ToPort)
		}
	}

	for i := range ranges {
		for j := i + 1; j < len(ranges); j++ {
			if rangesOverlap(ranges[i], ranges[j]) {
				return fail(ErrInvalidPortRange, "port ranges %d-%d and %d-%d overlap",
					ranges[i].FromPort, ranges[i].ToPort, ranges[j].FromPort, ranges[j].ToPort)
			}
		}
	}

	return nil
}

func rangesOverlap(a, b PortRange) bool { return a.FromPort <= b.ToPort && b.FromPort <= a.ToPort }

func portInRanges(port int32, ranges []PortRange) bool {
	return slices.ContainsFunc(ranges, func(r PortRange) bool { return port >= r.FromPort && port <= r.ToPort })
}

func countPorts(ranges []PortRange) int64 {
	var n int64
	for _, r := range ranges {
		n += int64(r.ToPort-r.FromPort) + 1
	}

	return n
}

func validateProtocol(p string) error {
	if p != protocolTCP && p != protocolUDP {
		return fail(ErrInvalidArgument, "protocol %q must be TCP or UDP", p)
	}

	return nil
}

func validateEndpointID(id string) error {
	if id == "" {
		return fail(ErrInvalidArgument, "endpointId is required")
	}

	if strings.HasPrefix(id, "arn:") || strings.HasPrefix(id, "i-") || strings.HasPrefix(id, "eipalloc-") {
		return nil
	}

	return fail(ErrInvalidArgument, "endpointId %q must be a resource ARN, an instance ID or an EIP allocation ID", id)
}

func validateCIDR(cidr string) error {
	p, err := netip.ParsePrefix(cidr)
	if err != nil || p.Masked() != p {
		return fail(ErrInvalidArgument, "%q is not a valid CIDR block", cidr)
	}

	bits := p.Bits()

	switch {
	case p.Addr().Is4() && bits >= minCIDRv4Bits && bits <= maxCIDRv4Bits:
		return nil
	case p.Addr().Is6() && bits >= minCIDRv6Bits && bits <= maxCIDRv6Bits:
		return nil
	default:
		return fail(ErrInvalidArgument, "CIDR %q is outside the supported prefix lengths", cidr)
	}
}

// paginate applies MaxResults/NextToken; a bad token is InvalidNextTokenException.
func paginate[T any](all []T, token string, maxResults *int32) (page.Page[T], error) {
	limit := defaultPageSize

	if maxResults != nil {
		if *maxResults < 1 || *maxResults > maxPageSize {
			return page.Page[T]{}, fail(ErrInvalidArgument, "maxResults must be between 1 and %d", maxPageSize)
		}

		limit = int(*maxResults)
	}

	if err := checkToken(token, len(all)); err != nil {
		return page.Page[T]{}, err
	}

	return page.New(all, token, limit, defaultPageSize), nil
}

func checkToken(token string, total int) error {
	if err := page.ValidateToken(token); err != nil {
		return fail(ErrInvalidNextToken, "the nextToken is invalid")
	}

	if token != "" && page.DecodeToken(token) > total {
		return fail(ErrInvalidNextToken, "the nextToken is invalid")
	}

	return nil
}

func compareStrings(a, b string) int { return strings.Compare(a, b) }

func addrInCIDR(ip, cidr string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}

	p, err := netip.ParsePrefix(cidr)

	return err == nil && p.Contains(a)
}
