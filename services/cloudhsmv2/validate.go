package cloudhsmv2

import (
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxTagKey   = 128
	maxTagValue = 256
	maxSubnets  = 10
)

var regionPattern = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d$`)

func validateTags(in []wireTag) (map[string]string, error) {
	out := make(map[string]string, len(in))

	for _, t := range in {
		if t.Key == "" || len(t.Key) > maxTagKey || len(t.Value) > maxTagValue {
			return nil, fail(ErrTag, "tag keys must be 1-%d and values at most %d characters", maxTagKey, maxTagValue)
		}

		if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return nil, fail(ErrTag, "tag keys cannot start with aws:")
		}

		out[t.Key] = t.Value
	}

	if len(out) > maxTags {
		return nil, fail(ErrTag, "a resource can have at most %d tags", maxTags)
	}

	return out, nil
}

func validateHsmType(t string) error {
	if t != hsmTypeV1 && t != hsmTypeV2 {
		return fail(ErrInvalidRequest, "HsmType must be %s or %s", hsmTypeV1, hsmTypeV2)
	}

	return nil
}

// resolveMode defaults the mode by HSM type and rejects combinations the service does not offer.
func resolveMode(hsmType, mode string) (string, error) {
	switch mode {
	case "":
		if hsmType == hsmTypeV2 {
			return modeNonFIPS, nil
		}

		return modeFIPS, nil
	case modeFIPS:
		return mode, nil
	case modeNonFIPS:
		if hsmType == hsmTypeV1 {
			return "", fail(ErrInvalidRequest, "%s clusters only support %s mode", hsmTypeV1, modeFIPS)
		}

		return mode, nil
	default:
		return "", fail(ErrInvalidRequest, "Mode must be %s or %s", modeFIPS, modeNonFIPS)
	}
}

func resolveNetworkType(t string) (string, error) {
	switch t {
	case "":
		return networkIPv4, nil
	case networkIPv4, networkDualstack:
		return t, nil
	default:
		return "", fail(ErrInvalidRequest, "NetworkType must be %s or %s", networkIPv4, networkDualstack)
	}
}

// retentionOf returns the retention days of a policy, or the default when nil.
func retentionOf(p *wireRetention) (int, error) {
	if p == nil {
		return defaultRetentionDays, nil
	}

	if p.Type != retentionDays {
		return 0, fail(ErrInvalidRequest, "BackupRetentionPolicy.Type must be %s", retentionDays)
	}

	n, err := strconv.Atoi(p.Value)
	if err != nil || n < minRetentionDays || n > maxRetentionDays {
		return 0, fail(
			ErrInvalidRequest,
			"BackupRetentionPolicy.Value must be a number of days between %d and %d",
			minRetentionDays,
			maxRetentionDays,
		)
	}

	return n, nil
}

func validateRegion(r string) error {
	if !regionPattern.MatchString(r) {
		return fail(ErrInvalidRequest, "%q is not a valid region", r)
	}

	return nil
}

func validatePaging(token string, maxResults *int32) (int, error) {
	if err := page.ValidateToken(token); err != nil {
		return 0, fail(ErrInvalidRequest, "NextToken is not valid")
	}

	if maxResults == nil {
		return 0, nil
	}

	if *maxResults < 1 || *maxResults > maxPage {
		return 0, fail(ErrInvalidRequest, "MaxResults must be between 1 and %d", maxPage)
	}

	return int(*maxResults), nil
}

func matchesFilter(filters map[string][]string, key, value string) bool {
	vals, ok := filters[key]
	if !ok || len(vals) == 0 {
		return true
	}

	return slices.Contains(vals, value)
}

func checkFilterKeys(filters map[string][]string, allowed ...string) error {
	for k := range filters {
		if !slices.Contains(allowed, k) {
			return fail(ErrInvalidRequest, "unsupported filter %q", k)
		}
	}

	return nil
}
