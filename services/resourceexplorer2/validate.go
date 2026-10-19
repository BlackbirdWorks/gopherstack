package resourceexplorer2

import (
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxTags        = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxViewName    = 64
	maxViewsPerReg = 50
	maxQueryLen    = 2048
)

var (
	viewNameRE = regexp.MustCompile(`^[a-zA-Z0-9-]{1,64}$`)
	regionRE   = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d$`)
)

func validateTags(m map[string]string) error {
	if len(m) > maxTags {
		return invalid("at most %d tags are allowed", maxTags)
	}

	for k, v := range m {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("tag %q is invalid", k)
		}
	}

	return nil
}

func validateRegion(r string) error {
	if !regionRE.MatchString(r) {
		return invalid("%q is not a valid AWS Region", r)
	}

	return nil
}

func validateIndexType(t string) error {
	if t != indexTypeLocal && t != indexTypeAggregator {
		return invalid("Type must be LOCAL or AGGREGATOR")
	}

	return nil
}

const (
	maxPage           = 100
	maxViewPage       = 20
	maxMemberAccounts = 10
	maxBatchViews     = 20
)

func paginate[T any](all []T, token string, maxResults, maxAllowed int) (page.Page[T], error) {
	return restroute.Paginate(all, token, maxResults, maxAllowed, maxAllowed)
}
