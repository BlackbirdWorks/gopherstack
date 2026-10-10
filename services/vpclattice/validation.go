package vpclattice

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minNameLen         = 3
	maxServiceNameLen  = 40
	maxNetworkNameLen  = 63
	maxTargetGroupName = 128
	maxChildNameLen    = 63
	minPort            = 1
	maxPort            = 65535
	minRulePriority    = 1
	maxRulePriority    = 2000
	maxListResults     = 100
)

var nameCharsRe = regexp.MustCompile(`^[a-z0-9-]+$`)

// validateName applies the documented VPC Lattice name constraints: length
// bounds, [a-z0-9-] only, no leading/trailing/doubled hyphen, and no
// resource-ID prefix such as "svc-".
func validateName(kind, name, idPrefix string, maxLen int) error {
	switch {
	case len(name) < minNameLen || len(name) > maxLen:
		return invalidf(
			"Invalid %s name %q: length must be between %d and %d characters",
			kind,
			name,
			minNameLen,
			maxLen,
		)
	case !nameCharsRe.MatchString(name),
		strings.HasPrefix(name, "-"), strings.HasSuffix(name, "-"), strings.Contains(name, "--"):
		return invalidf("Invalid %s name %q: valid characters are a-z, 0-9, and hyphens (-); "+
			"a hyphen cannot be first, last, or follow another hyphen", kind, name)
	case idPrefix != "" && strings.HasPrefix(name, idPrefix):
		return invalidf("Invalid %s name %q: the name cannot start with %q", kind, name, idPrefix)
	}

	return nil
}

func validatePort(port int32) error {
	if port < minPort || port > maxPort {
		return invalidf("Invalid port %d: must be between %d and %d", port, minPort, maxPort)
	}

	return nil
}

func validateListQuery(c *echo.Context) error {
	q := c.Request().URL.Query()

	if tok := q.Get("nextToken"); tok != "" {
		if err := page.ValidateToken(tok); err != nil {
			return invalidf("Invalid nextToken: %q", tok)
		}
	}

	if v := q.Get("maxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListResults {
			return invalidf("Invalid maxResults %q: must be between 1 and %d", v, maxListResults)
		}
	}

	return nil
}

// checkActionTargetGroups rejects forward actions that name a target group
// which does not exist. Caller holds the lock.
func (b *InMemoryBackend) checkActionTargetGroups(action *RuleAction) error {
	if action == nil {
		return nil
	}

	for _, wtg := range action.ForwardTargetGroups {
		if _, ok := b.resolveTargetGroupID(wtg.TargetGroupID); !ok {
			return &detailError{base: ErrNotFound, msg: "Resource " + wtg.TargetGroupID + " not found"}
		}
	}

	return nil
}
