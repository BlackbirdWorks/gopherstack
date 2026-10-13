package grafana

import (
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxListResults = 100

//nolint:gochecknoglobals // immutable lookup tables
var (
	workspaceNameRe = regexp.MustCompile(`^[a-zA-Z0-9-._~]{1,255}$`)

	validDataSources = map[string]bool{
		"AMAZON_OPENSEARCH_SERVICE": true, "CLOUDWATCH": true, "PROMETHEUS": true, "XRAY": true,
		"TIMESTREAM": true, "SITEWISE": true, "ATHENA": true, "REDSHIFT": true, "TWINMAKER": true,
	}
	validNotificationDestinations = map[string]bool{"SNS": true}
)

func validateWorkspaceName(name string) error {
	if name != "" && !workspaceNameRe.MatchString(name) {
		return validationError(
			"invalid workspaceName: must be 1-255 characters of letters, digits, '-', '.', '_' or '~'",
		)
	}

	return nil
}

func validateEnumList(field string, values []string, allowed map[string]bool) error {
	for _, v := range values {
		if !allowed[v] {
			return validationError("invalid " + field + " value: " + v)
		}
	}

	return nil
}

// validateListQuery rejects malformed pagination parameters on any request.
func validateListQuery(r *http.Request) error {
	if r.Method != http.MethodGet {
		return nil
	}

	q := r.URL.Query()

	if tok := q.Get("nextToken"); tok != "" {
		if err := page.ValidateToken(tok); err != nil {
			return validationError("invalid nextToken")
		}
	}

	if v := q.Get("maxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListResults {
			return validationError("maxResults must be between 1 and " + strconv.Itoa(maxListResults))
		}
	}

	return nil
}

// SetLifecycleDelay overrides the built-in workspace transition window;
// 0 keeps the built-in value.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// transitionDelay returns the active transition window. Caller holds b.mu
// or runs before any concurrent use.
func (b *InMemoryBackend) transitionDelay() time.Duration {
	if b.lifecycleDelay > 0 {
		return b.lifecycleDelay
	}

	return workspaceTransitionDelay
}
