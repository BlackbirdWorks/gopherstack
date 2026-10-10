package dsql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minListResults = 1
	maxListResults = 100

	resourceTypeCluster = "cluster"
	resourceTypeStream  = "stream"

	resourceContextKey = "dsql.resource"
)

var clusterIdentifierRe = regexp.MustCompile(`^[a-z0-9]{26}$`)

// SetLifecycleDelay overrides the CREATING/UPDATING/DELETING windows for
// clusters and streams; 0 keeps the built-in values.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// dwell returns the configured transition window or def; callers hold b.mu.
func (b *InMemoryBackend) dwell(def time.Duration) time.Duration {
	if b.lifecycleDelay > 0 {
		return b.lifecycleDelay
	}

	return def
}

func validatePolicyDocument(policy string) error {
	var doc map[string]any
	if policy == "" || json.Unmarshal([]byte(policy), &doc) != nil || doc == nil {
		return fmt.Errorf("%w: policy must be a valid JSON policy document", ErrValidation)
	}

	return nil
}

// validateRequest checks path identifiers and list parameters before dispatch.
func (h *Handler) validateRequest(c *echo.Context, op, resource string) (bool, error) {
	ref := resourceRef{clusterID: resource}

	switch op {
	case opCreateCluster, opListClusters, opTagResource, opUntagResource, opListTagsForResource:
		ref.clusterID = ""
	case opGetStream, opDeleteStream:
		ref.clusterID, ref.streamID = splitStreamKey(resource)
	}

	c.Set(resourceContextKey, ref)
	clusterID := ref.clusterID

	if clusterID != "" && !clusterIdentifierRe.MatchString(clusterID) {
		return true, h.writeError(c, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("1 validation error detected: Value '%s' at 'identifier' failed to satisfy constraint: "+
				"Member must satisfy regular expression pattern: ^[a-z0-9]{26}$", clusterID),
			validationReasonFieldError)
	}

	if op == opListClusters || op == opListStreams {
		return h.validateListParams(c)
	}

	return false, nil
}

func (h *Handler) validateListParams(c *echo.Context) (bool, error) {
	q := c.Request().URL.Query()

	if v := q.Get("max-results"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < minListResults || n > maxListResults {
			return true, h.writeError(c, http.StatusBadRequest, "ValidationException",
				fmt.Sprintf("Value '%s' at 'maxResults' failed to satisfy constraint: "+
					"Member must have value between %d and %d", v, minListResults, maxListResults),
				validationReasonFieldError)
		}
	}

	if err := page.ValidateToken(q.Get("next-token")); err != nil {
		return true, h.writeError(c, http.StatusBadRequest, "ValidationException",
			"Invalid nextToken", validationReasonFieldError)
	}

	return false, nil
}

type resourceRef struct {
	clusterID string
	streamID  string
}

func (r resourceRef) forError(err error) (string, string) {
	if errors.Is(err, ErrStreamNotFound) {
		return r.streamID, resourceTypeStream
	}

	return r.clusterID, resourceTypeCluster
}
