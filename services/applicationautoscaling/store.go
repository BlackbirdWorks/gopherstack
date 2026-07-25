package applicationautoscaling

import (
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// maxTagsPerResource is the AWS limit on the number of tags per resource.
const maxTagsPerResource = 50

// maxDescribeResults is the upper bound for MaxResults on Describe* operations.
const maxDescribeResults = 100

// maxForecastWindow is the maximum allowed [startTime, endTime) range for
// GetPredictiveScalingForecast, matching the real AWS constraint of 14 days.
const maxForecastWindow = 14 * 24 * time.Hour

// InMemoryBackend stores Application Auto Scaling state in memory.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment.
	registry        *store.Registry
	scalableTargets *store.Table[ScalableTarget]
	// targetsByARN is a secondary index over scalableTargets grouping by ARN,
	// answering the "target for resource ARN X" lookups TagResource,
	// ListTagsForResource, and UntagResource need. It replaces the previous
	// map[string]string targetARNIndex reverse-lookup map.
	targetsByARN    *store.Index[ScalableTarget]
	scalingPolicies *store.Table[ScalingPolicy]
	// policiesByName is a secondary index over scalingPolicies grouping by
	// the (serviceNamespace,resourceId,scalableDimension,policyName)
	// composite key built by policyNameKey. It replaces the previous
	// map[string]string policyNameIndex reverse-lookup map.
	policiesByName   *store.Index[ScalingPolicy]
	scheduledActions *store.Table[ScheduledAction]
	// actionsByName is a secondary index over scheduledActions grouping by
	// the (serviceNamespace,resourceId,scalableDimension,scheduledActionName)
	// composite key built by actionNameKey. It replaces the previous
	// map[string]string actionNameIndex reverse-lookup map.
	actionsByName *store.Index[ScheduledAction]
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
	// scalingActivities is append-order-sensitive: DescribeScalingActivities
	// returns entries most-recent-first via slices.Backward over this exact
	// slice. store.Table has no defined insertion order (see pkgs/store's
	// package doc), so this is intentionally left as a raw slice rather than
	// converted.
	scalingActivities []*ScalingActivity
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("applicationautoscaling"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// scalableTargetKey returns the backend key for a scalable target.
func scalableTargetKey(serviceNamespace, resourceID, scalableDimension string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension
}

// policyNameKey returns the secondary-index key for a scaling policy.
func policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension + "/" + policyName
}

// actionNameKey returns the secondary-index key for a scheduled action.
func actionNameKey(serviceNamespace, resourceID, scalableDimension, scheduledActionName string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension + "/" + scheduledActionName
}

// mergeTags merges src into dst enforcing the per-resource tag limit.
// dst must be non-nil; callers are responsible for initialising it before the call.
// overLimitErr is the sentinel to wrap when the merge would exceed the limit:
// callers must pass the AWS exception type modeled for their operation --
// TagResource models TooManyTagsException (ErrTooManyTags), while
// RegisterScalableTarget's modeled error set has no TooManyTagsException at
// all and uses LimitExceededException (ErrLimitExceeded) instead (confirmed
// against each op's deserializeOpError* switch in the vendored SDK).
func mergeTags(dst map[string]string, src map[string]string, overLimitErr error) error {
	if len(src) == 0 {
		return nil
	}

	// Count net-new keys (keys that do not already exist in dst).
	netNew := 0
	for k := range src {
		if _, exists := dst[k]; !exists {
			netNew++
		}
	}

	if len(dst)+netNew > maxTagsPerResource {
		return fmt.Errorf(
			"%w: tag count would exceed maximum allowed (%d)",
			overLimitErr,
			maxTagsPerResource,
		)
	}

	maps.Copy(dst, src)

	return nil
}

// encodePageToken opaquely encodes a sort-key cursor. Real AWS NextToken
// values are opaque (never a raw resource identifier); base64-encoding here
// also gives paginate a cheap, genuine way to detect a malformed NextToken
// and return InvalidNextTokenException, matching every Describe* op's
// modeled error set (confirmed against each op's deserializeOpError* switch
// in the vendored SDK -- all four Describe ops here model
// InvalidNextTokenException).
func encodePageToken(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

// decodePageToken reverses [encodePageToken]. An empty token decodes to "",
// which paginate treats as "start of list". A non-empty token that fails to
// base64-decode is reported via ok=false so the caller can surface
// InvalidNextTokenException instead of silently treating it as valid.
func decodePageToken(token string) (string, bool) {
	if token == "" {
		return "", true
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", false
	}

	return string(b), true
}

// paginate sorts list by keyFn, applies the opaque nextToken cursor, and returns
// at most maxResults items plus the token for the following page (empty when the
// page is the last). The cursor is the base64-encoded sort key of the first item
// of the next page (see [encodePageToken]), which is a stable cursor as long as
// keyFn is unique and ordering is deterministic. This is what lets Application
// Auto Scaling Describe* ops report a real NextToken rather than always-empty.
// Returns ErrInvalidNextToken if nextToken is non-empty and fails to decode.
func paginate[T any](list []T, maxResults int32, nextToken string, keyFn func(T) string) ([]T, string, error) {
	sort.Slice(list, func(i, j int) bool {
		return keyFn(list[i]) < keyFn(list[j])
	})

	cursor, ok := decodePageToken(nextToken)
	if !ok {
		return nil, "", fmt.Errorf("%w: NextToken is invalid", ErrInvalidNextToken)
	}

	start := 0

	if cursor != "" {
		for i := range list {
			if keyFn(list[i]) >= cursor {
				start = i

				break
			}

			start = i + 1
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > int(maxDescribeResults) {
		limit = int(maxDescribeResults)
	}

	end := min(start+limit, len(list))

	page := list[start:end]

	next := ""
	if end < len(list) {
		next = encodePageToken(keyFn(list[end]))
	}

	return page, next, nil
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.scalingActivities = nil
}

// Purge removes all resources from the backend. It is safe to call concurrently.
func (b *InMemoryBackend) Purge() {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.scalingActivities = nil
}
