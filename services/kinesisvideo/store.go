package kinesisvideo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	maxStreamNameLen  = 256
	maxChannelNameLen = 256
	maxTagsPerStream  = 50
	minDataRetention  = 0
	maxDataRetention  = 87600
	defaultListLimit  = 500

	channelTypeSingleMaster = "SINGLE_MASTER"
	defaultMessageTTLSecs   = int32(60)

	comparisonOperatorBeginsWith = "BEGINS_WITH"

	operationIncreaseDataRetention = "INCREASE_DATA_RETENTION"
	operationDecreaseDataRetention = "DECREASE_DATA_RETENTION"
)

var resourceNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	streams  *store.Table[Stream]
	channels *store.Table[Channel]
	registry *store.Registry
	mu       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory Kinesis Video Streams backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry: store.NewRegistry(),
		mu:       lockmetrics.New("kinesisvideo"),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func validateResourceName(name string, maxLen int) error {
	if len(name) == 0 || len(name) > maxLen || !resourceNameRE.MatchString(name) {
		return ErrValidation
	}

	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerStream {
		return ErrValidation
	}

	for k := range tags {
		if k == "" {
			return ErrValidation
		}
	}

	return nil
}

func newVersion() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// shortHash returns a short, deterministic hex digest of s, used to derive
// stable-looking pseudo-random endpoint hostnames.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])[:8]
}

func streamARN(region, accountID, name string, creationTime int64) string {
	return arn.Build("kinesisvideo", region, accountID, fmt.Sprintf("stream/%s/%d", name, creationTime))
}

func channelARN(region, accountID, name string, creationTime int64) string {
	return arn.Build("kinesisvideo", region, accountID, fmt.Sprintf("channel/%s/%d", name, creationTime))
}

// streamNameFromARN extracts the stream name from a well-formed KVS stream ARN
// (arn:{partition}:kinesisvideo:{region}:{account}:stream/{name}/{creationEpochMillis}).
func streamNameFromARN(streamARNStr string) (string, bool) {
	return resourceNameFromARN(streamARNStr, "stream/")
}

// channelNameFromARN extracts the channel name from a well-formed KVS channel ARN
// (arn:{partition}:kinesisvideo:{region}:{account}:channel/{name}/{creationEpochMillis}).
func channelNameFromARN(channelARNStr string) (string, bool) {
	return resourceNameFromARN(channelARNStr, "channel/")
}

func resourceNameFromARN(arnStr, prefix string) (string, bool) {
	parts := strings.SplitN(arnStr, ":", 6) //nolint:mnd // arn:partition:service:region:account:resource
	if len(parts) != 6 || !strings.HasPrefix(parts[5], prefix) {
		return "", false
	}

	rest := strings.TrimPrefix(parts[5], prefix)

	name, _, _ := strings.Cut(rest, "/")
	if name == "" {
		return "", false
	}

	return name, true
}

// resolveStreamLocked returns the stream identified by name or ARN (name takes
// precedence when both are set, matching AWS's documented behavior). Callers
// must hold b.mu.
func (b *InMemoryBackend) resolveStreamLocked(name, streamARNStr string) (*Stream, error) {
	if name != "" {
		s, ok := b.streams.Get(name)
		if !ok {
			return nil, ErrStreamNotFound
		}

		return s, nil
	}

	if streamARNStr != "" {
		resolved, ok := streamNameFromARN(streamARNStr)
		if ok {
			if s, exists := b.streams.Get(resolved); exists {
				return s, nil
			}
		}

		return nil, ErrStreamNotFound
	}

	return nil, ErrValidation
}

// resolveChannelLocked returns the channel identified by name or ARN. Callers
// must hold b.mu.
func (b *InMemoryBackend) resolveChannelLocked(name, channelARNStr string) (*Channel, error) {
	if name != "" {
		c, ok := b.channels.Get(name)
		if !ok {
			return nil, ErrChannelNotFound
		}

		return c, nil
	}

	if channelARNStr != "" {
		resolved, ok := channelNameFromARN(channelARNStr)
		if ok {
			if c, exists := b.channels.Get(resolved); exists {
				return c, nil
			}
		}

		return nil, ErrChannelNotFound
	}

	return nil, ErrValidation
}
