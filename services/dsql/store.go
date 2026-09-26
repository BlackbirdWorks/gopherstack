package dsql

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	dsqlServiceName = "dsql"

	// clusterActivationDelay/clusterDeletionDelay bound how long a cluster
	// stays CREATING/UPDATING or DELETING before this backend lazily
	// advances it to ACTIVE or removes it on the next read -- see
	// PARITY.md's items_still_open for why this is a lazy deadline rather
	// than a background reconciler.
	clusterActivationDelay = 750 * time.Millisecond
	clusterDeletionDelay   = 750 * time.Millisecond
	streamActivationDelay  = 500 * time.Millisecond

	maxClustersPerAccountRegion = 20
	maxStreamsPerCluster        = 20
	maxTagsPerResource          = 50

	identifierLength = 26
	identifierAlpha  = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	clusters *store.Table[Cluster]
	streams  *store.Table[Stream]
	registry *store.Registry
	mu       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory Aurora DSQL backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry: store.NewRegistry(),
		mu:       lockmetrics.New(dsqlServiceName),
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

// newIdentifier returns a random lowercase-alphanumeric identifier matching
// the shape of a real DSQL cluster/stream identifier (documented as an
// opaque generated ID, not caller-supplied).
func newIdentifier() string {
	buf := make([]byte, identifierLength)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand.Read failing is not something callers can recover from
		// meaningfully; fall back to a hex timestamp so the backend never
		// panics on a degraded entropy source.
		return hex.EncodeToString([]byte(fmt.Sprintf("%x", time.Now().UnixNano())))[:identifierLength]
	}

	out := make([]byte, identifierLength)
	for i, v := range buf {
		out[i] = identifierAlpha[int(v)%len(identifierAlpha)]
	}

	return string(out)
}

// newVersionToken returns a short random hex token used for cluster policy
// optimistic-concurrency versions.
func newVersionToken() string {
	buf := make([]byte, versionTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return hex.EncodeToString([]byte(fmt.Sprintf("%x", time.Now().UnixNano())))
	}

	return hex.EncodeToString(buf)
}

const versionTokenBytes = 8

func clusterARN(region, accountID, identifier string) string {
	return arn.Build(dsqlServiceName, region, accountID, "cluster/"+identifier)
}

func clusterEndpoint(identifier, region string) string {
	return fmt.Sprintf("%s.dsql.%s.on.aws", identifier, region)
}

func streamARN(region, accountID, clusterIdentifier, streamIdentifier string) string {
	return arn.Build(dsqlServiceName, region, accountID,
		fmt.Sprintf("cluster/%s/stream/%s", clusterIdentifier, streamIdentifier))
}

// streamKey returns the composite primary key for the streams table: stream
// identifiers are only unique within their owning cluster.
func streamKey(clusterIdentifier, streamIdentifier string) string {
	return clusterIdentifier + "/" + streamIdentifier
}

// splitStreamKey is the inverse of streamKey.
func splitStreamKey(key string) (string, string) {
	clusterIdentifier, streamIdentifier, _ := strings.Cut(key, "/")

	return clusterIdentifier, streamIdentifier
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return ErrValidation
	}

	for k := range tags {
		if k == "" {
			return ErrValidation
		}
	}

	return nil
}

// resolveClusterLocked returns the live (mutable) cluster for identifier
// after applying any due lazy status transition. Callers must hold b.mu for
// writing. A cluster whose DELETING deadline has passed is removed from the
// table and reported as not found, matching real AWS once deletion completes.
func (b *InMemoryBackend) resolveClusterLocked(identifier string) (*Cluster, error) {
	c, ok := b.clusters.Get(identifier)
	if !ok {
		return nil, ErrClusterNotFound
	}

	b.advanceClusterLocked(c)

	if c.Status == statusDeleting && time.Now().After(c.PendingUntil) {
		b.clusters.Delete(identifier)

		return nil, ErrClusterNotFound
	}

	return c, nil
}

// advanceClusterLocked flips a CREATING/UPDATING cluster to ACTIVE once its
// PendingUntil deadline has passed. Callers must hold b.mu for writing.
func (b *InMemoryBackend) advanceClusterLocked(c *Cluster) {
	if c.PendingUntil.IsZero() || time.Now().Before(c.PendingUntil) {
		return
	}

	switch c.Status {
	case statusCreating, statusUpdating:
		c.Status = statusActive
		c.PendingUntil = time.Time{}
	}
}

// resolveStreamLocked returns the live (mutable) stream, applying any due
// lazy activation. Callers must hold b.mu for writing.
func (b *InMemoryBackend) resolveStreamLocked(clusterIdentifier, streamIdentifier string) (*Stream, error) {
	s, ok := b.streams.Get(streamKey(clusterIdentifier, streamIdentifier))
	if !ok {
		return nil, ErrStreamNotFound
	}

	b.advanceStreamLocked(s)

	return s, nil
}

func (b *InMemoryBackend) advanceStreamLocked(s *Stream) {
	if s.PendingUntil.IsZero() || time.Now().Before(s.PendingUntil) {
		return
	}

	if s.Status == streamStatusCreating {
		s.Status = streamStatusActive
		s.PendingUntil = time.Time{}
	}
}

// clusterIdentifierFromResourceARN extracts the cluster identifier from a
// DSQL cluster ARN (arn:{partition}:dsql:{region}:{account}:cluster/{id}),
// used by TagResource/UntagResource/ListTagsForResource, which key off an
// ARN rather than a bare identifier.
func clusterIdentifierFromResourceARN(resourceARN string) (string, bool) {
	// arn:partition:service:region:account:resource
	parts := strings.SplitN(resourceARN, ":", arnPartsCount)
	if len(parts) != arnPartsCount || parts[2] != dsqlServiceName {
		return "", false
	}

	name, ok := strings.CutPrefix(parts[5], "cluster/")
	if !ok {
		return "", false
	}

	return name, true
}

const arnPartsCount = 6
