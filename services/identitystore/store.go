package identitystore

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// defaultMaxResults is the page size used when MaxResults is 0 or > maxListPageSize.
const (
	defaultMaxResults = 100
	// maxListPageSize is the upper bound a caller may request per page.
	maxListPageSize = 100
)

// attrDisplayName is the normalized attribute path for a group's or user's display name.
const attrDisplayName = "displayname"

// ----------------------------------------
// InMemoryBackend
// ----------------------------------------

// InMemoryBackend is the in-memory store for the Identity Store service.
//
// Users, groups, and memberships were previously nested by region
// (map[region]map[id]*T, with IdentityStoreID carried as a field for
// per-store filtering within a region). Phase 3.3 flattens each into a
// single *store.Table keyed by the composite "region#id" string (regionKey
// in this file), with secondary *store.Index lookups replacing the old
// per-region reverse-lookup maps -- see store_setup.go for every key
// function and index, and persistence.go for how the hidden `region` field
// each value type carries is round-tripped through Snapshot/Restore.
// Callers must hold b.mu.
type InMemoryBackend struct {
	users                    *store.Table[User]
	usersByStore             *store.Index[User]
	usersByUserName          *store.Index[User]
	usersByPrimaryEmail      *store.Index[User]
	groups                   *store.Table[Group]
	groupsByStore            *store.Index[Group]
	groupsByDisplayName      *store.Index[Group]
	memberships              *store.Table[GroupMembership]
	membershipsByGroup       *store.Index[GroupMembership]
	membershipsByMember      *store.Index[GroupMembership]
	membershipsByGroupMember *store.Index[GroupMembership]
	registry                 *store.Registry
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string
	counter                  int
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if accountID == "" {
		accountID = config.DefaultAccountID
	}

	if region == "" {
		region = config.DefaultRegion
	}

	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("identitystore"),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region#id") for
// the resources flattened from this backend's old region-nested maps.
func regionKey(region, id string) string { return region + "#" + id }

// storeKey builds the composite index key ("region#storeID") isolating an
// identity store within a region -- the shared prefix for every
// byStore/byUserName/byDisplayName/byGroup/byMember/byGroupMember index key
// in store_setup.go.
func storeKey(region, storeID string) string { return region + "#" + storeID }

// Region returns the backend default region.
func (b *InMemoryBackend) Region() string { return b.region }

// generateID creates a UUID-format unique ID matching the AWS Identity Store format.
func (b *InMemoryBackend) generateID() string {
	return uuid.New().String()
}

// simulatedCallerARN returns a deterministic placeholder ARN for the
// CreatedBy/UpdatedBy fields real AWS populates with the calling principal's
// ARN. gopherstack does not model IAM caller identity for Identity Store, so
// this returns a stable, wire-shape-valid ARN instead of leaving the field
// empty (mirrors services/bedrock's identical helper).
func (b *InMemoryBackend) simulatedCallerARN() string {
	return arn.Build("iam", "", b.accountID, "root")
}

// splitExternalIDCompound splits a compound ExternalId key (encoded by extractAlternateIdentifier)
// into its Issuer and Id parts. Both Issuer and Id must match for a lookup to succeed.
func splitExternalIDCompound(compound string) (string, string) {
	if i := strings.IndexByte(compound, externalIDSep[0]); i >= 0 {
		return compound[:i], compound[i+1:]
	}

	return "", compound
}

// paginateSlice returns a single page of items from a slice and the next-page
// token. maxResults <= 0 means "use default". A nil token is returned when
// there are no more pages.
func paginateSlice[T any](items []T, maxResults int32, nextToken string) ([]T, *string) {
	if len(items) == 0 {
		return items, nil
	}

	// Decode the offset from the cursor token.
	offset := 0
	if nextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n >= 0 {
				offset = n
			}
		}
	}

	if offset >= len(items) {
		return nil, nil
	}

	items = items[offset:]

	limit := int(maxResults)
	if limit <= 0 || limit > maxListPageSize {
		limit = defaultMaxResults
	}

	if limit >= len(items) {
		return items, nil
	}

	page := items[:limit]
	encoded := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + limit)))

	return page, &encoded
}

// Reset clears all user, group, and membership state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.counter = 0
}
