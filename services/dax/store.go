package dax

import (
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	// maxClustersDefault is the default maximum number of clusters per describe call.
	maxClustersDefault = 100

	// maxPageSizeDefault is the default page size for paginated describe calls.
	maxPageSizeDefault = 100

	// maxResourceNameLength is the maximum allowed length for parameter/subnet group names.
	maxResourceNameLength = 255
)

// nameRegexp validates DAX resource names: must start with a letter, contain only
// letters/digits/hyphens, and not end with a hyphen. Used for clusters, parameter groups,
// and subnet groups.
var nameRegexp = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// InMemoryBackend is the in-memory DAX backend.
//
// clusters, paramGroups, and subnetGroups are *store.Table[T] (see
// store_setup.go). Each keys off a real, non-json:"-" identity field the
// value type already carries, so all three register directly on registry --
// no DTO indirection and no secondary store.Index are needed (arnExists and
// clusterByARN parse the resource name back out of the ARN string and look it
// up directly, so there is no ARN-keyed reverse-lookup map to replace). tags
// and events are left as plain fields; see store_setup.go's file doc comment.
type InMemoryBackend struct {
	clusters     *store.Table[Cluster]
	paramGroups  *store.Table[ParameterGroup]
	subnetGroups *store.Table[SubnetGroup]
	registry     *store.Registry
	tags         map[string]map[string]string
	mu           *lockmetrics.RWMutex
	AccountID    string
	Region       string
	events       []*Event
}

// NewInMemoryBackend creates a new DAX backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
		events:    make([]*Event, 0),
		mu:        lockmetrics.New("dax"),
		AccountID: accountID,
		Region:    region,
	}

	registerAllTables(b)
	b.seedDefaults()

	return b
}

// seedDefaults populates factory-default parameter and subnet groups.
func (b *InMemoryBackend) seedDefaults() {
	params := make(map[string]string, len(defaultParameterValues))
	maps.Copy(params, defaultParameterValues)

	b.paramGroups.Put(&ParameterGroup{
		ParameterGroupName: DefaultParameterGroupName,
		Description:        "Default parameter group for DAX 1.0",
		Parameters:         params,
	})

	b.subnetGroups.Put(&SubnetGroup{
		SubnetGroupName:       DefaultSubnetGroupName,
		Description:           "Default subnet group",
		VpcID:                 "vpc-default",
		Subnets:               []SubnetEntry{{SubnetID: "subnet-default", AvailabilityZone: b.Region + "a"}},
		SupportedNetworkTypes: []string{NetworkTypeIPv4},
	})
}

// validateResourceName validates a parameter group or subnet group name.
func validateResourceName(name, kind string) error {
	if name == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameterValue, kind)
	}

	if len(name) > maxResourceNameLength {
		return fmt.Errorf(
			"%w: %s %q exceeds maximum length of %d characters",
			ErrInvalidParameterValue, kind, name, maxResourceNameLength,
		)
	}

	if !nameRegexp.MatchString(name) {
		return fmt.Errorf(
			"%w: %s %q is invalid: must start with a letter, "+
				"contain only letters, numbers, and hyphens, and not end with a hyphen",
			ErrInvalidParameterValue, kind, name,
		)
	}

	if strings.Contains(name, "--") {
		return fmt.Errorf(
			"%w: %s %q is invalid: must not contain consecutive hyphens",
			ErrInvalidParameterValue, kind, name,
		)
	}

	return nil
}

func paginateList[T any](
	all []T,
	maxResults int,
	nextToken string,
	getName func(T) string,
	copyFunc func(T) T,
) ([]T, string) {
	sort.Slice(all, func(i, j int) bool {
		return getName(all[i]) < getName(all[j])
	})

	start := 0
	if nextToken != "" {
		for i, item := range all {
			if getName(item) == nextToken {
				start = i

				break
			}
		}
	}

	if start >= len(all) {
		return []T{}, ""
	}

	end := start + maxResults
	newNextToken := ""
	if end < len(all) {
		newNextToken = getName(all[end])
	} else {
		end = len(all)
	}

	page := all[start:end]
	result := make([]T, 0, len(page))
	for _, item := range page {
		result = append(result, copyFunc(item))
	}

	return result, newNextToken
}

// describeNamedGroups implements the shared DAX "describe named groups with
// pagination" pattern: a named lookup returns all matches unpaginated (erroring
// on any missing name), while an unfiltered request returns one paginated page.
func describeNamedGroups[T any](
	tbl *store.Table[T],
	names []string,
	maxResults int,
	nextToken string,
	notFound error,
	nameOf func(*T) string,
	copyFn func(*T) *T,
) ([]*T, string, error) {
	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	if len(names) > 0 {
		all := make([]*T, 0, len(names))
		for _, name := range names {
			item, ok := tbl.Get(name)
			if !ok {
				return nil, "", fmt.Errorf("%w: %s", notFound, name)
			}
			all = append(all, item)
		}

		result := make([]*T, 0, len(all))
		for _, item := range all {
			result = append(result, copyFn(item))
		}

		return result, "", nil
	}

	result, newNextToken := paginateList(tbl.All(), maxResults, nextToken, nameOf, copyFn)

	return result, newNextToken, nil
}

// Reset clears all DAX state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.events = make([]*Event, 0)

	b.seedDefaults()
}

const defaultTTL = 5 * time.Minute

// GetDefaultTTL returns the TTLs configured in the first available cluster's param group, or defaults.
func (b *InMemoryBackend) GetDefaultTTL() (time.Duration, time.Duration) {
	b.mu.RLock("GetDefaultTTL")
	defer b.mu.RUnlock()

	// Default to 5 minutes if no clusters exist.
	recordTTL := defaultTTL
	queryTTL := defaultTTL

	b.clusters.Range(func(c *Cluster) bool {
		pgName := c.ParameterGroup.ParameterGroupName
		pg, ok := b.paramGroups.Get(pgName)
		if !ok {
			return false
		}

		if val, has := pg.Parameters[paramRecordTTL]; has {
			if ms, err := strconv.ParseInt(val, 10, 64); err == nil {
				recordTTL = time.Duration(ms) * time.Millisecond
			}
		}
		if val, has := pg.Parameters[paramQueryTTL]; has {
			if ms, err := strconv.ParseInt(val, 10, 64); err == nil {
				queryTTL = time.Duration(ms) * time.Millisecond
			}
		}

		return false // just use the first cluster's param group
	})

	return recordTTL, queryTTL
}
