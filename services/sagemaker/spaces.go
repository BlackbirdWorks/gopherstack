package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrSpaceNotFound is returned when a space does not exist.
var ErrSpaceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

// Space represents a SageMaker Studio space. OwnershipSettings/SpaceSettings/
// SpaceSharingSettings are stored as opaque JSON (the json.RawMessage
// passthrough convention used elsewhere in this service for deeply-nested
// config shapes) — the client's Create payload is echoed back verbatim on
// Describe.
type Space struct {
	CreationTime         time.Time         `json:"CreationTime"`
	LastModifiedTime     time.Time         `json:"LastModifiedTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	SpaceName            string            `json:"SpaceName"`
	SpaceArn             string            `json:"SpaceArn"`
	DomainID             string            `json:"DomainId"`
	SpaceStatus          string            `json:"SpaceStatus"`
	SpaceDisplayName     string            `json:"SpaceDisplayName,omitempty"`
	OwnershipSettings    json.RawMessage   `json:"OwnershipSettings,omitempty"`
	SpaceSettings        json.RawMessage   `json:"SpaceSettings,omitempty"`
	SpaceSharingSettings json.RawMessage   `json:"SpaceSharingSettings,omitempty"`
}

func cloneSpace(s *Space) *Space {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)
	cp.OwnershipSettings = append(json.RawMessage(nil), s.OwnershipSettings...)
	cp.SpaceSettings = append(json.RawMessage(nil), s.SpaceSettings...)
	cp.SpaceSharingSettings = append(json.RawMessage(nil), s.SpaceSharingSettings...)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeSpace.
func (s *Space) MarshalJSON() ([]byte, error) {
	type alias Space

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(s),
		CreationTime:     epochSeconds(s.CreationTime),
		LastModifiedTime: epochSeconds(s.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [Space.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (s *Space) UnmarshalJSON(data []byte) error {
	type alias Space

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(s)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	s.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

func spaceKey(domainID, spaceName string) string {
	return domainID + "/" + spaceName
}

// CreateSpaceOptions bundles CreateSpace's optional fields.
type CreateSpaceOptions struct {
	SpaceDisplayName     string
	OwnershipSettings    json.RawMessage
	SpaceSettings        json.RawMessage
	SpaceSharingSettings json.RawMessage
}

// CreateSpace creates a SageMaker Studio space.
func (b *InMemoryBackend) CreateSpace(
	ctx context.Context,
	domainID, spaceName string,
	tags map[string]string,
	opts CreateSpaceOptions,
) (*Space, error) {
	b.mu.Lock("CreateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if domainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", ErrValidation)
	}

	if spaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", ErrValidation)
	}

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); ok {
		return nil, fmt.Errorf("%w: space %q already exists in domain %q", ErrValidation, spaceName, domainID)
	}

	spaceARN := arn.Build("sagemaker", region, b.accountID, "space/"+domainID+"/"+spaceName)
	now := time.Now()

	s := &Space{
		SpaceName:            spaceName,
		SpaceArn:             spaceARN,
		DomainID:             domainID,
		SpaceStatus:          "InService",
		Tags:                 mergeTags(nil, tags),
		CreationTime:         now,
		LastModifiedTime:     now,
		SpaceDisplayName:     opts.SpaceDisplayName,
		OwnershipSettings:    opts.OwnershipSettings,
		SpaceSettings:        opts.SpaceSettings,
		SpaceSharingSettings: opts.SpaceSharingSettings,
	}
	b.spacesStore(region).Put(s)

	return cloneSpace(s), nil
}

// DescribeSpace returns a space by domain ID and space name.
func (b *InMemoryBackend) DescribeSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.RLock("DescribeSpace")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	s, ok := b.spacesStoreRO(region).Get(spaceKey(domainID, spaceName))
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	return cloneSpace(s), nil
}

// DeleteSpace removes a space.
func (b *InMemoryBackend) DeleteSpace(ctx context.Context, domainID, spaceName string) error {
	b.mu.Lock("DeleteSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); !ok {
		return fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	store := b.spacesStore(region)
	store.Delete(key)

	return nil
}

// Enum values for ListSpaces' SortBy (aws-sdk-go-v2/service/sagemaker
// types.SpaceSortKey).
const (
	spaceSortKeyCreationTime     = "CreationTime"
	spaceSortKeyLastModifiedTime = "LastModifiedTime"
)

// ListSpacesParams bundles ListSpaces' filter/sort/pagination criteria.
type ListSpacesParams struct {
	DomainIDEquals    string
	SpaceNameContains string
	SortBy            string
	SortOrder         string
	NextToken         string
	MaxResults        int32
}

// ListSpaces returns spaces matching params, sorted per params.SortBy
// (default CreationTime)/params.SortOrder (default Ascending), capped at
// params.MaxResults.
func (b *InMemoryBackend) ListSpaces(ctx context.Context, params ListSpacesParams) ([]*Space, string) {
	b.mu.RLock("ListSpaces")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	all := b.spacesStoreRO(region).All()
	list := make([]*Space, 0, len(all))

	for _, s := range all {
		if params.DomainIDEquals != "" && s.DomainID != params.DomainIDEquals {
			continue
		}

		if params.SpaceNameContains != "" && !strings.Contains(s.SpaceName, params.SpaceNameContains) {
			continue
		}

		list = append(list, cloneSpace(s))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch params.SortBy {
		case spaceSortKeyLastModifiedTime:
			less = list[i].LastModifiedTime.Before(list[j].LastModifiedTime)
		case spaceSortKeyCreationTime:
			fallthrough
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// UpdateSpace updates a space in a domain. Returns the updated space.
func (b *InMemoryBackend) UpdateSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.Lock("UpdateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	s, ok := b.spacesStore(region).Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	s.LastModifiedTime = time.Now()

	return cloneSpace(s), nil
}
