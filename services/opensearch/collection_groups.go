package opensearch

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// slCollectionGroupGenerationDefault is used when CreateCollectionGroup omits
// the optional Generation field. Real AOSS collections predating collection
// groups are "Classic" (developer guide, "Managing capacity limits":
// "Classic collections can be created without a collection group"), so a
// newly created group defaults to the same generation.
const slCollectionGroupGenerationDefault = "CLASSIC"

// CollectionGroupCapacityLimits mirrors
// types.CollectionGroupCapacityLimits (opensearchserverless@v1.34.4
// types/types.go:216-233): four independently optional OCU bounds.
type CollectionGroupCapacityLimits struct {
	MaxIndexingCapacityInOCU *float64 `json:"maxIndexingCapacityInOCU,omitempty"`
	MaxSearchCapacityInOCU   *float64 `json:"maxSearchCapacityInOCU,omitempty"`
	MinIndexingCapacityInOCU *float64 `json:"minIndexingCapacityInOCU,omitempty"`
	MinSearchCapacityInOCU   *float64 `json:"minSearchCapacityInOCU,omitempty"`
}

// ServerlessCollectionGroup represents an OpenSearch Serverless collection
// group -- a container that owns collections and shares OCU capacity across
// them (developer guide, "Amazon OpenSearch Serverless collection groups").
type ServerlessCollectionGroup struct {
	CapacityLimits   *CollectionGroupCapacityLimits `json:"capacityLimits,omitempty"`
	Tags             map[string]string              `json:"tags,omitempty"`
	Description      string                         `json:"description,omitempty"`
	Generation       string                         `json:"generation,omitempty"`
	StandbyReplicas  string                         `json:"standbyReplicas,omitempty"`
	ID               string                         `json:"id"`
	Name             string                         `json:"name"`
	Arn              string                         `json:"arn"`
	CreatedDate      float64                        `json:"createdDate"`
	LastModifiedDate float64                        `json:"lastModifiedDate"`
}

func serverlessCollectionGroupKey(id string) string { return "sl-cg:" + id }

// NumberOfCollectionsForServerlessCollectionGroup always reports 0: this
// pass adds collection GROUPS themselves, not the CollectionGroupName field
// on CreateCollection/UpdateCollection that would let a collection actually
// join one (UpdateCollection stays in sdk_completeness_test.go's
// notImplemented list) -- so no collection group can genuinely have any
// members yet. Returning 0 reflects real state, not a stubbed count.
const serverlessCollectionGroupMemberCount = 0

// CreateServerlessCollectionGroup creates a new collection group.
func (b *InMemoryBackend) CreateServerlessCollectionGroup(
	name, standbyReplicas, description, generation string,
	capacityLimits *CollectionGroupCapacityLimits,
	tagMap map[string]string,
) (*ServerlessCollectionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if standbyReplicas == "" {
		return nil, fmt.Errorf("%w: StandbyReplicas is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessCollectionGroup")
	defer b.mu.Unlock()

	for _, cg := range b.slCollectionGroups.All() {
		if cg.Name == name {
			return nil, fmt.Errorf("%w: collection group %s already exists", ErrApplicationAlreadyExists, name)
		}
	}

	if generation == "" {
		generation = slCollectionGroupGenerationDefault
	}

	b.slCollGroupCounter++
	id := fmt.Sprintf("cg-%d", b.slCollGroupCounter)
	now := float64(time.Now().Unix())

	cg := &ServerlessCollectionGroup{
		ID:               id,
		Name:             name,
		Arn:              arn.Build("aoss", b.region, b.accountID, fmt.Sprintf("collection-group/%s", id)),
		StandbyReplicas:  standbyReplicas,
		Generation:       generation,
		Description:      description,
		CapacityLimits:   capacityLimits,
		Tags:             tagMap,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slCollectionGroups.Put(cg)

	cp := *cg

	return &cp, nil
}

// UpdateServerlessCollectionGroup updates a collection group's description
// and/or capacity limits. StandbyReplicas and Generation are immutable
// (UpdateCollectionGroupInput has no such fields, types.go and
// api_op_UpdateCollectionGroup.go).
func (b *InMemoryBackend) UpdateServerlessCollectionGroup(
	id, description string, capacityLimits *CollectionGroupCapacityLimits,
) (*ServerlessCollectionGroup, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateServerlessCollectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.slCollectionGroups.Get(serverlessCollectionGroupKey(id))
	if !ok {
		return nil, fmt.Errorf("%w: collection group %s not found", ErrApplicationNotFound, id)
	}

	if description != "" {
		cg.Description = description
	}

	if capacityLimits != nil {
		cg.CapacityLimits = capacityLimits
	}

	cg.LastModifiedDate = float64(time.Now().Unix())

	cp := *cg

	return &cp, nil
}

// DeleteServerlessCollectionGroup removes a collection group by ID.
func (b *InMemoryBackend) DeleteServerlessCollectionGroup(id string) error {
	b.mu.Lock("DeleteServerlessCollectionGroup")
	defer b.mu.Unlock()

	key := serverlessCollectionGroupKey(id)
	if !b.slCollectionGroups.Has(key) {
		return fmt.Errorf("%w: collection group %s not found", ErrApplicationNotFound, id)
	}

	b.slCollectionGroups.Delete(key)

	return nil
}

// ListServerlessCollectionGroups returns every collection group, sorted by
// name for deterministic pagination-free output.
func (b *InMemoryBackend) ListServerlessCollectionGroups() []*ServerlessCollectionGroup {
	b.mu.RLock("ListServerlessCollectionGroups")
	defer b.mu.RUnlock()

	out := make([]*ServerlessCollectionGroup, 0, b.slCollectionGroups.Len())
	for _, cg := range b.slCollectionGroups.All() {
		cp := *cg
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// serverlessCollectionGroupError is a not-found entry for
// BatchGetCollectionGroup (CollectionGroupErrorDetail, types.go:274-293).
type serverlessCollectionGroupError struct {
	ID, Name, ErrorCode, ErrorMessage string
}

// BatchGetServerlessCollectionGroups resolves collection groups by ID or
// name, reporting a CollectionGroupErrorDetail-shaped miss for each ID/name
// that doesn't resolve. Empty ids/names returns every collection group,
// matching BatchGetServerlessCollections' (serverless.go) existing
// no-filter convention.
func (b *InMemoryBackend) BatchGetServerlessCollectionGroups(
	ids, names []string,
) ([]*ServerlessCollectionGroup, []serverlessCollectionGroupError) {
	b.mu.RLock("BatchGetServerlessCollectionGroups")
	defer b.mu.RUnlock()

	var found []*ServerlessCollectionGroup

	var errs []serverlessCollectionGroupError

	if len(ids) == 0 && len(names) == 0 {
		for _, cg := range b.slCollectionGroups.All() {
			cp := *cg
			found = append(found, &cp)
		}

		return found, nil
	}

	byID := make(map[string]*ServerlessCollectionGroup, b.slCollectionGroups.Len())
	byName := make(map[string]*ServerlessCollectionGroup, b.slCollectionGroups.Len())

	for _, cg := range b.slCollectionGroups.All() {
		byID[cg.ID] = cg
		byName[cg.Name] = cg
	}

	for _, id := range ids {
		if cg, ok := byID[id]; ok {
			cp := *cg
			found = append(found, &cp)

			continue
		}

		errs = append(errs, serverlessCollectionGroupError{
			ID:           id,
			ErrorCode:    slErrorCodeNotFound,
			ErrorMessage: fmt.Sprintf("The specified Collection Group %s is not found", id),
		})
	}

	for _, name := range names {
		if cg, ok := byName[name]; ok {
			cp := *cg
			found = append(found, &cp)

			continue
		}

		errs = append(errs, serverlessCollectionGroupError{
			Name:         name,
			ErrorCode:    slErrorCodeNotFound,
			ErrorMessage: fmt.Sprintf("The specified Collection Group %s is not found", name),
		})
	}

	return found, errs
}

// cloneCollectionGroupTags returns a defensive copy of a collection group's
// tags for wire serialization.
func cloneCollectionGroupTags(cg *ServerlessCollectionGroup) map[string]string {
	return maps.Clone(cg.Tags)
}
