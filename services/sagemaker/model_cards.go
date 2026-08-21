package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrModelCardNotFound is returned when a model card does not exist.
var ErrModelCardNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

// modelCardStatusValues mirrors types.ModelCardStatus's real enum values
// (types/enums.go:5906-5913): Draft/PendingReview/Approved/Archived.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var modelCardStatusValues = []string{"Draft", "PendingReview", "Approved", "Archived"}

// ModelCardSecurityConfig mirrors types.ModelCardSecurityConfig
// (types/types.go), which has exactly one member.
type ModelCardSecurityConfig struct {
	KmsKeyID string `json:"KmsKeyId,omitempty"`
}

// ModelCard represents a SageMaker model card.
type ModelCard struct {
	CreationTime     time.Time                `json:"CreationTime"`
	LastModifiedTime time.Time                `json:"LastModifiedTime"`
	SecurityConfig   *ModelCardSecurityConfig `json:"SecurityConfig,omitempty"`
	Tags             map[string]string        `json:"Tags,omitempty"`
	ModelCardName    string                   `json:"ModelCardName"`
	ModelCardArn     string                   `json:"ModelCardArn"`
	ModelCardStatus  string                   `json:"ModelCardStatus"`
	Content          string                   `json:"Content,omitempty"`
	ModelCardVersion int                      `json:"ModelCardVersion"`
}

func cloneModelCard(c *ModelCard) *ModelCard {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	if c.SecurityConfig != nil {
		sc := *c.SecurityConfig
		cp.SecurityConfig = &sc
	}

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeModelCard.
func (c *ModelCard) MarshalJSON() ([]byte, error) {
	type alias ModelCard

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(c),
		CreationTime:     epochSeconds(c.CreationTime),
		LastModifiedTime: epochSeconds(c.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [ModelCard.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (c *ModelCard) UnmarshalJSON(data []byte) error {
	type alias ModelCard

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(c)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	c.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	c.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateModelCardOptions holds input fields for CreateModelCard, mirroring
// CreateModelCardInput (api_op_CreateModelCard.go:32-66).
type CreateModelCardOptions struct {
	SecurityConfig *ModelCardSecurityConfig
	Tags           map[string]string
	Name           string
	Content        string
	Status         string
}

// CreateModelCard creates a model card. ModelCardStatus is required by the
// real API (api_op_CreateModelCard.go:47-59) — a previous version of this
// backend hardcoded it to "Draft" and never read the caller's value at all.
func (b *InMemoryBackend) CreateModelCard(ctx context.Context, opts CreateModelCardOptions) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateModelCard")
	defer b.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", ErrValidation)
	}

	if opts.Status == "" {
		return nil, fmt.Errorf("%w: ModelCardStatus is required", ErrValidation)
	}

	if !slices.Contains(modelCardStatusValues, opts.Status) {
		return nil, fmt.Errorf("%w: unknown ModelCardStatus %q", ErrValidation, opts.Status)
	}

	store := b.modelCardsStore(region)

	if _, ok := store.Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: model card %q already exists", ErrValidation, opts.Name)
	}

	cardARN := arn.Build("sagemaker", region, b.accountID, "model-card/"+opts.Name)
	now := time.Now()

	c := &ModelCard{
		ModelCardName:    opts.Name,
		ModelCardArn:     cardARN,
		ModelCardStatus:  opts.Status,
		ModelCardVersion: 1,
		Content:          opts.Content,
		SecurityConfig:   opts.SecurityConfig,
		Tags:             mergeTags(nil, opts.Tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	store.Put(c)

	return cloneModelCard(c), nil
}

// DescribeModelCard returns a model card by name. If version is non-zero it
// must match the card's current (only tracked) version — this backend keeps
// no historical per-version snapshot, matching the precedent already
// established for ClusterSchedulerConfig/ComputeQuota's own version
// parameter (parity-15). If includedData is "MetadataOnly", Content is
// sanitized down to the fixed set of JSON paths the real op documents
// (api_op_DescribeModelCard.go:66-90); any other value (including empty,
// which defaults to "AllData") returns Content verbatim.
func (b *InMemoryBackend) DescribeModelCard(
	ctx context.Context,
	name string,
	version int32,
	includedData string,
) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeModelCard")
	defer b.mu.RUnlock()

	c, ok := b.modelCardsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	if version != 0 && int(version) != c.ModelCardVersion {
		return nil, fmt.Errorf(
			"%w: model card %q version %d not found (current version is %d)",
			ErrModelCardNotFound, name, version, c.ModelCardVersion,
		)
	}

	out := cloneModelCard(c)
	if includedData == "MetadataOnly" {
		out.Content = sanitizeModelCardContent(out.Content)
	}

	return out, nil
}

// modelCardMetadataOnlyPaths are the dotted JSON paths DescribeModelCard's
// doc comment lists as surviving MetadataOnly sanitization
// (api_op_DescribeModelCard.go:70-79).
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var modelCardMetadataOnlyPaths = [][2]string{
	{"model_overview", "model_id"},
	{"model_overview", "model_name"},
	{"intended_uses", "risk_rating"},
	{"model_package_details", "model_package_group_name"},
	{"model_package_details", "model_package_arn"},
}

// sanitizeModelCardContent reduces a model card Content JSON document down
// to modelCardMetadataOnlyPaths. Content the backend cannot parse as a JSON
// object sanitizes to "{}", since a non-object document trivially has none
// of the preserved paths.
func sanitizeModelCardContent(content string) string {
	var doc map[string]json.RawMessage

	out := map[string]map[string]json.RawMessage{}

	if unmarshalErr := json.Unmarshal([]byte(content), &doc); unmarshalErr == nil {
		for _, path := range modelCardMetadataOnlyPaths {
			section, value := path[0], path[1]

			var fields map[string]json.RawMessage
			if json.Unmarshal(doc[section], &fields) != nil {
				continue
			}

			if v, ok := fields[value]; ok {
				if out[section] == nil {
					out[section] = map[string]json.RawMessage{}
				}

				out[section][value] = v
			}
		}
	}

	sanitized, err := json.Marshal(out)
	if err != nil {
		return "{}"
	}

	return string(sanitized)
}

// UpdateModelCard updates a model card's content (bumping its version) or
// its approval status, per UpdateModelCardInput
// (api_op_UpdateModelCard.go:31-59) — its doc states "You cannot update both
// model card content and model card status in a single call", enforced
// here. A status-only update does not bump the version: real model card
// versioning tracks content revisions, not approval-workflow transitions.
func (b *InMemoryBackend) UpdateModelCard(ctx context.Context, name, content, status string) (*ModelCard, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateModelCard")
	defer b.mu.Unlock()

	if content != "" && status != "" {
		return nil, fmt.Errorf(
			"%w: cannot update both model card content and model card status in a single call",
			ErrValidation,
		)
	}

	if status != "" && !slices.Contains(modelCardStatusValues, status) {
		return nil, fmt.Errorf("%w: unknown ModelCardStatus %q", ErrValidation, status)
	}

	c, ok := b.modelCardsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	switch {
	case content != "":
		c.Content = content
		c.ModelCardVersion++
	case status != "":
		c.ModelCardStatus = status
	}

	c.LastModifiedTime = time.Now()

	return cloneModelCard(c), nil
}

// DeleteModelCard removes a model card by name.
func (b *InMemoryBackend) DeleteModelCard(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteModelCard")
	defer b.mu.Unlock()

	store := b.modelCardsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListModelCardsParams bundles the filter/sort criteria for ListModelCards,
// mirroring ListModelCardsInput (api_op_ListModelCards.go:30-59).
type ListModelCardsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	ModelCardStatus    string
	NameContains       string
	SortBy             string
	SortOrder          string
	NextToken          string
	MaxResults         int32
}

// ListModelCards returns model cards, optionally filtered and sorted per
// params. SortBy has no documented default; kept as the disclosed
// undocumented-default CreationTime fallback, this campaign's recurring
// precedent. SortOrder likewise has no documented default; kept as
// Ascending.
func (b *InMemoryBackend) ListModelCards(ctx context.Context, params ListModelCardsParams) ([]*ModelCard, string) {
	b.mu.RLock("ListModelCards")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*ModelCard, 0)

	for _, c := range b.modelCardsStoreRO(region).All() {
		if !matchesModelCardListParams(c, params) {
			continue
		}

		list = append(list, cloneModelCard(c))
	}

	sortModelCardsByParams(list, params)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func matchesModelCardListParams(c *ModelCard, params ListModelCardsParams) bool {
	if params.ModelCardStatus != "" && c.ModelCardStatus != params.ModelCardStatus {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(c.ModelCardName), strings.ToLower(params.NameContains)) {
		return false
	}

	if params.CreationTimeAfter != nil && !c.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !c.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	return true
}

func sortModelCardsByParams(list []*ModelCard, params ListModelCardsParams) {
	desc := params.SortOrder == sortOrderDescending

	sort.Slice(list, func(i, j int) bool {
		var less bool

		if params.SortBy == keyGenericName {
			less = list[i].ModelCardName < list[j].ModelCardName
		} else {
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}
