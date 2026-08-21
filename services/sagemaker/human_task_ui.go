package sagemaker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrHumanTaskUINotFound is returned when a human task UI does not exist.
var ErrHumanTaskUINotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// HumanTaskUI
// ---------------------------------------------------------------------------

// HumanTaskUI represents a SageMaker human task UI.
type HumanTaskUI struct {
	CreationTime      time.Time         `json:"CreationTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	HumanTaskUIName   string            `json:"HumanTaskUiName"`
	HumanTaskUIArn    string            `json:"HumanTaskUiArn"`
	HumanTaskUIStatus string            `json:"HumanTaskUiStatus"`
	// UITemplateContentSha256 backs the required
	// DescribeHumanTaskUiOutput.UiTemplate.ContentSha256
	// (api_op_DescribeHumanTaskUi.go:62, types.UiTemplateInfo) -- emitted
	// nested under "UiTemplate" by [HumanTaskUI.MarshalJSON], not at the
	// top level, so it carries json:"-" here.
	UITemplateContentSha256 string `json:"-"`
}

// uiTemplateInfo mirrors the wire shape of types.UiTemplateInfo
// (types/types.go:23979-23988) -- Url is never populated, disclosed: this
// backend hosts no template-rendering URL.
type uiTemplateInfo struct {
	ContentSha256 string `json:"ContentSha256"`
}

func cloneHumanTaskUI(h *HumanTaskUI) *HumanTaskUI {
	cp := *h
	cp.Tags = maps.Clone(h.Tags)

	return &cp
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number
// rather than Go's default RFC3339 string — this struct is marshaled
// directly by handleDescribeHumanTaskUI.
func (h *HumanTaskUI) MarshalJSON() ([]byte, error) {
	type alias HumanTaskUI

	return json.Marshal(struct {
		*alias
		UITemplate   uiTemplateInfo `json:"UiTemplate"`
		CreationTime float64        `json:"CreationTime"`
	}{
		alias:        (*alias)(h),
		CreationTime: epochSeconds(h.CreationTime),
		UITemplate:   uiTemplateInfo{ContentSha256: h.UITemplateContentSha256},
	})
}

// UnmarshalJSON is the inverse of [HumanTaskUI.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (h *HumanTaskUI) UnmarshalJSON(data []byte) error {
	type alias HumanTaskUI

	aux := struct {
		*alias
		UITemplate   uiTemplateInfo `json:"UiTemplate"`
		CreationTime float64        `json:"CreationTime"`
	}{alias: (*alias)(h)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	h.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	h.UITemplateContentSha256 = aux.UITemplate.ContentSha256

	return nil
}

// CreateHumanTaskUI creates a human task UI. contentSha256 backs the
// required DescribeHumanTaskUiOutput.UiTemplate.ContentSha256 -- the raw
// template content itself is never returned by the real DescribeHumanTaskUi
// response (only its digest), so only the digest is retained.
func (b *InMemoryBackend) CreateHumanTaskUI(
	ctx context.Context,
	name, content string,
	tags map[string]string,
) (*HumanTaskUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateHumanTaskUI")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", ErrValidation)
	}

	if content == "" {
		return nil, fmt.Errorf("%w: UiTemplate.Content is required", ErrValidation)
	}

	store := b.humanTaskUisStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: human task UI %q already exists", ErrValidation, name)
	}

	uiARN := arn.Build("sagemaker", region, b.accountID, "human-task-ui/"+name)
	sum := sha256.Sum256([]byte(content))

	ui := &HumanTaskUI{
		HumanTaskUIName:         name,
		HumanTaskUIArn:          uiARN,
		HumanTaskUIStatus:       statusActive,
		Tags:                    mergeTags(nil, tags),
		CreationTime:            time.Now(),
		UITemplateContentSha256: hex.EncodeToString(sum[:]),
	}
	store.Put(ui)

	return cloneHumanTaskUI(ui), nil
}

// DescribeHumanTaskUI returns a human task UI by name.
func (b *InMemoryBackend) DescribeHumanTaskUI(ctx context.Context, name string) (*HumanTaskUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeHumanTaskUI")
	defer b.mu.RUnlock()

	ui, ok := b.humanTaskUisStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	return cloneHumanTaskUI(ui), nil
}

// HumanTaskUIExistsByARN reports whether a human task UI with the given ARN exists.
func (b *InMemoryBackend) HumanTaskUIExistsByARN(ctx context.Context, humanTaskUIArn string) bool {
	region := getRegion(ctx, b.region)

	b.mu.RLock("HumanTaskUIExistsByARN")
	defer b.mu.RUnlock()

	for _, ui := range b.humanTaskUisStoreRO(region).All() {
		if ui.HumanTaskUIArn == humanTaskUIArn {
			return true
		}
	}

	return false
}

// DeleteHumanTaskUI removes a human task UI by name.
func (b *InMemoryBackend) DeleteHumanTaskUI(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteHumanTaskUI")
	defer b.mu.Unlock()

	store := b.humanTaskUisStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListHumanTaskUIsFilter bundles the filter/sort criteria for
// ListHumanTaskUis (api_op_ListHumanTaskUis.go:30-53). Previously this
// decoded only NextToken and dropped CreationTimeAfter/Before, MaxResults
// and SortOrder entirely.
type ListHumanTaskUIsFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	SortOrder          string
	MaxResults         int32
}

// ListHumanTaskUIs returns human task UIs matching f, paginated. The op
// declares no SortBy field at all -- only SortOrder -- and its own doc
// states no default order either way. Rather than invent a sort dimension
// (e.g. CreationTime) no doc supports, SortOrder is applied to this
// backend's own pre-existing default order (ascending by name, the
// accidental byproduct of its former name-keyed pagination) so an
// unspecified SortOrder reproduces prior behavior exactly and an explicit
// "Descending" reverses it -- the same conservative stance ListFeatureGroups
// took on its own undocumented SortBy/SortOrder defaults.
func (b *InMemoryBackend) ListHumanTaskUIs(
	ctx context.Context,
	nextToken string,
	f ListHumanTaskUIsFilter,
) ([]*HumanTaskUI, string) {
	b.mu.RLock("ListHumanTaskUIs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return filterSortPaginateByNameWindow(
		b.humanTaskUisStoreRO(region).All(),
		nextToken,
		nameWindowSortParams(f),
		func(v *HumanTaskUI) time.Time { return v.CreationTime },
		func(v *HumanTaskUI) string { return v.HumanTaskUIName },
		cloneHumanTaskUI,
	)
}
