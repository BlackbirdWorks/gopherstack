package sagemaker

import (
	"context"
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
}

func cloneHumanTaskUI(h *HumanTaskUI) *HumanTaskUI {
	cp := *h
	cp.Tags = maps.Clone(h.Tags)

	return &cp
}

// CreateHumanTaskUI creates a human task UI.
func (b *InMemoryBackend) CreateHumanTaskUI(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*HumanTaskUI, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateHumanTaskUI")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", ErrValidation)
	}

	store := b.humanTaskUisStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: human task UI %q already exists", ErrValidation, name)
	}

	uiARN := arn.Build("sagemaker", region, b.accountID, "human-task-ui/"+name)

	ui := &HumanTaskUI{
		HumanTaskUIName:   name,
		HumanTaskUIArn:    uiARN,
		HumanTaskUIStatus: statusActive,
		Tags:              mergeTags(nil, tags),
		CreationTime:      time.Now(),
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

// ListHumanTaskUIs returns all human task UIs.
func (b *InMemoryBackend) ListHumanTaskUIs(ctx context.Context, nextToken string) ([]*HumanTaskUI, string) {
	b.mu.RLock("ListHumanTaskUIs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.humanTaskUisStoreRO(region),
		nextToken,
		cloneHumanTaskUI,
		func(v *HumanTaskUI) string { return v.HumanTaskUIName },
	)
}
