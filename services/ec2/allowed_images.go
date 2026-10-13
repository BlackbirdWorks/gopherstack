package ec2

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	maxImageCriteria         = 10
	maxImageProviders        = 200
	maxImageNames            = 50
	maxMarketplaceCodes      = 50
	maxImageWatermarkFilters = 50
	imageProviderNone        = "none"
	hoursPerDay              = 24
)

// ImageWatermarkFilter is one ImageCriterion.ImageWatermarks entry; every set field must match.
type ImageWatermarkFilter struct {
	MaximumDaysSinceSourceImageCreated *int32 `json:"maximumDaysSinceSourceImageCreated,omitempty"`
	MaximumDaysSinceWatermarkCreated   *int32 `json:"maximumDaysSinceWatermarkCreated,omitempty"`
	SourceImageRegion                  string `json:"sourceImageRegion,omitempty"`
	WatermarkKey                       string `json:"watermarkKey,omitempty"`
}

// ImageWatermarkRecord is a watermark attached to an AMI (types.ImageWatermark).
type ImageWatermarkRecord struct {
	SourceImageCreationTime time.Time
	WatermarkCreationTime   time.Time
	SourceImageID           string
	SourceImageRegion       string
	WatermarkKey            string
}

func validateImageCriteria(criteria []ImageCriterion) error {
	if len(criteria) > maxImageCriteria {
		return fmt.Errorf("%w: at most %d ImageCriterion are allowed", ErrInvalidParameter, maxImageCriteria)
	}

	for _, c := range criteria {
		if err := validateImageCriterion(c); err != nil {
			return err
		}
	}

	return nil
}

func validateImageCriterion(c ImageCriterion) error {
	limits := []struct {
		name string
		n    int
		max  int
	}{
		{"ImageProviders", len(c.ImageProviders), maxImageProviders},
		{"ImageNames", len(c.ImageNames), maxImageNames},
		{"MarketplaceProductCodes", len(c.MarketplaceProductCodes), maxMarketplaceCodes},
		{"ImageWatermarks", len(c.ImageWatermarks), maxImageWatermarkFilters},
	}
	for _, l := range limits {
		if l.n > l.max {
			return fmt.Errorf("%w: at most %d values are allowed for %s", ErrInvalidParameter, l.max, l.name)
		}
	}

	if slices.Contains(c.ImageProviders, imageProviderNone) && len(c.ImageProviders) > 1 {
		return fmt.Errorf(
			"%w: no other ImageProviders can be specified with %q", ErrInvalidParameter, imageProviderNone,
		)
	}

	if c.CreationDateCondition != nil && c.CreationDateCondition.MaximumDaysSinceCreated < 0 {
		return fmt.Errorf("%w: MaximumDaysSinceCreated must not be negative", ErrInvalidParameter)
	}

	if c.DeprecationTimeCondition != nil && c.DeprecationTimeCondition.MaximumDaysSinceDeprecated < 0 {
		return fmt.Errorf("%w: MaximumDaysSinceDeprecated must not be negative", ErrInvalidParameter)
	}

	for _, w := range c.ImageWatermarks {
		if (w.MaximumDaysSinceSourceImageCreated != nil && *w.MaximumDaysSinceSourceImageCreated < 0) ||
			(w.MaximumDaysSinceWatermarkCreated != nil && *w.MaximumDaysSinceWatermarkCreated < 0) {
			return fmt.Errorf("%w: watermark day limits must not be negative", ErrInvalidParameter)
		}
	}

	return nil
}

func daysSince(then, now time.Time) int64 {
	return int64(now.Sub(then) / (hoursPerDay * time.Hour))
}

func parseDeprecationTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func providerMatches(img *AMIStub, providers []string) bool {
	for _, p := range providers {
		if p != imageProviderNone && img.OwnerID == p {
			return true
		}
	}

	return false
}

func nameMatches(name string, patterns []string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, name) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) deprecationConditionMet(img *AMIStub, c *DeprecationTimeCondition, now time.Time) bool {
	at, ok := parseDeprecationTime(b.imageDeprecated[img.ImageID])
	if !ok || at.After(now) {
		return true
	}

	return c.MaximumDaysSinceDeprecated > 0 && daysSince(at, now) <= int64(c.MaximumDaysSinceDeprecated)
}

func watermarkFilterMatches(f ImageWatermarkFilter, w ImageWatermarkRecord, now time.Time) bool {
	if f.WatermarkKey != "" && !wildcardMatch(f.WatermarkKey, w.WatermarkKey) {
		return false
	}

	if f.SourceImageRegion != "" && !wildcardMatch(f.SourceImageRegion, w.SourceImageRegion) {
		return false
	}

	if f.MaximumDaysSinceSourceImageCreated != nil && !w.SourceImageCreationTime.IsZero() &&
		daysSince(w.SourceImageCreationTime, now) > int64(*f.MaximumDaysSinceSourceImageCreated) {
		return false
	}

	if f.MaximumDaysSinceWatermarkCreated != nil && !w.WatermarkCreationTime.IsZero() &&
		daysSince(w.WatermarkCreationTime, now) > int64(*f.MaximumDaysSinceWatermarkCreated) {
		return false
	}

	return true
}

func (b *InMemoryBackend) watermarksMatch(img *AMIStub, filters []ImageWatermarkFilter, now time.Time) bool {
	for _, w := range b.imageWatermarkRecordsLocked(img) {
		for _, f := range filters {
			if watermarkFilterMatches(f, w, now) {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) criterionMatchesLocked(c ImageCriterion, img *AMIStub, now time.Time) bool {
	if len(c.ImageProviders) > 0 && !providerMatches(img, c.ImageProviders) {
		return false
	}

	if len(c.ImageNames) > 0 && !nameMatches(img.Name, c.ImageNames) {
		return false
	}

	if len(c.MarketplaceProductCodes) > 0 &&
		!slices.ContainsFunc(img.ProductCodes, func(code string) bool {
			return slices.Contains(c.MarketplaceProductCodes, code)
		}) {
		return false
	}

	if c.CreationDateCondition != nil && !img.CreationTime.IsZero() &&
		daysSince(img.CreationTime, now) > int64(c.CreationDateCondition.MaximumDaysSinceCreated) {
		return false
	}

	if c.DeprecationTimeCondition != nil && !b.deprecationConditionMet(img, c.DeprecationTimeCondition, now) {
		return false
	}

	return len(c.ImageWatermarks) == 0 || b.watermarksMatch(img, c.ImageWatermarks, now)
}

// imageAllowedLocked evaluates the Allowed AMIs criteria; own-account AMIs are always allowed.
func (b *InMemoryBackend) imageAllowedLocked(img *AMIStub, now time.Time) bool {
	if img.OwnerID == b.AccountID {
		return true
	}

	return slices.ContainsFunc(b.allowedImagesSettings.ImageCriteria, func(c ImageCriterion) bool {
		return b.criterionMatchesLocked(c, img, now)
	})
}

func (b *InMemoryBackend) imageWatermarkRecordsLocked(img *AMIStub) []ImageWatermarkRecord {
	keys := b.imageWatermarks[img.ImageID]
	out := make([]ImageWatermarkRecord, 0, len(keys))

	for _, k := range keys {
		out = append(out, ImageWatermarkRecord{
			WatermarkKey:            k,
			WatermarkCreationTime:   b.imageWatermarkTimes[img.ImageID][k],
			SourceImageID:           img.ImageID,
			SourceImageRegion:       b.Region,
			SourceImageCreationTime: img.CreationTime,
		})
	}

	return out
}

// ImageWatermarksFor returns the watermarks attached to an AMI.
func (b *InMemoryBackend) ImageWatermarksFor(imageID string) []ImageWatermarkRecord {
	b.mu.RLock("ImageWatermarksFor")
	defer b.mu.RUnlock()

	img := b.lookupImageLocked(imageID)
	if img == nil {
		return nil
	}

	return b.imageWatermarkRecordsLocked(img)
}

// EvaluateAllowedImages returns the Allowed AMIs state and a per-image verdict (empty when disabled).
func (b *InMemoryBackend) EvaluateAllowedImages(imageIDs []string) (string, map[string]bool) {
	b.mu.RLock("EvaluateAllowedImages")
	defer b.mu.RUnlock()

	state := b.allowedImagesSettings.State
	verdicts := make(map[string]bool, len(imageIDs))

	if state != allowedImagesStateEnabled && state != allowedImagesStateAudit {
		return state, verdicts
	}

	now := time.Now().UTC()

	for _, id := range imageIDs {
		if img := b.lookupImageLocked(id); img != nil {
			verdicts[id] = b.imageAllowedLocked(img, now)
		}
	}

	return state, verdicts
}

// checkImageLaunchAllowedLocked rejects launching a known AMI the enabled Allowed AMIs criteria exclude.
func (b *InMemoryBackend) checkImageLaunchAllowedLocked(imageID string) error {
	if b.allowedImagesSettings.State != allowedImagesStateEnabled {
		return nil
	}

	img := b.lookupImageLocked(imageID)
	if img == nil || b.imageAllowedLocked(img, time.Now().UTC()) {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
}

func imageAllowedWire(state string, verdicts map[string]bool, imageID string) *bool {
	if state != allowedImagesStateEnabled && state != allowedImagesStateAudit {
		return nil
	}

	v, ok := verdicts[imageID]
	if !ok {
		return nil
	}

	return &v
}

func matchesImageAllowedFilter(state string, allowed *bool, values []string) bool {
	if allowed == nil {
		if state != allowedImagesStateDisabled {
			return false
		}

		t := true
		allowed = &t
	}

	want := strconv.FormatBool(*allowed)

	return slices.ContainsFunc(values, func(v string) bool { return strings.EqualFold(v, want) })
}

func idSet(images []*AMIStub) map[string]struct{} {
	out := make(map[string]struct{}, len(images))
	for _, a := range images {
		out[a.ImageID] = struct{}{}
	}

	return out
}

// applyAllowedImages hides disallowed AMIs when Allowed AMIs is enabled and applies the
// image-allowed filter (removed from filters); it returns the state and per-image verdicts.
func (h *Handler) applyAllowedImages(
	images []*AMIStub, requested map[string]struct{}, filters map[string][]string,
) ([]*AMIStub, string, map[string]bool, error) {
	ids := make([]string, 0, len(images))
	for _, a := range images {
		ids = append(ids, a.ImageID)
	}

	state, verdicts := h.Backend.EvaluateAllowedImages(ids)
	if state == allowedImagesStateEnabled {
		images = slices.DeleteFunc(images, func(a *AMIStub) bool {
			allowed, known := verdicts[a.ImageID]

			return known && !allowed
		})

		if err := firstMissingID(requested, idSet(images), ErrImageNotFound); err != nil {
			return nil, state, nil, err
		}
	}

	if vs, ok := filters["image-allowed"]; ok {
		delete(filters, "image-allowed")

		images = slices.DeleteFunc(images, func(a *AMIStub) bool {
			return !matchesImageAllowedFilter(state, imageAllowedWire(state, verdicts, a.ImageID), vs)
		})
	}

	return images, state, verdicts, nil
}
