package account

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
)

// ListRegions returns regions filtered by opt-in status, honouring AWS's
// maxResults/nextToken pagination. nextToken is an opaque cursor (base64 of the
// exclusive-start RegionName); when maxResults <= 0 the full filtered list is returned.
func (b *InMemoryBackend) ListRegions(
	statusFilter []RegionOptStatus,
	maxResults int,
	nextToken string,
) ([]*Region, string, error) {
	b.mu.RLock("ListRegions")
	defer b.mu.RUnlock()

	filtered := make([]*Region, 0, len(b.regions))

	for _, r := range b.regions {
		if len(statusFilter) == 0 || slices.Contains(statusFilter, r.RegionOptStatus) {
			filtered = append(filtered, r)
		}
	}

	// Order deterministically by RegionName so the name-based pagination cursor is
	// stable across pages (AWS returns regions in alphabetical order).
	slices.SortFunc(filtered, func(a, b *Region) int {
		return strings.Compare(a.RegionName, b.RegionName)
	})

	// Apply the exclusive-start cursor: skip everything up to and including the
	// region named by the decoded token.
	if nextToken != "" {
		start, decErr := decodeRegionToken(nextToken)
		if decErr != nil {
			return nil, "", errInvalidNextToken
		}

		idx := 0
		for idx < len(filtered) && filtered[idx].RegionName <= start {
			idx++
		}

		filtered = filtered[idx:]
	}

	if maxResults <= 0 || maxResults >= len(filtered) {
		return filtered, "", nil
	}

	page := filtered[:maxResults]

	return page, encodeRegionToken(page[len(page)-1].RegionName), nil
}

// EnableRegion transitions an opt-in region from DISABLED to ENABLED.
// ENABLED_BY_DEFAULT regions return a ValidationException per AWS semantics.
func (b *InMemoryBackend) EnableRegion(regionName string) error {
	b.mu.Lock("EnableRegion")
	defer b.mu.Unlock()

	for _, r := range b.regions {
		if r.RegionName != regionName {
			continue
		}

		if r.RegionOptStatus == RegionOptStatusEnabledDefault {
			return fmt.Errorf("%w: %s", errRegionNotOptIn, regionName)
		}

		r.RegionOptStatus = RegionOptStatusEnabled

		return nil
	}

	return fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// DisableRegion transitions an opt-in region from ENABLED to DISABLED.
// ENABLED_BY_DEFAULT regions return a ValidationException per AWS semantics.
func (b *InMemoryBackend) DisableRegion(regionName string) error {
	b.mu.Lock("DisableRegion")
	defer b.mu.Unlock()

	for _, r := range b.regions {
		if r.RegionName != regionName {
			continue
		}

		if r.RegionOptStatus == RegionOptStatusEnabledDefault {
			return fmt.Errorf("%w: %s", errRegionNotOptIn, regionName)
		}

		r.RegionOptStatus = RegionOptStatusDisabled

		return nil
	}

	return fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// GetRegionOptStatus returns the current opt-in status for a single region.
func (b *InMemoryBackend) GetRegionOptStatus(regionName string) (RegionOptStatus, error) {
	b.mu.RLock("GetRegionOptStatus")
	defer b.mu.RUnlock()

	for _, r := range b.regions {
		if r.RegionName == regionName {
			return r.RegionOptStatus, nil
		}
	}

	return "", fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// encodeRegionToken produces an opaque pagination cursor for the given RegionName.
func encodeRegionToken(regionName string) string {
	return base64.StdEncoding.EncodeToString([]byte(regionName))
}

// decodeRegionToken reverses encodeRegionToken.
func decodeRegionToken(token string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}

	return string(raw), nil
}
