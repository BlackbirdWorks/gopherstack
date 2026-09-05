package quicksight

import (
	"maps"
	"sort"
	"strconv"
	"time"
)

const (
	brandStatusCreateSucceeded  = "CREATE_SUCCEEDED"
	brandVersionStatusSucceeded = "CREATE_SUCCEEDED"
)

// storedBrandVersion is the persisted representation of one version of a brand's
// definition, keyed by VersionId.
type storedBrandVersion struct {
	CreatedTime time.Time      `json:"createdTime"`
	Definition  map[string]any `json:"definition,omitempty"`
	VersionID   string         `json:"versionId"`
	Status      string         `json:"status"`
}

// storedBrand is the persisted representation of a QuickSight brand.
type storedBrand struct {
	CreatedTime        time.Time                      `json:"createdTime"`
	LastUpdatedTime    time.Time                      `json:"lastUpdatedTime"`
	Versions           map[string]*storedBrandVersion `json:"versions"`
	BrandID            string                         `json:"brandId"`
	Arn                string                         `json:"arn"`
	Status             string                         `json:"status"`
	CurrentVersionID   string                         `json:"currentVersionId"`
	PublishedVersionID string                         `json:"publishedVersionId,omitempty"`
}

func (b *storedBrand) toBrand() *Brand {
	var def map[string]any
	if v, ok := b.Versions[b.CurrentVersionID]; ok {
		def = v.Definition
	}

	return &Brand{
		CreatedTime:        b.CreatedTime,
		LastUpdatedTime:    b.LastUpdatedTime,
		Definition:         def,
		BrandID:            b.BrandID,
		Arn:                b.Arn,
		Status:             b.Status,
		CurrentVersionID:   b.CurrentVersionID,
		PublishedVersionID: b.PublishedVersionID,
	}
}

func (b *storedBrand) toBrandVersion(versionID string) (*Brand, error) {
	v, ok := b.Versions[versionID]
	if !ok {
		return nil, ErrBrandVersionNotFound
	}

	return &Brand{
		CreatedTime:        b.CreatedTime,
		LastUpdatedTime:    b.LastUpdatedTime,
		Definition:         v.Definition,
		BrandID:            b.BrandID,
		Arn:                b.Arn,
		Status:             b.Status,
		CurrentVersionID:   v.VersionID,
		PublishedVersionID: b.PublishedVersionID,
	}, nil
}

// ---- Brands ----

func (b *InMemoryBackend) CreateBrand(
	accountID, brandID string,
	definition map[string]any,
	tags map[string]string,
) (*Brand, error) {
	if brandID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateBrand")
	defer b.mu.Unlock()

	key := brandKey(accountID, brandID)
	if b.brands.Has(key) {
		return nil, ErrBrandAlreadyExists
	}

	now := time.Now().UTC()
	brand := &storedBrand{
		CreatedTime:      now,
		LastUpdatedTime:  now,
		BrandID:          brandID,
		Arn:              b.buildARN("brand", brandID),
		Status:           brandStatusCreateSucceeded,
		CurrentVersionID: "1",
		Versions: map[string]*storedBrandVersion{
			"1": {
				VersionID:   "1",
				Definition:  definition,
				Status:      brandVersionStatusSucceeded,
				CreatedTime: now,
			},
		},
	}
	b.brands.Put(brand)

	if len(tags) > 0 {
		b.tags[brand.Arn] = maps.Clone(tags)
	}

	return brand.toBrand(), nil
}

func (b *InMemoryBackend) DescribeBrand(accountID, brandID, versionID string) (*Brand, error) {
	b.mu.RLock("DescribeBrand")
	defer b.mu.RUnlock()

	brand, ok := b.brands.Get(brandKey(accountID, brandID))
	if !ok {
		return nil, ErrBrandNotFound
	}

	if versionID == "" {
		return brand.toBrand(), nil
	}

	return brand.toBrandVersion(versionID)
}

func (b *InMemoryBackend) UpdateBrand(accountID, brandID string, definition map[string]any) (*Brand, error) {
	b.mu.Lock("UpdateBrand")
	defer b.mu.Unlock()

	brand, ok := b.brands.Get(brandKey(accountID, brandID))
	if !ok {
		return nil, ErrBrandNotFound
	}

	nextVersion := strconv.Itoa(len(brand.Versions) + 1)
	now := time.Now().UTC()
	brand.Versions[nextVersion] = &storedBrandVersion{
		VersionID:   nextVersion,
		Definition:  definition,
		Status:      brandVersionStatusSucceeded,
		CreatedTime: now,
	}
	brand.CurrentVersionID = nextVersion
	brand.LastUpdatedTime = now

	return brand.toBrand(), nil
}

func (b *InMemoryBackend) DeleteBrand(accountID, brandID string) error {
	b.mu.Lock("DeleteBrand")
	defer b.mu.Unlock()

	key := brandKey(accountID, brandID)
	brand, ok := b.brands.Get(key)
	if !ok {
		return ErrBrandNotFound
	}

	if assigned, exists := b.brandAssignments[accountID]; exists && assigned == brand.Arn {
		return ErrBrandInUse
	}

	b.brands.Delete(key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListBrands(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*Brand, string, error) {
	b.mu.RLock("ListBrands")
	defer b.mu.RUnlock()

	all := b.brands.All()
	sort.Slice(all, func(i, j int) bool { return all[i].BrandID < all[j].BrandID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}
	// A token issued before items were deleted can name an offset past the
	// current end -- clamp instead of letting all[start:end] panic.
	if start > len(all) {
		start = len(all)
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = encodePageToken(end)
	} else {
		end = len(all)
	}

	result := make([]*Brand, 0, end-start)
	for _, brand := range all[start:end] {
		result = append(result, brand.toBrand())
	}

	return result, next, nil
}

func (b *InMemoryBackend) DescribeBrandPublishedVersion(accountID, brandID string) (*Brand, error) {
	b.mu.RLock("DescribeBrandPublishedVersion")
	defer b.mu.RUnlock()

	brand, ok := b.brands.Get(brandKey(accountID, brandID))
	if !ok {
		return nil, ErrBrandNotFound
	}

	if brand.PublishedVersionID == "" {
		return nil, ErrBrandVersionNotFound
	}

	return brand.toBrandVersion(brand.PublishedVersionID)
}

func (b *InMemoryBackend) UpdateBrandPublishedVersion(accountID, brandID, versionID string) error {
	b.mu.Lock("UpdateBrandPublishedVersion")
	defer b.mu.Unlock()

	brand, ok := b.brands.Get(brandKey(accountID, brandID))
	if !ok {
		return ErrBrandNotFound
	}

	if _, exists := brand.Versions[versionID]; !exists {
		return ErrBrandVersionNotFound
	}

	brand.PublishedVersionID = versionID
	brand.LastUpdatedTime = time.Now().UTC()

	return nil
}

// ---- Brand assignment (singular, account-scoped) ----

func (b *InMemoryBackend) DescribeBrandAssignment(accountID string) (string, error) {
	b.mu.RLock("DescribeBrandAssignment")
	defer b.mu.RUnlock()

	return b.brandAssignments[accountID], nil
}

func (b *InMemoryBackend) UpdateBrandAssignment(accountID, brandArn string) (string, error) {
	if brandArn == "" {
		return "", ErrValidation
	}

	b.mu.Lock("UpdateBrandAssignment")
	defer b.mu.Unlock()

	found := false
	for _, brand := range b.brands.All() {
		if brand.Arn == brandArn {
			found = true

			break
		}
	}
	if !found {
		return "", ErrBrandNotFound
	}

	b.brandAssignments[accountID] = brandArn

	return brandArn, nil
}

func (b *InMemoryBackend) DeleteBrandAssignment(accountID string) error {
	b.mu.Lock("DeleteBrandAssignment")
	defer b.mu.Unlock()

	delete(b.brandAssignments, accountID)

	return nil
}
