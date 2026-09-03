package glue

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// AssetTypeFormReference references a form type included in an asset type,
// field-diffed against types.AssetTypeFormReference.
type AssetTypeFormReference struct {
	FormTypeIdentifier string `json:"FormTypeIdentifier"`
}

// AssetType defines the structure of assets by specifying which forms they
// include. Field-diffed against PutAssetTypeOutput/GetAssetTypeOutput, which
// share exactly these three fields.
type AssetType struct {
	Forms map[string]AssetTypeFormReference `json:"Forms,omitempty"`
	ID    string                            `json:"Id"`
	Name  string                            `json:"Name"`
}

// AssetTypeItem is the summary shape returned by ListAssetTypes, field-diffed
// against types.AssetTypeItem (Id/Name only -- no Forms).
type AssetTypeItem struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// AssetFormEntry is a form on an asset: the form type identifier plus its
// JSON content, field-diffed against types.AssetFormEntry.
type AssetFormEntry struct {
	Content    string `json:"Content,omitempty"`
	FormTypeID string `json:"FormTypeId,omitempty"`
}

// IterableFormEntry identifies an iterable form available on an asset (e.g.
// "columns" on a table asset), field-diffed against types.IterableFormEntry.
type IterableFormEntry struct {
	FormTypeID string `json:"FormTypeId,omitempty"`
}

// Asset represents an entry in the Glue Data Catalog asset catalog.
// Field-diffed against GetAssetOutput (the richest of the Asset-shaped
// outputs); PutAssetOutput/UpdateAssetOutput are narrower subsets built
// explicitly in handler_assets.go rather than reusing this type, so as not to
// put fields on their wire response that AWS does not document there.
type Asset struct {
	Forms         map[string]AssetFormEntry    `json:"Forms,omitempty"`
	Attachments   map[string]AssetFormEntry    `json:"Attachments,omitempty"`
	IterableForms map[string]IterableFormEntry `json:"IterableForms,omitempty"`
	ID            string                       `json:"Id"`
	Name          string                       `json:"Name,omitempty"`
	Description   string                       `json:"Description,omitempty"`
	AssetTypeID   string                       `json:"AssetTypeId"`
	GlossaryTerms []string                     `json:"GlossaryTerms,omitempty"`
	CreatedAt     float64                      `json:"CreatedAt,omitempty"`
	UpdatedAt     float64                      `json:"UpdatedAt,omitempty"`
}

// iterableFormItemsMap is the type of InMemoryBackend.iterableFormItems,
// aliased so it doesn't have to be spelled out in full at every declaration
// site (store.go, persistence.go).
type iterableFormItemsMap = map[string]map[string]map[string]*iterableFormItemRecord

// iterableFormItemRecord is the backend's internal storage for an item within
// an asset's iterable form (e.g. one column of a table asset's "columns"
// form). It is deliberately NOT a store.Table entry -- see the doc comment on
// InMemoryBackend.iterableFormItems in store.go for why -- and is persisted
// directly as a raw nested map (see persistence.go). Real Glue documents no
// operation that explicitly creates one of these; the only write path this
// operation set provides is PutAttachment targeting an item via
// ItemIdentifier+IterableFormName (see putIterableFormAttachmentLocked
// below), so an item's existence is entirely derived from attachments having
// been put on it.
type iterableFormItemRecord struct {
	Attachments   map[string]AssetFormEntry `json:"attachments,omitempty"`
	Forms         map[string]AssetFormEntry `json:"forms,omitempty"`
	ItemID        string                    `json:"itemId"`
	ItemName      string                    `json:"itemName,omitempty"`
	GlossaryTerms []string                  `json:"glossaryTerms,omitempty"`
}

// IterableFormItem is the wire shape returned by BatchGetIterableForms,
// field-diffed against types.IterableFormItem.
type IterableFormItem struct {
	Attachments   map[string]AssetFormEntry `json:"Attachments,omitempty"`
	Forms         map[string]AssetFormEntry `json:"Forms,omitempty"`
	ItemID        string                    `json:"ItemId,omitempty"`
	ItemName      string                    `json:"ItemName,omitempty"`
	GlossaryTerms []string                  `json:"GlossaryTerms,omitempty"`
}

// IterableFormListItem is the wire shape returned by ListIterableForms,
// field-diffed against types.IterableFormListItem. Description is always
// empty in this backend (no write path ever populates a per-item
// description), so it is always omitted rather than fabricated.
type IterableFormListItem struct {
	ItemID        string   `json:"ItemId,omitempty"`
	ItemName      string   `json:"ItemName,omitempty"`
	Description   string   `json:"Description,omitempty"`
	GlossaryTerms []string `json:"GlossaryTerms,omitempty"`
}

// ItemErrorDetail describes an item BatchGetIterableForms could not retrieve,
// field-diffed against types.ItemError.
type ItemErrorDetail struct {
	Code           string `json:"Code,omitempty"`
	ItemIdentifier string `json:"ItemIdentifier,omitempty"`
	Message        string `json:"Message,omitempty"`
}

// SearchResultItem is a single SearchAssets match, field-diffed against
// types.SearchResultItem.
type SearchResultItem struct {
	AssetDescription string  `json:"AssetDescription,omitempty"`
	AssetName        string  `json:"AssetName,omitempty"`
	AssetTypeID      string  `json:"AssetTypeId,omitempty"`
	ID               string  `json:"Id,omitempty"`
	UpdatedAt        float64 `json:"UpdatedAt,omitempty"`
}

// nowEpochSeconds returns the current time as Unix epoch seconds -- glue is
// awsjson1.1, which serializes timestamps as JSON numbers, never RFC3339
// strings (see the epoch-seconds bug class documented in PARITY.md).
func nowEpochSeconds() float64 { return float64(time.Now().Unix()) }

func cloneAssetFormEntries(m map[string]AssetFormEntry) map[string]AssetFormEntry {
	return maps.Clone(m)
}

func cloneAssetType(a *AssetType) *AssetType {
	cp := *a
	cp.Forms = maps.Clone(a.Forms)

	return &cp
}

func cloneAsset(a *Asset) *Asset {
	cp := *a
	cp.Forms = cloneAssetFormEntries(a.Forms)
	cp.Attachments = cloneAssetFormEntries(a.Attachments)
	cp.GlossaryTerms = append([]string(nil), a.GlossaryTerms...)
	cp.IterableForms = maps.Clone(a.IterableForms)

	return &cp
}

// PutAssetType creates or updates (upsert, keyed by Name) an asset type. Every
// referenced form type must already exist, matching DeleteFormType's
// documented "referenced by an asset type" foreign-key relationship.
func (b *InMemoryBackend) PutAssetType(name string, forms map[string]AssetTypeFormReference) (*AssetType, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if len(forms) == 0 {
		return nil, fmt.Errorf("%w: Forms is required", ErrValidation)
	}

	b.mu.Lock("PutAssetType")
	defer b.mu.Unlock()

	for formName, ref := range forms {
		if ref.FormTypeIdentifier == "" {
			return nil, fmt.Errorf("%w: Forms[%q].FormTypeIdentifier is required", ErrValidation, formName)
		}
		if !b.formTypes.Has(ref.FormTypeIdentifier) {
			return nil, fmt.Errorf("form type %q not found: %w", ref.FormTypeIdentifier, ErrNotFound)
		}
	}

	at := &AssetType{ID: name, Name: name, Forms: maps.Clone(forms)}
	b.assetTypes.Put(at)

	return cloneAssetType(at), nil
}

// GetAssetType returns an asset type by its identifier.
func (b *InMemoryBackend) GetAssetType(id string) (*AssetType, error) {
	b.mu.RLock("GetAssetType")
	defer b.mu.RUnlock()

	at, ok := b.assetTypes.Get(id)
	if !ok {
		return nil, fmt.Errorf("asset type %q not found: %w", id, ErrNotFound)
	}

	return cloneAssetType(at), nil
}

// DeleteAssetType deletes an asset type. AWS documents no ConflictException
// for this operation (confirmed in deserializers.go's error switch for
// DeleteAssetType, unlike DeleteFormType/DeleteGlossary), so deleting an
// asset type still referenced by existing assets is allowed -- matching real
// AWS behavior rather than inventing an undocumented guard.
// DeleteAssetType's error switch also has no EntityNotFoundException case,
// unlike GetAssetType's, so an unknown Identifier surfaces as
// InvalidInputException.
func (b *InMemoryBackend) DeleteAssetType(id string) error {
	b.mu.Lock("DeleteAssetType")
	defer b.mu.Unlock()

	if !b.assetTypes.Has(id) {
		return fmt.Errorf("asset type %q not found: %w", id, ErrValidation)
	}

	b.assetTypes.Delete(id)

	return nil
}

// ListAssetTypes returns every asset type, sorted by ID.
func (b *InMemoryBackend) ListAssetTypes() []*AssetType {
	b.mu.RLock("ListAssetTypes")
	defer b.mu.RUnlock()

	src := b.assetTypes.All()
	out := make([]*AssetType, 0, len(src))
	for _, at := range src {
		out = append(out, cloneAssetType(at))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// PutAsset creates or updates (upsert, keyed by the caller-supplied
// Identifier) an asset. Attachments/GlossaryTerms/IterableForms are preserved
// across an update-via-Put, matching real AWS semantics where PutAsset only
// documents Forms/Name/Description as replaced.
func (b *InMemoryBackend) PutAsset(
	id, name, description, assetTypeID string,
	forms map[string]AssetFormEntry,
) (*Asset, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Identifier is required", ErrValidation)
	}
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if assetTypeID == "" {
		return nil, fmt.Errorf("%w: AssetTypeId is required", ErrValidation)
	}

	b.mu.Lock("PutAsset")
	defer b.mu.Unlock()

	if !b.assetTypes.Has(assetTypeID) {
		return nil, fmt.Errorf("asset type %q not found: %w", assetTypeID, ErrNotFound)
	}

	now := nowEpochSeconds()
	a := &Asset{
		ID:          id,
		Name:        name,
		Description: description,
		AssetTypeID: assetTypeID,
		Forms:       maps.Clone(forms),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if existing, ok := b.assets.Get(id); ok {
		a.CreatedAt = existing.CreatedAt
		a.Attachments = existing.Attachments
		a.GlossaryTerms = existing.GlossaryTerms
		a.IterableForms = existing.IterableForms
	}

	b.assets.Put(a)

	return cloneAsset(a), nil
}

// GetAsset returns an asset by its identifier.
func (b *InMemoryBackend) GetAsset(id string) (*Asset, error) {
	b.mu.RLock("GetAsset")
	defer b.mu.RUnlock()

	a, ok := b.assets.Get(id)
	if !ok {
		return nil, fmt.Errorf("asset %q not found: %w", id, ErrNotFound)
	}

	return cloneAsset(a), nil
}

// UpdateAsset updates an asset's name and/or description. A nil field leaves
// the current value unchanged, matching UpdateAssetInput's optional members.
func (b *InMemoryBackend) UpdateAsset(id string, name, description *string) (*Asset, error) {
	b.mu.Lock("UpdateAsset")
	defer b.mu.Unlock()

	a, ok := b.assets.Get(id)
	if !ok {
		return nil, fmt.Errorf("asset %q not found: %w", id, ErrNotFound)
	}

	if name != nil {
		a.Name = *name
	}
	if description != nil {
		a.Description = *description
	}
	a.UpdatedAt = nowEpochSeconds()

	return cloneAsset(a), nil
}

// DeleteAsset deletes an asset and cascades to its iterable form items (their
// only owner), matching the ownership rule that iterable form items cannot
// outlive the asset they belong to. DeleteAsset's error switch has no
// EntityNotFoundException case, unlike GetAsset's, so an unknown Identifier
// surfaces as InvalidInputException.
func (b *InMemoryBackend) DeleteAsset(id string) error {
	b.mu.Lock("DeleteAsset")
	defer b.mu.Unlock()

	if !b.assets.Has(id) {
		return fmt.Errorf("asset %q not found: %w", id, ErrValidation)
	}

	b.assets.Delete(id)
	delete(b.iterableFormItems, id)

	return nil
}

// PutAttachment attaches a form to an asset, or -- when iterableFormName and
// itemIdentifier are both given -- to an item within one of the asset's
// iterable forms. An attachment with the same name is overwritten, matching
// PutAttachment's documented upsert behavior.
func (b *InMemoryBackend) PutAttachment(
	assetID, attachmentName, formTypeID, content, iterableFormName, itemIdentifier string,
) error {
	if assetID == "" || attachmentName == "" || formTypeID == "" {
		return fmt.Errorf("%w: AssetIdentifier, AttachmentName, and FormTypeId are required", ErrValidation)
	}
	if iterableFormName != "" && itemIdentifier == "" {
		return fmt.Errorf("%w: ItemIdentifier is required when IterableFormName is specified", ErrValidation)
	}

	b.mu.Lock("PutAttachment")
	defer b.mu.Unlock()

	a, ok := b.assets.Get(assetID)
	if !ok {
		return fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}
	if !b.formTypes.Has(formTypeID) {
		return fmt.Errorf("form type %q not found: %w", formTypeID, ErrNotFound)
	}

	entry := AssetFormEntry{Content: content, FormTypeID: formTypeID}

	if iterableFormName == "" {
		if a.Attachments == nil {
			a.Attachments = make(map[string]AssetFormEntry)
		}
		a.Attachments[attachmentName] = entry
		a.UpdatedAt = nowEpochSeconds()

		return nil
	}

	b.putIterableFormAttachmentLocked(a, iterableFormName, itemIdentifier, formTypeID, attachmentName, entry)

	return nil
}

// putIterableFormAttachmentLocked upserts attachmentName on the given item
// within an asset's iterable form, creating the iterable form entry and/or
// item on first use. Caller must hold b.mu for writing.
func (b *InMemoryBackend) putIterableFormAttachmentLocked(
	a *Asset,
	iterableFormName, itemIdentifier, formTypeID, attachmentName string,
	entry AssetFormEntry,
) {
	if a.IterableForms == nil {
		a.IterableForms = make(map[string]IterableFormEntry)
	}
	if _, exists := a.IterableForms[iterableFormName]; !exists {
		a.IterableForms[iterableFormName] = IterableFormEntry{FormTypeID: formTypeID}
	}

	if b.iterableFormItems[a.ID] == nil {
		b.iterableFormItems[a.ID] = make(map[string]map[string]*iterableFormItemRecord)
	}
	if b.iterableFormItems[a.ID][iterableFormName] == nil {
		b.iterableFormItems[a.ID][iterableFormName] = make(map[string]*iterableFormItemRecord)
	}

	items := b.iterableFormItems[a.ID][iterableFormName]

	item, ok := items[itemIdentifier]
	if !ok {
		item = &iterableFormItemRecord{ItemID: itemIdentifier, ItemName: itemIdentifier}
		items[itemIdentifier] = item
	}
	if item.Attachments == nil {
		item.Attachments = make(map[string]AssetFormEntry)
	}
	item.Attachments[attachmentName] = entry

	a.UpdatedAt = nowEpochSeconds()
}

// DeleteAttachment deletes a form attachment from an asset, or from an item
// within one of its iterable forms. Deleting an attachment that does not
// exist is a no-op, matching the lenient delete semantics used elsewhere in
// this backend; only the owning asset's existence is enforced (the
// documented EntityNotFoundException).
func (b *InMemoryBackend) DeleteAttachment(assetID, attachmentName, iterableFormName, itemIdentifier string) error {
	if assetID == "" || attachmentName == "" {
		return fmt.Errorf("%w: AssetIdentifier and AttachmentName are required", ErrValidation)
	}

	b.mu.Lock("DeleteAttachment")
	defer b.mu.Unlock()

	a, ok := b.assets.Get(assetID)
	if !ok {
		return fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}

	if iterableFormName == "" {
		delete(a.Attachments, attachmentName)
		a.UpdatedAt = nowEpochSeconds()

		return nil
	}

	if items, itemsOK := b.iterableFormItems[assetID][iterableFormName]; itemsOK {
		if item, itemOK := items[itemIdentifier]; itemOK {
			delete(item.Attachments, attachmentName)
		}
	}
	a.UpdatedAt = nowEpochSeconds()

	return nil
}

// BatchGetIterableForms retrieves multiple items from one iterable form on an
// asset. Items not found are reported per-item in the returned errors slice
// rather than failing the whole request, matching BatchGetIterableFormsOutput
// having both Items and Errors.
func (b *InMemoryBackend) BatchGetIterableForms(
	assetID, iterableFormName string,
	itemIDs []string,
) ([]*IterableFormItem, []ItemErrorDetail, error) {
	b.mu.RLock("BatchGetIterableForms")
	defer b.mu.RUnlock()

	if !b.assets.Has(assetID) {
		return nil, nil, fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}

	items := b.iterableFormItems[assetID][iterableFormName]

	found := make([]*IterableFormItem, 0, len(itemIDs))

	var errs []ItemErrorDetail

	for _, id := range itemIDs {
		rec, ok := items[id]
		if !ok {
			errs = append(errs, ItemErrorDetail{
				Code:           errEntityNotFoundCode,
				ItemIdentifier: id,
				Message:        fmt.Sprintf("item %q not found in iterable form %q", id, iterableFormName),
			})

			continue
		}

		found = append(found, &IterableFormItem{
			Attachments:   cloneAssetFormEntries(rec.Attachments),
			Forms:         cloneAssetFormEntries(rec.Forms),
			GlossaryTerms: append([]string(nil), rec.GlossaryTerms...),
			ItemID:        rec.ItemID,
			ItemName:      rec.ItemName,
		})
	}

	return found, errs, nil
}

// ListIterableForms lists every item in one iterable form on an asset,
// sorted by item ID.
func (b *InMemoryBackend) ListIterableForms(assetID, iterableFormName string) ([]*IterableFormListItem, error) {
	b.mu.RLock("ListIterableForms")
	defer b.mu.RUnlock()

	if !b.assets.Has(assetID) {
		return nil, fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}

	items := b.iterableFormItems[assetID][iterableFormName]
	out := make([]*IterableFormListItem, 0, len(items))

	for _, rec := range items {
		out = append(out, &IterableFormListItem{
			GlossaryTerms: append([]string(nil), rec.GlossaryTerms...),
			ItemID:        rec.ItemID,
			ItemName:      rec.ItemName,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })

	return out, nil
}

// SearchAssets returns every asset matching searchText (a case-insensitive
// substring of Name or Description) and filter (a possibly-nested boolean
// filter clause), sorted by sortAttr (defaulting to ID when unrecognized or
// empty).
func (b *InMemoryBackend) SearchAssets(
	searchText string,
	filter *searchFilterClause,
	sortAttr string,
	sortDesc bool,
) []*Asset {
	b.mu.RLock("SearchAssets")
	defer b.mu.RUnlock()

	lowerText := strings.ToLower(searchText)

	out := make([]*Asset, 0)

	for _, a := range b.assets.All() {
		if searchText != "" &&
			!strings.Contains(strings.ToLower(a.Name), lowerText) &&
			!strings.Contains(strings.ToLower(a.Description), lowerText) {
			continue
		}

		if !matchesFilter(a, filter) {
			continue
		}

		out = append(out, cloneAsset(a))
	}

	sortAssets(out, sortAttr, sortDesc)

	return out
}

// sortAssets sorts assets in place by attr, an Asset field name as it appears
// in SearchSort.Attribute; an unrecognized or empty attr falls back to ID for
// deterministic ordering.
func sortAssets(assets []*Asset, attr string, desc bool) {
	// None of Name/Description/AssetTypeId/CreatedAt/UpdatedAt is unique
	// across assets, so each falls through to ID -- the real primary
	// key -- as a final tiebreak, making the order total.
	less := func(i, j int) bool {
		switch attr {
		case "Name":
			if assets[i].Name != assets[j].Name {
				return assets[i].Name < assets[j].Name
			}
		case "Description":
			if assets[i].Description != assets[j].Description {
				return assets[i].Description < assets[j].Description
			}
		case "AssetTypeId":
			if assets[i].AssetTypeID != assets[j].AssetTypeID {
				return assets[i].AssetTypeID < assets[j].AssetTypeID
			}
		case "CreatedAt":
			if assets[i].CreatedAt != assets[j].CreatedAt {
				return assets[i].CreatedAt < assets[j].CreatedAt
			}
		case "UpdatedAt":
			if assets[i].UpdatedAt != assets[j].UpdatedAt {
				return assets[i].UpdatedAt < assets[j].UpdatedAt
			}
		}

		return assets[i].ID < assets[j].ID
	}

	if desc {
		sort.Slice(assets, func(i, j int) bool { return less(j, i) })

		return
	}

	sort.Slice(assets, less)
}
