package ssm

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	customInventoryPrefix   = "Custom:"
	schemaDeleteDisable     = "DisableSchema"
	schemaDeleteDelete      = "DeleteSchema"
	attrDataTypeString      = "string"
	attrDataTypeNumber      = "number"
	defaultCustomSchemaVers = inventorySchemaV10
)

var (
	ErrInvalidTypeName                   = errors.New("InvalidTypeNameException")
	ErrUnsupportedInventorySchemaVersion = errors.New("UnsupportedInventorySchemaVersionException")
)

// InventorySchemaRecord is a custom inventory schema registered by PutInventory.
type InventorySchemaRecord struct {
	Attributes      map[string]string `json:"Attributes,omitempty"`
	TypeName        string            `json:"TypeName"`
	Version         string            `json:"Version"`
	DisabledThrough string            `json:"DisabledThrough,omitempty"`
}

// InventoryItemAttribute is one attribute of an inventory type schema.
type InventoryItemAttribute struct {
	Name     string `json:"Name"`
	DataType string `json:"DataType"`
}

func (b *InMemoryBackend) inventorySchemasFor(region string) map[string]*InventorySchemaRecord {
	if b.inventorySchemas[region] == nil {
		b.inventorySchemas[region] = make(map[string]*InventorySchemaRecord)
	}

	return b.inventorySchemas[region]
}

// compareSchemaVersions compares dotted numeric versions; non-numeric parts compare as zero.
func compareSchemaVersions(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")

	for i := range max(len(pa), len(pb)) {
		var x, y int

		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}

		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}

		if x != y {
			if x < y {
				return -1
			}

			return 1
		}
	}

	return 0
}

// registerCustomSchemasLocked validates items against disabled custom schemas, then registers or
// refreshes each custom type's schema. Caller holds the write lock.
func (b *InMemoryBackend) registerCustomSchemasLocked(region string, items []InventoryItem) error {
	schemas := b.inventorySchemasFor(region)

	for _, item := range items {
		if !strings.HasPrefix(item.TypeName, customInventoryPrefix) {
			continue
		}

		version := item.SchemaVersion
		if version == "" {
			version = defaultCustomSchemaVers
		}

		rec := schemas[item.TypeName]
		if rec != nil && rec.DisabledThrough != "" && compareSchemaVersions(version, rec.DisabledThrough) <= 0 {
			return fmt.Errorf("%w: schema version %s of %s is disabled; use a version greater than %s",
				ErrUnsupportedInventorySchemaVersion, version, item.TypeName, rec.DisabledThrough)
		}
	}

	for _, item := range items {
		if !strings.HasPrefix(item.TypeName, customInventoryPrefix) {
			continue
		}

		version := item.SchemaVersion
		if version == "" {
			version = defaultCustomSchemaVers
		}

		rec := schemas[item.TypeName]
		if rec == nil {
			rec = &InventorySchemaRecord{TypeName: item.TypeName, Attributes: map[string]string{}}
			schemas[item.TypeName] = rec
		}

		rec.Version = version
		rec.DisabledThrough = ""

		for _, entry := range item.Content {
			for k, v := range entry {
				rec.Attributes[k] = mergeAttrType(rec.Attributes[k], v)
			}
		}
	}

	return nil
}

func mergeAttrType(existing, value string) string {
	if existing == attrDataTypeString {
		return existing
	}

	if _, err := strconv.ParseFloat(value, 64); err != nil {
		return attrDataTypeString
	}

	return attrDataTypeNumber
}

func (r *InventorySchemaRecord) item() InventorySchemaItem {
	names := make([]string, 0, len(r.Attributes))
	for k := range r.Attributes {
		names = append(names, k)
	}

	slices.Sort(names)

	attrs := make([]InventoryItemAttribute, 0, len(names))
	for _, n := range names {
		attrs = append(attrs, InventoryItemAttribute{Name: n, DataType: r.Attributes[n]})
	}

	return InventorySchemaItem{TypeName: r.TypeName, Version: r.Version, Attributes: attrs}
}

// customSchemaItemsLocked lists the registered custom schemas sorted by type name.
func (b *InMemoryBackend) customSchemaItemsLocked(region string) []InventorySchemaItem {
	recs := b.inventorySchemas[region]
	out := make([]InventorySchemaItem, 0, len(recs))

	for _, r := range recs {
		out = append(out, r.item())
	}

	slices.SortFunc(out, func(a, c InventorySchemaItem) int { return strings.Compare(a.TypeName, c.TypeName) })

	return out
}

// applySchemaDeleteLocked applies DeleteInventory's SchemaDeleteOption to a registered custom schema.
func (b *InMemoryBackend) applySchemaDeleteLocked(region, typeName, option string) error {
	rec := b.inventorySchemas[region][typeName]
	if rec == nil {
		return fmt.Errorf("%w: %s is not a registered custom inventory type", ErrInvalidTypeName, typeName)
	}

	switch option {
	case schemaDeleteDisable:
		rec.DisabledThrough = rec.Version
	case schemaDeleteDelete:
		delete(b.inventorySchemas[region], typeName)
	}

	return nil
}

func deletionSummaryItems(byVersion map[string]int) []any {
	versions := make([]string, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}

	slices.Sort(versions)

	out := make([]any, 0, len(versions))
	for _, v := range versions {
		out = append(out, InventoryDeletionSummaryItem{Version: v, Count: byVersion[v]})
	}

	return out
}

func (b *InMemoryBackend) resetInventoryState() {
	b.inventory = make(map[string]map[string][]InventoryItem)
	b.inventoryDeletions = make(map[string][]InventoryDeletion)
	b.inventorySchemas = make(map[string]map[string]*InventorySchemaRecord)
}

// removeInventoryType counts typeName's items per schema version and, unless dryRun, deletes them.
func removeInventoryType(store map[string][]InventoryItem, typeName string, dryRun bool) (int, map[string]int) {
	removed := 0
	byVersion := map[string]int{}

	for instanceID, items := range store {
		for _, item := range items {
			if item.TypeName == typeName {
				removed++
				byVersion[item.SchemaVersion]++
			}
		}

		if dryRun {
			continue
		}

		filtered := slices.DeleteFunc(items, func(i InventoryItem) bool { return i.TypeName == typeName })
		if len(filtered) == 0 {
			delete(store, instanceID)
		} else {
			store[instanceID] = filtered
		}
	}

	return removed, byVersion
}
