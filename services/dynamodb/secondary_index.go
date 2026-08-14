// Package dynamodb implements the AWS DynamoDB mock service.
// secondary_index.go maintains a per-GSI/LSI index structure so Query against
// a global or local secondary index can look candidates up directly instead
// of scanning every item in the table (gopherstack-anlc).
//
// Unlike the base table's pkIndex/pkskIndex, GSI/LSI keys are not unique --
// many items can share the same index partition (and even partition+sort) key
// -- so each key maps to a *set* of item offsets rather than a single one.
// Items missing a key attribute the index requires are not present in the
// index at all (DynamoDB's sparse-index behaviour).
package dynamodb

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// secondaryIndex is the item-offset index for one GSI or LSI. Exactly one of
// pkOnly (index has no sort key) or pksk (index has a sort key) is used,
// matching the pkIndex/pkskIndex split on the base table.
type secondaryIndex struct {
	pkOnly map[string]map[int]struct{}
	pksk   map[string]map[string]map[int]struct{}
}

func newSecondaryIndex(hasSK bool) *secondaryIndex {
	if hasSK {
		return &secondaryIndex{pksk: make(map[string]map[string]map[int]struct{})}
	}

	return &secondaryIndex{pkOnly: make(map[string]map[int]struct{})}
}

// add records index at (pkVal[, skVal]) in the index's offset set.
func (si *secondaryIndex) add(pkVal, skVal string, hasSK bool, index int) {
	if hasSK {
		skMap, ok := si.pksk[pkVal]
		if !ok {
			skMap = make(map[string]map[int]struct{})
			si.pksk[pkVal] = skMap
		}

		set, ok := skMap[skVal]
		if !ok {
			set = make(map[int]struct{})
			skMap[skVal] = set
		}
		set[index] = struct{}{}

		return
	}

	set, ok := si.pkOnly[pkVal]
	if !ok {
		set = make(map[int]struct{})
		si.pkOnly[pkVal] = set
	}
	set[index] = struct{}{}
}

// remove drops index from the offset set at (pkVal[, skVal]), pruning any
// now-empty intermediate maps so a fully-vacated key leaves no trace.
func (si *secondaryIndex) remove(pkVal, skVal string, hasSK bool, index int) {
	if hasSK {
		skMap, ok := si.pksk[pkVal]
		if !ok {
			return
		}

		set, ok := skMap[skVal]
		if !ok {
			return
		}
		delete(set, index)

		if len(set) == 0 {
			delete(skMap, skVal)
		}
		if len(skMap) == 0 {
			delete(si.pksk, pkVal)
		}

		return
	}

	set, ok := si.pkOnly[pkVal]
	if !ok {
		return
	}
	delete(set, index)

	if len(set) == 0 {
		delete(si.pkOnly, pkVal)
	}
}

// offsetsForPK returns the offset set of every item indexed under pkVal, or
// nil when pkVal has no matching items. For a sort-keyed index this unions
// every sort-key bucket under pkVal -- range/other conditions on the sort key
// are applied afterwards as a post-filter over this candidate set, exactly as
// the base-table pkskIndex path already does. The returned set (or, for the
// sort-keyed case, its constituent buckets) must not be mutated by the
// caller.
func (si *secondaryIndex) offsetsForPK(pkVal string, hasSK bool) map[int]struct{} {
	if !hasSK {
		set, ok := si.pkOnly[pkVal]
		if !ok {
			return nil
		}

		return set
	}

	skMap, ok := si.pksk[pkVal]
	if !ok {
		return nil
	}

	if len(skMap) == 1 {
		for _, set := range skMap {
			return set
		}
	}

	union := make(map[int]struct{})
	for _, set := range skMap {
		for idx := range set {
			union[idx] = struct{}{}
		}
	}

	return union
}

// copySecondaryIndex returns a deep copy of si, or nil if si is nil.
func copySecondaryIndex(si *secondaryIndex) *secondaryIndex {
	if si == nil {
		return nil
	}

	cp := &secondaryIndex{}

	if si.pkOnly != nil {
		cp.pkOnly = make(map[string]map[int]struct{}, len(si.pkOnly))
		for pk, set := range si.pkOnly {
			setCopy := make(map[int]struct{}, len(set))
			maps.Copy(setCopy, set)
			cp.pkOnly[pk] = setCopy
		}
	}

	if si.pksk != nil {
		cp.pksk = make(map[string]map[string]map[int]struct{}, len(si.pksk))
		for pk, skMap := range si.pksk {
			skCopy := make(map[string]map[int]struct{}, len(skMap))
			for sk, set := range skMap {
				setCopy := make(map[int]struct{}, len(set))
				maps.Copy(setCopy, set)
				skCopy[sk] = setCopy
			}
			cp.pksk[pk] = skCopy
		}
	}

	return cp
}

// copySecondaryIndexMap deep-copies a name -> *secondaryIndex map (used for
// transaction snapshot/rollback).
func copySecondaryIndexMap(m map[string]*secondaryIndex) map[string]*secondaryIndex {
	cp := make(map[string]*secondaryIndex, len(m))
	for name, si := range m {
		cp[name] = copySecondaryIndex(si)
	}

	return cp
}

// snapshotSecondaryIndexForQuery returns a scoped copy of idxName's secondary
// index appropriate for a single Query call, mirroring snapshotIndexForQuery's
// targeted-vs-full-copy split for the primary index (item_ops.go). Must be
// called with table.mu held (read lock).
//
//   - idxName == "":        not a GSI/LSI query; return nil.
//   - idxName unknown:      return nil (Query reports ResourceNotFoundException
//     once extractKeySchema runs against the snapshot).
//   - pkValue known:        copy only that PK's entries -- avoids an O(index
//     size) copy on every query, the same reason a full pkIndex copy is
//     avoided for base-table queries.
//   - pkValue unknown (rare -- e.g. a non-equality KeyConditionExpression):
//     fall back to copying the whole index.
func (db *InMemoryDB) snapshotSecondaryIndexForQuery(
	table *Table,
	idxName, pkValue string,
) *secondaryIndex {
	if idxName == "" {
		return nil
	}

	si := table.secondaryIndexFor(idxName)
	if si == nil {
		return nil
	}

	if pkValue == "" {
		return copySecondaryIndex(si)
	}

	return snapshotSecondaryIndexSinglePK(si, pkValue)
}

// snapshotSecondaryIndexSinglePK copies only pkValue's entries from si. The
// returned index is never nil, even when pkValue has no entries in si, so
// callers can distinguish "index unusable, fall back to scan" (nil) from
// "index usable, this PK just has no matches" (non-nil, empty).
func snapshotSecondaryIndexSinglePK(si *secondaryIndex, pkValue string) *secondaryIndex {
	if si.pksk != nil {
		cp := &secondaryIndex{pksk: make(map[string]map[string]map[int]struct{}, 1)}
		if skMap, ok := si.pksk[pkValue]; ok {
			skCopy := make(map[string]map[int]struct{}, len(skMap))
			for sk, set := range skMap {
				setCopy := make(map[int]struct{}, len(set))
				maps.Copy(setCopy, set)
				skCopy[sk] = setCopy
			}
			cp.pksk[pkValue] = skCopy
		}

		return cp
	}

	cp := &secondaryIndex{pkOnly: make(map[string]map[int]struct{}, 1)}
	if set, ok := si.pkOnly[pkValue]; ok {
		setCopy := make(map[int]struct{}, len(set))
		maps.Copy(setCopy, set)
		cp.pkOnly[pkValue] = setCopy
	}

	return cp
}

// allOffsets returns every item offset referenced anywhere in si, or nil if
// si is nil. Used to build a scoped itemsByOffset snapshot for a GSI/LSI
// query, mirroring snapshotItemsByOffset's role for the primary index.
func (si *secondaryIndex) allOffsets() map[int]struct{} {
	if si == nil {
		return nil
	}

	out := make(map[int]struct{})

	for _, set := range si.pkOnly {
		for idx := range set {
			out[idx] = struct{}{}
		}
	}

	for _, skMap := range si.pksk {
		for _, set := range skMap {
			for idx := range set {
				out[idx] = struct{}{}
			}
		}
	}

	return out
}

// secondaryIndexFor returns the named GSI or LSI's index structure, or nil if
// idxName does not name a known index.
func (t *Table) secondaryIndexFor(idxName string) *secondaryIndex {
	if si, ok := t.gsiIndexes[idxName]; ok {
		return si
	}
	if si, ok := t.lsiIndexes[idxName]; ok {
		return si
	}

	return nil
}

// updateSecondaryIndexes applies a single item-slot change to every GSI/LSI
// index. oldItem/oldIndex describe the slot's previous occupant (oldItem nil
// means the slot was previously empty -- a fresh insert); newItem/newIndex
// describe its new occupant (newItem nil means the slot is now vacated -- a
// delete). Passing oldIndex/newIndex explicitly (rather than inferring
// membership purely from key-value equality) is what makes this correct for
// the delete-by-swap case: the item's key values are unchanged but its
// physical offset moves, which still requires a remove-at-old-offset +
// add-at-new-offset pair.
func (t *Table) updateSecondaryIndexes(
	oldItem map[string]any, oldIndex int,
	newItem map[string]any, newIndex int,
) {
	for i := range t.GlobalSecondaryIndexes {
		gsi := &t.GlobalSecondaryIndexes[i]
		if si := t.gsiIndexes[gsi.IndexName]; si != nil {
			updateOneSecondaryIndex(si, gsi.KeySchema, oldItem, oldIndex, newItem, newIndex)
		}
	}

	for i := range t.LocalSecondaryIndexes {
		lsi := &t.LocalSecondaryIndexes[i]
		if si := t.lsiIndexes[lsi.IndexName]; si != nil {
			updateOneSecondaryIndex(si, lsi.KeySchema, oldItem, oldIndex, newItem, newIndex)
		}
	}
}

// updateOneSecondaryIndex applies a single item-slot change to one GSI/LSI's
// index structure.
func updateOneSecondaryIndex(
	si *secondaryIndex,
	keySchema []models.KeySchemaElement,
	oldItem map[string]any, oldIndex int,
	newItem map[string]any, newIndex int,
) {
	pkDef, skDef := getPKAndSK(keySchema)
	hasSK := skDef.AttributeName != ""

	if pkVal, skVal, ok := secondaryItemKeyValues(oldItem, pkDef, skDef); ok {
		si.remove(pkVal, skVal, hasSK, oldIndex)
	}

	if pkVal, skVal, ok := secondaryItemKeyValues(newItem, pkDef, skDef); ok {
		si.add(pkVal, skVal, hasSK, newIndex)
	}
}

// secondaryItemKeyValues returns the (pkVal, skVal) strings for item under an
// index's (pkDef, skDef), and ok=false when item is nil or missing any key
// attribute the index requires -- DynamoDB's sparse-index rule: an item
// absent a GSI/LSI key attribute simply does not appear in that index.
func secondaryItemKeyValues(
	item map[string]any,
	pkDef, skDef models.KeySchemaElement,
) (string, string, bool) {
	if item == nil {
		return "", "", false
	}

	if _, ok := item[pkDef.AttributeName]; !ok {
		return "", "", false
	}

	if skDef.AttributeName == "" {
		return BuildKeyString(item, pkDef.AttributeName), "", true
	}

	if _, ok := item[skDef.AttributeName]; !ok {
		return "", "", false
	}

	return BuildKeyString(item, pkDef.AttributeName), BuildKeyString(item, skDef.AttributeName), true
}
