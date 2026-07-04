package lakeformation

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot is the serialisable form of InMemoryBackend state.
type backendSnapshot struct {
	Resources              map[string]*ResourceInfo                `json:"Resources"`
	LFTags                 map[string]*LFTag                       `json:"LFTags"`
	Transactions           map[string]*transactionInfo             `json:"Transactions"`
	DataCellsFilters       map[string]*DataCellsFilter             `json:"DataCellsFilters"`
	LFTagExpressions       map[string]*LFTagExpression             `json:"LFTagExpressions"`
	IdentityCenterConfigs  map[string]*IdentityCenterConfiguration `json:"IdentityCenterConfigs"`
	ResourceLFTags         map[string][]LFTagPair                  `json:"ResourceLFTags"`
	Queries                map[string]string                       `json:"Queries,omitempty"`
	TableStorageOptimizers map[string][]StorageOptimizer           `json:"TableStorageOptimizers,omitempty"`
	DataLakeSettings       *DataLakeSettings                       `json:"DataLakeSettings"`
	Permissions            []*PermissionEntry                      `json:"Permissions"`
	LakeFormationOptIns    []*LFOptIn                              `json:"LakeFormationOptIns"`
}

// Snapshot serialises the backend state to JSON for persistence.
func (b *InMemoryBackend) Snapshot() ([]byte, error) {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		DataLakeSettings:       copyDataLakeSettings(b.dataLakeSettings),
		Resources:              make(map[string]*ResourceInfo, len(b.resources)),
		Permissions:            make([]*PermissionEntry, len(b.permissionsList)),
		LFTags:                 make(map[string]*LFTag, len(b.lfTags)),
		Transactions:           make(map[string]*transactionInfo, len(b.transactions)),
		DataCellsFilters:       make(map[string]*DataCellsFilter, len(b.dataCellsFilters)),
		LFTagExpressions:       make(map[string]*LFTagExpression, len(b.lfTagExpressions)),
		IdentityCenterConfigs:  make(map[string]*IdentityCenterConfiguration, len(b.identityCenterConfigs)),
		LakeFormationOptIns:    make([]*LFOptIn, len(b.lakeFormationOptIns)),
		ResourceLFTags:         make(map[string][]LFTagPair, len(b.resourceLFTags)),
		Queries:                make(map[string]string, len(b.queries)),
		TableStorageOptimizers: make(map[string][]StorageOptimizer, len(b.tableStorageOptimizers)),
	}

	for k, v := range b.resources {
		snap.Resources[k] = copyResourceInfo(v)
	}

	// Deep-copy permissions including Principal/Resource pointer fields.
	for i, p := range b.permissionsList {
		snap.Permissions[i] = deepCopyPermissionEntry(p)
	}

	for k, v := range b.lfTags {
		snap.LFTags[snapshotLFTagKey(k)] = copyLFTag(v)
	}

	for k, v := range b.transactions {
		cp := *v
		snap.Transactions[k] = &cp
	}

	maps.Copy(snap.Queries, b.queries)

	for k, v := range b.tableStorageOptimizers {
		cp := make([]StorageOptimizer, len(v))
		copy(cp, v)
		snap.TableStorageOptimizers[k] = cp
	}

	for k, v := range b.dataCellsFilters {
		cp := *v
		snap.DataCellsFilters[snapshotDCFKey(k)] = &cp
	}

	for k, v := range b.lfTagExpressions {
		expr := *v

		if v.Expression != nil {
			expr.Expression = make([]LFTag, len(v.Expression))
			copy(expr.Expression, v.Expression)
		}

		snap.LFTagExpressions[snapshotExprKey(k)] = &expr
	}

	for k, v := range b.identityCenterConfigs {
		cp := *v
		snap.IdentityCenterConfigs[k] = &cp
	}

	// Deep-copy opt-ins including Principal/Resource pointer fields.
	for i, o := range b.lakeFormationOptIns {
		cp := &LFOptIn{}

		if o.Principal != nil {
			p := *o.Principal
			cp.Principal = &p
		}

		if o.Resource != nil {
			cp.Resource = copyResource(o.Resource)
		}

		snap.LakeFormationOptIns[i] = cp
	}

	for k, v := range b.resourceLFTags {
		pairs := make([]LFTagPair, len(v))
		copy(pairs, v)
		snap.ResourceLFTags[k] = pairs
	}

	// Marshal failure is returned to the caller (the Persistable wrapper logs it
	// via the context-aware logger); do not log through slog.Default() here.
	return json.Marshal(snap)
}

// Restore deserialises a snapshot produced by Snapshot back into the backend.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "lakeformation", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.dataLakeSettings = snap.DataLakeSettings
	if b.dataLakeSettings == nil {
		b.dataLakeSettings = &DataLakeSettings{}
	}

	b.resources = make(map[string]*ResourceInfo, len(snap.Resources))
	maps.Copy(b.resources, snap.Resources)

	b.permissionsList = snap.Permissions
	if b.permissionsList == nil {
		b.permissionsList = make([]*PermissionEntry, 0)
	}
	b.permissionsMap = make(map[string]*PermissionEntry)
	for _, p := range b.permissionsList {
		b.permissionsMap[permissionKey(p)] = p
	}

	b.lfTags = make(map[lfTagKey]*LFTag, len(snap.LFTags))
	for ks, v := range snap.LFTags {
		b.lfTags[parseLFTagKey(ks)] = v
	}

	b.transactions = snap.Transactions
	if b.transactions == nil {
		b.transactions = make(map[string]*transactionInfo)
	}

	b.queries = snap.Queries
	if b.queries == nil {
		b.queries = make(map[string]string)
	}

	b.tableStorageOptimizers = snap.TableStorageOptimizers
	if b.tableStorageOptimizers == nil {
		b.tableStorageOptimizers = make(map[string][]StorageOptimizer)
	}

	b.dataCellsFilters = make(map[dataCellsFilterKey]*DataCellsFilter, len(snap.DataCellsFilters))
	for ks, v := range snap.DataCellsFilters {
		b.dataCellsFilters[parseDCFKey(ks)] = v
	}

	b.lfTagExpressions = make(map[lfTagExpressionKey]*LFTagExpression, len(snap.LFTagExpressions))
	for ks, v := range snap.LFTagExpressions {
		b.lfTagExpressions[parseExprKey(ks)] = v
	}

	b.identityCenterConfigs = snap.IdentityCenterConfigs
	if b.identityCenterConfigs == nil {
		b.identityCenterConfigs = make(map[string]*IdentityCenterConfiguration)
	}

	b.lakeFormationOptIns = snap.LakeFormationOptIns
	if b.lakeFormationOptIns == nil {
		b.lakeFormationOptIns = make([]*LFOptIn, 0)
	}

	b.resourceLFTags = snap.ResourceLFTags
	if b.resourceLFTags == nil {
		b.resourceLFTags = make(map[string][]LFTagPair)
	}

	return nil
}

// snapshotLFTagKey serialises an lfTagKey for JSON map keys.
func snapshotLFTagKey(k lfTagKey) string {
	return k.CatalogID + "|" + k.TagKey
}

// parseLFTagKey deserialises an lfTagKey produced by snapshotLFTagKey.
func parseLFTagKey(s string) lfTagKey {
	for i, c := range s {
		if c == '|' {
			return lfTagKey{CatalogID: s[:i], TagKey: s[i+1:]}
		}
	}

	return lfTagKey{TagKey: s}
}

// dcfKeyParts is the number of pipe-delimited parts in a serialised dataCellsFilterKey.
const dcfKeyParts = 4

// snapshotDCFKey serialises a dataCellsFilterKey for JSON map keys.
func snapshotDCFKey(k dataCellsFilterKey) string {
	return k.TableCatalogID + "|" + k.DatabaseName + "|" + k.TableName + "|" + k.Name
}

// parseDCFKey deserialises a dataCellsFilterKey produced by snapshotDCFKey.
func parseDCFKey(s string) dataCellsFilterKey {
	parts := splitN(s, '|', dcfKeyParts)
	if len(parts) == dcfKeyParts {
		return dataCellsFilterKey{
			TableCatalogID: parts[0],
			DatabaseName:   parts[1],
			TableName:      parts[2],
			Name:           parts[3],
		}
	}

	return dataCellsFilterKey{Name: s}
}

// snapshotExprKey serialises an lfTagExpressionKey for JSON map keys.
func snapshotExprKey(k lfTagExpressionKey) string {
	return k.CatalogID + "|" + k.Name
}

// parseExprKey deserialises an lfTagExpressionKey produced by snapshotExprKey.
func parseExprKey(s string) lfTagExpressionKey {
	for i, c := range s {
		if c == '|' {
			return lfTagExpressionKey{CatalogID: s[:i], Name: s[i+1:]}
		}
	}

	return lfTagExpressionKey{Name: s}
}

// splitN splits s into at most n parts using sep.
func splitN(s string, sep rune, n int) []string {
	parts := make([]string, 0, n)
	start := 0

	for i, c := range s {
		if c == sep && len(parts) < n-1 {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}

	parts = append(parts, s[start:])

	return parts
}
