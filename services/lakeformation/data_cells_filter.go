package lakeformation

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// dataCellsFilterVersionBytes is the byte length of the random VersionId
// gopherstack assigns to a data cells filter on Create/Update, matching the
// synthetic-ID pattern used for temporary credentials (see credentials.go).
const dataCellsFilterVersionBytes = 8

// validateDataCellsFilterColumns enforces the real API's documented
// constraint that a data cells filter must specify exactly one of
// ColumnNames or ColumnWildcard (see types.DataCellsFilter.ColumnWildcard's
// doc comment: "You must specify either a ColumnNames list or the
// ColumnWildCard").
func validateDataCellsFilterColumns(filter *DataCellsFilter) error {
	hasNames := len(filter.ColumnNames) > 0
	hasWildcard := filter.ColumnWildcard != nil

	if hasNames && hasWildcard {
		return fmt.Errorf("only one of ColumnNames or ColumnWildcard may be specified: %w", ErrValidation)
	}

	return nil
}

// AddDataCellsFilterInternal seeds a DataCellsFilter directly for testing.
func (b *InMemoryBackend) AddDataCellsFilterInternal(filter *DataCellsFilter) {
	b.mu.Lock("AddDataCellsFilterInternal")
	defer b.mu.Unlock()

	cp := *filter
	b.dataCellsFilters.Put(&cp)
}

// CreateDataCellsFilter stores a new data cells filter.
func (b *InMemoryBackend) CreateDataCellsFilter(filter *DataCellsFilter) error {
	if filter == nil {
		return fmt.Errorf("filter is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.TableCatalogID) == "" {
		return fmt.Errorf("TableCatalogId is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.DatabaseName) == "" {
		return fmt.Errorf("DatabaseName is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.TableName) == "" {
		return fmt.Errorf("TableName is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.Name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}

	if err := validateDataCellsFilterColumns(filter); err != nil {
		return err
	}

	b.mu.Lock("CreateDataCellsFilter")
	defer b.mu.Unlock()

	k := dataCellsFilterKeyStr(filter.TableCatalogID, filter.DatabaseName, filter.TableName, filter.Name)

	if b.dataCellsFilters.Has(k) {
		return awserr.New(
			"data cells filter already exists: "+filter.Name,
			awserr.ErrAlreadyExists,
		)
	}

	cp := *filter
	cp.VersionID = randomHex(dataCellsFilterVersionBytes)
	b.dataCellsFilters.Put(&cp)

	return nil
}

// DeleteDataCellsFilter removes the named data cells filter.
func (b *InMemoryBackend) DeleteDataCellsFilter(tableCatalogID, databaseName, tableName, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteDataCellsFilter")
	defer b.mu.Unlock()

	k := dataCellsFilterKeyStr(tableCatalogID, databaseName, tableName, name)

	if !b.dataCellsFilters.Has(k) {
		return awserr.New(
			"data cells filter not found: "+name,
			awserr.ErrNotFound,
		)
	}

	b.dataCellsFilters.Delete(k)

	return nil
}

// ListDataCellsFilter returns a paginated list of data cells filters.
// Optional tableCatalogID, databaseName, and tableName act as filters.
func (b *InMemoryBackend) ListDataCellsFilter(
	tableCatalogID, databaseName, tableName string,
	maxResults int,
	nextToken string,
) ([]*DataCellsFilter, string) {
	b.mu.RLock("ListDataCellsFilter")
	defer b.mu.RUnlock()

	all := make([]*DataCellsFilter, 0, b.dataCellsFilters.Len())

	for _, v := range b.dataCellsFilters.All() {
		if tableCatalogID != "" && v.TableCatalogID != tableCatalogID {
			continue
		}

		if databaseName != "" && v.DatabaseName != databaseName {
			continue
		}

		if tableName != "" && v.TableName != tableName {
			continue
		}

		cp := *v
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		ki := all[i].TableCatalogID + "|" + all[i].DatabaseName + "|" + all[i].TableName + "|" + all[i].Name
		kj := all[j].TableCatalogID + "|" + all[j].DatabaseName + "|" + all[j].TableName + "|" + all[j].Name

		return ki < kj
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// GetDataCellsFilter returns the named data cells filter.
func (b *InMemoryBackend) GetDataCellsFilter(
	tableCatalogID, databaseName, tableName, name string,
) (*DataCellsFilter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.RLock("GetDataCellsFilter")
	defer b.mu.RUnlock()
	k := dataCellsFilterKeyStr(tableCatalogID, databaseName, tableName, name)
	f, ok := b.dataCellsFilters.Get(k)
	if !ok {
		return nil, awserr.New("data cells filter not found: "+name, awserr.ErrNotFound)
	}
	cp := *f

	return &cp, nil
}

// UpdateDataCellsFilter replaces an existing data cells filter.
func (b *InMemoryBackend) UpdateDataCellsFilter(filter *DataCellsFilter) error {
	if filter == nil {
		return fmt.Errorf("filter is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.TableCatalogID) == "" {
		return fmt.Errorf("TableCatalogId is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.DatabaseName) == "" {
		return fmt.Errorf("DatabaseName is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.TableName) == "" {
		return fmt.Errorf("TableName is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.Name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}
	if err := validateDataCellsFilterColumns(filter); err != nil {
		return err
	}
	b.mu.Lock("UpdateDataCellsFilter")
	defer b.mu.Unlock()
	k := dataCellsFilterKeyStr(filter.TableCatalogID, filter.DatabaseName, filter.TableName, filter.Name)
	if !b.dataCellsFilters.Has(k) {
		return awserr.New("data cells filter not found: "+filter.Name, awserr.ErrNotFound)
	}
	cp := *filter
	cp.VersionID = randomHex(dataCellsFilterVersionBytes)
	b.dataCellsFilters.Put(&cp)

	return nil
}
