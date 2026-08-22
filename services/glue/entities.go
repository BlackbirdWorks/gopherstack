package glue

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Entity field types (a subset of the AWS Glue connector FieldType enum). STRING
// reuses the existing tokString constant.
const (
	fieldTypeInt       = "INT"
	fieldTypeDecimal   = "DECIMAL"
	fieldTypeBoolean   = "BOOLEAN"
	fieldTypeTimestamp = "TIMESTAMP"
	fieldTypeDate      = "DATE"
)

// Entity categories.
const (
	categoryCRM      = "CRM"
	categoryCommerce = "COMMERCE"
	// categoryDatabases and categoryTables are used only by the native Amazon S3
	// Glue Data Catalog path (Entity.Category's doc: "databases or schemas or
	// tables for sources like Amazon Redshift").
	categoryDatabases = "DATABASES"
	categoryTables    = "TABLES"
)

// Field filter operators advertised for filterable entity fields.
const (
	opEqualTo     = "EQUAL_TO"
	opNotEqualTo  = "NOT_EQUAL_TO"
	opGreaterThan = "GREATER_THAN"
	opLessThan    = "LESS_THAN"
	opContains    = "CONTAINS"
)

// EntityField describes a single schema field of a connector entity, matching the
// shape AWS Glue returns from DescribeEntity's Fields list.
type EntityField struct {
	FieldName                string   `json:"FieldName"`
	Label                    string   `json:"Label,omitempty"`
	Description              string   `json:"Description,omitempty"`
	FieldType                string   `json:"FieldType"`
	NativeDataType           string   `json:"NativeDataType,omitempty"`
	SupportedFilterOperators []string `json:"SupportedFilterOperators,omitempty"`
	IsNullable               bool     `json:"IsNullable"`
	IsRetrievable            bool     `json:"IsRetrievable"`
	IsPartitionable          bool     `json:"IsPartitionable"`
	IsCreateable             bool     `json:"IsCreateable"`
	IsUpdateable             bool     `json:"IsUpdateable"`
	IsUpsertable             bool     `json:"IsUpsertable"`
	IsFilterable             bool     `json:"IsFilterable"`
}

// EntityDescriptor describes an entity exposed by a connector, matching the shape AWS
// Glue returns from ListEntities' Entities list.
type EntityDescriptor struct {
	EntityName     string `json:"EntityName"`
	Label          string `json:"Label,omitempty"`
	Category       string `json:"Category,omitempty"`
	IsParentEntity bool   `json:"IsParentEntity"`
}

// entityDefinition is an internal catalog entry: an entity's descriptor plus its
// schema fields.
type entityDefinition struct {
	descriptor EntityDescriptor
	fields     []EntityField
}

// stringFilterOps are the operators advertised for filterable string fields.
func stringFilterOps() []string {
	return []string{opEqualTo, opNotEqualTo, opGreaterThan, opLessThan, opContains}
}

func strField(name, label string, filterable bool) EntityField {
	f := EntityField{
		FieldName: name, Label: label, FieldType: tokString, NativeDataType: "varchar",
		IsNullable: true, IsRetrievable: true, IsCreateable: true, IsUpdateable: true,
		IsUpsertable: true, IsFilterable: filterable,
	}
	if filterable {
		f.SupportedFilterOperators = stringFilterOps()
	}

	return f
}

func numField(name, label, fieldType, native string) EntityField {
	return EntityField{
		FieldName: name, Label: label, FieldType: fieldType, NativeDataType: native,
		IsNullable: true, IsRetrievable: true, IsCreateable: true, IsUpdateable: true,
		IsUpsertable: true, IsFilterable: true, SupportedFilterOperators: stringFilterOps(),
	}
}

func boolField(name string) EntityField {
	return EntityField{
		FieldName: name, Label: name, FieldType: fieldTypeBoolean, NativeDataType: "boolean",
		IsNullable: true, IsRetrievable: true, IsCreateable: true, IsUpdateable: true,
		IsUpsertable: true, IsFilterable: true, SupportedFilterOperators: []string{opEqualTo, opNotEqualTo},
	}
}

func tsField(name, label string) EntityField {
	return EntityField{
		FieldName: name, Label: label, FieldType: fieldTypeTimestamp, NativeDataType: "datetime",
		IsNullable: true, IsRetrievable: true, IsFilterable: true,
		SupportedFilterOperators: []string{opGreaterThan, opLessThan, opEqualTo},
	}
}

func idField() EntityField {
	return EntityField{
		FieldName: "Id", Label: "Record ID", FieldType: tokString, NativeDataType: "id",
		IsNullable: false, IsRetrievable: true, IsFilterable: true,
		SupportedFilterOperators: []string{opEqualTo, opNotEqualTo},
	}
}

// entityCatalog returns the deterministic catalog of connector entities the emulator
// exposes. AWS discovers entities from the underlying data store; with no real store,
// the emulator advertises a fixed, well-known SaaS/database entity set with accurate
// field shapes so DescribeEntity/GetEntityRecords/ListEntities are internally
// coherent rather than silently returning empty results. It is a function (not a
// package var) to stay lint-clean without a global.
func entityCatalog() map[string]entityDefinition {
	return map[string]entityDefinition{
		"ACCOUNT": {
			descriptor: EntityDescriptor{
				EntityName:     "Account",
				Label:          "Account",
				Category:       categoryCRM,
				IsParentEntity: true,
			},
			fields: []EntityField{
				idField(),
				strField("Name", "Account Name", true),
				strField("Industry", "Industry", true),
				numField("AnnualRevenue", "Annual Revenue", fieldTypeDecimal, "decimal"),
				numField("NumberOfEmployees", "Employees", fieldTypeInt, "int"),
				boolField("IsActive"),
				strField("BillingCountry", "Billing Country", true),
				tsField("CreatedDate", "Created Date"),
				tsField("LastModifiedDate", "Last Modified Date"),
			},
		},
		"CONTACT": {
			descriptor: EntityDescriptor{EntityName: "Contact", Label: "Contact", Category: categoryCRM},
			fields: []EntityField{
				idField(),
				strField("FirstName", "First Name", true),
				strField("LastName", "Last Name", true),
				strField("Email", "Email", true),
				strField("Phone", "Phone", false),
				strField("AccountId", "Account ID", true),
				tsField("CreatedDate", "Created Date"),
			},
		},
		"CUSTOMER": {
			descriptor: EntityDescriptor{
				EntityName:     "Customer",
				Label:          "Customer",
				Category:       categoryCRM,
				IsParentEntity: true,
			},
			fields: []EntityField{
				idField(),
				strField("FullName", "Full Name", true),
				strField("Email", "Email", true),
				strField("Segment", "Segment", true),
				numField("LifetimeValue", "Lifetime Value", fieldTypeDecimal, "decimal"),
				boolField("IsActive"),
				tsField("SignupDate", "Signup Date"),
			},
		},
		"ORDER": {
			descriptor: EntityDescriptor{EntityName: "Order", Label: "Order", Category: categoryCommerce},
			fields: []EntityField{
				idField(),
				strField("CustomerId", "Customer ID", true),
				strField("Status", "Status", true),
				numField("TotalAmount", "Total Amount", fieldTypeDecimal, "decimal"),
				numField("ItemCount", "Item Count", fieldTypeInt, "int"),
				strField("Currency", "Currency", true),
				tsField("OrderDate", "Order Date"),
			},
		},
		"PRODUCT": {
			descriptor: EntityDescriptor{EntityName: "Product", Label: "Product", Category: categoryCommerce},
			fields: []EntityField{
				idField(),
				strField("Name", "Product Name", true),
				strField("Sku", "SKU", true),
				numField("Price", "Price", fieldTypeDecimal, "decimal"),
				numField("StockQuantity", "Stock Quantity", fieldTypeInt, "int"),
				boolField("IsAvailable"),
				tsField("CreatedDate", "Created Date"),
			},
		},
		"OPPORTUNITY": {
			descriptor: EntityDescriptor{EntityName: "Opportunity", Label: "Opportunity", Category: categoryCRM},
			fields: []EntityField{
				idField(),
				strField("Name", "Opportunity Name", true),
				strField("StageName", "Stage", true),
				numField("Amount", "Amount", fieldTypeDecimal, "decimal"),
				numField("Probability", "Probability", fieldTypeInt, "int"),
				strField("AccountId", "Account ID", true),
				boolField("IsClosed"),
				tsField("CloseDate", "Close Date"),
			},
		},
	}
}

// normalizeEntityName upper-cases an entity name for case-insensitive catalog lookup.
func normalizeEntityName(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

// lookupEntity returns the catalog definition for an entity name (case-insensitive)
// and whether it exists.
func lookupEntity(name string) (entityDefinition, bool) {
	def, ok := entityCatalog()[normalizeEntityName(name)]

	return def, ok
}

// nativeEntityName is the fully-qualified "database.table" form
// listNativeCatalogEntities advertises for a native-catalog table entity, matching
// ListEntitiesInput.ParentEntityName's doc ("a fully-qualified path of the
// entity").
func nativeEntityName(dbName, tableName string) string {
	return dbName + "." + tableName
}

// splitNativeEntityName splits a "database.table" native entity name back apart.
// ok is false for anything not in that form (including a bare database name).
func splitNativeEntityName(name string) (string, string, bool) {
	idx := strings.LastIndex(name, ".")
	if idx <= 0 || idx == len(name)-1 {
		return "", "", false
	}

	return name[:idx], name[idx+1:], true
}

// nativeEntityDefFromTable builds the entityDefinition GetEntityRecords' shared
// sample-record synthesis needs for a native S3 Glue Data Catalog table: its
// columns, including partition keys (equally queryable columns on a real table),
// mapped to EntityField.
func nativeEntityDefFromTable(t *Table) entityDefinition {
	fields := make([]EntityField, 0, len(t.StorageDescriptor.Columns)+len(t.PartitionKeys))
	for _, c := range t.StorageDescriptor.Columns {
		fields = append(fields, columnToEntityField(c))
	}

	for _, c := range t.PartitionKeys {
		fields = append(fields, columnToEntityField(c))
	}

	return entityDefinition{
		descriptor: EntityDescriptor{
			EntityName: nativeEntityName(t.DatabaseName, t.Name),
			Label:      t.Name,
			Category:   categoryTables,
		},
		fields: fields,
	}
}

// columnToEntityField converts a Glue table Column into the EntityField shape
// DescribeEntity/GetEntityRecords advertise, approximating AWS Glue's own
// data-type mapping for native S3 Glue Data Catalog columns.
func columnToEntityField(col Column) EntityField {
	f := EntityField{
		FieldName: col.Name, Label: col.Name, Description: col.Comment,
		FieldType: tokString, NativeDataType: col.Type,
		IsNullable: true, IsRetrievable: true,
		IsFilterable: true, SupportedFilterOperators: stringFilterOps(),
	}

	switch nativeGlueTypeCategory(col.Type) {
	case fieldTypeInt:
		f.FieldType = fieldTypeInt
	case fieldTypeDecimal:
		f.FieldType = fieldTypeDecimal
	case fieldTypeBoolean:
		f.FieldType = fieldTypeBoolean
		f.SupportedFilterOperators = []string{opEqualTo, opNotEqualTo}
	case fieldTypeTimestamp:
		f.FieldType = fieldTypeTimestamp
		f.SupportedFilterOperators = []string{opGreaterThan, opLessThan, opEqualTo}
	case fieldTypeDate:
		f.FieldType = fieldTypeDate
		f.SupportedFilterOperators = []string{opGreaterThan, opLessThan, opEqualTo}
	}

	return f
}

// nativeGlueTypeCategory maps a Glue/Hive column type string (e.g. "bigint",
// "decimal(10,2)", "varchar(255)") onto one of this file's EntityField FieldType
// constants by prefix, since Hive types carry parameters gopherstack's schema
// tracking does not parse further. Returns tokString for anything unrecognized.
func nativeGlueTypeCategory(hiveType string) string {
	t := strings.ToLower(strings.TrimSpace(hiveType))

	switch {
	case strings.HasPrefix(t, "int"), strings.HasPrefix(t, "bigint"),
		strings.HasPrefix(t, "smallint"), strings.HasPrefix(t, "tinyint"):
		return fieldTypeInt
	case strings.HasPrefix(t, "double"), strings.HasPrefix(t, "float"), strings.HasPrefix(t, "decimal"):
		return fieldTypeDecimal
	case strings.HasPrefix(t, "boolean"):
		return fieldTypeBoolean
	case strings.HasPrefix(t, "timestamp"):
		return fieldTypeTimestamp
	case strings.HasPrefix(t, "date"):
		return fieldTypeDate
	default:
		return tokString
	}
}

// entitySampleSeed is the deterministic number of sample records the emulator
// synthesizes per entity, so GetEntityRecords returns AWS-shaped data rather than an
// empty list. Records are generated deterministically from the schema.
const entitySampleSeed = 5

// DescribeEntity returns the schema fields for an entity reachable through a
// connection. It validates the connection exists and the entity is a known catalog
// entity, returning EntityNotFoundException otherwise — never an empty success.
func (b *InMemoryBackend) DescribeEntity(connectionName, entityName string) ([]EntityField, error) {
	if _, err := b.GetConnection(connectionName); err != nil {
		return nil, err
	}

	def, ok := lookupEntity(entityName)
	if !ok {
		return nil, awserr.New(
			"entity "+entityName+" was not found for connection "+connectionName,
			awserr.ErrNotFound,
		)
	}

	return def.fields, nil
}

// ListEntities returns the entities reachable through a connection, sorted by name.
// ConnectionName is optional, matching ListEntitiesInput: with none given, AWS lists
// entities from the native Amazon S3 based Glue Data Catalog instead of a connector,
// and this backend serves that from its own database/table catalog. parentEntityName
// filters to that native database's tables; it is not honored in connector mode
// because entityCatalog's canned entities do not model any children (see
// PARITY.md).
func (b *InMemoryBackend) ListEntities(connectionName, parentEntityName string) ([]EntityDescriptor, error) {
	if connectionName == "" {
		return b.listNativeCatalogEntities(parentEntityName)
	}

	if _, err := b.GetConnection(connectionName); err != nil {
		return nil, err
	}

	catalog := entityCatalog()

	names := make([]string, 0, len(catalog))
	for k := range catalog {
		names = append(names, k)
	}

	sort.Strings(names)

	out := make([]EntityDescriptor, 0, len(names))
	for _, k := range names {
		out = append(out, catalog[k].descriptor)
	}

	return out, nil
}

// listNativeCatalogEntities serves ListEntities' native-catalog path. With no
// parentEntityName, it lists the catalog's databases (top-level entities, each
// IsParentEntity). With parentEntityName set to a database name, it lists that
// database's tables, named "database.table" — the fully-qualified form
// ListEntitiesInput.ParentEntityName's doc describes and that GetEntityRecords'
// native-catalog path parses back apart.
func (b *InMemoryBackend) listNativeCatalogEntities(parentEntityName string) ([]EntityDescriptor, error) {
	if parentEntityName == "" {
		dbs := b.GetDatabases()

		out := make([]EntityDescriptor, 0, len(dbs))
		for _, db := range dbs {
			out = append(out, EntityDescriptor{
				EntityName:     db.Name,
				Label:          db.Name,
				Category:       categoryDatabases,
				IsParentEntity: true,
			})
		}

		sort.Slice(out, func(i, j int) bool { return out[i].EntityName < out[j].EntityName })

		return out, nil
	}

	tables, err := b.GetTables(parentEntityName)
	if err != nil {
		return nil, err
	}

	out := make([]EntityDescriptor, 0, len(tables))
	for _, t := range tables {
		out = append(out, EntityDescriptor{
			EntityName: nativeEntityName(t.DatabaseName, t.Name),
			Label:      t.Name,
			Category:   categoryTables,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].EntityName < out[j].EntityName })

	return out, nil
}

// GetEntityRecords returns deterministic sample records for an entity reachable
// through a connection, or, with no ConnectionName, through the native Amazon S3
// based Glue Data Catalog (GetEntityRecordsInput.ConnectionName is optional — see
// api_op_GetEntityRecords.go's doc comment). It validates the connection/catalog
// table and entity, then returns records conforming to the entity schema
// (AWS-shaped documents), honoring limit and nextToken index-based pagination. It
// never silently returns an empty success for a valid entity.
func (b *InMemoryBackend) GetEntityRecords(
	connectionName, entityName string,
	limit int,
	nextToken string,
) ([]map[string]any, string, error) {
	if connectionName == "" {
		return b.getNativeCatalogRecords(entityName, limit, nextToken)
	}

	if _, err := b.GetConnection(connectionName); err != nil {
		return nil, "", err
	}

	def, ok := lookupEntity(entityName)
	if !ok {
		return nil, "", awserr.New(
			"entity "+entityName+" was not found for connection "+connectionName,
			awserr.ErrNotFound,
		)
	}

	all := make([]map[string]any, 0, entitySampleSeed)
	for i := range entitySampleSeed {
		all = append(all, sampleRecord(def, i))
	}

	page, token := paginateSlice(all, nextToken, effectiveEntityLimit(limit))

	return page, token, nil
}

// getNativeCatalogRecords serves GetEntityRecords' native-catalog path. entityName
// must be the "database.table" form listNativeCatalogEntities advertises; anything
// else — including a bare database name, which has no records of its own — is
// EntityNotFoundException rather than an empty success.
func (b *InMemoryBackend) getNativeCatalogRecords(
	entityName string,
	limit int,
	nextToken string,
) ([]map[string]any, string, error) {
	dbName, tableName, ok := splitNativeEntityName(entityName)
	if !ok {
		return nil, "", awserr.New(
			"entity "+entityName+" was not found in the Glue Data Catalog",
			awserr.ErrNotFound,
		)
	}

	t, err := b.GetTable(dbName, tableName)
	if err != nil {
		return nil, "", err
	}

	def := nativeEntityDefFromTable(t)

	all := make([]map[string]any, 0, entitySampleSeed)
	for i := range entitySampleSeed {
		all = append(all, sampleRecord(def, i))
	}

	page, token := paginateSlice(all, nextToken, effectiveEntityLimit(limit))

	return page, token, nil
}

// effectiveEntityLimit clamps the caller-supplied record limit to a sane default and
// maximum, mirroring AWS's bounded page sizes.
func effectiveEntityLimit(limit int) int {
	const maxEntityLimit = 100

	if limit <= 0 {
		return entitySampleSeed
	}

	if limit > maxEntityLimit {
		return maxEntityLimit
	}

	return limit
}

// Sample-record generation constants keep the deterministic synthesis free of magic
// numbers.
const (
	sampleDecimalScale = 100  // per-row multiplier for DECIMAL sample values
	sampleDecimalFrac  = 0.5  // fractional part appended to DECIMAL sample values
	sampleBoolModulus  = 2    // alternate BOOLEAN sample values by row parity
	sampleBaseYear     = 2024 // base year for TIMESTAMP/DATE sample values
)

// sampleRecord synthesizes one deterministic record for def at row index i. Values are
// derived from the field type so the record conforms to the advertised schema. Any
// timestamp value is emitted as an epoch number (never a marshaled time string).
func sampleRecord(def entityDefinition, i int) map[string]any {
	rec := make(map[string]any, len(def.fields))
	base := time.Date(sampleBaseYear, time.January, 1, 0, 0, 0, 0, time.UTC)

	for idx, f := range def.fields {
		switch f.FieldType {
		case fieldTypeInt:
			rec[f.FieldName] = (i + 1) * (idx + 1)
		case fieldTypeDecimal:
			rec[f.FieldName] = float64((i+1)*sampleDecimalScale) + sampleDecimalFrac
		case fieldTypeBoolean:
			rec[f.FieldName] = i%sampleBoolModulus == 0
		case fieldTypeTimestamp, fieldTypeDate:
			rec[f.FieldName] = float64(base.AddDate(0, 0, i).Unix())
		default:
			rec[f.FieldName] = fmt.Sprintf("%s-%d", strings.ToLower(f.FieldName), i+1)
		}
	}

	return rec
}
