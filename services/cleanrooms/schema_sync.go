package cleanrooms

import "slices"

const (
	schemaTypeTable = "TABLE"
	columnTypeKey   = "type"
)

// SchemaColumn is a column name and SQL type read from a Glue table.
type SchemaColumn struct {
	Name string
	Type string
}

// GlueTableReader reads column definitions of Glue tables behind configured tables.
type GlueTableReader interface {
	// TableColumns returns the table's columns and partition keys; ok is false when the table does not exist.
	TableColumns(region, database, table string) (columns, partitionKeys []SchemaColumn, ok bool)
}

// SetGlueTableReader wires the Glue accessor used to derive schemas from configured table associations.
func (b *InMemoryBackend) SetGlueTableReader(g GlueTableReader) {
	b.mu.Lock("SetGlueTableReader")
	defer b.mu.Unlock()

	b.glue = g
}

func (b *InMemoryBackend) glueReader() GlueTableReader {
	b.mu.RLock("glueReader")
	defer b.mu.RUnlock()

	return b.glue
}

// SetGlueTableReader wires g on the home backend and every region sibling.
func (h *Handler) SetGlueTableReader(g GlueTableReader) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetGlueTableReader(g)
		}
	}
}

type glueRef struct{ region, database, table string }

func glueTableRef(tableReference map[string]any) (glueRef, bool) {
	glue, isMap := tableReference["glue"].(map[string]any)
	if !isMap {
		return glueRef{}, false
	}

	var ref glueRef

	ref.database, _ = glue["databaseName"].(string)
	ref.table, _ = glue["tableName"].(string)
	ref.region, _ = glue["region"].(string)

	return ref, ref.database != "" && ref.table != ""
}

func schemaColumns(cols []SchemaColumn, allowed []string) []map[string]any {
	out := make([]map[string]any, 0, len(cols))

	for _, c := range cols {
		if len(allowed) == 0 || slices.Contains(allowed, c.Name) {
			out = append(out, map[string]any{"name": c.Name, columnTypeKey: c.Type})
		}
	}

	return out
}

// syncAssociationSchemaLocked rebuilds the collaboration schema (and its analysis rules) that a configured
// table association exposes. Caller holds the write lock. No schema exists unless Glue can describe the table.
func (b *InMemoryBackend) syncAssociationSchemaLocked(assoc *ConfiguredTableAssociation) {
	if b.glue == nil {
		return
	}

	mem, ok := b.memberships.Get(assoc.MembershipID)
	if !ok {
		return
	}

	ct, ok := b.configuredTables.Get(assoc.ConfiguredTableID)
	if !ok {
		return
	}

	ref, ok := glueTableRef(ct.TableReference)
	if !ok {
		return
	}

	cols, partitionKeys, found := b.glue.TableColumns(ref.region, ref.database, ref.table)
	if !found {
		return
	}

	collab, _ := b.collaborations.Get(mem.CollaborationID)
	key := collaborationKey(mem.CollaborationID, assoc.Name)
	now := b.now()

	schema, exists := b.schemas.Get(key)
	if !exists {
		schema = &Schema{CollaborationID: mem.CollaborationID, Name: assoc.Name, CreateTime: now}
	}

	schema.CollaborationArn = mem.CollaborationArn
	schema.CollaborationIdentifier = mem.CollaborationID
	schema.CreatorAccountID = b.accountID
	schema.Type = schemaTypeTable
	schema.AnalysisMethod = ct.AnalysisMethod
	schema.Columns = schemaColumns(cols, ct.AllowedColumns)
	schema.PartitionKeys = schemaColumns(partitionKeys, ct.AllowedColumns)
	schema.UpdateTime = now

	if collab != nil && schema.CollaborationArn == "" {
		schema.CollaborationArn = collab.Arn
	}

	ruleTypes := b.syncSchemaAnalysisRulesLocked(assoc, mem.CollaborationID, ct.ID, now)
	schema.AnalysisRuleTypes = ruleTypes
	b.schemas.Put(schema)
}

func (b *InMemoryBackend) syncSchemaAnalysisRulesLocked(
	assoc *ConfiguredTableAssociation, collaborationID, configuredTableID string, now float64,
) []string {
	rules := b.ctAnalysisRulesByTable.Get(configuredTableID)
	types := make([]string, 0, len(rules))

	for _, rule := range rules {
		types = append(types, rule.Type)
	}

	slices.Sort(types)

	for _, stale := range []string{"AGGREGATION", "LIST", "CUSTOM"} {
		if !slices.Contains(types, stale) {
			b.schemaAnalysisRules.Delete(schemaAnalysisRuleKey(collaborationID, assoc.Name, stale))
		}
	}

	for _, rule := range rules {
		key := schemaAnalysisRuleKey(collaborationID, assoc.Name, rule.Type)

		sr, exists := b.schemaAnalysisRules.Get(key)
		if !exists {
			sr = &SchemaAnalysisRule{
				CollaborationID: collaborationID, Name: assoc.Name, Type: rule.Type, CreateTime: now,
			}
		}

		sr.Policy = rule.Policy
		sr.UpdateTime = now

		if assocRule, ok := b.ctaAnalysisRules.Get(ctaAnalysisRuleKey(assoc.ID, rule.Type)); ok {
			sr.CollaborationPolicy = assocRule.Policy
		} else {
			sr.CollaborationPolicy = nil
		}

		b.schemaAnalysisRules.Put(sr)
	}

	return types
}

// syncTableSchemasLocked rebuilds the schemas of every association of configuredTableID.
func (b *InMemoryBackend) syncTableSchemasLocked(configuredTableID string) {
	for _, assoc := range b.ctAssociations.All() {
		if assoc.ConfiguredTableID == configuredTableID {
			b.syncAssociationSchemaLocked(assoc)
		}
	}
}

// deleteAssociationSchemaLocked removes the schema and analysis rules an association exposed.
func (b *InMemoryBackend) deleteAssociationSchemaLocked(assoc *ConfiguredTableAssociation) {
	mem, ok := b.memberships.Get(assoc.MembershipID)
	if !ok {
		return
	}

	b.schemas.Delete(collaborationKey(mem.CollaborationID, assoc.Name))

	for _, ruleType := range []string{"AGGREGATION", "LIST", "CUSTOM"} {
		b.schemaAnalysisRules.Delete(schemaAnalysisRuleKey(mem.CollaborationID, assoc.Name, ruleType))
	}
}
