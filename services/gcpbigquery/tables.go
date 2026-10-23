package gcpbigquery

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func tableManaged() map[string]bool {
	return map[string]bool{
		litKind: true, "id": true, litEtag: true, litSelflink: true, "tableReference": true, litCreationtime: true,
		"lastModifiedTime": true, "numRows": true, "numBytes": true, "numLongTermBytes": true, litType: true,
		litLocation: true, "schema": true, "view": true, "streamingBuffer": true, "numActiveLogicalBytes": true,
		"numTotalLogicalBytes": true, "numActivePhysicalBytes": true, "numPhysicalBytes": true,
	}
}

func decodeSchema(raw any) ([]bqsql.Field, error) {
	if raw == nil {
		return nil, nil
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return nil, errInvalid("Invalid schema")
	}

	var s struct {
		Fields []bqsql.Field `json:"fields"`
	}

	if err = json.Unmarshal(b, &s); err != nil {
		return nil, errInvalid("Invalid schema: %v", err)
	}

	if err = bqsql.NormalizeSchema(s.Fields); err != nil {
		return nil, errInvalid("%v", err)
	}

	return s.Fields, nil
}

func (b *InMemoryBackend) tableLocked(project, ds, id string) (*Dataset, *Table, error) {
	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return nil, nil, err
	}

	t, ok := d.Tables[id]
	if !ok {
		return nil, nil, errNotFound("Not found: Table %s:%s.%s", project, ds, id)
	}

	return d, t, nil
}

func viewQueryOf(body map[string]any) string {
	v, _ := body["view"].(map[string]any)
	q, _ := v["query"].(string)

	return q
}

// tableIDFromBody validates the tableReference of an insert body against the request path.
func tableIDFromBody(project, ds string, body map[string]any) (string, error) {
	ref := refIDs(body, "tableReference")
	id := ref["tableId"]

	if id == "" {
		return "", errInvalid("Table ID is required")
	}

	if len(id) > maxIDLen || !tableIDPattern().MatchString(id) {
		return "", errInvalid("Invalid table ID %q.", id)
	}

	if p := ref["projectId"]; p != "" && p != project {
		return "", errInvalid(
			"Project ID in the table reference (%s) does not match the request project (%s)",
			p,
			project,
		)
	}

	if d := ref["datasetId"]; d != "" && d != ds {
		return "", errInvalid("Dataset ID in the table reference (%s) does not match the request dataset (%s)", d, ds)
	}

	return id, nil
}

// CreateTable implements tables.insert.
func (b *InMemoryBackend) CreateTable(project, ds string, body map[string]any) (map[string]any, error) {
	id, err := tableIDFromBody(project, ds, body)
	if err != nil {
		return nil, err
	}

	schema, err := decodeSchema(body["schema"])
	if err != nil {
		return nil, err
	}

	view := viewQueryOf(body)

	if view == "" && len(schema) == 0 && body["externalDataConfiguration"] == nil {
		schema = []bqsql.Field{}
	}

	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return nil, err
	}

	if _, ok := d.Tables[id]; ok {
		return nil, errDuplicate("Already Exists: Table %s:%s.%s", project, ds, id)
	}

	if view != "" {
		if schema, err = b.viewSchema(project, ds, view); err != nil {
			return nil, err
		}
	}

	now := nowMs()
	t := &Table{
		Meta: metaFrom(body, tableManaged()), Schema: schema, Rows: [][]any{}, ViewQuery: view, Created: now,
		Modified: now, Seq: b.nextSeq(),
	}
	d.Tables[id] = t

	return tableWire(project, ds, id, t, d.Location), nil
}

// viewSchema validates a view query and returns its output schema.
func (b *InMemoryBackend) viewSchema(project, ds, query string) ([]bqsql.Field, error) {
	st, err := bqsql.ParseStatement(query)
	if err != nil || st.Kind != bqsql.StmtQuery {
		return nil, errInvalidQuery("The view query is not a valid SELECT statement")
	}

	res, err := bqsql.Run(
		context.Background(),
		st,
		&bqsql.Input{Catalog: catalog{b: b}, Project: project, Dataset: ds},
		true,
	)
	if err != nil {
		return nil, fromSQL(err)
	}

	return res.Fields, nil
}

// GetTable implements tables.get.
func (b *InMemoryBackend) GetTable(project, ds, id string) (map[string]any, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	d, t, err := b.tableLocked(project, ds, id)
	if err != nil {
		return nil, err
	}

	return tableWire(project, ds, id, t, d.Location), nil
}

// UpdateTable implements tables.patch (merge) and tables.update (replace).
func (b *InMemoryBackend) UpdateTable(
	project, ds, id string,
	body map[string]any,
	replace bool,
) (map[string]any, error) {
	schema, err := decodeSchema(body["schema"])
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	d, t, err := b.tableLocked(project, ds, id)
	if err != nil {
		return nil, err
	}

	if body["schema"] != nil && t.ViewQuery == "" {
		if err = t.applySchema(schema); err != nil {
			return nil, err
		}
	}

	if q := viewQueryOf(body); q != "" && t.ViewQuery != "" {
		if _, err = b.viewSchema(project, ds, q); err != nil {
			return nil, err
		}

		t.ViewQuery = q
	}

	if replace {
		t.Meta = metaFrom(body, tableManaged())
	} else {
		patch := cloneMap(body)
		for k := range tableManaged() {
			delete(patch, k)
		}

		mergePatch(t.Meta, patch)
	}

	t.Modified = nowMs()
	t.Seq = b.nextSeq()

	return tableWire(project, ds, id, t, d.Location), nil
}

// applySchema accepts only additive or mode-relaxing schema changes, as BigQuery does.
func (t *Table) applySchema(next []bqsql.Field) error {
	if len(next) < len(t.Schema) {
		return errInvalid("Provided Schema does not match Table: fields cannot be removed")
	}

	for i, old := range t.Schema {
		n := next[i]

		if !strings.EqualFold(old.Name, n.Name) || old.Type != n.Type {
			return errInvalid("Provided Schema does not match Table: field %s changed", old.Name)
		}

		if n.Mode != old.Mode && (old.Mode != bqsql.ModeRequired || n.Mode != bqsql.ModeNullable) {
			return errInvalid("Provided Schema does not match Table: field %s changed mode", old.Name)
		}
	}

	for i := len(t.Schema); i < len(next); i++ {
		if next[i].Mode == bqsql.ModeRequired && len(t.Rows) > 0 {
			return errInvalid("Provided Schema does not match Table: cannot add required field %s", next[i].Name)
		}
	}

	pad := len(next) - len(t.Schema)
	for i, row := range t.Rows {
		t.Rows[i] = append(row, make([]any, pad)...)
	}

	t.Schema = next

	return nil
}

// DeleteTable implements tables.delete.
func (b *InMemoryBackend) DeleteTable(project, ds, id string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	d, _, err := b.tableLocked(project, ds, id)
	if err != nil {
		return err
	}

	delete(d.Tables, id)

	return nil
}

// ListTables implements tables.list.
func (b *InMemoryBackend) ListTables(project, ds string, o ListOptions) (page.Page[map[string]any], error) {
	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return page.Page[map[string]any]{}, err
	}

	ids := make([]string, 0, len(d.Tables))
	for id := range d.Tables {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	all := make([]map[string]any, len(ids))
	for i, id := range ids {
		all[i] = tableSummary(project, ds, id, d.Tables[id])
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax), nil
}
