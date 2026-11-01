package ocinosql

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type indexKeyIn struct {
	ColumnName    string `json:"columnName"`
	JSONPath      string `json:"jsonPath"`
	JSONFieldType string `json:"jsonFieldType"`
}

type createIndexIn struct {
	IfNotExists   *bool        `json:"isIfNotExists"`
	Name          string       `json:"name"`
	CompartmentID string       `json:"compartmentId"`
	Keys          []indexKeyIn `json:"keys"`
}

//nolint:gochecknoglobals // immutable lookup
var indexJSONTypes = []string{kInteger, kLong, kDouble, kNumber, kString, kBoolean, kTimestamp}

func buildIndexKeys(t *Table, in []indexKeyIn) ([]IndexKey, *APIError) {
	if len(in) == 0 {
		return nil, errMissing("keys must contain at least one index key")
	}

	sv := newSchemaView(t.Columns)
	keys := make([]IndexKey, 0, len(in))

	for _, k := range in {
		i, ok := sv.lookup(k.ColumnName)
		if !ok {
			return nil, errParam("index key column '" + k.ColumnName + "' is not a column of table " + t.Name)
		}

		key := IndexKey{Column: sv.cols[i].Name, JSONPath: k.JSONPath, JSONType: strings.ToUpper(k.JSONFieldType)}
		if aerr := checkIndexKey(sv.types[i], key); aerr != nil {
			return nil, aerr
		}

		if slices.ContainsFunc(keys, func(o IndexKey) bool {
			return o.Column == key.Column && o.JSONPath == key.JSONPath
		}) {
			return nil, errParam("duplicate index key '" + k.ColumnName + "'")
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func checkIndexKey(ty *dtype, k IndexKey) *APIError {
	switch ty.kind {
	case kJSON:
		if k.JSONPath == "" || k.JSONType == "" {
			return errParam("jsonPath and jsonFieldType are required for JSON column '" + k.Column + "'")
		}

		if !slices.Contains(indexJSONTypes, k.JSONType) {
			return errParam("jsonFieldType '" + k.JSONType + "' is not a supported index type")
		}
	case kArray, kMap, kRecord, kBinary:
		return errParam("column '" + k.Column + "' of type " + ty.kind + " cannot be indexed")
	default:
		if k.JSONPath != "" || k.JSONType != "" {
			return errParam("jsonPath and jsonFieldType are only valid for JSON columns")
		}
	}

	return nil
}

// CreateIndex adds a secondary index to a table and returns its work request id.
func (b *InMemoryBackend) CreateIndex(ref string, in createIndexIn) (string, error) {
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, in.CompartmentID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = b.modifiable(t); aerr != nil {
		return "", aerr
	}

	if aerr = validIdent("index", in.Name); aerr != nil {
		return "", aerr
	}

	if len(t.Indexes) >= maxIndexes {
		return "", errLimit("a table can have at most " + strconv.Itoa(maxIndexes) + " indexes")
	}

	keys, aerr := buildIndexKeys(t, in.Keys)
	if aerr != nil {
		return "", aerr
	}

	if _, exists := t.Indexes[strs.Fold(in.Name)]; exists {
		if in.IfNotExists != nil && *in.IfNotExists {
			return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", b.nowUTC()), nil
		}

		return "", errConflict("Index " + in.Name + " already exists on table " + t.Name)
	}

	at := b.transitionAt()
	t.Indexes[strs.Fold(in.Name)] = &Index{
		Name: in.Name, State: stateCreating, Keys: keys, TransitionAt: at, Created: b.nowUTC(),
	}
	t.Etag, t.TimeUpdated = newEtag(), b.nowUTC()

	return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", *at), nil
}

// DeleteIndex removes an index; ifExists turns a missing index into a no-op (empty work request id).
func (b *InMemoryBackend) DeleteIndex(ref, name, compartment string, ifExists bool) (string, error) {
	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return "", aerr
	}

	ix, ok := t.Indexes[strs.Fold(name)]
	if !ok {
		if ifExists {
			return "", nil
		}

		return "", errNotFound("index " + name + " of table " + t.Name)
	}

	if ix.State != stateActive {
		return "", errState("Index " + ix.Name + " is in state " + ix.State + "; it must be ACTIVE")
	}

	ix.State, ix.TransitionAt = stateDeleting, b.transitionAt()
	t.Etag, t.TimeUpdated = newEtag(), b.nowUTC()

	return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", *ix.TransitionAt), nil
}

type indexKeyWire struct {
	ColumnName    string `json:"columnName"`
	JSONPath      string `json:"jsonPath,omitempty"`
	JSONFieldType string `json:"jsonFieldType,omitempty"`
}

// IndexWire is the full index representation.
type IndexWire struct {
	Name           string         `json:"name"`
	CompartmentID  string         `json:"compartmentId,omitempty"`
	TableName      string         `json:"tableName,omitempty"`
	TableID        string         `json:"tableId,omitempty"`
	LifecycleState string         `json:"lifecycleState"`
	Keys           []indexKeyWire `json:"keys"`
}

func indexView(t *Table, ix *Index, full bool) IndexWire {
	keys := make([]indexKeyWire, len(ix.Keys))
	for i, k := range ix.Keys {
		keys[i] = indexKeyWire{ColumnName: k.Column, JSONPath: k.JSONPath, JSONFieldType: k.JSONType}
	}

	w := IndexWire{Name: ix.Name, LifecycleState: ix.State, Keys: keys}
	if full {
		w.CompartmentID, w.TableName, w.TableID = t.CompartmentID, t.Name, t.ID
	}

	return w
}

// GetIndex returns one index.
func (b *InMemoryBackend) GetIndex(ref, name, compartment string) (IndexWire, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return IndexWire{}, aerr
	}

	ix, ok := t.Indexes[strs.Fold(name)]
	if !ok {
		return IndexWire{}, errNotFound("index " + name + " of table " + t.Name)
	}

	return indexView(t, ix, true), nil
}

// ListIndexes lists the indexes of a table.
func (b *InMemoryBackend) ListIndexes(ref, compartment, name, state string, o ListOpts) (page.Page[IndexWire], error) {
	b.mu.RLock("ListIndexes")
	defer b.mu.RUnlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return page.Page[IndexWire]{}, aerr
	}

	var all []*Index

	for _, ix := range t.Indexes {
		if (name != "" && !strs.Equal(ix.Name, name)) || (state != "" && state != filterAll && ix.State != state) {
			continue
		}

		all = append(all, ix)
	}

	pg := paginate(all, o, func(i *Index) string { return i.Name },
		func(i *Index) (time.Time, int64) { return i.Created, 0 })

	out := make([]IndexWire, 0, len(pg.Data))
	for _, ix := range pg.Data {
		out = append(out, indexView(t, ix, false))
	}

	return page.Page[IndexWire]{Data: out, Next: pg.Next}, nil
}

func (h *Handler) createIndex(q *req) error {
	var in createIndexIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateIndex(q.param("tableNameOrId"), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteIndex(q *req) error {
	wr, err := h.Backend.DeleteIndex(q.param("tableNameOrId"), q.param("indexName"), q.query("compartmentId"),
		q.flag("isIfExists"))
	if err == nil && wr == "" {
		q.w.WriteHeader(http.StatusNoContent)

		return nil
	}

	return acceptedWR(q, wr, err)
}

func (h *Handler) getIndex(q *req) error {
	w, err := h.Backend.GetIndex(q.param("tableNameOrId"), q.param("indexName"), q.query("compartmentId"))

	return reply(q, w, "", err)
}

func (h *Handler) listIndexes(q *req) error {
	pg, err := h.Backend.ListIndexes(q.param("tableNameOrId"), q.query("compartmentId"), q.query("name"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}
