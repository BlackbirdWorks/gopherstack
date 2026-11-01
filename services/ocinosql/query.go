package ocinosql

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	preparedPrefix    = "gsp1:"
	defaultQueryLimit = 1000
)

type queryIn struct {
	IsPrepared    *bool          `json:"isPrepared"`
	Variables     map[string]any `json:"variables"`
	CompartmentID string         `json:"compartmentId"`
	Statement     string         `json:"statement"`
	Consistency   string         `json:"consistency"`
}

// QueryResult is the Query response body.
type QueryResult struct {
	Items []map[string]any `json:"items"`
	Usage usageWire        `json:"usage"`
}

// PreparedStatement is the PrepareStatement response body.
type PreparedStatement struct {
	QueryPlan any       `json:"queryPlan,omitempty"`
	Statement string    `json:"statement"`
	Usage     usageWire `json:"usage"`
}

// StatementSummary is the SummarizeStatement response body.
type StatementSummary struct {
	SyntaxError   *string `json:"syntaxError,omitempty"`
	Operation     string  `json:"operation,omitempty"`
	TableName     string  `json:"tableName,omitempty"`
	IndexName     string  `json:"indexName,omitempty"`
	IsIfExists    bool    `json:"isIfExists"`
	IsIfNotExists bool    `json:"isIfNotExists"`
}

func decodeStatement(stmt string, prepared bool) (string, *APIError) {
	if !prepared {
		return stmt, nil
	}

	raw, ok := strings.CutPrefix(stmt, preparedPrefix)
	if !ok {
		return "", errParam("statement is not a prepared statement returned by PrepareStatement")
	}

	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", errParam("prepared statement is corrupt")
	}

	return string(b), nil
}

// selectOrReject parses a SELECT and explains why other statement kinds are refused.
func selectOrReject(sql string) (*selectStmt, *APIError) {
	st, aerr := parseSelect(sql)
	if aerr == nil {
		return st, nil
	}

	switch summarize(sql).operation {
	case sqlInsert, sqlUpdate, sqlDelete:
		return nil, errParam("DML statements are not supported by Query; use UpdateRow and DeleteRow")
	case "CREATE_TABLE", "ALTER_TABLE", "DROP_TABLE":
		return nil, errParam("table DDL is not accepted by Query; use CreateTable, UpdateTable and DeleteTable")
	case "CREATE_INDEX", "DROP_INDEX":
		return nil, errParam("index DDL is not accepted by Query; use CreateIndex and DeleteIndex")
	}

	return nil, aerr
}

func (b *InMemoryBackend) planFor(in queryIn, prepared bool) (*plan, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, aerr
	}

	sql, aerr := decodeStatement(in.Statement, prepared)
	if aerr != nil {
		return nil, aerr
	}

	if len(sql) > maxQueryLen {
		return nil, errLimit("the query string exceeds " + strconv.Itoa(maxQueryLen) + " bytes")
	}

	st, aerr := selectOrReject(sql)
	if aerr != nil {
		return nil, aerr
	}

	t, aerr := b.rowTable(st.table, in.CompartmentID)
	if aerr != nil {
		return nil, aerr
	}

	return newPlan(t, st, in.Variables)
}

// Query runs a SELECT statement and returns one page of results.
func (b *InMemoryBackend) Query(in queryIn, limit int, token string) (page.Page[map[string]any], usageWire, error) {
	b.mu.Lock("Query")
	defer b.mu.Unlock()

	if err := page.ValidateToken(token); err != nil {
		return page.Page[map[string]any]{}, usageWire{}, errParam("page token is invalid")
	}

	if in.Consistency != "" && in.Consistency != consEventual && in.Consistency != consAbsolute {
		return page.Page[map[string]any]{}, usageWire{}, errParam("consistency must be EVENTUAL or ABSOLUTE")
	}

	pl, aerr := b.planFor(in, in.IsPrepared != nil && *in.IsPrepared)
	if aerr != nil {
		return page.Page[map[string]any]{}, usageWire{}, aerr
	}

	rows, scanned := pl.run(b.nowUTC)
	units := unitsFor(scanned)

	if in.Consistency == consAbsolute {
		units *= 2
	}

	pl.table.record(b.nowUTC(), units, 0)

	return page.New(rows, token, limit, defaultQueryLimit), usageWire{ReadUnitsConsumed: units}, nil
}

// PrepareStatement validates a SELECT against its table and returns an opaque prepared form.
func (b *InMemoryBackend) PrepareStatement(compartment, statement string, withPlan bool) (PreparedStatement, error) {
	b.mu.RLock("PrepareStatement")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return PreparedStatement{}, aerr
	}

	st, aerr := selectOrReject(statement)
	if aerr != nil {
		return PreparedStatement{}, aerr
	}

	t, aerr := b.rowTable(st.table, compartment)
	if aerr != nil {
		return PreparedStatement{}, aerr
	}

	vars := map[string]any{}
	for name := range st.vars {
		vars[name] = nil
	}

	pl := &plan{table: t, sv: newSchemaView(t.Columns), stmt: st, vars: vars}
	if aerr = pl.validateOnly(); aerr != nil {
		return PreparedStatement{}, aerr
	}

	out := PreparedStatement{Statement: preparedPrefix + base64.StdEncoding.EncodeToString([]byte(statement))}
	if withPlan {
		out.QueryPlan = pl.describe()
	}

	return out, nil
}

func (pl *plan) validateOnly() *APIError {
	st := pl.stmt

	if st.where != nil {
		if aerr := pl.bind(st.where); aerr != nil {
			return aerr
		}
	}

	for i := range st.items {
		var aerr *APIError
		if st.items[i].path, aerr = pl.normPath(st.items[i].path); aerr != nil {
			return aerr
		}
	}

	for i := range st.order {
		var aerr *APIError
		if st.order[i].path, aerr = pl.normPath(st.order[i].path); aerr != nil {
			return aerr
		}
	}

	return pl.checkOrder()
}

func (pl *plan) describe() map[string]any {
	access := "PRIMARY_KEY_SCAN"
	if len(pl.stmt.order) > 0 && !matchesKey(pl.stmt.order, pl.table.PrimaryKey) {
		access = "INDEX_SCAN"
	}

	d := map[string]any{
		"table":    pl.table.Name,
		"access":   access,
		"filtered": pl.stmt.where != nil,
		"count":    pl.stmt.count,
	}
	if pl.stmt.limit != nil {
		d["limit"] = *pl.stmt.limit
	}

	if pl.stmt.offset != nil {
		d["offset"] = *pl.stmt.offset
	}

	return d
}

// SummarizeStatement classifies a statement and reports syntax errors without executing anything.
func (b *InMemoryBackend) SummarizeStatement(compartment, statement string) (StatementSummary, error) {
	b.mu.RLock("SummarizeStatement")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return StatementSummary{}, aerr
	}

	s := summarize(statement)
	out := StatementSummary{
		Operation: s.operation, TableName: s.table, IndexName: s.index, IsIfExists: s.ifExists,
		IsIfNotExists: s.ifNotExists,
	}

	if s.syntaxError != "" {
		out.SyntaxError = &s.syntaxError
	}

	return out, nil
}

func (h *Handler) query(q *req) error {
	var in queryIn
	if err := q.decode(&in); err != nil {
		return err
	}

	limit, _ := strconv.Atoi(q.query("limit"))

	pg, usage, err := h.Backend.Query(in, limit, q.query("page"))
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, QueryResult{Items: pg.Data, Usage: usage}, "")
}

func (h *Handler) prepareStatement(q *req) error {
	res, err := h.Backend.PrepareStatement(q.query("compartmentId"), q.query("statement"),
		q.flag("isGetQueryPlan"))

	return reply(q, res, "", err)
}

func (h *Handler) summarizeStatement(q *req) error {
	res, err := h.Backend.SummarizeStatement(q.query("compartmentId"), q.query("statement"))

	return reply(q, res, "", err)
}
