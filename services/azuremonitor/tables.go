package azuremonitor

import (
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

var reservedColumns = []string{ //nolint:gochecknoglobals // static lookup
	"_ResourceId", "id", "_SubscriptionId", "TenantId", "Type", "UniqueId", "Title",
}

func parseColumns(raw any, requireTimeGenerated bool) ([]Column, error) {
	list, _ := raw.([]any)
	if len(list) == 0 {
		return nil, invalid("the schema must declare at least one column")
	}

	out := make([]Column, 0, len(list))
	seen := map[string]bool{}

	for _, item := range list {
		m := mapOf(item)
		name := strOf(m["name"])

		t, ok := kqlTypeOf(strOf(m["type"]))
		if !ok {
			return nil, invalid("column %q has unsupported type %q", name, strOf(m["type"]))
		}

		if !columnNameRe.MatchString(name) {
			return nil, invalid("column name %q must start with a letter and contain at most 45 letters, "+
				"digits and underscores", name)
		}

		if slices.ContainsFunc(reservedColumns, func(r string) bool { return strings.EqualFold(r, name) }) {
			return nil, invalid("column name %q is reserved", name)
		}

		if seen[strings.ToLower(name)] {
			return nil, invalid("column %q is declared more than once", name)
		}

		seen[strings.ToLower(name)] = true
		out = append(out, Column{
			Name: name, Type: string(t), Description: strOf(m["description"]), DisplayName: strOf(m["displayName"]),
		})
	}

	if requireTimeGenerated {
		i := slices.IndexFunc(out, func(c Column) bool { return c.Name == timeGenerated })
		if i < 0 || out[i].Type != string(kql.TDatetime) {
			return nil, invalid("the schema must contain a %s column of type dateTime", timeGenerated)
		}
	}

	return out, nil
}

func (b *Backend) putTable(ref Ref, body map[string]any) (map[string]any, error) {
	name := ref.Names[1]
	props := mapOf(body[fieldProperties])

	b.mu.Lock("putTable")
	defer b.mu.Unlock()

	w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.Names[0])
	if !ok {
		return nil, notFound("workspace %q", ref.Names[0])
	}

	if t, exists := w.table(name); exists {
		if err := t.applyProperties(w, props, true); err != nil {
			return nil, err
		}

		return t.body(w), nil
	}

	if !tableNameRe.MatchString(name) {
		return nil, invalid("custom table name %q must end with _CL and contain only letters, digits and "+
			"underscores (at most 45 characters before the suffix)", name)
	}

	t := &Table{Name: name, Custom: true, Plan: "Analytics", Created: b.now()}
	if err := t.applyProperties(w, props, true); err != nil {
		return nil, err
	}

	w.Tables[name] = t

	return t.body(w), nil
}

func retentionOf(v any, min, max int) (int, bool, error) {
	if v == nil {
		return 0, false, nil
	}

	f, ok := v.(float64)
	if !ok {
		return 0, false, invalid("retention values must be numbers")
	}

	n := int(f)
	if n == -1 {
		return 0, true, nil
	}

	if n < min || n > max {
		return 0, false, invalid("retention must be between %d and %d days", min, max)
	}

	return n, true, nil
}

// applyProperties merges table properties; full is true for PUT (schema required when creating).
func (t *Table) applyProperties(w *Workspace, props map[string]any, full bool) error {
	if v, ok := props["plan"]; ok {
		plan := strOf(v)
		if !strings.EqualFold(plan, "Analytics") && !strings.EqualFold(plan, "Basic") &&
			!strings.EqualFold(plan, "Auxiliary") {
			return invalid("plan %q is not supported", plan)
		}

		t.Plan = canonicalPlan(plan)
	}

	if err := t.applyRetention(props); err != nil {
		return err
	}

	schema := mapOf(props["schema"])

	if desc, ok := schema["description"].(string); ok {
		t.Description = desc
	}

	if !t.Custom {
		return nil
	}

	if _, has := schema["columns"]; has || (full && t.Columns == nil) {
		cols, err := parseColumns(schema["columns"], true)
		if err != nil {
			return err
		}

		if err = t.setColumns(cols); err != nil {
			return err
		}
	}

	_ = w

	return nil
}

func canonicalPlan(p string) string {
	switch strings.ToLower(p) {
	case "basic":
		return "Basic"
	case "auxiliary":
		return "Auxiliary"
	}

	return "Analytics"
}

func (t *Table) applyRetention(props map[string]any) error {
	if n, set, err := retentionOf(props["retentionInDays"], minRetention, maxRetention); err != nil {
		return err
	} else if set {
		t.Retention = n
	}

	if n, set, err := retentionOf(props["totalRetentionInDays"], minRetention, maxTotalRetention); err != nil {
		return err
	} else if set {
		t.TotalRet = n
	}

	if t.Retention > 0 && t.TotalRet > 0 && t.TotalRet < t.Retention {
		return invalid("totalRetentionInDays must not be lower than retentionInDays")
	}

	return nil
}

// setColumns replaces the schema, remapping stored rows by column name.
func (t *Table) setColumns(cols []Column) error {
	for _, nc := range cols {
		if i := slices.IndexFunc(t.Columns, func(c Column) bool { return c.Name == nc.Name }); i >= 0 &&
			t.Columns[i].Type != nc.Type {
			return invalid("the type of column %q cannot be changed", nc.Name)
		}
	}

	if len(t.Rows) > 0 {
		rows := make([][]any, len(t.Rows))

		for r, row := range t.Rows {
			nr := make([]any, len(cols))

			for j, nc := range cols {
				if i := slices.IndexFunc(t.Columns, func(c Column) bool { return c.Name == nc.Name }); i >= 0 {
					nr[j] = row[i]
				} else if nc.Type == string(kql.TString) {
					nr[j] = ""
				}
			}

			rows[r] = nr
		}

		t.Rows = rows
	}

	t.Columns = cols

	return nil
}

func (t *Table) body(w *Workspace) map[string]any {
	retention := t.Retention
	if retention == 0 {
		retention = w.Retention
	}

	total := t.TotalRet
	if total == 0 {
		total = retention
	}

	columns := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		col := map[string]any{"name": c.Name, "type": armTypeOf(kql.Type(c.Type))}
		if c.Description != "" {
			col["description"] = c.Description
		}

		if c.DisplayName != "" {
			col["displayName"] = c.DisplayName
		}

		columns[i] = col
	}

	schema := map[string]any{
		"name": t.Name, "columns": columns, "solutions": []any{"LogManagement"},
		"standardColumns": []any{
			map[string]any{"name": "TenantId", "type": "guid"},
			map[string]any{"name": "Type", "type": "string"},
			map[string]any{"name": "_ResourceId", "type": "string"},
		},
	}

	if t.Description != "" {
		schema["description"] = t.Description
	}

	if t.Custom {
		schema["tableType"], schema["tableSubType"], schema["source"] = "CustomLog", "DataCollectionRuleBased", "customer"
	} else {
		schema["tableType"], schema["tableSubType"], schema["source"] = "Microsoft", "Any", "microsoft"
	}

	out := envelope(
		w.armID()+"/"+typeTables+"/"+t.Name, t.Name, nsOperationalInsights+"/"+typeWorkspaces+"/"+typeTables)
	out[fieldProperties] = map[string]any{
		"retentionInDays": retention, "totalRetentionInDays": total,
		"archiveRetentionInDays":        total - retention,
		"retentionInDaysAsDefault":      t.Retention == 0,
		"totalRetentionInDaysAsDefault": t.TotalRet == 0,
		"plan":                          t.Plan, "schema": schema, fieldProvisioning: provisioningSucceeded,
	}

	return out
}

// relOf builds the query-engine view of a table. Callers hold the read lock.
func (t *Table) relOf(tenant string, from, to time.Time) *kql.Rel {
	cols := make([]kql.Col, 0, len(t.Columns)+3)
	cols = append(cols, kql.Col{Name: "TenantId", Type: kql.TString})

	tg := -1

	for i, c := range t.Columns {
		cols = append(cols, kql.Col{Name: c.Name, Type: kql.Type(c.Type)})

		if c.Name == timeGenerated {
			tg = i
		}
	}

	cols = append(cols, kql.Col{Name: "Type", Type: kql.TString}, kql.Col{Name: "_ResourceId", Type: kql.TString})
	rel := &kql.Rel{Cols: cols, Rows: make([][]any, 0, len(t.Rows))}

	for _, row := range t.Rows {
		if tg >= 0 && (!from.IsZero() || !to.IsZero()) {
			ts, _ := row[tg].(time.Time)
			if (!from.IsZero() && ts.Before(from)) || (!to.IsZero() && ts.After(to)) {
				continue
			}
		}

		nr := make([]any, 0, len(cols))
		nr = append(nr, tenant)
		nr = append(nr, row...)
		nr = append(nr, t.Name, "")
		rel.Rows = append(rel.Rows, nr)
	}

	return rel
}
