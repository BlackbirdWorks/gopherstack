package dmsengine

import (
	"fmt"
	"slices"
	"strings"
)

func (m Mappings) tableRules(t Table, target string) []Rule {
	var out []Rule

	for _, r := range m.Rules {
		if r.Type == ruleTransformation && r.Target == target && MatchPattern(r.Locator.Schema, t.Schema) &&
			MatchPattern(r.Locator.Table, t.Name) {
			out = append(out, r)
		}
	}

	return out
}

func columnMatches(r Rule, c Column) bool {
	if !MatchPattern(r.Locator.Column, c.Name) {
		return false
	}

	if r.Locator.DataType == "" {
		return true
	}

	return strings.EqualFold(r.Locator.DataType, c.DMS.Name) ||
		(c.DMS.Name == "" && strings.EqualFold(r.Locator.DataType, dmsTypeOf(c.Native)))
}

type colPlan struct {
	out     []TargetColumn
	rename  map[string]string
	rules   []Rule
	include bool
}

func (m Mappings) planColumns(t Table, out *TargetTable) error {
	cp := colPlan{rules: m.tableRules(t, targetColumn), rename: make(map[string]string, len(t.Columns))}

	for _, r := range cp.rules {
		cp.include = cp.include || r.Action == actIncludeColumn
	}

	for i, c := range t.Columns {
		tc, keep, err := cp.sourceColumn(i, c)
		if err != nil {
			return err
		}

		if keep {
			cp.rename[c.Name] = tc.Name
			cp.out = append(cp.out, tc)
		}
	}

	if err := cp.addColumns(t); err != nil {
		return err
	}

	cp.beforeImages(t)

	if err := checkDuplicates(cp.out, out.Key()); err != nil {
		return err
	}

	out.Columns = cp.out
	out.PK = make([]string, 0, len(t.PK))

	for _, p := range t.PK {
		if n, ok := cp.rename[p]; ok {
			out.PK = append(out.PK, n)
		}
	}

	for _, c := range cp.out {
		out.HasBeforeImage = out.HasBeforeImage || c.Before
	}

	return nil
}

func checkDuplicates(cols []TargetColumn, table string) error {
	seen := make(map[string]bool, len(cols))

	for _, c := range cols {
		if seen[c.Name] {
			return fmt.Errorf("%w: column %q appears twice on %s", ErrMapping, c.Name, table)
		}

		seen[c.Name] = true
	}

	return nil
}

func (cp *colPlan) sourceColumn(i int, c Column) (TargetColumn, bool, error) {
	name, included, removed := c.Name, !cp.include, false
	tc := TargetColumn{Column: c, Source: i}

	for _, r := range cp.rules {
		if !columnMatches(r, c) {
			continue
		}

		switch r.Action {
		case actRemoveColumn:
			removed = true
		case actIncludeColumn:
			included = true
		case actChangeType:
			dt, err := r.DataType.parse()
			if err != nil {
				return tc, false, err
			}

			tc.Native, tc.Kind, tc.DMS = dt.Name, dt.Kind(), dt
		case actAddColumn, actBeforeImage:
		default:
			name = applyNameAction(r.Action, name, r.Value, r.Old)
		}
	}

	tc.Name = name

	return tc, included && !removed, nil
}

func (cp *colPlan) addColumns(t Table) error {
	for _, r := range cp.rules {
		if r.Action != actAddColumn {
			continue
		}

		dt, err := r.DataType.parse()
		if err != nil {
			return err
		}

		col := TargetColumn{Column: dt.Column(r.Value), Source: -1, Computed: true}

		if strings.TrimSpace(r.Expression) != "" {
			e, eerr := CompileExpr(r.Expression, t)
			if eerr != nil {
				return fmt.Errorf("add-column %q on %s: %w", r.Value, t.Key(), eerr)
			}

			col.Expr = e
		}

		cp.out = append(cp.out, col)
	}

	return nil
}

func isLOB(c Column) bool {
	switch c.DMS.Name {
	case "blob", "clob", "nclob":
		return true
	}

	return slices.Contains([]string{
		"blob", "tinyblob", "mediumblob", "longblob", "text", "tinytext", "mediumtext", "longtext", "json", "jsonb",
		"bytea",
	}, parseNative(c.Native).base)
}

func (cp *colPlan) beforeImages(t Table) {
	for _, r := range cp.rules {
		if r.Action != actBeforeImage {
			continue
		}

		def := r.imageDef()

		for i, c := range t.Columns {
			if _, kept := cp.rename[c.Name]; !kept || !imageWanted(def.Filter, t, c) {
				continue
			}

			col := c
			col.Name, col.Nullable = *def.Prefix+c.Name+def.Suffix, true
			cp.out = append(cp.out, TargetColumn{Column: col, Source: i, Before: true})
		}
	}
}

func imageWanted(filter string, t Table, c Column) bool {
	if isLOB(c) {
		return false
	}

	return filter != imageFilterPK || slices.Contains(t.PK, c.Name)
}

func (m Mappings) definePrimaryKey(t Table, out *TargetTable) error {
	for _, r := range m.tableRules(t, targetTable) {
		if r.Action != actDefinePK || r.Locator.Schema != t.Schema || r.Locator.Table != t.Name {
			continue
		}

		cols := make([]string, 0, len(r.PrimaryKey.Columns))

		for _, name := range r.PrimaryKey.Columns {
			target, ok := out.columnNamed(t, name)
			if !ok {
				return fmt.Errorf("%w: primary-key-def column %q does not exist on %s", ErrMapping, name, t.Key())
			}

			cols = append(cols, target)
		}

		out.PK, out.KeyName, out.KeyUnique = cols, r.PrimaryKey.Name, r.PrimaryKey.Origin == pkOriginUnique
	}

	return nil
}

func (tt TargetTable) columnNamed(t Table, name string) (string, bool) {
	for _, c := range tt.Columns {
		if c.Name == name || (!c.Computed && !c.Before && t.Columns[c.Source].Name == name) {
			return c.Name, true
		}
	}

	return "", false
}
