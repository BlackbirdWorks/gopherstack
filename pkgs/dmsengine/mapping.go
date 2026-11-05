package dmsengine

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// ErrMapping reports an invalid table-mappings document.
var ErrMapping = errors.New("invalid table mappings")

// Rule actions and targets used by table mappings.
const (
	ruleSelection      = "selection"
	ruleTransformation = "transformation"
	actInclude         = "include"
	actExplicit        = "explicit"
	actExclude         = "exclude"
	actRename          = "rename"
	actRemoveColumn    = "remove-column"
	targetSchema       = "schema"
	targetTable        = "table"
	targetColumn       = "column"
)

type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	var n jsonx.Number
	if err := json.Unmarshal(b, &n, jsonx.V1()); err == nil {
		*f = flexString(n.String())

		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s, jsonx.V1()); err != nil {
		return err
	}

	*f = flexString(s)

	return nil
}

// Rule is one entry of a table-mappings document.
type Rule struct {
	Locator struct {
		Schema string `json:"schema-name"`
		Table  string `json:"table-name"`
		Column string `json:"column-name"`
	} `json:"object-locator"`
	ID     flexString `json:"rule-id"`
	Name   flexString `json:"rule-name"`
	Type   string     `json:"rule-type"`
	Action string     `json:"rule-action"`
	Target string     `json:"rule-target"`
	Value  string     `json:"value"`
	Old    string     `json:"old-value"`
}

// Mappings is a parsed table-mappings document.
type Mappings struct {
	Rules []Rule `json:"rules"`
}

// ParseMappings parses and validates a TableMappings JSON document.
func ParseMappings(doc string) (Mappings, error) {
	var m Mappings
	if strings.TrimSpace(doc) == "" {
		return m, nil
	}

	if err := json.Unmarshal([]byte(doc), &m, jsonx.V1()); err != nil {
		return m, fmt.Errorf("%w: %w", ErrMapping, err)
	}

	for _, r := range m.Rules {
		if err := validateRule(r); err != nil {
			return m, err
		}
	}

	sort.SliceStable(m.Rules, func(i, j int) bool { return ruleOrder(m.Rules[i]) < ruleOrder(m.Rules[j]) })

	return m, nil
}

const unorderedRule = 1 << 30

func ruleOrder(r Rule) int {
	n, err := strconv.Atoi(string(r.ID))
	if err != nil {
		return unorderedRule
	}

	return n
}

func validateRule(r Rule) error {
	switch r.Type {
	case ruleSelection:
		switch r.Action {
		case actInclude, actExclude, actExplicit:
			return nil
		}
	case ruleTransformation:
		return validateTransformation(r)
	default:
		return fmt.Errorf("%w: unsupported rule-type %q", ErrMapping, r.Type)
	}

	return fmt.Errorf("%w: unsupported selection rule-action %q", ErrMapping, r.Action)
}

func validateTransformation(r Rule) error {
	switch r.Target {
	case targetSchema, targetTable, targetColumn:
	default:
		return fmt.Errorf("%w: unsupported rule-target %q", ErrMapping, r.Target)
	}

	switch r.Action {
	case actRename, "add-prefix", "remove-prefix", "replace-prefix", "add-suffix", "remove-suffix",
		"replace-suffix", "convert-lowercase", "convert-uppercase":
		return nil
	case actRemoveColumn:
		if r.Target == targetColumn {
			return nil
		}
	}

	return fmt.Errorf("%w: unsupported transformation rule-action %q for %s", ErrMapping, r.Action, r.Target)
}

// MatchPattern matches s against a DMS locator pattern where % matches any run of characters.
func MatchPattern(pattern, s string) bool {
	if pattern == "" {
		pattern = "%"
	}

	if !strings.Contains(pattern, "%") {
		return pattern == s
	}

	parts := strings.Split(pattern, "%")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}

	return regexp.MustCompile("^" + strings.Join(parts, ".*") + "$").MatchString(s)
}

func applyNameAction(action, name, value, old string) string {
	switch action {
	case actRename:
		return value
	case "add-prefix":
		return value + name
	case "remove-prefix":
		return strings.TrimPrefix(name, value)
	case "replace-prefix":
		if rest, ok := strings.CutPrefix(name, old); ok {
			return value + rest
		}
	case "add-suffix":
		return name + value
	case "remove-suffix":
		return strings.TrimSuffix(name, value)
	case "replace-suffix":
		if rest, ok := strings.CutSuffix(name, old); ok {
			return rest + value
		}
	case "convert-lowercase":
		return strings.ToLower(name)
	case "convert-uppercase":
		return strings.ToUpper(name)
	}

	return name
}

// Select returns the source tables chosen by the selection rules, in source order.
func (m Mappings) Select(tables []Table) []Table {
	chosen := make(map[string]bool, len(tables))

	for _, r := range m.Rules {
		if r.Type != ruleSelection {
			continue
		}

		for _, t := range tables {
			if !MatchPattern(r.Locator.Schema, t.Schema) || !MatchPattern(r.Locator.Table, t.Name) {
				continue
			}

			chosen[t.Key()] = r.Action != actExclude
		}
	}

	out := make([]Table, 0, len(chosen))

	for _, t := range tables {
		if chosen[t.Key()] {
			out = append(out, t)
		}
	}

	return out
}

// Plan maps a selected source table to its target shape. overrideSchema (TargetMetadata.TargetSchema) wins
// over schema renames when set.
func (m Mappings) Plan(t Table, src Family, overrideSchema string) TargetTable {
	out := TargetTable{
		SourceFamily: src, Schema: t.Schema, Name: t.Name, SourceSchema: t.Schema, SourceName: t.Name,
	}

	for _, r := range m.Rules {
		if r.Type != ruleTransformation || !MatchPattern(r.Locator.Schema, t.Schema) {
			continue
		}

		if r.Target != targetSchema && !MatchPattern(r.Locator.Table, t.Name) {
			continue
		}

		switch r.Target {
		case targetSchema:
			out.Schema = applyNameAction(r.Action, out.Schema, r.Value, r.Old)
		case targetTable:
			out.Name = applyNameAction(r.Action, out.Name, r.Value, r.Old)
		}
	}

	if overrideSchema != "" {
		out.Schema = overrideSchema
	}

	out.Columns, out.PK = m.planColumns(t)

	return out
}

func (m Mappings) planColumns(t Table) ([]TargetColumn, []string) {
	cols := make([]TargetColumn, 0, len(t.Columns))
	rename := make(map[string]string, len(t.Columns))

	for i, c := range t.Columns {
		name, removed := c.Name, false

		for _, r := range m.Rules {
			if r.Type != ruleTransformation || r.Target != targetColumn || !m.columnRuleApplies(r, t, c.Name) {
				continue
			}

			if r.Action == actRemoveColumn {
				removed = true

				break
			}

			name = applyNameAction(r.Action, name, r.Value, r.Old)
		}

		if removed {
			continue
		}

		rename[c.Name] = name
		c.Name = name
		cols = append(cols, TargetColumn{Column: c, Source: i})
	}

	pk := make([]string, 0, len(t.PK))

	for _, p := range t.PK {
		if n, ok := rename[p]; ok {
			pk = append(pk, n)
		}
	}

	return cols, pk
}

func (m Mappings) columnRuleApplies(r Rule, t Table, col string) bool {
	return MatchPattern(r.Locator.Schema, t.Schema) && MatchPattern(r.Locator.Table, t.Name) &&
		MatchPattern(r.Locator.Column, col)
}
