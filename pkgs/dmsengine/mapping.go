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
	actIncludeColumn   = "include-column"
	actAddColumn       = "add-column"
	actChangeType      = "change-data-type"
	actBeforeImage     = "add-before-image-columns"
	actDefinePK        = "define-primary-key"
	targetSchema       = "schema"
	targetTable        = "table"
	targetColumn       = "column"
	pkOriginUnique     = "unique-index"
	pkOriginPrimary    = "primary-key"
	imageFilterPK      = "pk-only"
	imageFilterNonLOB  = "non-lob"
	imageFilterAll     = "all"
	defaultImagePrefix = "BI_"
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

type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	var s flexString
	if err := s.UnmarshalJSON(b); err != nil {
		return err
	}

	if strings.TrimSpace(string(s)) == "" {
		*f = 0

		return nil
	}

	n, err := strconv.Atoi(strings.TrimSpace(string(s)))
	if err != nil {
		return fmt.Errorf("%w: %q is not an integer", ErrMapping, string(s))
	}

	*f = flexInt(n)

	return nil
}

type ruleDataType struct {
	Type      string  `json:"type"`
	Length    flexInt `json:"length"`
	Precision flexInt `json:"precision"`
	Scale     flexInt `json:"scale"`
}

func (d ruleDataType) parse() (DMSType, error) {
	t, err := ParseDMSType(d.Type)
	if err != nil {
		return t, err
	}

	t.Length, t.Precision, t.Scale = int(d.Length), int(d.Precision), int(d.Scale)

	return t, nil
}

type primaryKeyDef struct {
	Name    string   `json:"name"`
	Origin  string   `json:"origin"`
	Columns []string `json:"columns"`
}

type beforeImageDef struct {
	Prefix *string `json:"column-prefix"`
	Suffix string  `json:"column-suffix"`
	Filter string  `json:"column-filter"`
}

// Rule is one entry of a table-mappings document.
type Rule struct {
	Locator struct {
		Schema   string `json:"schema-name"`
		Table    string `json:"table-name"`
		Column   string `json:"column-name"`
		DataType string `json:"data-type"`
	} `json:"object-locator"`
	DataType    *ruleDataType   `json:"data-type"`
	PrimaryKey  *primaryKeyDef  `json:"primary-key-def"`
	BeforeImage *beforeImageDef `json:"before-image-def"`
	ID          flexString      `json:"rule-id"`
	Name        flexString      `json:"rule-name"`
	LoadOrder   flexString      `json:"load-order"`
	Type        string          `json:"rule-type"`
	Action      string          `json:"rule-action"`
	Target      string          `json:"rule-target"`
	Value       string          `json:"value"`
	Old         string          `json:"old-value"`
	Expression  string          `json:"expression"`
	Filters     []RuleFilter    `json:"filters"`
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

func hasWildcard(s string) bool { return strings.ContainsAny(s, "%_") }

func validateRule(r Rule) error {
	switch r.Type {
	case ruleSelection:
		return validateSelection(r)
	case ruleTransformation:
		return validateTransformation(r)
	default:
		return fmt.Errorf("%w: unsupported rule-type %q", ErrMapping, r.Type)
	}
}

func validateSelection(r Rule) error {
	switch r.Action {
	case actInclude, actExclude, actExplicit:
	default:
		return fmt.Errorf("%w: unsupported selection rule-action %q", ErrMapping, r.Action)
	}

	if r.Action != actInclude {
		return nil
	}

	if len(r.Filters) > 0 && (hasWildcard(r.Locator.Schema) || hasWildcard(r.Locator.Table)) {
		return fmt.Errorf("%w: filters need an exact schema-name and table-name", ErrMapping)
	}

	for _, f := range r.Filters {
		if err := validateFilter(f); err != nil {
			return err
		}
	}

	return nil
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
	case actRemoveColumn, actIncludeColumn:
		if r.Target == targetColumn {
			return nil
		}
	case actAddColumn, actChangeType, actBeforeImage:
		return validateColumnRule(r)
	case actDefinePK:
		return validatePrimaryKeyRule(r)
	}

	return fmt.Errorf("%w: unsupported transformation rule-action %q for %s", ErrMapping, r.Action, r.Target)
}

func validateColumnRule(r Rule) error {
	if r.Target != targetColumn {
		return fmt.Errorf("%w: %s needs rule-target column", ErrMapping, r.Action)
	}

	switch r.Action {
	case actAddColumn, actChangeType:
		if r.DataType == nil {
			return fmt.Errorf("%w: %s needs a data-type", ErrMapping, r.Action)
		}

		if _, err := r.DataType.parse(); err != nil {
			return err
		}

		if r.Action == actAddColumn && r.Value == "" {
			return fmt.Errorf("%w: add-column needs a value naming the new column", ErrMapping)
		}
	default:
		return validateBeforeImage(r)
	}

	return nil
}

func validateBeforeImage(r Rule) error {
	def := r.imageDef()

	switch def.Filter {
	case imageFilterPK, imageFilterNonLOB, imageFilterAll:
	default:
		return fmt.Errorf("%w: unsupported column-filter %q", ErrMapping, def.Filter)
	}

	if *def.Prefix == "" && def.Suffix == "" {
		return fmt.Errorf("%w: before-image-def needs a column-prefix or column-suffix", ErrMapping)
	}

	return nil
}

func (r Rule) imageDef() beforeImageDef {
	def := beforeImageDef{Filter: imageFilterPK}
	prefix := defaultImagePrefix
	def.Prefix = &prefix

	if r.BeforeImage != nil {
		def.Suffix = r.BeforeImage.Suffix

		if r.BeforeImage.Prefix != nil {
			def.Prefix = r.BeforeImage.Prefix
		}

		if r.BeforeImage.Filter != "" {
			def.Filter = r.BeforeImage.Filter
		}
	}

	return def
}

func validatePrimaryKeyRule(r Rule) error {
	if r.Target != targetTable {
		return fmt.Errorf("%w: define-primary-key needs rule-target table", ErrMapping)
	}

	if hasWildcard(r.Locator.Schema) || hasWildcard(r.Locator.Table) {
		return fmt.Errorf("%w: define-primary-key takes no wildcards in schema-name or table-name", ErrMapping)
	}

	if r.PrimaryKey == nil || len(r.PrimaryKey.Columns) == 0 {
		return fmt.Errorf("%w: define-primary-key needs primary-key-def.columns", ErrMapping)
	}

	switch r.PrimaryKey.Origin {
	case "", pkOriginPrimary, pkOriginUnique:
		return nil
	}

	return fmt.Errorf("%w: unsupported primary-key-def origin %q", ErrMapping, r.PrimaryKey.Origin)
}

// MatchPattern matches s against a DMS locator pattern where % matches any run of characters and _ any one
// character (DMS User Guide, "Wildcards in table mapping").
func MatchPattern(pattern, s string) bool {
	if pattern == "" {
		pattern = "%"
	}

	if !hasWildcard(pattern) {
		return pattern == s
	}

	var sb strings.Builder

	sb.WriteString("^")

	for _, r := range pattern {
		switch r {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	sb.WriteString("$")

	return regexp.MustCompile("(?s)" + sb.String()).MatchString(s)
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

// SelectedTable is a source table chosen by the selection rules with its source filter.
type SelectedTable struct {
	Filter *TableFilter
	Table  Table
}

func (m Mappings) chosen(tables []Table) map[string]*Rule {
	chosen := make(map[string]*Rule, len(tables))

	for i := range m.Rules {
		r := &m.Rules[i]
		if r.Type != ruleSelection {
			continue
		}

		for _, t := range tables {
			if !r.selects(t) {
				continue
			}

			if r.Action == actExclude {
				delete(chosen, t.Key())
			} else {
				chosen[t.Key()] = r
			}
		}
	}

	return chosen
}

func (r Rule) selects(t Table) bool {
	return MatchPattern(r.Locator.Schema, t.Schema) && MatchPattern(r.Locator.Table, t.Name)
}

// Select returns the source tables chosen by the selection rules, in source order.
func (m Mappings) Select(tables []Table) []Table {
	chosen := m.chosen(tables)
	out := make([]Table, 0, len(chosen))

	for _, t := range tables {
		if _, ok := chosen[t.Key()]; ok {
			out = append(out, t)
		}
	}

	return out
}

// SelectFiltered is Select plus each table's compiled source filter; tables with a higher load-order come first.
func (m Mappings) SelectFiltered(tables []Table) ([]SelectedTable, error) {
	chosen := m.chosen(tables)
	out := make([]SelectedTable, 0, len(chosen))

	for _, t := range tables {
		r, ok := chosen[t.Key()]
		if !ok {
			continue
		}

		var fs []RuleFilter
		if r.Action == actInclude {
			fs = r.Filters
		}

		f, err := newTableFilter(t, fs)
		if err != nil {
			return nil, err
		}

		out = append(out, SelectedTable{Table: t, Filter: f})
	}

	order := func(st SelectedTable) int {
		n, _ := strconv.Atoi(string(chosen[st.Table.Key()].LoadOrder))

		return n
	}
	sort.SliceStable(out, func(i, j int) bool { return order(out[i]) > order(out[j]) })

	return out, nil
}

// Plan maps a selected source table to its target shape. overrideSchema (TargetMetadata.TargetSchema) wins
// over schema renames when set.
func (m Mappings) Plan(t Table, src Family, overrideSchema string) (TargetTable, error) {
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

	if err := m.planColumns(t, &out); err != nil {
		return out, err
	}

	return out, m.definePrimaryKey(t, &out)
}
