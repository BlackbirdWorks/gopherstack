package connect

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	cmpExact      = "EXACT"
	cmpStartsWith = "STARTS_WITH"
	cmpContains   = "CONTAINS"
	matchChildren = "WITH_CHILD_GROUPS"
)

func stringComparisons() []string { return []string{cmpExact, cmpStartsWith, cmpContains} }

type stringCond struct {
	FieldName      string `json:"FieldName"`
	Value          string `json:"Value"`
	ComparisonType string `json:"ComparisonType"`
}

type tagCond struct {
	TagKey   string `json:"TagKey"`
	TagValue string `json:"TagValue"`
}

type hierarchyCond struct {
	HierarchyGroupMatchType string `json:"HierarchyGroupMatchType"`
	Value                   string `json:"Value"`
}

type numberCond struct {
	MinValue       *int32 `json:"MinValue"`
	MaxValue       *int32 `json:"MaxValue"`
	ComparisonType string `json:"ComparisonType"`
	FieldName      string `json:"FieldName"`
}

type dateCond struct {
	ComparisonType string `json:"ComparisonType"`
	FieldName      string `json:"FieldName"`
	Value          string `json:"Value"`
}

type listCondition struct {
	String *stringCond `json:"StringCondition"`
	Number *numberCond `json:"NumberCondition"`
}

type listCond struct {
	TargetListType string          `json:"TargetListType"`
	Conditions     []listCondition `json:"Conditions"`
}

// searchCriteria is the union of every SearchCriteria shape; each member is optional and members combine with AND.
type searchCriteria struct {
	String     *stringCond      `json:"StringCondition"`
	Hierarchy  *hierarchyCond   `json:"HierarchyGroupCondition"`
	List       *listCond        `json:"ListCondition"`
	Date       *dateCond        `json:"DateCondition"`
	QueueType  string           `json:"QueueTypeCondition"`
	State      string           `json:"StateCondition"`
	Status     string           `json:"StatusCondition"`
	Type       string           `json:"TypeCondition"`
	ViewStatus string           `json:"ViewStatusCondition"`
	ViewType   string           `json:"ViewTypeCondition"`
	And        []searchCriteria `json:"AndConditions"`
	Or         []searchCriteria `json:"OrConditions"`
}

type flowTypeCond struct {
	ContactFlowType string `json:"ContactFlowType"`
}

type tagFilter struct {
	Tag *tagCond    `json:"TagCondition"`
	And []tagCond   `json:"AndConditions"`
	Or  [][]tagCond `json:"OrConditions"`
}

type attrAnd struct {
	Hierarchy *hierarchyCond `json:"HierarchyGroupCondition"`
	FlowType  *flowTypeCond  `json:"ContactFlowTypeCondition"`
	Tags      []tagCond      `json:"TagConditions"`
}

type attrFilter struct {
	And       *attrAnd       `json:"AndCondition"`
	Tag       *tagCond       `json:"TagCondition"`
	Hierarchy *hierarchyCond `json:"HierarchyGroupCondition"`
	FlowType  *flowTypeCond  `json:"ContactFlowTypeCondition"`
	Or        []attrAnd      `json:"OrConditions"`
}

// searchFilter is the union of every SearchFilter shape.
type searchFilter struct {
	Tag      *tagFilter  `json:"TagFilter"`
	Attr     *attrFilter `json:"AttributeFilter"`
	UserAttr *attrFilter `json:"UserAttributeFilter"`
	FlowAttr *attrFilter `json:"FlowAttributeFilter"`
}

type proficiency struct {
	name, value string
	level       float64
}

// searchSubject is one resource as the search conditions see it.
type searchSubject struct {
	fields    map[string]string
	conds     map[string]string
	tags      map[string]string
	dates     map[string]string
	flowType  string
	hierarchy []string
	profs     []proficiency
}

type searchSpec struct {
	fields []string
}

func parseSearch(rawCriteria, rawFilter json.RawMessage, spec searchSpec) (*searchCriteria, *searchFilter, error) {
	var c searchCriteria

	var f searchFilter

	for raw, dst := range map[*json.RawMessage]any{&rawCriteria: &c, &rawFilter: &f} {
		if len(*raw) == 0 || string(*raw) == jsonNull {
			continue
		}

		if err := json.Unmarshal(*raw, dst); err != nil {
			return nil, nil, invalidParamErr("SearchCriteria or SearchFilter is not valid")
		}
	}

	if err := c.validate(spec); err != nil {
		return nil, nil, err
	}

	return &c, &f, nil
}

func (c *searchCriteria) validate(spec searchSpec) error {
	if sc := c.String; sc != nil {
		if err := oneOf("StringCondition.ComparisonType", sc.ComparisonType, stringComparisons()...); err != nil {
			return err
		}

		if !slices.Contains(spec.fields, strs.Fold(sc.FieldName)) {
			return invalidParamErr("StringCondition.FieldName %q is not searchable", sc.FieldName)
		}
	}

	if h := c.Hierarchy; h != nil {
		if err := oneOf("HierarchyGroupMatchType", h.HierarchyGroupMatchType, cmpExact, matchChildren); err != nil {
			return err
		}
	}

	if c.List != nil {
		if err := oneOf("TargetListType", c.List.TargetListType, "PROFICIENCIES"); err != nil {
			return err
		}
	}

	for _, sub := range slices.Concat(c.And, c.Or) {
		if err := sub.validate(spec); err != nil {
			return err
		}
	}

	return nil
}

func compareString(c *stringCond, have string) bool {
	have, want := strs.Fold(have), strs.Fold(c.Value)

	switch c.ComparisonType {
	case cmpStartsWith:
		return strings.HasPrefix(have, want)
	case cmpContains:
		return strings.Contains(have, want)
	default:
		return have == want
	}
}

func compareNumber(c *numberCond, have float64) bool {
	lo, hi := 0.0, 0.0
	if c.MinValue != nil {
		lo = float64(*c.MinValue)
	}

	if c.MaxValue != nil {
		hi = float64(*c.MaxValue)
	}

	switch c.ComparisonType {
	case "GREATER_OR_EQUAL":
		return have >= lo
	case "GREATER":
		return have > lo
	case "LESSER_OR_EQUAL":
		return have <= hi
	case "LESSER":
		return have < hi
	case "NOT_EQUAL":
		return have != lo
	case "RANGE":
		return have >= lo && have <= hi
	default:
		return have == lo
	}
}

func compareDate(c *dateCond, have string) bool {
	if have == "" {
		return false
	}

	switch c.ComparisonType {
	case "GREATER_THAN":
		return have > c.Value
	case "LESS_THAN":
		return have < c.Value
	case "GREATER_THAN_OR_EQUAL_TO":
		return have >= c.Value
	case "LESS_THAN_OR_EQUAL_TO":
		return have <= c.Value
	default:
		return have == c.Value
	}
}

func matchHierarchy(c *hierarchyCond, path []string) bool {
	if c.HierarchyGroupMatchType == matchChildren {
		return slices.Contains(path, c.Value)
	}

	return len(path) > 0 && path[0] == c.Value
}

func (l *listCond) matches(profs []proficiency) bool {
	return slices.ContainsFunc(profs, func(p proficiency) bool {
		for _, c := range l.Conditions {
			if c.String != nil && !compareString(c.String, p.field(c.String.FieldName)) {
				return false
			}

			if c.Number != nil && !compareNumber(c.Number, p.level) {
				return false
			}
		}

		return true
	})
}

func (p proficiency) field(name string) string {
	if strings.EqualFold(name, "ProficiencyValue") {
		return p.value
	}

	return p.name
}

func (c *searchCriteria) matches(s *searchSubject) bool {
	for kind, want := range map[string]string{
		"queuetype": c.QueueType, fieldState: c.State, fieldStatus: c.Status, fieldType: c.Type,
		"viewstatus": c.ViewStatus, "viewtype": c.ViewType,
	} {
		if want != "" && s.conds[kind] != want {
			return false
		}
	}

	switch {
	case c.String != nil && !compareString(c.String, s.fields[strs.Fold(c.String.FieldName)]):
		return false
	case c.Hierarchy != nil && !matchHierarchy(c.Hierarchy, s.hierarchy):
		return false
	case c.List != nil && !c.List.matches(s.profs):
		return false
	case c.Date != nil && !compareDate(c.Date, s.dates[strs.Fold(c.Date.FieldName)]):
		return false
	}

	return c.matchesNested(s)
}

func (c *searchCriteria) matchesNested(s *searchSubject) bool {
	for i := range c.And {
		if !c.And[i].matches(s) {
			return false
		}
	}

	if len(c.Or) == 0 {
		return true
	}

	for i := range c.Or {
		if c.Or[i].matches(s) {
			return true
		}
	}

	return false
}

func (t tagCond) matches(tags map[string]string) bool {
	v, ok := tags[t.TagKey]

	return ok && (t.TagValue == "" || v == t.TagValue)
}

func allTags(conds []tagCond, tags map[string]string) bool {
	return !slices.ContainsFunc(conds, func(t tagCond) bool { return !t.matches(tags) })
}

func (f *tagFilter) matches(tags map[string]string) bool {
	if f.Tag != nil && !f.Tag.matches(tags) {
		return false
	}

	if !allTags(f.And, tags) {
		return false
	}

	return len(f.Or) == 0 || slices.ContainsFunc(f.Or, func(group []tagCond) bool { return allTags(group, tags) })
}

func (a *attrAnd) matches(s *searchSubject) bool {
	return allTags(a.Tags, s.tags) &&
		(a.Hierarchy == nil || matchHierarchy(a.Hierarchy, s.hierarchy)) &&
		(a.FlowType == nil || a.FlowType.ContactFlowType == s.flowType)
}

func (f *attrFilter) matches(s *searchSubject) bool {
	switch {
	case f.Tag != nil && !f.Tag.matches(s.tags):
		return false
	case f.Hierarchy != nil && !matchHierarchy(f.Hierarchy, s.hierarchy):
		return false
	case f.FlowType != nil && f.FlowType.ContactFlowType != s.flowType:
		return false
	case f.And != nil && !f.And.matches(s):
		return false
	}

	return len(f.Or) == 0 || slices.ContainsFunc(f.Or, func(a attrAnd) bool { return a.matches(s) })
}

func (f *searchFilter) matches(s *searchSubject) bool {
	if f.Tag != nil && !f.Tag.matches(s.tags) {
		return false
	}

	for _, a := range []*attrFilter{f.Attr, f.UserAttr, f.FlowAttr} {
		if a != nil && !a.matches(s) {
			return false
		}
	}

	return true
}
