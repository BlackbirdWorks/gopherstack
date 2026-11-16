package connect

import (
	"cmp"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	cmpGreaterOrEqual = "GREATER_OR_EQUAL"
	cmpGreater        = "GREATER"
	cmpLesserOrEqual  = "LESSER_OR_EQUAL"
	cmpLesser         = "LESSER"
	cmpNotEqual       = "NOT_EQUAL"
	cmpRange          = "RANGE"
	maxEvalSearchPage = 100
	attrContactAgent  = "contactagentid"
	cmpIsTrue         = "IS_TRUE"
	cmpIsFalse        = "IS_FALSE"
)

func evalStringFields() []string {
	return []string{
		"evaluationid", "evaluationarn", "evaluationformid", "evaluationformtitle", "evaluationtype", "status",
		"contactid", "evaluatorarn", attrContactAgent,
	}
}

func evalNumberFields() []string {
	return []string{"evaluationformversion", "earnedpoints", "maxbasepoint"}
}
func evalDecimalField() []string { return []string{"scorepercentage"} }

func evalBoolFields() []string {
	return []string{"autoevaluationenabled", "scoreautomaticfail", "scorenotapplicable"}
}

func evalTimeFields() []string { return []string{"createdtime", "lastmodifiedtime"} }

type evalRangeCond struct {
	MinValue       *float64 `json:"MinValue"`
	MaxValue       *float64 `json:"MaxValue"`
	ComparisonType string   `json:"ComparisonType"`
	FieldName      string   `json:"FieldName"`
}

type evalTimeCond struct {
	MinValue       string `json:"MinValue"`
	MaxValue       string `json:"MaxValue"`
	ComparisonType string `json:"ComparisonType"`
	FieldName      string `json:"FieldName"`
}

type evalBoolCond struct {
	ComparisonType string `json:"ComparisonType"`
	FieldName      string `json:"FieldName"`
}

type evalCriteria struct {
	String   *stringCond    `json:"StringCondition"`
	Number   *evalRangeCond `json:"NumberCondition"`
	Decimal  *evalRangeCond `json:"DecimalCondition"`
	Bool     *evalBoolCond  `json:"BooleanCondition"`
	DateTime *evalTimeCond  `json:"DateTimeCondition"`
	And      []evalCriteria `json:"AndConditions"`
	Or       []evalCriteria `json:"OrConditions"`
}

type evalAttrCond struct {
	AttributeValue *struct {
		StringValue string `json:"StringValue"`
	} `json:"AttributeValue"`
	AttributeKey   string `json:"AttributeKey"`
	ComparisonType string `json:"ComparisonType"`
}

type evalAttrAnd struct {
	Attributes []evalAttrCond `json:"AttributeConditions"`
	Tags       []tagCond      `json:"TagConditions"`
}

type evalAttrFilter struct {
	And       *evalAttrAnd  `json:"AndCondition"`
	Condition *evalAttrCond `json:"ContactEvaluationAttributeCondition"`
	Tag       *tagCond      `json:"TagCondition"`
	Or        []evalAttrAnd `json:"OrConditions"`
}

type evalSearchFilter struct {
	Attr     *attrFilter     `json:"AttributeFilter"`
	EvalAttr *evalAttrFilter `json:"ContactEvaluationAttributeFilter"`
}

type evalSubject struct {
	strs  map[string]string
	nums  map[string]float64
	bools map[string]bool
	times map[string]time.Time
	tags  map[string]string
}

func (c *evalCriteria) validate() error {
	if s := c.String; s != nil {
		if err := oneOf("StringCondition.ComparisonType", s.ComparisonType, stringComparisons()...); err != nil {
			return err
		}

		if err := knownField("StringCondition", s.FieldName, evalStringFields()); err != nil {
			return err
		}
	}

	if err := c.validateRanges(); err != nil {
		return err
	}

	for _, sub := range slices.Concat(c.And, c.Or) {
		if err := sub.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (c *evalCriteria) validateRanges() error {
	if n := c.Number; n != nil {
		if err := n.validate("NumberCondition", evalNumberFields()); err != nil {
			return err
		}
	}

	if d := c.Decimal; d != nil {
		if err := d.validate("DecimalCondition", evalDecimalField()); err != nil {
			return err
		}
	}

	if bc := c.Bool; bc != nil {
		if err := oneOf("BooleanCondition.ComparisonType", bc.ComparisonType, cmpIsTrue, cmpIsFalse); err != nil {
			return err
		}

		if err := knownField("BooleanCondition", bc.FieldName, evalBoolFields()); err != nil {
			return err
		}
	}

	if t := c.DateTime; t != nil {
		return t.validate()
	}

	return nil
}

func knownField(kind, name string, fields []string) error {
	if !slices.Contains(fields, strs.Fold(name)) {
		return invalidParamErr("%s.FieldName %q is not searchable", kind, name)
	}

	return nil
}

func rangeComparisons() []string {
	return []string{cmpGreaterOrEqual, cmpGreater, cmpLesserOrEqual, cmpLesser, "EQUAL", cmpNotEqual, cmpRange}
}

func (r *evalRangeCond) validate(kind string, fields []string) error {
	if err := oneOf(kind+".ComparisonType", r.ComparisonType, rangeComparisons()...); err != nil {
		return err
	}

	if err := knownField(kind, r.FieldName, fields); err != nil {
		return err
	}

	return requireBounds(kind, r.ComparisonType, r.MinValue != nil, r.MaxValue != nil)
}

// requireBounds follows the documented example: EQUAL and the GREATER types read MinValue, the LESSER types MaxValue.
func requireBounds(kind, cmpName string, hasMin, hasMax bool) error {
	needMin := cmpName != cmpLesser && cmpName != cmpLesserOrEqual
	needMax := cmpName == cmpLesser || cmpName == cmpLesserOrEqual || cmpName == cmpRange

	if (needMin && !hasMin) || (needMax && !hasMax) {
		return invalidParamErr("%s needs the bound values its ComparisonType reads", kind)
	}

	return nil
}

func timeComparison(name string) (string, bool) {
	cmp, ok := map[string]string{
		"GREATER_THAN": cmpGreater, "LESS_THAN": cmpLesser, "GREATER_THAN_OR_EQUAL_TO": cmpGreaterOrEqual,
		"LESS_THAN_OR_EQUAL_TO": cmpLesserOrEqual, "EQUAL_TO": "EQUAL", "RANGE": cmpRange,
	}[name]

	return cmp, ok
}

func (t *evalTimeCond) validate() error {
	cmpName, ok := timeComparison(t.ComparisonType)
	if !ok {
		return invalidParamErr("DateTimeCondition.ComparisonType %q is not valid", t.ComparisonType)
	}

	if err := knownField("DateTimeCondition", t.FieldName, evalTimeFields()); err != nil {
		return err
	}

	if err := requireBounds("DateTimeCondition", cmpName, t.MinValue != "", t.MaxValue != ""); err != nil {
		return err
	}

	for _, v := range []string{t.MinValue, t.MaxValue} {
		if v == "" {
			continue
		}

		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return invalidParamErr("DateTimeCondition value %q is not an RFC 3339 timestamp", v)
		}
	}

	return nil
}

func (a *evalAttrCond) validate() error {
	if strs.Fold(a.AttributeKey) != attrContactAgent {
		return invalidParamErr("AttributeKey must be ContactAgentId")
	}

	if a.ComparisonType != cmpExact {
		return invalidParamErr("ComparisonType must be EXACT")
	}

	if a.AttributeValue == nil {
		return invalidParamErr("AttributeValue is required")
	}

	return nil
}

func (f *evalAttrFilter) validate() error {
	conds := []evalAttrCond{}
	if f.Condition != nil {
		conds = append(conds, *f.Condition)
	}

	if f.And != nil {
		conds = append(conds, f.And.Attributes...)
	}

	for _, o := range f.Or {
		conds = append(conds, o.Attributes...)
	}

	for i := range conds {
		if err := conds[i].validate(); err != nil {
			return err
		}
	}

	return nil
}

func compareRange(cmpName string, have float64, lo, hi *float64) bool {
	switch cmpName {
	case cmpLesser:
		return hi != nil && have < *hi
	case cmpLesserOrEqual:
		return hi != nil && have <= *hi
	case cmpRange:
		return lo != nil && hi != nil && have >= *lo && have <= *hi
	}

	return lo != nil && compareFromLower(cmpName, have, *lo)
}

func compareFromLower(cmpName string, have, lo float64) bool {
	switch cmpName {
	case cmpGreaterOrEqual:
		return have >= lo
	case cmpGreater:
		return have > lo
	case cmpNotEqual:
		return have != lo
	default:
		return have == lo
	}
}

func (r *evalRangeCond) matches(have map[string]float64) bool {
	v, ok := have[strs.Fold(r.FieldName)]

	return ok && compareRange(r.ComparisonType, v, r.MinValue, r.MaxValue)
}

func (t *evalTimeCond) matches(have map[string]time.Time) bool {
	v, ok := have[strs.Fold(t.FieldName)]
	if !ok {
		return false
	}

	bound := func(raw string) *float64 {
		if raw == "" {
			return nil
		}

		p, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil
		}

		f := float64(p.UnixMilli())

		return &f
	}

	cmpName, _ := timeComparison(t.ComparisonType)

	return compareRange(cmpName, float64(v.UnixMilli()), bound(t.MinValue), bound(t.MaxValue))
}

func (c *evalCriteria) matches(s *evalSubject) bool {
	switch {
	case c.String != nil && !compareString(c.String, s.strs[strs.Fold(c.String.FieldName)]):
		return false
	case c.Number != nil && !c.Number.matches(s.nums):
		return false
	case c.Decimal != nil && !c.Decimal.matches(s.nums):
		return false
	case c.Bool != nil && s.bools[strs.Fold(c.Bool.FieldName)] != (c.Bool.ComparisonType == cmpIsTrue):
		return false
	case c.DateTime != nil && !c.DateTime.matches(s.times):
		return false
	}

	for i := range c.And {
		if !c.And[i].matches(s) {
			return false
		}
	}

	return len(c.Or) == 0 || slices.ContainsFunc(c.Or, func(o evalCriteria) bool { return o.matches(s) })
}

func (a *evalAttrCond) matches(s *evalSubject) bool {
	return a.AttributeValue != nil && s.strs[attrContactAgent] == a.AttributeValue.StringValue
}

func (a *evalAttrAnd) matches(s *evalSubject) bool {
	for i := range a.Attributes {
		if !a.Attributes[i].matches(s) {
			return false
		}
	}

	return allTags(a.Tags, s.tags)
}

func (f *evalAttrFilter) matches(s *evalSubject) bool {
	switch {
	case f.Tag != nil && !f.Tag.matches(s.tags):
		return false
	case f.Condition != nil && !f.Condition.matches(s):
		return false
	case f.And != nil && !f.And.matches(s):
		return false
	}

	return len(f.Or) == 0 || slices.ContainsFunc(f.Or, func(a evalAttrAnd) bool { return a.matches(s) })
}

func (f *evalSearchFilter) matches(s *evalSubject) bool {
	if f.Attr != nil && !f.Attr.matches(&searchSubject{tags: s.tags}) {
		return false
	}

	return f.EvalAttr == nil || f.EvalAttr.matches(s)
}

func parseEvalSearch(rawCriteria, rawFilter jsontext.Value) (*evalCriteria, *evalSearchFilter, error) {
	var c evalCriteria

	var f evalSearchFilter

	for raw, dst := range map[*jsontext.Value]any{&rawCriteria: &c, &rawFilter: &f} {
		if len(*raw) == 0 || string(*raw) == jsonNull {
			continue
		}

		if err := json.Unmarshal(*raw, dst, jsonx.V1()); err != nil {
			return nil, nil, invalidParamErr("SearchCriteria or SearchFilter is not valid")
		}
	}

	if err := c.validate(); err != nil {
		return nil, nil, err
	}

	if f.EvalAttr != nil {
		if err := f.EvalAttr.validate(); err != nil {
			return nil, nil, err
		}
	}

	return &c, &f, nil
}

// EvaluationSearchMetadata is the metadata of a search hit.
type EvaluationSearchMetadata struct {
	EarnedPoints          *int32  `json:"EarnedPoints,omitempty"`
	MaxBasePoint          *int32  `json:"MaxBasePoint,omitempty"`
	ContactID             string  `json:"ContactId"`
	EvaluatorArn          string  `json:"EvaluatorArn,omitempty"`
	ContactAgentID        string  `json:"ContactAgentId,omitempty"`
	ScorePercentage       float64 `json:"ScorePercentage,omitempty"`
	AutoEvaluationEnabled bool    `json:"AutoEvaluationEnabled,omitempty"`
	ScoreAutomaticFail    bool    `json:"ScoreAutomaticFail,omitempty"`
	ScoreNotApplicable    bool    `json:"ScoreNotApplicable,omitempty"`
}

// EvaluationSearchSummary is one SearchContactEvaluations hit.
type EvaluationSearchSummary struct {
	Tags                  map[string]string        `json:"Tags,omitempty"`
	CreatedTime           awstime.EpochTime        `json:"CreatedTime"`
	LastModifiedTime      awstime.EpochTime        `json:"LastModifiedTime"`
	EvaluationID          string                   `json:"EvaluationId"`
	EvaluationArn         string                   `json:"EvaluationArn"`
	EvaluationFormID      string                   `json:"EvaluationFormId"`
	EvaluationFormTitle   string                   `json:"EvaluationFormTitle"`
	EvaluationType        string                   `json:"EvaluationType"`
	Status                string                   `json:"Status"`
	Metadata              EvaluationSearchMetadata `json:"Metadata"`
	EvaluationFormVersion int32                    `json:"EvaluationFormVersion"`
}

func (b *Backend) evalSearchEntry(inst string, r *evaluationRec) (evalSubject, EvaluationSearchSummary) {
	title := ""
	if v, err := b.evalFormVersion(inst, r); err == nil {
		title = v.Content.Title
	}

	meta := EvaluationSearchMetadata{
		ContactID: r.ContactID, EvaluatorArn: r.EvaluatorArn, AutoEvaluationEnabled: r.AutoEvaluation,
	}
	if c, ok := b.contacts.table.Get(inst + "/" + r.ContactID); ok {
		meta.ContactAgentID = c.agentID()
	}

	subj := evalSubject{
		strs: map[string]string{
			"evaluationid": r.ID, "evaluationarn": r.Arn, "evaluationformid": r.FormID,
			"evaluationformtitle": title, "evaluationtype": evalTypeStandard, "status": r.Status,
			"contactid": r.ContactID, "evaluatorarn": r.EvaluatorArn, attrContactAgent: meta.ContactAgentID,
		},
		nums:  map[string]float64{"evaluationformversion": float64(r.FormVersion)},
		bools: map[string]bool{"autoevaluationenabled": r.AutoEvaluation},
		times: map[string]time.Time{"createdtime": r.CreatedTime.Time, "lastmodifiedtime": r.LastModified.Time},
		tags:  r.Tags,
	}

	if sc := r.Score; sc != nil {
		meta.EarnedPoints, meta.MaxBasePoint = &sc.EarnedPoints, &sc.MaxBasePoint
		meta.ScorePercentage = sc.Percentage
		meta.ScoreAutomaticFail, meta.ScoreNotApplicable = sc.AutomaticFail, sc.NotApplicable
		subj.nums["earnedpoints"], subj.nums["maxbasepoint"] = float64(sc.EarnedPoints), float64(sc.MaxBasePoint)
		subj.nums["scorepercentage"] = sc.Percentage
		subj.bools["scoreautomaticfail"], subj.bools["scorenotapplicable"] = sc.AutomaticFail, sc.NotApplicable
	}

	return subj, EvaluationSearchSummary{
		Metadata: meta, Tags: cloneTags(r.Tags), CreatedTime: r.CreatedTime, LastModifiedTime: r.LastModified,
		EvaluationID: r.ID, EvaluationArn: r.Arn, EvaluationFormID: r.FormID, EvaluationFormTitle: title,
		EvaluationType: evalTypeStandard, Status: r.Status, EvaluationFormVersion: r.FormVersion,
	}
}

// SearchContactEvaluations searches the contact evaluations of an instance, newest first.
func (b *Backend) SearchContactEvaluations(in *SearchRequest) (*SearchResult, error) {
	if in.MaxResults > maxEvalSearchPage {
		return nil, invalidParamErr("MaxResults must be 1-%d", maxEvalSearchPage)
	}

	crit, filt, err := parseEvalSearch(in.SearchCriteria, in.SearchFilter)
	if err != nil {
		return nil, err
	}

	all, err := readInstance(b, "SearchContactEvaluations", in.InstanceID,
		func(inst string) ([]EvaluationSearchSummary, error) {
			recs := b.evaluations.in(inst)
			slices.SortStableFunc(recs, func(a, c *evaluationRec) int {
				return cmp.Or(c.CreatedTime.Time.Compare(a.CreatedTime.Time), strings.Compare(c.ID, a.ID))
			})

			var res []EvaluationSearchSummary

			for _, r := range recs {
				if subj, sum := b.evalSearchEntry(inst, r); crit.matches(&subj) && filt.matches(&subj) {
					res = append(res, sum)
				}
			}

			return res, nil
		})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	body := SearchResult{"EvaluationSearchSummaryList": orEmpty(page), keyApproxTotal: int64(len(all))}
	if next != "" {
		body["NextToken"] = next
	}

	return &body, nil
}
