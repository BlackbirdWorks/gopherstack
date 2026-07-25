package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
)

func (h *Handler) handleCreateRule(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	if vals.Get("Priority") == "" {
		return nil, fmt.Errorf("%w: Priority is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")
	if len(actions) == 0 {
		return nil, fmt.Errorf("%w: Actions must contain at least one action", ErrInvalidParameter)
	}

	if actErr := validateActionTypes(actions); actErr != nil {
		return nil, actErr
	}

	conditions, err := parseConditions(vals, "Conditions.member")
	if err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateRule(CreateRuleInput{
		ListenerArn: listenerArn,
		Priority:    vals.Get("Priority"),
		Actions:     actions,
		Conditions:  conditions,
		Tags:        parseTagKVs(vals),
	})
	if err != nil {
		return nil, err
	}

	return &createRuleResponse{
		Xmlns: elbv2XMLNS,
		Result: createRuleResult{
			Rules: xmlRuleList{
				Members: []xmlRule{toXMLRule(rule)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-rule"},
	}, nil
}

func (h *Handler) handleDeleteRule(vals url.Values) (any, error) {
	ruleArn := vals.Get("RuleArn")
	if ruleArn == "" {
		return nil, fmt.Errorf("%w: RuleArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRule(ruleArn); err != nil {
		return nil, err
	}

	return &deleteRuleResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-rule"},
	}, nil
}

func (h *Handler) handleDescribeRules(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	ruleArns := parseMembers(vals, "RuleArns.member")

	rules, err := h.Backend.DescribeRules(listenerArn, ruleArns)
	if err != nil {
		return nil, err
	}

	marker, pageSize := parsePagination(vals)
	rules, nextMarker := applyRulePage(rules, marker, pageSize)

	members := make([]xmlRule, 0, len(rules))
	for i := range rules {
		members = append(members, toXMLRule(&rules[i]))
	}

	return &describeRulesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeRulesResult{
			NextMarker: nextMarker,
			Rules:      xmlRuleList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-rules"},
	}, nil
}

// applyRulePage applies marker-based pagination to a rule slice.
func applyRulePage(rules []Rule, marker string, pageSize int) ([]Rule, string) {
	if marker != "" {
		for i, r := range rules {
			if r.RuleArn == marker {
				rules = rules[i+1:]

				break
			}
		}
	}

	var nextMarker string
	if len(rules) > pageSize {
		nextMarker = rules[pageSize-1].RuleArn
		rules = rules[:pageSize]
	}

	return rules, nextMarker
}

func (h *Handler) handleModifyRule(vals url.Values) (any, error) {
	ruleArn := vals.Get("RuleArn")
	if ruleArn == "" {
		return nil, fmt.Errorf("%w: RuleArn is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")

	conditions, err := parseConditions(vals, "Conditions.member")
	if err != nil {
		return nil, err
	}

	rule, err := h.Backend.ModifyRule(ruleArn, actions, conditions)
	if err != nil {
		return nil, err
	}

	return &modifyRuleResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyRuleResult{
			Rules: xmlRuleList{
				Members: []xmlRule{toXMLRule(rule)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-rule"},
	}, nil
}

func (h *Handler) handleSetRulePriorities(vals url.Values) (any, error) {
	priorities := make([]RulePriority, 0)

	for i := 1; ; i++ {
		ruleArn := vals.Get(fmt.Sprintf("RulePriorities.member.%d.RuleArn", i))
		if ruleArn == "" {
			break
		}

		priorities = append(priorities, RulePriority{
			RuleArn:  ruleArn,
			Priority: vals.Get(fmt.Sprintf("RulePriorities.member.%d.Priority", i)),
		})
	}

	if len(priorities) == 0 {
		return nil, fmt.Errorf("%w: at least one RulePriority is required", ErrInvalidParameter)
	}

	rules, err := h.Backend.SetRulePriorities(priorities)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRule, 0, len(rules))
	for i := range rules {
		members = append(members, toXMLRule(&rules[i]))
	}

	return &setRulePrioritiesResponse{
		Xmlns: elbv2XMLNS,
		Result: setRulePrioritiesResult{
			Rules: xmlRuleList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-rule-priorities"},
	}, nil
}

// allowedHTTPMethods returns the whitelist of allowed HTTP methods for http-request-method conditions.
func allowedHTTPMethods() map[string]bool {
	return map[string]bool{
		"GET":     true,
		"HEAD":    true,
		"POST":    true,
		"PUT":     true,
		"DELETE":  true,
		"OPTIONS": true,
		"PATCH":   true,
	}
}

// parseConditions extracts rule conditions from form values.
// Supported fields: host-header, path-pattern, http-header, http-request-method,
// query-string, source-ip.
func parseConditions(vals url.Values, prefix string) ([]Condition, error) {
	result := make([]Condition, 0)

	for i := 1; ; i++ {
		ok, err := parseConditionAt(vals, prefix, i, &result)
		if err != nil {
			return nil, err
		}

		if !ok {
			break
		}
	}

	return result, nil
}

// parseConditionAt parses a single indexed condition and appends it to result.
// Returns (false, nil) when there are no more conditions to parse.
func parseConditionAt(vals url.Values, prefix string, i int, result *[]Condition) (bool, error) {
	field := vals.Get(fmt.Sprintf("%s.%d.Field", prefix, i))
	if field == "" {
		return false, nil
	}

	cond := Condition{Field: field}

	switch field {
	case "host-header":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HostHeaderConfig.Values.member", prefix, i))
		if len(cond.Values) == 0 {
			// Legacy top-level Values field (deprecated by AWS in favor of
			// HostHeaderConfig, but still accepted on the wire).
			cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.Values.member", prefix, i))
		}

		cond.RegexValues = parseRegexValues(vals, prefix, i, "HostHeaderConfig")
	case "path-pattern":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.PathPatternConfig.Values.member", prefix, i))
		if len(cond.Values) == 0 {
			// Legacy top-level Values field (deprecated by AWS in favor of
			// PathPatternConfig, but still accepted on the wire).
			cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.Values.member", prefix, i))
		}

		cond.RegexValues = parseRegexValues(vals, prefix, i, "PathPatternConfig")
	case "http-request-method":
		methods := parseMembers(vals, fmt.Sprintf("%s.%d.HttpRequestMethodConfig.Values.member", prefix, i))
		for _, m := range methods {
			if !allowedHTTPMethods()[strings.ToUpper(m)] {
				return false, fmt.Errorf(
					"%w: invalid HTTP method %q; valid methods are GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH",
					ErrInvalidParameter, m,
				)
			}
		}

		cond.Values = methods
	case "source-ip":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.SourceIpConfig.Values.member", prefix, i))
	case "http-header":
		cond.HTTPHeaderName = vals.Get(fmt.Sprintf("%s.%d.HttpHeaderConfig.HttpHeaderName", prefix, i))
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HttpHeaderConfig.Values.member", prefix, i))
		cond.RegexValues = parseRegexValues(vals, prefix, i, "HttpHeaderConfig")
	case "query-string":
		cond.QueryStringPairs = parseQueryStringPairs(vals, prefix, i)
	}

	*result = append(*result, cond)

	return true, nil
}

// parseRegexValues extracts the RegexValues for the Nth condition, checking the
// modern nested *Config.RegexValues form first and falling back to the top-level
// Conditions.member.N.RegexValues.member.M form (types.RuleCondition.RegexValues is
// a sibling of the per-field *Config structs on the real wire; both are valid).
// Valid only for host-header, path-pattern, and http-header conditions.
func parseRegexValues(vals url.Values, prefix string, i int, configName string) []string {
	regexValues := parseMembers(vals, fmt.Sprintf("%s.%d.%s.RegexValues.member", prefix, i, configName))
	if len(regexValues) == 0 {
		regexValues = parseMembers(vals, fmt.Sprintf("%s.%d.RegexValues.member", prefix, i))
	}

	return regexValues
}

// parseQueryStringPairs extracts query-string key/value pairs for the Nth condition.
func parseQueryStringPairs(vals url.Values, prefix string, condIdx int) []QueryStringPair {
	pairs := make([]QueryStringPair, 0)
	j := 1

	for parseQueryStringPairAt(vals, prefix, condIdx, j, &pairs) {
		j++
	}

	return pairs
}

// parseQueryStringPairAt parses a single query-string pair.
// Returns false when there are no more pairs to parse.
func parseQueryStringPairAt(vals url.Values, prefix string, condIdx, pairIdx int, pairs *[]QueryStringPair) bool {
	v := vals.Get(fmt.Sprintf("%s.%d.QueryStringConfig.Values.member.%d.Value", prefix, condIdx, pairIdx))
	if v == "" {
		return false
	}

	*pairs = append(*pairs, QueryStringPair{
		Key:   vals.Get(fmt.Sprintf("%s.%d.QueryStringConfig.Values.member.%d.Key", prefix, condIdx, pairIdx)),
		Value: v,
	})

	return true
}

// toXMLStringList converts a slice of strings into an xmlStringList value.
func toXMLStringList(values []string) xmlStringList {
	members := make([]xmlStringValue, 0, len(values))
	for _, v := range values {
		members = append(members, xmlStringValue{Value: v})
	}

	return xmlStringList{Members: members}
}

// toXMLStringListPtr converts a slice of strings into a *xmlStringList, nil when
// empty so it is omitted from the response (matching the AlpnPolicy/Certificates
// convention elsewhere in this package).
func toXMLStringListPtr(values []string) *xmlStringList {
	if len(values) == 0 {
		return nil
	}

	list := toXMLStringList(values)

	return &list
}

// toStringValuesConfig converts a condition's Values/RegexValues into an
// xmlConditionValuesConfig pointer.
func toStringValuesConfig(values, regexValues []string) *xmlConditionValuesConfig {
	return &xmlConditionValuesConfig{
		Values:      toXMLStringList(values),
		RegexValues: toXMLStringListPtr(regexValues),
	}
}

// buildXMLCondition converts a single backend Condition into its XML representation.
func buildXMLCondition(c Condition) xmlCondition {
	xc := xmlCondition{Field: c.Field}

	switch c.Field {
	case "host-header":
		xc.HostHeaderConfig = toStringValuesConfig(c.Values, c.RegexValues)
	case "path-pattern":
		xc.PathPatternConfig = toStringValuesConfig(c.Values, c.RegexValues)
	case "http-request-method":
		xc.HTTPRequestMethodConfig = toStringValuesConfig(c.Values, nil)
	case "source-ip":
		xc.SourceIPConfig = toStringValuesConfig(c.Values, nil)
	case "http-header":
		xc.HTTPHeaderConfig = &xmlHTTPHeaderConfig{
			HTTPHeaderName: c.HTTPHeaderName,
			Values:         toXMLStringList(c.Values),
			RegexValues:    toXMLStringListPtr(c.RegexValues),
		}
	case "query-string":
		pairs := make([]xmlQueryStringKeyValue, 0, len(c.QueryStringPairs))
		for _, p := range c.QueryStringPairs {
			pairs = append(pairs, xmlQueryStringKeyValue(p))
		}
		xc.QueryStringConfig = &xmlQueryStringConfig{Values: xmlQueryStringList{Members: pairs}}
	}

	return xc
}

func toXMLRule(r *Rule) xmlRule {
	actions := make([]xmlAction, 0, len(r.Actions))
	for _, a := range r.Actions {
		actions = append(actions, toXMLAction(a))
	}

	conds := make([]xmlCondition, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		conds = append(conds, buildXMLCondition(c))
	}

	return xmlRule{
		RuleArn:    r.RuleArn,
		Priority:   r.Priority,
		IsDefault:  r.IsDefault,
		Actions:    xmlActionList{Members: actions},
		Conditions: xmlConditionList{Members: conds},
	}
}

type xmlConditionValuesConfig struct {
	RegexValues *xmlStringList `xml:"RegexValues,omitempty"`
	Values      xmlStringList  `xml:"Values"`
}

type xmlHTTPHeaderConfig struct {
	RegexValues    *xmlStringList `xml:"RegexValues,omitempty"`
	HTTPHeaderName string         `xml:"HttpHeaderName"`
	Values         xmlStringList  `xml:"Values"`
}

type xmlQueryStringKeyValue struct {
	Key   string `xml:"Key,omitempty"`
	Value string `xml:"Value"`
}

type xmlQueryStringList struct {
	Members []xmlQueryStringKeyValue `xml:"member"`
}

type xmlQueryStringConfig struct {
	Values xmlQueryStringList `xml:"Values"`
}

type xmlCondition struct {
	HostHeaderConfig        *xmlConditionValuesConfig `xml:"HostHeaderConfig,omitempty"`
	PathPatternConfig       *xmlConditionValuesConfig `xml:"PathPatternConfig,omitempty"`
	HTTPHeaderConfig        *xmlHTTPHeaderConfig      `xml:"HttpHeaderConfig,omitempty"`
	HTTPRequestMethodConfig *xmlConditionValuesConfig `xml:"HttpRequestMethodConfig,omitempty"`
	QueryStringConfig       *xmlQueryStringConfig     `xml:"QueryStringConfig,omitempty"`
	SourceIPConfig          *xmlConditionValuesConfig `xml:"SourceIpConfig,omitempty"`
	Field                   string                    `xml:"Field"`
}

type xmlConditionList struct {
	Members []xmlCondition `xml:"member"`
}

type xmlRule struct {
	RuleArn    string           `xml:"RuleArn"`
	Priority   string           `xml:"Priority"`
	Actions    xmlActionList    `xml:"Actions"`
	Conditions xmlConditionList `xml:"Conditions"`
	IsDefault  bool             `xml:"IsDefault"`
}

type xmlRuleList struct {
	Members []xmlRule `xml:"member"`
}

type createRuleResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type createRuleResponse struct {
	XMLName          xml.Name            `xml:"CreateRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           createRuleResult    `xml:"CreateRuleResult"`
}

type deleteRuleResponse struct {
	XMLName          xml.Name            `xml:"DeleteRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeRulesResult struct {
	NextMarker string      `xml:"NextMarker,omitempty"`
	Rules      xmlRuleList `xml:"Rules"`
}

type describeRulesResponse struct {
	XMLName          xml.Name            `xml:"DescribeRulesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeRulesResult `xml:"DescribeRulesResult"`
}

type modifyRuleResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type modifyRuleResponse struct {
	XMLName          xml.Name            `xml:"ModifyRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           modifyRuleResult    `xml:"ModifyRuleResult"`
}

type setRulePrioritiesResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type setRulePrioritiesResponse struct {
	XMLName          xml.Name                `xml:"SetRulePrioritiesResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           setRulePrioritiesResult `xml:"SetRulePrioritiesResult"`
}
