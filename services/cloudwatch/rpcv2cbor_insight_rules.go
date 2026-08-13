package cloudwatch

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborPutInsightRule(input cbor.Map, c *echo.Context) error {
	return h.cborPutInsightRuleWithName(cborStr(input, "RuleName"), input, c)
}

func (h *Handler) cborPutInsightRuleWithName(
	ruleName string,
	input cbor.Map,
	c *echo.Context,
) error {
	if ruleName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleName is required",
		)
	}

	definition := cborStr(input, "RuleDefinition")
	if err := validateInsightRuleDefinition(definition); err != nil {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: definition,
		State:      cborStr(input, "RuleState"),
	}); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	if rule, err := h.Backend.GetInsightRule(ruleName); err == nil {
		h.applyCreationTags(input, rule.Arn)
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDeleteInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborDescribeInsightRules(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeInsightRules(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	rules := make(cbor.List, 0, len(p.Data))
	for _, r := range p.Data {
		entry := cbor.Map{
			keyName:       cbor.String(r.Name),
			keyState:      cbor.String(r.State),
			"Schema":      cbor.String(r.Schema),
			"Definition":  cbor.String(r.Definition),
			"ManagedRule": cbor.Bool(r.ManagedRule),
		}
		if r.Arn != "" {
			entry["RuleArn"] = cbor.String(r.Arn)
		}
		if !r.CreatedAt.IsZero() {
			entry["CreatedAt"] = cborFromTime(r.CreatedAt)
		}
		rules = append(rules, entry)
	}

	out := cbor.Map{
		"InsightRules": rules,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDisableInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DisableInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborEnableInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.EnableInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborGetInsightRuleReport(input cbor.Map, c *echo.Context) error {
	ruleName := cborStr(input, "RuleName")
	if ruleName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleName is required",
		)
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	maxContributors := int(cborInt32(input, "MaxContributorCount"))
	if maxContributors <= 0 {
		maxContributors = 10
	}
	orderBy := cborStr(input, "OrderBy")

	startTime := time.Now().UTC().Add(-time.Hour)
	if _, ok := input["StartTime"]; ok {
		startTime = cborTime(input, "StartTime")
	}
	endTime := time.Now().UTC()
	if _, ok := input["EndTime"]; ok {
		endTime = cborTime(input, "EndTime")
	}

	var contributors []InsightRuleContributor
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		var innerErr error
		func() {
			bk.mu.RLock("GetInsightRuleReport")
			defer bk.mu.RUnlock()
			contributors, innerErr = bk.GetInsightRuleContributors(
				ruleName,
				startTime,
				endTime,
				maxContributors,
				orderBy,
			)
		}()
		if innerErr != nil {
			return h.cborError(
				c,
				http.StatusBadRequest,
				"ResourceNotFoundException",
				innerErr.Error(),
			)
		}
	}

	contribList := make(cbor.List, 0, len(contributors))
	var aggregateValue float64
	for _, contrib := range contributors {
		keys := make(cbor.List, 0, len(contrib.Keys))
		for _, k := range contrib.Keys {
			keys = append(keys, cbor.String(k))
		}
		contribList = append(contribList, cbor.Map{
			"Keys":                      keys,
			"ApproximateAggregateValue": cbor.Float64(contrib.Sum),
		})
		aggregateValue += contrib.Sum
	}

	// KeyLabels: extract key expressions from the rule definition (best-effort).
	rule, _ := h.Backend.GetInsightRule(ruleName)
	keyLabels := extractInsightRuleKeyLabels(rule)

	return writeCBOR(c, cbor.Map{
		"KeyLabels":              keyLabels,
		"AggregationStatistic":   cbor.String("Sum"),
		"AggregateValue":         cbor.Float64(aggregateValue),
		"ApproximateUniqueCount": cbor.Uint(uint64(len(contributors))),
		"Contributors":           contribList,
		"MetricDatapoints":       cbor.List{},
	})
}

// extractInsightRuleKeyLabels returns a cbor.List of key-label strings from the
// Contribution.Keys array in the rule definition JSON. Returns an empty list if
// the rule is nil or the definition cannot be parsed.
func extractInsightRuleKeyLabels(rule *InsightRule) cbor.List {
	if rule == nil || rule.Definition == "" {
		return cbor.List{}
	}
	var def struct {
		Contribution struct {
			Keys []string `json:"Keys"`
		} `json:"Contribution"`
	}
	if err := json.Unmarshal([]byte(rule.Definition), &def); err != nil {
		return cbor.List{}
	}
	labels := make(cbor.List, 0, len(def.Contribution.Keys))
	for _, k := range def.Contribution.Keys {
		labels = append(labels, cbor.String(k))
	}

	return labels
}

func (h *Handler) cborListManagedInsightRules(input cbor.Map, c *echo.Context) error {
	resourceARN := cborStr(input, "ResourceARN")
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.ListManagedInsightRules(resourceARN, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	rules := make(cbor.List, 0, len(p.Data))
	for _, rule := range p.Data {
		entry := cbor.Map{
			"TemplateName": cbor.String(rule.Name),
		}
		if rule.Arn != "" {
			entry["ResourceARN"] = cbor.String(rule.Arn)
		}
		ruleState := cbor.Map{
			"RuleName": cbor.String(rule.Name),
		}
		if rule.State != "" {
			ruleState[keyState] = cbor.String(rule.State)
		}
		entry["RuleState"] = ruleState
		rules = append(rules, entry)
	}

	out := cbor.Map{
		"ManagedRules": rules,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborPutManagedInsightRules(input cbor.Map, c *echo.Context) error {
	var failures []InsightRuleFailure

	//nolint:nestif // nested CBOR decoding; structure mirrors the wire format
	if rulesRaw, ok := input["ManagedRules"]; ok {
		if rulesList, isList := rulesRaw.(cbor.List); isList {
			for _, ruleRaw := range rulesList {
				rule, isMap := ruleRaw.(cbor.Map)
				if !isMap {
					continue
				}
				ruleName := cborStr(rule, "RuleName")
				if ruleName == "" {
					continue
				}

				if err := h.Backend.PutInsightRule(&InsightRule{
					Name:        ruleName,
					State:       insightRuleStateEnabled,
					Definition:  cborStr(rule, "TemplateName"),
					Arn:         cborStr(rule, "ResourceARN"),
					ManagedRule: true,
				}); err != nil {
					failures = append(failures, InsightRuleFailure{
						RuleName:           ruleName,
						FailureCode:        errCodeInternalFailure,
						FailureDescription: err.Error(),
					})
				}
			}
		}
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

// buildInsightRuleFailureCBOR builds a CBOR map for insight rule failure responses.
func buildInsightRuleFailureCBOR(failures []InsightRuleFailure) cbor.Map {
	failList := make(cbor.List, 0, len(failures))
	for _, f := range failures {
		failList = append(failList, cbor.Map{
			"RuleName":           cbor.String(f.RuleName),
			"FailureCode":        cbor.String(f.FailureCode),
			"FailureDescription": cbor.String(f.FailureDescription),
		})
	}

	return cbor.Map{
		"Failures": failList,
	}
}
