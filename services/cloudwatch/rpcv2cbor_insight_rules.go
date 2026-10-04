package cloudwatch

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

const cborKeyTimestamp = "Timestamp"

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
		if errors.Is(err, ErrInsightRuleLimit) {
			return h.cborError(c, http.StatusBadRequest, "LimitExceededException", err.Error())
		}

		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: definition,
		State:      cborStr(input, "RuleState"),
	}); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
		}

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

	arns := h.insightRuleARNs(ruleNames)

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	for _, a := range arns {
		h.deleteResourceTags(a)
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
	req := InsightRuleReportRequest{
		RuleName:        cborStr(input, "RuleName"),
		Period:          int(cborInt32(input, "Period")),
		MaxContributors: int(cborInt32(input, "MaxContributorCount")),
		OrderBy:         cborStr(input, "OrderBy"),
		Metrics:         cborStringSlice(input["Metrics"]),
	}
	if _, ok := input["StartTime"]; ok {
		req.StartTime = cborTime(input, "StartTime")
	}

	if _, ok := input["EndTime"]; ok {
		req.EndTime = cborTime(input, "EndTime")
	}

	report, err := h.Backend.GetInsightRuleReport(req)
	if err != nil {
		switch {
		case errors.Is(err, ErrMissingParameter):
			return h.cborError(c, http.StatusBadRequest, "MissingRequiredParameterException", err.Error())
		case errors.Is(err, ErrInsightRuleNotFound):
			return h.cborError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	return writeCBOR(c, insightReportToCBOR(report))
}

func insightReportToCBOR(r *InsightRuleReport) cbor.Map {
	labels := make(cbor.List, 0, len(r.KeyLabels))
	for _, k := range r.KeyLabels {
		labels = append(labels, cbor.String(k))
	}

	contribs := make(cbor.List, 0, len(r.Contributors))

	for _, c := range r.Contributors {
		keys := make(cbor.List, 0, len(c.Keys))
		for _, k := range c.Keys {
			keys = append(keys, cbor.String(k))
		}

		dps := make(cbor.List, 0, len(c.Datapoints))
		for _, d := range c.Datapoints {
			dps = append(dps, cbor.Map{
				cborKeyTimestamp:   cborFromTime(d.Timestamp),
				"ApproximateValue": cbor.Float64(d.ApproximateValue),
			})
		}

		contribs = append(contribs, cbor.Map{
			"Keys":                      keys,
			"ApproximateAggregateValue": cbor.Float64(c.ApproximateAggregateValue),
			"Datapoints":                dps,
		})
	}

	metrics := make(cbor.List, 0, len(r.MetricDatapoints))
	for _, d := range r.MetricDatapoints {
		metrics = append(metrics, insightMetricDatapointToCBOR(d))
	}

	return cbor.Map{
		"KeyLabels":              labels,
		"AggregationStatistic":   cbor.String(r.AggregationStatistic),
		"AggregateValue":         cbor.Float64(r.AggregateValue),
		"ApproximateUniqueCount": cbor.Uint(uint64(r.ApproximateUniqueCount)), //nolint:gosec // count is never negative
		"Contributors":           contribs,
		"MetricDatapoints":       metrics,
	}
}

func insightMetricDatapointToCBOR(d InsightRuleMetricDatapoint) cbor.Map {
	m := cbor.Map{cborKeyTimestamp: cborFromTime(d.Timestamp)}

	stats := []struct {
		v    *float64
		name string
	}{
		{d.UniqueContributors, statUniqueContributors}, {d.MaxContributorValue, statMaxContributorValue},
		{d.SampleCount, statSampleCount}, {d.Sum, aggregateSum}, {d.Minimum, statMinimum},
		{d.Maximum, orderByMaximum}, {d.Average, widgetDefaultStat},
	}

	for _, st := range stats {
		if st.v != nil {
			m[st.name] = cbor.Float64(*st.v)
		}
	}

	return m
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
		// rule.Definition holds the managed rule's TemplateName (set by
		// PutManagedInsightRules); rule.Name is the RuleName, which belongs
		// under the nested RuleState below, not here.
		entry := cbor.Map{
			"TemplateName": cbor.String(rule.Definition),
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
	rulesRaw, ok := input["ManagedRules"]
	if !ok {
		return writeCBOR(c, buildInsightRuleFailureCBOR(nil))
	}

	rulesList, isList := rulesRaw.(cbor.List)
	if !isList {
		return writeCBOR(c, buildInsightRuleFailureCBOR(nil))
	}

	var failures []InsightRuleFailure
	for _, ruleRaw := range rulesList {
		if rule, isMap := ruleRaw.(cbor.Map); isMap {
			if fail := h.putOneManagedInsightRule(rule); fail != nil {
				failures = append(failures, *fail)
			}
		}
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

// putOneManagedInsightRule creates a single managed insight rule from one
// ManagedRules list entry, returning the resulting failure (or nil on
// success). A real client never sends RuleName (ManagedRule has no such
// member, only ResourceARN/TemplateName/Tags); a name is synthesized when
// one isn't present so internal callers/tests can still pass an explicit
// name.
func (h *Handler) putOneManagedInsightRule(rule cbor.Map) *InsightRuleFailure {
	resourceARN := cborStr(rule, "ResourceARN")
	templateName := cborStr(rule, "TemplateName")
	ruleName := cborStr(rule, "RuleName")
	if ruleName == "" {
		ruleName = managedInsightRuleName(resourceARN, templateName)
	}
	if ruleName == "" {
		return nil
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:        ruleName,
		State:       insightRuleStateEnabled,
		Definition:  templateName,
		Arn:         resourceARN,
		ManagedRule: true,
	}); err != nil {
		return &InsightRuleFailure{
			RuleName:           ruleName,
			FailureCode:        errCodeInternalFailure,
			FailureDescription: err.Error(),
		}
	}

	return nil
}

// buildInsightRuleFailureCBOR builds a CBOR map for insight rule failure responses.
func buildInsightRuleFailureCBOR(failures []InsightRuleFailure) cbor.Map {
	failList := make(cbor.List, 0, len(failures))
	for _, f := range failures {
		failList = append(failList, cbor.Map{
			// Real member name is FailureResource, not RuleName
			// (cloudwatch@v1.66.3 schemas/schemas.go:3271, PartialFailure).
			"FailureResource":    cbor.String(f.RuleName),
			"FailureCode":        cbor.String(f.FailureCode),
			"FailureDescription": cbor.String(f.FailureDescription),
		})
	}

	return cbor.Map{
		"Failures": failList,
	}
}
