package cloudwatch

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// insightRuleFailureXML is the XML representation of a failed insight rule operation.
// The real member is FailureResource, not RuleName (cloudwatch@v1.66.3
// schemas/schemas.go:3271, PartialFailure -- shared across both CBOR and this
// service's legacy Query surface since it comes from the Smithy model, not
// the protocol).
type insightRuleFailureXML struct {
	FailureResource    string `xml:"FailureResource"`
	FailureCode        string `xml:"FailureCode"`
	FailureDescription string `xml:"FailureDescription,omitempty"`
}

// insightRuleFailResult holds the failures portion of insight rule batch operation responses.
type insightRuleFailResult struct {
	Failures []insightRuleFailureXML `xml:"Failures>member"`
}

// buildInsightRuleFailResult converts backend failures into the XML result struct.
func buildInsightRuleFailResult(failures []InsightRuleFailure) insightRuleFailResult {
	if len(failures) == 0 {
		return insightRuleFailResult{}
	}

	members := make([]insightRuleFailureXML, 0, len(failures))
	for _, f := range failures {
		members = append(members, insightRuleFailureXML{
			FailureResource:    f.RuleName,
			FailureCode:        f.FailureCode,
			FailureDescription: f.FailureDescription,
		})
	}

	return insightRuleFailResult{Failures: members}
}

// insightRuleXML is the XML representation of an InsightRule.
type insightRuleXML struct {
	CreatedAt   string `xml:"CreatedAt,omitempty"`
	Name        string `xml:"Name"`
	State       string `xml:"State"`
	Schema      string `xml:"Schema,omitempty"`
	Definition  string `xml:"Definition,omitempty"`
	Arn         string `xml:"RuleArn,omitempty"`
	ManagedRule bool   `xml:"ManagedRule"`
}

func (h *Handler) handlePutInsightRule(form url.Values, c *echo.Context) error {
	if err := h.putInsightRule(form.Get("RuleName"), form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name       `xml:"PutInsightRuleResponse"`
		Result    xmlEmptyResult `xml:"PutInsightRuleResult"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) putInsightRule(ruleName string, form url.Values, c *echo.Context) error {
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}

	definition := form.Get("RuleDefinition")
	if err := validateInsightRuleDefinition(definition); err != nil {
		if errors.Is(err, ErrInsightRuleLimit) {
			return h.xmlError(c, http.StatusBadRequest, "LimitExceeded", err.Error())
		}

		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: definition,
		State:      form.Get("RuleState"),
	}); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handleDeleteInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	// Collect ARNs before deleting so tag entries can be cleaned up (insight
	// rules are a taggable resource kind alongside alarms/dashboards/metric
	// streams, all of which already clean up tags on delete).
	arns := h.insightRuleARNs(ruleNames)

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	for _, a := range arns {
		h.deleteResourceTags(a)
	}

	type response struct {
		XMLName   xml.Name              `xml:"DeleteInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"DeleteInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

// insightRuleARNs resolves the ARNs of the named insight rules that currently
// exist, skipping names that don't (used to clean up tags before deletion).
func (h *Handler) insightRuleARNs(names []string) []string {
	var arns []string

	for _, name := range names {
		if rule, err := h.Backend.GetInsightRule(name); err == nil {
			arns = append(arns, rule.Arn)
		}
	}

	return arns
}

func (h *Handler) handleDescribeInsightRules(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeInsightRules(nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make([]insightRuleXML, 0, len(p.Data))
	for _, r := range p.Data {
		members = append(members, insightRuleXML{
			Name:        r.Name,
			State:       r.State,
			Schema:      r.Schema,
			Definition:  r.Definition,
			ManagedRule: r.ManagedRule,
			Arn:         r.Arn,
			CreatedAt:   formatTimeOmitZero(r.CreatedAt),
		})
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		InsightRules []insightRuleXML `xml:"InsightRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeInsightRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{InsightRules: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDisableInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DisableInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name              `xml:"DisableInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"DisableInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

func (h *Handler) handleEnableInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.EnableInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name              `xml:"EnableInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"EnableInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

type insightContributorDatapointXML struct {
	Timestamp        string  `xml:"Timestamp"`
	ApproximateValue float64 `xml:"ApproximateValue"`
}

type insightContributorXML struct {
	Keys                      []string                         `xml:"Keys>member"`
	Datapoints                []insightContributorDatapointXML `xml:"Datapoints>member"`
	ApproximateAggregateValue float64                          `xml:"ApproximateAggregateValue"`
}

type insightMetricDatapointXML struct {
	UniqueContributors  *float64 `xml:"UniqueContributors,omitempty"`
	MaxContributorValue *float64 `xml:"MaxContributorValue,omitempty"`
	SampleCount         *float64 `xml:"SampleCount,omitempty"`
	Sum                 *float64 `xml:"Sum,omitempty"`
	Minimum             *float64 `xml:"Minimum,omitempty"`
	Maximum             *float64 `xml:"Maximum,omitempty"`
	Average             *float64 `xml:"Average,omitempty"`
	Timestamp           string   `xml:"Timestamp"`
}

type insightReportResultXML struct {
	AggregationStatistic   string                      `xml:"AggregationStatistic"`
	KeyLabels              []string                    `xml:"KeyLabels>member"`
	Contributors           []insightContributorXML     `xml:"Contributors>member"`
	MetricDatapoints       []insightMetricDatapointXML `xml:"MetricDatapoints>member"`
	AggregateValue         float64                     `xml:"AggregateValue"`
	ApproximateUniqueCount int64                       `xml:"ApproximateUniqueCount"`
}

func parseQueryTime(s string) time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}

	return time.Time{}
}

func (h *Handler) handleGetInsightRuleReport(form url.Values, c *echo.Context) error {
	maxContributors, _ := strconv.Atoi(form.Get("MaxContributorCount"))
	period, _ := strconv.Atoi(form.Get("Period"))

	report, err := h.Backend.GetInsightRuleReport(InsightRuleReportRequest{
		RuleName:        form.Get("RuleName"),
		StartTime:       parseQueryTime(form.Get("StartTime")),
		EndTime:         parseQueryTime(form.Get("EndTime")),
		Period:          period,
		MaxContributors: maxContributors,
		OrderBy:         form.Get("OrderBy"),
		Metrics:         parseMemberList(form, "Metrics."),
	})
	if err != nil {
		return h.insightReportError(c, err)
	}

	type response struct {
		XMLName   xml.Name               `xml:"GetInsightRuleReportResponse"`
		Xmlns     string                 `xml:"xmlns,attr"`
		RequestID string                 `xml:"ResponseMetadata>RequestId"`
		Result    insightReportResultXML `xml:"GetInsightRuleReportResult"`
	}

	return writeXML(c, response{
		Xmlns: cloudwatchNS, RequestID: uuid.New().String(), Result: insightReportToXML(report),
	})
}

func (h *Handler) insightReportError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrMissingParameter):
		return h.xmlError(c, http.StatusBadRequest, "MissingParameter", err.Error())
	case errors.Is(err, ErrInsightRuleNotFound):
		return h.xmlError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	}

	return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
}

func insightReportToXML(r *InsightRuleReport) insightReportResultXML {
	out := insightReportResultXML{
		AggregationStatistic:   r.AggregationStatistic,
		KeyLabels:              r.KeyLabels,
		AggregateValue:         r.AggregateValue,
		ApproximateUniqueCount: r.ApproximateUniqueCount,
	}

	for _, c := range r.Contributors {
		xc := insightContributorXML{Keys: c.Keys, ApproximateAggregateValue: c.ApproximateAggregateValue}
		for _, d := range c.Datapoints {
			xc.Datapoints = append(xc.Datapoints, insightContributorDatapointXML{
				Timestamp: d.Timestamp.UTC().Format(time.RFC3339), ApproximateValue: d.ApproximateValue,
			})
		}

		out.Contributors = append(out.Contributors, xc)
	}

	for _, d := range r.MetricDatapoints {
		out.MetricDatapoints = append(out.MetricDatapoints, insightMetricDatapointXML{
			Timestamp:           d.Timestamp.UTC().Format(time.RFC3339),
			UniqueContributors:  d.UniqueContributors,
			MaxContributorValue: d.MaxContributorValue,
			SampleCount:         d.SampleCount,
			Sum:                 d.Sum,
			Minimum:             d.Minimum,
			Maximum:             d.Maximum,
			Average:             d.Average,
		})
	}

	return out
}

func (h *Handler) handleListManagedInsightRules(form url.Values, c *echo.Context) error {
	resourceARN := form.Get("ResourceARN")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListManagedInsightRules(resourceARN, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type ruleStateXML struct {
		RuleName string `xml:"RuleName,omitempty"`
		State    string `xml:"State,omitempty"`
	}
	type managedRuleXML struct {
		TemplateName string       `xml:"TemplateName,omitempty"`
		ResourceARN  string       `xml:"ResourceARN,omitempty"`
		RuleState    ruleStateXML `xml:"RuleState"`
	}
	type listResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		ManagedRules []managedRuleXML `xml:"ManagedRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListManagedInsightRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListManagedInsightRulesResult"`
	}

	// rule.Definition holds the managed rule's TemplateName (set by
	// PutManagedInsightRules); rule.Name is the RuleName, which belongs
	// under the nested RuleState, not at the top level (cloudwatch@v1.66.3
	// schemas/schemas.go:3795-3799, ManagedRuleDescription).
	members := make([]managedRuleXML, 0, len(p.Data))
	for _, rule := range p.Data {
		members = append(members, managedRuleXML{
			TemplateName: rule.Definition,
			ResourceARN:  rule.Arn,
			RuleState:    ruleStateXML{RuleName: rule.Name, State: rule.State},
		})
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{ManagedRules: members, NextToken: p.Next},
	})
}

func (h *Handler) handlePutManagedInsightRules(form url.Values, c *echo.Context) error {
	type failureXML struct {
		FailureResource    string `xml:"FailureResource"`
		FailureCode        string `xml:"FailureCode"`
		FailureDescription string `xml:"FailureDescription,omitempty"`
	}
	type putResult struct {
		Failures []failureXML `xml:"Failures>member,omitempty"`
	}
	type response struct {
		XMLName   xml.Name  `xml:"PutManagedInsightRulesResponse"`
		Xmlns     string    `xml:"xmlns,attr"`
		RequestID string    `xml:"ResponseMetadata>RequestId"`
		Result    putResult `xml:"PutManagedInsightRulesResult"`
	}

	var failures []failureXML
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("ManagedRules.member.%d.", i)
		templateName := form.Get(prefix + "TemplateName")
		resourceARN := form.Get(prefix + "ResourceARN")
		// A real client never sends RuleName (ManagedRule has no such
		// member, only ResourceARN/TemplateName/Tags); fall back to the
		// synthesized name only when one isn't present, so internal
		// callers/tests can still pass an explicit name.
		ruleName := form.Get(prefix + "RuleName")
		if templateName == "" && resourceARN == "" && ruleName == "" {
			break
		}
		if ruleName == "" {
			ruleName = managedInsightRuleName(resourceARN, templateName)
		}

		if err := h.Backend.PutInsightRule(&InsightRule{
			Name:        ruleName,
			State:       insightRuleStateEnabled,
			Definition:  templateName,
			Arn:         resourceARN,
			ManagedRule: true,
		}); err != nil {
			failures = append(failures, failureXML{
				FailureResource:    ruleName,
				FailureCode:        errCodeInternalFailure,
				FailureDescription: err.Error(),
			})
		}
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    putResult{Failures: failures},
	})
}
