package cloudwatch

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// insightRuleFailureXML is the XML representation of a failed insight rule operation.
type insightRuleFailureXML struct {
	RuleName           string `xml:"RuleName"`
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
		members = append(members, insightRuleFailureXML(f))
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
		XMLName   xml.Name `xml:"PutInsightRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) putInsightRule(ruleName string, form url.Values, c *echo.Context) error {
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: form.Get("RuleDefinition"),
		State:      form.Get("RuleState"),
	}); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handleUpdateInsightRule(form url.Values, c *echo.Context) error {
	ruleName := form.Get("RuleName")
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	if err := h.putInsightRule(ruleName, form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"UpdateInsightRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
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

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
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

// handleGetInsightRuleReport returns a contributor insights report by aggregating
// metric data grouped by dimension values for the named rule's log group.
func (h *Handler) handleGetInsightRuleReport(form url.Values, c *echo.Context) error {
	ruleName := form.Get("RuleName")
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	maxContributors, _ := strconv.Atoi(form.Get("MaxContributorCount"))
	if maxContributors <= 0 {
		maxContributors = 10
	}
	orderBy := form.Get("OrderBy")
	startStr := form.Get("StartTime")
	endStr := form.Get("EndTime")

	startTime := time.Now().UTC().Add(-time.Hour)
	if t, err := time.Parse(time.RFC3339, startStr); err == nil {
		startTime = t
	}
	endTime := time.Now().UTC()
	if t, err := time.Parse(time.RFC3339, endStr); err == nil {
		endTime = t
	}

	var contributors []AlarmContributor
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
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"ResourceNotFoundException",
				innerErr.Error(),
			)
		}
	}

	type keyXML struct {
		Keys []string `xml:"Keys>member"`
		Sum  float64  `xml:"ApproximateSum"`
	}
	type result struct {
		Contributors []keyXML `xml:"Contributors>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetInsightRuleReportResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetInsightRuleReportResult"`
	}

	resp := response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()}
	for _, c := range contributors {
		resp.Result.Contributors = append(resp.Result.Contributors, keyXML(c))
	}

	return writeXML(c, resp)
}

func (h *Handler) handleListManagedInsightRules(form url.Values, c *echo.Context) error {
	resourceARN := form.Get("ResourceARN")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListManagedInsightRules(resourceARN, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type managedRuleXML struct {
		RuleName     string `xml:"RuleName"`
		ResourceARN  string `xml:"ResourceARN,omitempty"`
		RuleState    string `xml:"RuleState>Value,omitempty"`
		TemplateName string `xml:"TemplateName,omitempty"`
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

	members := make([]managedRuleXML, 0, len(p.Data))
	for _, rule := range p.Data {
		members = append(members, managedRuleXML{
			RuleName:    rule.Name,
			ResourceARN: rule.Arn,
			RuleState:   rule.State,
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
		RuleName           string `xml:"RuleName"`
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
		ruleName := form.Get(prefix + "RuleName")
		if ruleName == "" {
			break
		}
		templateName := form.Get(prefix + "TemplateName")
		resourceARN := form.Get(prefix + "ResourceARN")

		if err := h.Backend.PutInsightRule(&InsightRule{
			Name:        ruleName,
			State:       insightRuleStateEnabled,
			Definition:  templateName,
			Arn:         resourceARN,
			ManagedRule: true,
		}); err != nil {
			failures = append(failures, failureXML{
				RuleName:           ruleName,
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
