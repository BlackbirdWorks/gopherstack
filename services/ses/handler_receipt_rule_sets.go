package ses

import (
	"encoding/xml"
	"net/url"
	"time"
)

func (h *Handler) handleCreateReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")

	if err := h.Backend.CreateReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &createReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleCloneReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	originalName := vals.Get("OriginalRuleSetName")
	newName := vals.Get("RuleSetName")

	if err := h.Backend.CloneReceiptRuleSet(originalName, newName); err != nil {
		return nil, err
	}

	return &cloneReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

type createReceiptRuleSetResult struct{}

type createReceiptRuleSetResponse struct {
	XMLName   xml.Name                   `xml:"CreateReceiptRuleSetResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	Result    createReceiptRuleSetResult `xml:"CreateReceiptRuleSetResult"`
	RequestID string                     `xml:"ResponseMetadata>RequestId"`
}

type cloneReceiptRuleSetResult struct{}

type cloneReceiptRuleSetResponse struct {
	XMLName   xml.Name                  `xml:"CloneReceiptRuleSetResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	Result    cloneReceiptRuleSetResult `xml:"CloneReceiptRuleSetResult"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleListReceiptRuleSets(vals url.Values, reqID string) any {
	p := h.Backend.ListReceiptRuleSets(vals.Get("NextToken"))
	members := make([]xmlRuleSetMetadata, 0, len(p.Data))
	for _, rs := range p.Data {
		members = append(members, xmlRuleSetMetadata{
			Name:      rs.Name,
			CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	return &listReceiptRuleSetsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: listReceiptRuleSetsResult{
			RuleSets:  xmlRuleSetMetadataList{Members: members},
			NextToken: p.Next,
		},
	}
}

func (h *Handler) handleDescribeReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	rs, err := h.Backend.DescribeReceiptRuleSet(name)
	if err != nil {
		return nil, err
	}
	rules := make([]xmlReceiptRule, 0, len(rs.Rules))
	for _, r := range rs.Rules {
		rules = append(rules, toXMLReceiptRule(r))
	}

	return &describeReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: describeReceiptRuleSetResult{
			Metadata: xmlRuleSetMetadata{
				Name:      rs.Name,
				CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
			},
			Rules: xmlReceiptRuleList{Members: rules},
		},
	}, nil
}

func (h *Handler) handleDeleteReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	if err := h.Backend.DeleteReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &deleteReceiptRuleSetResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleSetActiveReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	if err := h.Backend.SetActiveReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &setActiveReceiptRuleSetResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleDescribeActiveReceiptRuleSet(reqID string) (any, error) {
	rs, active, err := h.Backend.DescribeActiveReceiptRuleSet()
	if err != nil {
		return nil, err
	}
	result := describeActiveReceiptRuleSetResult{}
	if active {
		rules := make([]xmlReceiptRule, 0, len(rs.Rules))
		for _, r := range rs.Rules {
			rules = append(rules, toXMLReceiptRule(r))
		}
		result.Metadata = &xmlRuleSetMetadata{
			Name:      rs.Name,
			CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
		}
		result.Rules = xmlReceiptRuleList{Members: rules}
	}

	return &describeActiveReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    result,
	}, nil
}

type xmlRuleSetMetadata struct {
	Name      string `xml:"Name"`
	CreatedAt string `xml:"CreatedTimestamp"`
}

type xmlRuleSetMetadataList struct {
	Members []xmlRuleSetMetadata `xml:"member"`
}

type listReceiptRuleSetsResult struct {
	NextToken string                 `xml:"NextToken,omitempty"`
	RuleSets  xmlRuleSetMetadataList `xml:"RuleSets"`
}

type listReceiptRuleSetsResponse struct {
	XMLName   xml.Name                  `xml:"ListReceiptRuleSetsResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
	Result    listReceiptRuleSetsResult `xml:"ListReceiptRuleSetsResult"`
}

type describeReceiptRuleSetResult struct {
	Metadata xmlRuleSetMetadata `xml:"Metadata"`
	Rules    xmlReceiptRuleList `xml:"Rules"`
}

type describeReceiptRuleSetResponse struct {
	XMLName   xml.Name                     `xml:"DescribeReceiptRuleSetResponse"`
	Xmlns     string                       `xml:"xmlns,attr"`
	RequestID string                       `xml:"ResponseMetadata>RequestId"`
	Result    describeReceiptRuleSetResult `xml:"DescribeReceiptRuleSetResult"`
}

type deleteReceiptRuleSetResult struct{}

type deleteReceiptRuleSetResponse struct {
	XMLName   xml.Name                   `xml:"DeleteReceiptRuleSetResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	Result    deleteReceiptRuleSetResult `xml:"DeleteReceiptRuleSetResult"`
	RequestID string                     `xml:"ResponseMetadata>RequestId"`
}

type setActiveReceiptRuleSetResult struct{}

type setActiveReceiptRuleSetResponse struct {
	XMLName   xml.Name                      `xml:"SetActiveReceiptRuleSetResponse"`
	Xmlns     string                        `xml:"xmlns,attr"`
	Result    setActiveReceiptRuleSetResult `xml:"SetActiveReceiptRuleSetResult"`
	RequestID string                        `xml:"ResponseMetadata>RequestId"`
}

type describeActiveReceiptRuleSetResult struct {
	Metadata *xmlRuleSetMetadata `xml:"Metadata,omitempty"`
	Rules    xmlReceiptRuleList  `xml:"Rules"`
}

type describeActiveReceiptRuleSetResponse struct {
	XMLName   xml.Name                           `xml:"DescribeActiveReceiptRuleSetResponse"`
	Xmlns     string                             `xml:"xmlns,attr"`
	RequestID string                             `xml:"ResponseMetadata>RequestId"`
	Result    describeActiveReceiptRuleSetResult `xml:"DescribeActiveReceiptRuleSetResult"`
}
