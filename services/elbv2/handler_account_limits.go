package elbv2

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeAccountLimits(_ url.Values) (any, error) {
	limits := []xmlAccountLimit{
		{Name: "target-groups", Max: "3000"},
		{Name: "targets-per-target-group", Max: "1000"},
		{Name: "load-balancers", Max: "50"},
		{Name: "listeners-per-load-balancer", Max: "50"},
		{Name: "rules-per-load-balancer", Max: "200"},
		{Name: "certificates-per-listener", Max: "25"},
		{Name: "target-group-rules-per-listener", Max: "100"},
		{Name: "condition-values-per-alb-rule", Max: "5"},
		{Name: "condition-wildcards-per-alb-rule", Max: "5"},
		{Name: "target-groups-per-alb-listener-rule", Max: "5"},
		{Name: "target-groups-per-nlb-listener", Max: "1"},
		{Name: "subnets-per-load-balancer", Max: "8"},
	}

	return &describeAccountLimitsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeAccountLimitsResult{
			Limits: xmlAccountLimitList{Members: limits},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-account-limits"},
	}, nil
}

type xmlAccountLimit struct {
	Name string `xml:"Name"`
	Max  string `xml:"Max"`
}

type xmlAccountLimitList struct {
	Members []xmlAccountLimit `xml:"member"`
}

type describeAccountLimitsResult struct {
	Limits xmlAccountLimitList `xml:"Limits"`
}

type describeAccountLimitsResponse struct {
	XMLName          xml.Name                    `xml:"DescribeAccountLimitsResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeAccountLimitsResult `xml:"DescribeAccountLimitsResult"`
}
