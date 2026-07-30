package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

// ---- Handler registration ----

func registerDeclarativePoliciesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["StartDeclarativePoliciesReport"] = h.handleStartDeclarativePoliciesReport
	ops["CancelDeclarativePoliciesReport"] = h.handleCancelDeclarativePoliciesReport
	ops["DescribeDeclarativePoliciesReports"] = h.handleDescribeDeclarativePoliciesReports
	ops["GetDeclarativePoliciesReportSummary"] = h.handleGetDeclarativePoliciesReportSummary
}

func declarativePoliciesSupportedOperations() []string {
	return []string{
		"StartDeclarativePoliciesReport",
		"CancelDeclarativePoliciesReport",
		"DescribeDeclarativePoliciesReports",
		"GetDeclarativePoliciesReportSummary",
	}
}

// ---- XML entity types ----

type declarativePoliciesReportItem struct {
	ReportID  string          `xml:"reportId"`
	TargetID  string          `xml:"targetId,omitempty"`
	S3Bucket  string          `xml:"s3Bucket,omitempty"`
	S3Prefix  string          `xml:"s3Prefix,omitempty"`
	Status    string          `xml:"status"`
	StartTime string          `xml:"startTime,omitempty"`
	EndTime   string          `xml:"endTime,omitempty"`
	TagSet    []simpleTagItem `xml:"tagSet>item"`
}

func declarativePoliciesReportToItem(
	r *DeclarativePoliciesReport, tags map[string]string,
) declarativePoliciesReportItem {
	item := declarativePoliciesReportItem{
		ReportID: r.ReportID,
		TargetID: r.TargetID,
		S3Bucket: r.S3Bucket,
		S3Prefix: r.S3Prefix,
		Status:   r.Status,
		TagSet:   tagItemsFromMap(tags),
	}
	if !r.StartTime.IsZero() {
		item.StartTime = r.StartTime.Format(time.RFC3339)
	}

	if !r.EndTime.IsZero() {
		item.EndTime = r.EndTime.Format(time.RFC3339)
	}

	return item
}

// ---- XML response types ----

type startDeclarativePoliciesReportResponse struct {
	XMLName   xml.Name `xml:"StartDeclarativePoliciesReportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ReportID  string   `xml:"reportId"`
}

type cancelDeclarativePoliciesReportResponse struct {
	XMLName   xml.Name `xml:"CancelDeclarativePoliciesReportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type describeDeclarativePoliciesReportsResponse struct {
	XMLName   xml.Name                        `xml:"DescribeDeclarativePoliciesReportsResponse"`
	Xmlns     string                          `xml:"xmlns,attr"`
	RequestID string                          `xml:"requestId"`
	NextToken string                          `xml:"nextToken,omitempty"`
	Reports   []declarativePoliciesReportItem `xml:"reportSet>item"`
}

type getDeclarativePoliciesReportSummaryResponse struct {
	XMLName                xml.Name `xml:"GetDeclarativePoliciesReportSummaryResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	ReportID               string   `xml:"reportId"`
	TargetID               string   `xml:"targetId,omitempty"`
	S3Bucket               string   `xml:"s3Bucket,omitempty"`
	S3Prefix               string   `xml:"s3Prefix,omitempty"`
	StartTime              string   `xml:"startTime,omitempty"`
	EndTime                string   `xml:"endTime,omitempty"`
	NumberOfAccounts       int32    `xml:"numberOfAccounts"`
	NumberOfFailedAccounts int32    `xml:"numberOfFailedAccounts"`
}

// ---- handlers ----

func (h *Handler) handleStartDeclarativePoliciesReport(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "declarative-policies-report")

	report, err := h.Backend.StartDeclarativePoliciesReport(
		vals.Get("S3Bucket"),
		vals.Get("TargetId"),
		vals.Get("S3Prefix"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &startDeclarativePoliciesReportResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ReportID:  report.ReportID,
	}, nil
}

func (h *Handler) handleCancelDeclarativePoliciesReport(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.CancelDeclarativePoliciesReport(vals.Get("ReportId"))
	if err != nil {
		return nil, err
	}

	return &cancelDeclarativePoliciesReportResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}

func (h *Handler) handleDescribeDeclarativePoliciesReports(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReportId")
	reports := h.Backend.DescribeDeclarativePoliciesReports(ids)

	resp := &describeDeclarativePoliciesReportsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range reports {
		resp.Reports = append(
			resp.Reports, declarativePoliciesReportToItem(r, h.Backend.TagsForResource(r.ReportID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetDeclarativePoliciesReportSummary(vals url.Values, reqID string) (any, error) {
	summary, err := h.Backend.GetDeclarativePoliciesReportSummary(vals.Get("ReportId"))
	if err != nil {
		return nil, err
	}

	resp := &getDeclarativePoliciesReportSummaryResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		ReportID:               summary.ReportID,
		TargetID:               summary.TargetID,
		S3Bucket:               summary.S3Bucket,
		S3Prefix:               summary.S3Prefix,
		NumberOfAccounts:       summary.NumberOfAccounts,
		NumberOfFailedAccounts: summary.NumberOfFailedAccounts,
	}
	if !summary.StartTime.IsZero() {
		resp.StartTime = summary.StartTime.Format(time.RFC3339)
	}

	if !summary.EndTime.IsZero() {
		resp.EndTime = summary.EndTime.Format(time.RFC3339)
	}

	return resp, nil
}
