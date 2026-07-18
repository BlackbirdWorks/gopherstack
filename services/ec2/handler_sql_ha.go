package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

// handler_sql_ha.go implements the HTTP handlers for the SQL Server High
// Availability standby detection family, backed by sql_ha.go.

// Wire action names use AWS's exact spelling ("Sql", not "SQL"); Go
// identifiers use "SQL" per revive's var-naming convention.
func registerSQLHaOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["EnableInstanceSqlHaStandbyDetections"] = h.handleEnableInstanceSQLHaStandbyDetections
	ops["DisableInstanceSqlHaStandbyDetections"] = h.handleDisableInstanceSQLHaStandbyDetections
	ops["DescribeInstanceSqlHaStates"] = h.handleDescribeInstanceSQLHaStates
	ops["DescribeInstanceSqlHaHistoryStates"] = h.handleDescribeInstanceSQLHaHistoryStates
}

// sqlHaSupportedOperations lists the operation names registered by
// registerSQLHaOps, for GetSupportedOperations().
func sqlHaSupportedOperations() []string {
	return []string{
		"EnableInstanceSqlHaStandbyDetections",
		"DisableInstanceSqlHaStandbyDetections",
		"DescribeInstanceSqlHaStates",
		"DescribeInstanceSqlHaHistoryStates",
	}
}

// ---- Response shapes ----

type registeredSQLHaInstanceItem struct {
	InstanceID            string `xml:"instanceId,omitempty"`
	HaStatus              string `xml:"haStatus,omitempty"`
	LastUpdatedTime       string `xml:"lastUpdatedTime,omitempty"`
	ProcessingStatus      string `xml:"processingStatus,omitempty"`
	SQLServerLicenseUsage string `xml:"sqlServerLicenseUsage,omitempty"`
}

func toRegisteredSQLHaInstanceItem(r *RegisteredSQLHaInstance) registeredSQLHaInstanceItem {
	return registeredSQLHaInstanceItem{
		InstanceID:            r.InstanceID,
		HaStatus:              r.HaStatus,
		LastUpdatedTime:       r.LastUpdatedTime.UTC().Format(time.RFC3339),
		ProcessingStatus:      r.ProcessingStatus,
		SQLServerLicenseUsage: r.SQLServerLicenseUsage,
	}
}

type enableInstanceSQLHaStandbyDetectionsResponse struct {
	XMLName     xml.Name `xml:"EnableInstanceSqlHaStandbyDetectionsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	InstanceSet struct {
		Items []registeredSQLHaInstanceItem `xml:"item"`
	} `xml:"instanceSet"`
}

type disableInstanceSQLHaStandbyDetectionsResponse struct {
	XMLName     xml.Name `xml:"DisableInstanceSqlHaStandbyDetectionsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	InstanceSet struct {
		Items []registeredSQLHaInstanceItem `xml:"item"`
	} `xml:"instanceSet"`
}

type describeInstanceSQLHaStatesResponse struct {
	XMLName     xml.Name `xml:"DescribeInstanceSqlHaStatesResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	InstanceSet struct {
		Items []registeredSQLHaInstanceItem `xml:"item"`
	} `xml:"instanceSet"`
}

type describeInstanceSQLHaHistoryStatesResponse struct {
	XMLName     xml.Name `xml:"DescribeInstanceSqlHaHistoryStatesResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	InstanceSet struct {
		Items []registeredSQLHaInstanceItem `xml:"item"`
	} `xml:"instanceSet"`
}

// ---- Handlers ----

func (h *Handler) handleEnableInstanceSQLHaStandbyDetections(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")

	regs, err := h.Backend.EnableInstanceSQLHaStandbyDetections(instanceIDs, vals.Get("SqlServerCredentials"))
	if err != nil {
		return nil, err
	}

	resp := &enableInstanceSQLHaStandbyDetectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range regs {
		resp.InstanceSet.Items = append(resp.InstanceSet.Items, toRegisteredSQLHaInstanceItem(r))
	}

	return resp, nil
}

func (h *Handler) handleDisableInstanceSQLHaStandbyDetections(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")

	regs, err := h.Backend.DisableInstanceSQLHaStandbyDetections(instanceIDs)
	if err != nil {
		return nil, err
	}

	resp := &disableInstanceSQLHaStandbyDetectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range regs {
		resp.InstanceSet.Items = append(resp.InstanceSet.Items, toRegisteredSQLHaInstanceItem(r))
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceSQLHaStates(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")
	regs := h.Backend.DescribeInstanceSQLHaStates(instanceIDs)

	resp := &describeInstanceSQLHaStatesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range regs {
		resp.InstanceSet.Items = append(resp.InstanceSet.Items, toRegisteredSQLHaInstanceItem(r))
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceSQLHaHistoryStates(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")
	startTime, _ := time.Parse(time.RFC3339, vals.Get("StartTime"))
	endTime, _ := time.Parse(time.RFC3339, vals.Get("EndTime"))

	regs := h.Backend.DescribeInstanceSQLHaHistoryStates(instanceIDs, startTime, endTime)

	resp := &describeInstanceSQLHaHistoryStatesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range regs {
		resp.InstanceSet.Items = append(resp.InstanceSet.Items, toRegisteredSQLHaInstanceItem(r))
	}

	return resp, nil
}
