package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CreateSnapshotSchedule ----

type xmlSnapshotSchedule struct {
	ScheduleIdentifier  string   `xml:"ScheduleIdentifier"`
	Description         string   `xml:"ScheduleDescription,omitempty"`
	ScheduleDefinitions []string `xml:"ScheduleDefinitions>ScheduleDefinition,omitempty"`
}

type createSnapshotScheduleResponse struct {
	XMLName  xml.Name            `xml:"CreateSnapshotScheduleResponse"`
	Xmlns    string              `xml:"xmlns,attr"`
	Schedule xmlSnapshotSchedule `xml:"CreateSnapshotScheduleResult"`
}

func (h *Handler) handleCreateSnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")
	description := vals.Get("ScheduleDescription")
	definitions := parseStringList(vals, "ScheduleDefinitions.ScheduleDefinition")
	tagMap := parseRedshiftTags(vals)

	sched, err := h.Backend.CreateSnapshotSchedule(scheduleID, description, definitions, tagMap)
	if err != nil {
		return nil, err
	}

	return &createSnapshotScheduleResponse{
		Xmlns: redshiftXMLNS,
		Schedule: xmlSnapshotSchedule{
			ScheduleIdentifier:  sched.ScheduleIdentifier,
			Description:         sched.Description,
			ScheduleDefinitions: sched.ScheduleDefinitions,
		},
	}, nil
}

// ---- DeleteSnapshotSchedule ----

type deleteSnapshotScheduleResponse struct {
	XMLName   xml.Name `xml:"DeleteSnapshotScheduleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteSnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")

	if err := h.Backend.DeleteSnapshotSchedule(scheduleID); err != nil {
		return nil, err
	}

	return &deleteSnapshotScheduleResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeSnapshotSchedules ----

type xmlSnapshotScheduleList struct {
	Schedules []xmlSnapshotSchedule `xml:"SnapshotSchedule"`
}

type describeSnapshotSchedulesResponse struct {
	XMLName   xml.Name                `xml:"DescribeSnapshotSchedulesResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	Schedules xmlSnapshotScheduleList `xml:"DescribeSnapshotSchedulesResult>SnapshotSchedules"`
}

func (h *Handler) handleDescribeSnapshotSchedules(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")

	schedules, err := h.Backend.DescribeSnapshotSchedules(scheduleID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlSnapshotSchedule, 0, len(schedules))

	for _, s := range schedules {
		members = append(members, xmlSnapshotSchedule{
			ScheduleIdentifier:  s.ScheduleIdentifier,
			Description:         s.Description,
			ScheduleDefinitions: s.ScheduleDefinitions,
		})
	}

	return &describeSnapshotSchedulesResponse{
		Xmlns:     redshiftXMLNS,
		Schedules: xmlSnapshotScheduleList{Schedules: members},
	}, nil
}

// ---- ModifySnapshotSchedule ----

type modifySnapshotScheduleResponse struct {
	XMLName  xml.Name            `xml:"ModifySnapshotScheduleResponse"`
	Xmlns    string              `xml:"xmlns,attr"`
	Schedule xmlSnapshotSchedule `xml:"ModifySnapshotScheduleResult"`
}

func (h *Handler) handleModifySnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")
	definitions := parseStringList(vals, "ScheduleDefinitions.ScheduleDefinition")

	sched, err := h.Backend.ModifySnapshotSchedule(scheduleID, definitions)
	if err != nil {
		return nil, err
	}

	return &modifySnapshotScheduleResponse{
		Xmlns: redshiftXMLNS,
		Schedule: xmlSnapshotSchedule{
			ScheduleIdentifier:  sched.ScheduleIdentifier,
			Description:         sched.Description,
			ScheduleDefinitions: sched.ScheduleDefinitions,
		},
	}, nil
}

// ---- ModifyClusterSnapshotSchedule ----

type modifyClusterSnapshotScheduleResponse struct {
	XMLName   xml.Name `xml:"ModifyClusterSnapshotScheduleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleModifyClusterSnapshotSchedule(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	scheduleID := vals.Get("ScheduleIdentifier")
	disassociate := vals.Get("DisassociateSchedule") == paramValueTrue

	if err := h.Backend.ModifyClusterSnapshotSchedule(clusterID, scheduleID, disassociate); err != nil {
		return nil, err
	}

	return &modifyClusterSnapshotScheduleResponse{Xmlns: redshiftXMLNS}, nil
}
