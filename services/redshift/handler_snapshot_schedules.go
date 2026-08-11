package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CreateSnapshotSchedule ----

// xmlClusterScheduleAssoc mirrors types.ClusterAssociatedToSchedule. This
// backend only ever produces the "ACTIVE" state (association is applied
// synchronously by ModifyClusterSnapshotSchedule; MODIFYING/FAILED do not occur).
type xmlClusterScheduleAssoc struct {
	ClusterIdentifier        string `xml:"ClusterIdentifier"`
	ScheduleAssociationState string `xml:"ScheduleAssociationState"`
}

// redshift@v1.65.4 deserializers.go:23753 wraps each entry in
// <ClusterAssociatedToSchedule>, not <member>.
type xmlSnapshotSchedule struct {
	ScheduleIdentifier     string                    `xml:"ScheduleIdentifier"`
	Description            string                    `xml:"ScheduleDescription,omitempty"`
	ScheduleDefinitions    []string                  `xml:"ScheduleDefinitions>ScheduleDefinition,omitempty"`
	AssociatedClusters     []xmlClusterScheduleAssoc `xml:"AssociatedClusters>ClusterAssociatedToSchedule,omitempty"`
	AssociatedClusterCount int                       `xml:"AssociatedClusterCount"`
}

// snapshotScheduleToXML converts a backend SnapshotSchedule (with AssociatedClusters
// already populated by the backend, see cloneSnapshotSchedule) into its wire shape.
func snapshotScheduleToXML(s *SnapshotSchedule) xmlSnapshotSchedule {
	assoc := make([]xmlClusterScheduleAssoc, 0, len(s.AssociatedClusters))
	for _, clusterID := range s.AssociatedClusters {
		assoc = append(assoc, xmlClusterScheduleAssoc{
			ClusterIdentifier:        clusterID,
			ScheduleAssociationState: dataShareStatusActive,
		})
	}

	return xmlSnapshotSchedule{
		ScheduleIdentifier:     s.ScheduleIdentifier,
		Description:            s.Description,
		ScheduleDefinitions:    s.ScheduleDefinitions,
		AssociatedClusters:     assoc,
		AssociatedClusterCount: len(s.AssociatedClusters),
	}
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
		Xmlns:    redshiftXMLNS,
		Schedule: snapshotScheduleToXML(sched),
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

	for i := range schedules {
		members = append(members, snapshotScheduleToXML(&schedules[i]))
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
		Xmlns:    redshiftXMLNS,
		Schedule: snapshotScheduleToXML(sched),
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
