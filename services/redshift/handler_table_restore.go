package redshift

import (
	"encoding/xml"
	"net/url"
	"time"
)

// ---- RestoreTableFromClusterSnapshot ----

type restoreTableFromClusterSnapshotResponse struct {
	XMLName xml.Name `xml:"RestoreTableFromClusterSnapshotResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		TableRestoreStatus xmlTableRestoreStatus `xml:"TableRestoreStatus"`
	} `xml:"RestoreTableFromClusterSnapshotResult"`
}

func (h *Handler) handleRestoreTableFromClusterSnapshot(vals url.Values) (any, error) {
	tr, err := h.Backend.CreateTableRestoreStatus(
		vals.Get("ClusterIdentifier"),
		vals.Get("SnapshotIdentifier"),
		vals.Get("SourceDatabaseName"),
		vals.Get("SourceTableName"),
		vals.Get("TargetDatabaseName"),
		vals.Get("NewTableName"),
	)
	if err != nil {
		return nil, err
	}

	resp := &restoreTableFromClusterSnapshotResponse{Xmlns: redshiftXMLNS}
	resp.Result.TableRestoreStatus = tableRestoreStatusToXML(tr)

	return resp, nil
}

// ---- DescribeTableRestoreStatus ----

// xmlTableRestoreStatus mirrors types.TableRestoreStatus. Note the real wire field
// for the destination table is "NewTableName", not "TargetTableName" (confirmed
// against aws-sdk-go-v2/service/redshift@v1.62.3's deserializer) -- this backend's
// internal TableRestoreStatus.TargetTableName field predates that check and is kept
// as the Go name for continuity, but is serialized under the correct wire tag.
type xmlTableRestoreStatus struct {
	TableRestoreRequestID string `xml:"TableRestoreRequestId"`
	ClusterIdentifier     string `xml:"ClusterIdentifier"`
	SnapshotIdentifier    string `xml:"SnapshotIdentifier,omitempty"`
	RequestTime           string `xml:"RequestTime,omitempty"`
	Status                string `xml:"Status,omitempty"`
	Message               string `xml:"Message,omitempty"`
	SourceDatabaseName    string `xml:"SourceDatabaseName,omitempty"`
	SourceTableName       string `xml:"SourceTableName,omitempty"`
	TargetDatabaseName    string `xml:"TargetDatabaseName,omitempty"`
	NewTableName          string `xml:"NewTableName,omitempty"`
}

type xmlTableRestoreStatusList struct {
	Statuses []xmlTableRestoreStatus `xml:"TableRestoreStatus"`
}

type describeTableRestoreStatusResponse struct {
	XMLName  xml.Name                  `xml:"DescribeTableRestoreStatusResponse"`
	Xmlns    string                    `xml:"xmlns,attr"`
	Statuses xmlTableRestoreStatusList `xml:"DescribeTableRestoreStatusResult>TableRestoreStatusDetails"`
}

func tableRestoreStatusToXML(s *TableRestoreStatus) xmlTableRestoreStatus {
	x := xmlTableRestoreStatus{
		TableRestoreRequestID: s.TableRestoreRequestID,
		ClusterIdentifier:     s.ClusterIdentifier,
		SnapshotIdentifier:    s.SnapshotIdentifier,
		Status:                s.Status,
		Message:               s.Message,
		SourceDatabaseName:    s.SourceDatabaseName,
		SourceTableName:       s.SourceTableName,
		TargetDatabaseName:    s.TargetDatabaseName,
		NewTableName:          s.TargetTableName,
	}

	if !s.RequestTime.IsZero() {
		x.RequestTime = s.RequestTime.Format(time.RFC3339)
	}

	return x
}

func (h *Handler) handleDescribeTableRestoreStatus(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	requestID := vals.Get("TableRestoreRequestId")

	statuses, err := h.Backend.DescribeTableRestoreStatus(clusterID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTableRestoreStatus, 0, len(statuses))

	for i := range statuses {
		// If you don't specify a TableRestoreRequestId, DescribeTableRestoreStatus
		// returns the status of all IN-PROGRESS requests, not every request ever
		// made (api_op_DescribeTableRestoreStatus.go) -- a completed request must
		// still be reachable by its own TableRestoreRequestId.
		if requestID != "" {
			if statuses[i].TableRestoreRequestID != requestID {
				continue
			}
		} else if statuses[i].Status != tableRestoreStatusInProgress {
			continue
		}

		members = append(members, tableRestoreStatusToXML(&statuses[i]))
	}

	return &describeTableRestoreStatusResponse{
		Xmlns:    redshiftXMLNS,
		Statuses: xmlTableRestoreStatusList{Statuses: members},
	}, nil
}
