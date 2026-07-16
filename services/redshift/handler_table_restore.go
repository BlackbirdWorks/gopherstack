package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- RestoreTableFromClusterSnapshot ----

type restoreTableFromClusterSnapshotResponse struct {
	XMLName xml.Name `xml:"RestoreTableFromClusterSnapshotResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		TableRestoreStatus struct {
			TableRestoreRequestID string `xml:"TableRestoreRequestId"`
			Status                string `xml:"Status"`
		} `xml:"TableRestoreStatus"`
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
	resp.Result.TableRestoreStatus.TableRestoreRequestID = tr.TableRestoreRequestID
	resp.Result.TableRestoreStatus.Status = tr.Status

	return resp, nil
}

// ---- DescribeTableRestoreStatus ----

type xmlTableRestoreStatus struct {
	TableRestoreRequestID string `xml:"TableRestoreRequestId"`
	ClusterIdentifier     string `xml:"ClusterIdentifier"`
	Status                string `xml:"Status,omitempty"`
	Message               string `xml:"Message,omitempty"`
	SourceDatabaseName    string `xml:"SourceDatabaseName,omitempty"`
	SourceTableName       string `xml:"SourceTableName,omitempty"`
	TargetDatabaseName    string `xml:"TargetDatabaseName,omitempty"`
	TargetTableName       string `xml:"TargetTableName,omitempty"`
}

type xmlTableRestoreStatusList struct {
	Statuses []xmlTableRestoreStatus `xml:"TableRestoreStatus"`
}

type describeTableRestoreStatusResponse struct {
	XMLName  xml.Name                  `xml:"DescribeTableRestoreStatusResponse"`
	Xmlns    string                    `xml:"xmlns,attr"`
	Statuses xmlTableRestoreStatusList `xml:"DescribeTableRestoreStatusResult>TableRestoreStatusDetails"`
}

func (h *Handler) handleDescribeTableRestoreStatus(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	statuses, err := h.Backend.DescribeTableRestoreStatus(clusterID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTableRestoreStatus, 0, len(statuses))

	for _, s := range statuses {
		members = append(members, xmlTableRestoreStatus{
			TableRestoreRequestID: s.TableRestoreRequestID,
			ClusterIdentifier:     s.ClusterIdentifier,
			Status:                s.Status,
			Message:               s.Message,
			SourceDatabaseName:    s.SourceDatabaseName,
			SourceTableName:       s.SourceTableName,
			TargetDatabaseName:    s.TargetDatabaseName,
			TargetTableName:       s.TargetTableName,
		})
	}

	return &describeTableRestoreStatusResponse{
		Xmlns:    redshiftXMLNS,
		Statuses: xmlTableRestoreStatusList{Statuses: members},
	}, nil
}
