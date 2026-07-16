package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CancelResize ----

type xmlResizeProgress struct {
	TargetNodeType         string   `xml:"TargetNodeType,omitempty"`
	TargetClusterType      string   `xml:"TargetClusterType,omitempty"`
	Status                 string   `xml:"Status"`
	Message                string   `xml:"Message,omitempty"`
	ResizeType             string   `xml:"ResizeType,omitempty"`
	ImportTablesCompleted  []string `xml:"ImportTablesCompleted>member,omitempty"`
	ImportTablesInProgress []string `xml:"ImportTablesInProgress>member,omitempty"`
	ImportTablesNotStarted []string `xml:"ImportTablesNotStarted>member,omitempty"`
	TargetNumberOfNodes    int      `xml:"TargetNumberOfNodes,omitempty"`
	AllowCancelResize      bool     `xml:"AllowCancelResize"`
}

type cancelResizeResponse struct {
	XMLName xml.Name          `xml:"CancelResizeResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  xmlResizeProgress `xml:"CancelResizeResult"`
}

func resizeProgressToXML(rp *ResizeProgress) xmlResizeProgress {
	completed := make([]string, len(rp.ImportTablesCompleted))
	copy(completed, rp.ImportTablesCompleted)
	inProgress := make([]string, len(rp.ImportTablesInProgress))
	copy(inProgress, rp.ImportTablesInProgress)
	notStarted := make([]string, len(rp.ImportTablesNotStarted))
	copy(notStarted, rp.ImportTablesNotStarted)

	return xmlResizeProgress{
		TargetNodeType:         rp.TargetNodeType,
		TargetNumberOfNodes:    rp.TargetNumberOfNodes,
		TargetClusterType:      rp.TargetClusterType,
		Status:                 rp.Status,
		ImportTablesCompleted:  completed,
		ImportTablesInProgress: inProgress,
		ImportTablesNotStarted: notStarted,
		Message:                rp.Message,
		ResizeType:             rp.ResizeType,
		AllowCancelResize:      rp.AllowCancelResize,
	}
}

func (h *Handler) handleCancelResize(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	rp, err := h.Backend.CancelResize(clusterID)
	if err != nil {
		return nil, err
	}

	return &cancelResizeResponse{
		Xmlns:  redshiftXMLNS,
		Result: resizeProgressToXML(rp),
	}, nil
}

// ---- DescribeResize ----

type describeResizeResponse struct {
	XMLName xml.Name          `xml:"DescribeResizeResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  xmlResizeProgress `xml:"DescribeResizeResult"`
}

func (h *Handler) handleDescribeResize(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	rp, err := h.Backend.DescribeResize(clusterID)
	if err != nil {
		return nil, err
	}

	return &describeResizeResponse{
		Xmlns:  redshiftXMLNS,
		Result: resizeProgressToXML(rp),
	}, nil
}
