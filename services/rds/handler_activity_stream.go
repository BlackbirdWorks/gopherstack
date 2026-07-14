package rds

import (
	"encoding/xml"
	"net/url"
	"strings"
)

func (h *Handler) handleStartActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	kmsKeyID := vals.Get("KmsKeyId")
	mode := vals.Get("Mode")

	// The resource ARN encodes the cluster identifier at the end
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.StartActivityStream(clusterID, kmsKeyID, mode)
	if err != nil {
		return nil, err
	}

	return startActivityStreamResponse{
		Xmlns:             rdsXMLNS,
		KinesisStreamName: cluster.ActivityStreamKinesisStreamName,
		KMSKeyID:          cluster.ActivityStreamKMSKeyID,
		Status:            cluster.ActivityStreamStatus,
		Mode:              cluster.ActivityStreamMode,
		ApplyImmediately:  true,
	}, nil
}

func (h *Handler) handleStopActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.StopActivityStream(clusterID)
	if err != nil {
		return nil, err
	}

	return stopActivityStreamResponse{
		Xmlns:             rdsXMLNS,
		KinesisStreamName: cluster.ActivityStreamKinesisStreamName,
		KMSKeyID:          cluster.ActivityStreamKMSKeyID,
		Status:            cluster.ActivityStreamStatus,
	}, nil
}

func (h *Handler) handleModifyActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	auditPolicy := vals.Get("AuditPolicyState")
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.ModifyActivityStream(clusterID, auditPolicy)
	if err != nil {
		return nil, err
	}

	return modifyActivityStreamResponse{
		Xmlns:       rdsXMLNS,
		KMSKeyID:    cluster.ActivityStreamKMSKeyID,
		Status:      cluster.ActivityStreamStatus,
		AuditPolicy: cluster.ActivityStreamAuditPolicy,
	}, nil
}

// arnToClusterID extracts the cluster identifier from an ARN like
// "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster".
func arnToClusterID(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return arn
}

type startActivityStreamResponse struct {
	XMLName           xml.Name `xml:"StartActivityStreamResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	KinesisStreamName string   `xml:"StartActivityStreamResult>KinesisStreamName,omitempty"`
	KMSKeyID          string   `xml:"StartActivityStreamResult>KmsKeyId,omitempty"`
	Status            string   `xml:"StartActivityStreamResult>Status,omitempty"`
	Mode              string   `xml:"StartActivityStreamResult>Mode,omitempty"`
	ApplyImmediately  bool     `xml:"StartActivityStreamResult>ApplyImmediately,omitempty"`
}

type stopActivityStreamResponse struct {
	XMLName           xml.Name `xml:"StopActivityStreamResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	KinesisStreamName string   `xml:"StopActivityStreamResult>KinesisStreamName,omitempty"`
	KMSKeyID          string   `xml:"StopActivityStreamResult>KmsKeyId,omitempty"`
	Status            string   `xml:"StopActivityStreamResult>Status,omitempty"`
}

type modifyActivityStreamResponse struct {
	XMLName     xml.Name `xml:"ModifyActivityStreamResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	KMSKeyID    string   `xml:"ModifyActivityStreamResult>KmsKeyId,omitempty"`
	Status      string   `xml:"ModifyActivityStreamResult>Status,omitempty"`
	AuditPolicy string   `xml:"ModifyActivityStreamResult>AuditPolicy,omitempty"`
}
