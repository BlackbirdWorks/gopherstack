package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/url"
)

// createStorageLocationResult is the result body for CreateStorageLocation.
type createStorageLocationResult struct {
	S3Bucket string `xml:"S3Bucket"`
}

// createStorageLocationResponse is the XML response for CreateStorageLocation.
type createStorageLocationResponse struct {
	XMLName                     xml.Name                    `xml:"CreateStorageLocationResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	CreateStorageLocationResult createStorageLocationResult `xml:"CreateStorageLocationResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

// handleCreateStorageLocation creates (or returns) the S3 storage bucket.
func (h *Handler) handleCreateStorageLocation(ctx context.Context, _ url.Values) (any, error) {
	bucket := h.Backend.CreateStorageLocation(ctx)

	return &createStorageLocationResponse{
		Xmlns:                       ebXMLNS,
		CreateStorageLocationResult: createStorageLocationResult{S3Bucket: bucket},
		ResponseMetadata:            responseMetadata{RequestID: "eb-create-storage"},
	}, nil
}
