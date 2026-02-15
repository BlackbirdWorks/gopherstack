package s3

import "encoding/xml"

type ListAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Owner   *Owner      `xml:"Owner"`
	Buckets []BucketXML `xml:"Buckets>Bucket"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type BucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListBucketResult struct {
	XMLName     xml.Name    `xml:"ListBucketResult"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	Marker      string      `xml:"Marker,omitempty"`
	NextMarker  string      `xml:"NextMarker,omitempty"`
	Contents    []ObjectXML `xml:"Contents"`
	KeyCount    int         `xml:"KeyCount"`
	MaxKeys     int         `xml:"MaxKeys"`
	IsTruncated bool        `xml:"IsTruncated"`
}

type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type ObjectXML struct {
	Key               string `xml:"Key"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag"`
	StorageClass      string `xml:"StorageClass"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	Size              int64  `xml:"Size"`
}

type VersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"` // "Enabled" or "Suspended"
}

type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  TagSet   `xml:"TagSet"`
}

type TagSet struct {
	Tags []Tag `xml:"Tag"`
}

type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

type ListVersionsResult struct {
	XMLName       xml.Name           `xml:"ListVersionsResult"`
	Name          string             `xml:"Name"`
	Prefix        string             `xml:"Prefix"`
	KeyMarker     string             `xml:"KeyMarker"`
	VersionMarker string             `xml:"VersionIdMarker"`
	Versions      []ObjectVersionXML `xml:"Version"`
	DeleteMarkers []DeleteMarkerXML  `xml:"DeleteMarker"`
	MaxKeys       int                `xml:"MaxKeys"`
	IsTruncated   bool               `xml:"IsTruncated"`
}

type ObjectVersionXML struct {
	Owner        *Owner `xml:"Owner"`
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	StorageClass string `xml:"StorageClass"`
	Size         int64  `xml:"Size"`
	IsLatest     bool   `xml:"IsLatest"`
}

type DeleteMarkerXML struct {
	Owner        *Owner `xml:"Owner"`
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	LastModified string `xml:"LastModified"`
	IsLatest     bool   `xml:"IsLatest"`
}

// Multipart Upload Structures

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPartXML `xml:"Part"`
}

type CompletedPartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}
