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
	Contents    []ObjectXML `xml:"Contents"`
	KeyCount    int         `xml:"KeyCount"`
	MaxKeys     int         `xml:"MaxKeys"`
	IsTruncated bool        `xml:"IsTruncated"`
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
	MaxKeys       int                `xml:"MaxKeys"`
	IsTruncated   bool               `xml:"IsTruncated"`
	Versions      []ObjectVersionXML `xml:"Version"`
	DeleteMarkers []DeleteMarkerXML  `xml:"DeleteMarker"`
}

type ObjectVersionXML struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	Owner        *Owner `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
}

type DeleteMarkerXML struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	Owner        *Owner `xml:"Owner"`
}
