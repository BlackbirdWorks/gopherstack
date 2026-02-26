package s3

import (
	"encoding/xml"
	"time"
)

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
	XMLName        xml.Name          `xml:"ListBucketResult"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	Marker         string            `xml:"Marker,omitempty"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	Contents       []ObjectXML       `xml:"Contents"`
	CommonPrefixes []CommonPrefixXML `xml:"CommonPrefixes,omitempty"`
	KeyCount       int               `xml:"KeyCount"`
	MaxKeys        int               `xml:"MaxKeys"`
	IsTruncated    bool              `xml:"IsTruncated"`
}

// ListBucketV2Result is the XML response for ListObjectsV2.
type ListBucketV2Result struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	Prefix                string            `xml:"Prefix"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	Name                  string            `xml:"Name"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	Contents              []ObjectXML       `xml:"Contents"`
	CommonPrefixes        []CommonPrefixXML `xml:"CommonPrefixes"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	IsTruncated           bool              `xml:"IsTruncated"`
}

// CommonPrefixXML represents a common prefix entry in a listing response.
type CommonPrefixXML struct {
	Prefix string `xml:"Prefix"`
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

// LocationConstraintResponse is the XML response body for GetBucketLocation.
type LocationConstraintResponse struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Region  string   `xml:",chardata"`
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
	ETag       string `xml:"ETag"`
	PartNumber int    `xml:"PartNumber"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// Bulk Delete Structures

type DeleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []DeleteObject `xml:"Object"`
	Quiet   bool           `xml:"Quiet"`
}

type DeleteObject struct {
	VersionID *string `xml:"VersionId,omitempty"`
	Key       string  `xml:"Key"`
}

type DeleteResult struct {
	XMLName xml.Name         `xml:"DeleteResult"`
	Deleted []DeletedXML     `xml:"Deleted"`
	Errors  []DeleteErrorXML `xml:"Error"`
}

type DeletedXML struct {
	VersionID             *string `xml:"VersionId,omitempty"`
	DeleteMarkerVersionID *string `xml:"DeleteMarkerVersionId,omitempty"`
	Key                   string  `xml:"Key"`
	DeleteMarker          bool    `xml:"DeleteMarker,omitempty"`
}

type DeleteErrorXML struct {
	VersionID *string `xml:"VersionId,omitempty"`
	Key       string  `xml:"Key"`
	Code      string  `xml:"Code"`
	Message   string  `xml:"Message"`
}

// AccessControlPolicy is the XML response for GetBucketAcl.
type AccessControlPolicy struct {
	XMLName xml.Name          `xml:"AccessControlPolicy"`
	Xmlns   string            `xml:"xmlns,attr"`
	Owner   Owner             `xml:"Owner"`
	ACL     AccessControlList `xml:"AccessControlList"`
}

// AccessControlList contains the list of grants.
type AccessControlList struct {
	Grants []Grant `xml:"Grant"`
}

// Grant is a single permission grant.
type Grant struct {
	Grantee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

// Grantee identifies who is being granted permissions.
type Grantee struct {
	XmlnsXsi string `xml:"xmlns:xsi,attr"`
	XsiType  string `xml:"xsi:type,attr"`
	ID       string `xml:"ID"`
}

// ListMultipartUploadsResult is the XML response for ListMultipartUploads.
//

type ListMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns              string            `xml:"xmlns,attr,omitempty"`
	Bucket             string            `xml:"Bucket"`
	Prefix             string            `xml:"Prefix,omitempty"`
	KeyMarker          string            `xml:"KeyMarker,omitempty"`
	UploadIDMarker     string            `xml:"UploadIdMarker,omitempty"`
	NextKeyMarker      string            `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker,omitempty"`
	Uploads            []MultipartUpload `xml:"Upload"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
}

// MultipartUpload describes a single in-progress multipart upload.
type MultipartUpload struct {
	Initiated time.Time `xml:"Initiated"`
	Key       string    `xml:"Key"`
	UploadID  string    `xml:"UploadId"`
}

// ObjectLockConfiguration is the XML body for PutObjectLockConfiguration / GetObjectLockConfiguration.
type ObjectLockConfiguration struct {
	Rule              *ObjectLockRule `xml:"Rule,omitempty"`
	XMLName           xml.Name        `xml:"ObjectLockConfiguration"`
	Xmlns             string          `xml:"xmlns,attr,omitempty"`
	ObjectLockEnabled string          `xml:"ObjectLockEnabled"`
}

// ObjectLockRule holds the default retention rule for a bucket.
type ObjectLockRule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention,omitempty"`
}

// DefaultRetention specifies the default retention settings for objects placed in the bucket.
type DefaultRetention struct {
	Mode  string `xml:"Mode"`
	Days  int    `xml:"Days,omitempty"`
	Years int    `xml:"Years,omitempty"`
}

// ObjectRetention is the XML body for PutObjectRetention / GetObjectRetention.
type ObjectRetention struct {
	XMLName         xml.Name `xml:"Retention"`
	Xmlns           string   `xml:"xmlns,attr,omitempty"`
	Mode            string   `xml:"Mode"`
	RetainUntilDate string   `xml:"RetainUntilDate"`
}

// ObjectLegalHold is the XML body for PutObjectLegalHold / GetObjectLegalHold.
type ObjectLegalHold struct {
	XMLName xml.Name `xml:"LegalHold"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status"`
}

// ListPartsResult is the XML response for ListParts.
//

type ListPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr,omitempty"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	Parts                []PartXML `xml:"Part"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
}

// PartXML describes a single uploaded part in a multipart upload.
type PartXML struct {
	ETag       string `xml:"ETag"`
	Size       int64  `xml:"Size"`
	PartNumber int    `xml:"PartNumber"`
}
