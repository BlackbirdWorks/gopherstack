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

// CORSConfiguration is the XML structure for a bucket's CORS configuration.
type CORSConfiguration struct {
	XMLName xml.Name   `xml:"CORSConfiguration"`
	Rules   []CORSRule `xml:"CORSRule"`
}

// CORSRule represents a single CORS rule within a CORSConfiguration.
type CORSRule struct {
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedHeaders []string `xml:"AllowedHeader"`
	ExposeHeaders  []string `xml:"ExposeHeader"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty"`
}

// WebsiteConfiguration is the XML body for PutBucketWebsite / GetBucketWebsite.
type WebsiteConfiguration struct {
	XMLName               xml.Name              `xml:"WebsiteConfiguration"`
	Xmlns                 string                `xml:"xmlns,attr,omitempty"`
	IndexDocument         *WebsiteIndexDocument `xml:"IndexDocument,omitempty"`
	ErrorDocument         *WebsiteErrorDocument `xml:"ErrorDocument,omitempty"`
	RedirectAllRequestsTo *WebsiteRedirectAll   `xml:"RedirectAllRequestsTo,omitempty"`
	RoutingRules          []WebsiteRoutingRule  `xml:"RoutingRules>RoutingRule,omitempty"`
}

// WebsiteIndexDocument specifies the suffix of the object used as an index.
type WebsiteIndexDocument struct {
	Suffix string `xml:"Suffix"`
}

// WebsiteErrorDocument specifies the object to return on 4XX errors.
type WebsiteErrorDocument struct {
	Key string `xml:"Key"`
}

// WebsiteRedirectAll configures a redirect for all requests to a given host.
type WebsiteRedirectAll struct {
	HostName string `xml:"HostName"`
	Protocol string `xml:"Protocol,omitempty"`
}

// WebsiteRoutingRule is a single conditional routing rule.
type WebsiteRoutingRule struct {
	Condition *WebsiteRoutingRuleCondition `xml:"Condition,omitempty"`
	Redirect  WebsiteRoutingRuleRedirect   `xml:"Redirect"`
}

// WebsiteRoutingRuleCondition specifies the condition for a routing rule.
type WebsiteRoutingRuleCondition struct {
	KeyPrefixEquals             string `xml:"KeyPrefixEquals,omitempty"`
	HTTPErrorCodeReturnedEquals string `xml:"HttpErrorCodeReturnedEquals,omitempty"`
}

// WebsiteRoutingRuleRedirect specifies the redirect target for a routing rule.
type WebsiteRoutingRuleRedirect struct {
	HostName             string `xml:"HostName,omitempty"`
	Protocol             string `xml:"Protocol,omitempty"`
	ReplaceKeyPrefixWith string `xml:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith       string `xml:"ReplaceKeyWith,omitempty"`
	HTTPRedirectCode     string `xml:"HttpRedirectCode,omitempty"`
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

// ServerSideEncryptionConfiguration is the XML body for PutBucketEncryption / GetBucketEncryption.
type ServerSideEncryptionConfiguration struct {
	XMLName xml.Name                   `xml:"ServerSideEncryptionConfiguration"`
	Xmlns   string                     `xml:"xmlns,attr,omitempty"`
	Rules   []ServerSideEncryptionRule `xml:"Rule"`
}

// ServerSideEncryptionRule represents a single encryption rule.
type ServerSideEncryptionRule struct {
	ApplyServerSideEncryptionByDefault ServerSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault"`
	BucketKeyEnabled                   bool                          `xml:"BucketKeyEnabled,omitempty"`
}

// ServerSideEncryptionByDefault holds the default encryption algorithm and optional KMS key.
type ServerSideEncryptionByDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty"`
}

// PublicAccessBlockConfiguration is the XML body for PutPublicAccessBlock / GetPublicAccessBlock.
type PublicAccessBlockConfiguration struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	Xmlns                 string   `xml:"xmlns,attr,omitempty"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

// OwnershipControlsRule represents a single ownership controls rule.
type OwnershipControlsRule struct {
	ObjectOwnership string `xml:"ObjectOwnership"`
}

// OwnershipControls is the XML body for PutBucketOwnershipControls / GetBucketOwnershipControls.
type OwnershipControls struct {
	XMLName xml.Name                `xml:"OwnershipControls"`
	Xmlns   string                  `xml:"xmlns,attr,omitempty"`
	Rules   []OwnershipControlsRule `xml:"Rule"`
}

// BucketLoggingStatus is the full XML body for PutBucketLogging / GetBucketLogging.
type BucketLoggingStatus struct {
	LoggingEnabled *LoggingEnabled `xml:"LoggingEnabled,omitempty"`
	XMLName        xml.Name        `xml:"BucketLoggingStatus"`
	Xmlns          string          `xml:"xmlns,attr,omitempty"`
}

// LoggingEnabled holds the logging target configuration for a bucket.
type LoggingEnabled struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix"`
}

// ReplicationConfiguration is the XML body for PutBucketReplication / GetBucketReplication.
// The full structure is complex; we store and return the raw XML for fidelity.
type ReplicationConfiguration struct {
	XMLName xml.Name          `xml:"ReplicationConfiguration"`
	Xmlns   string            `xml:"xmlns,attr,omitempty"`
	Role    string            `xml:"Role"`
	Rules   []ReplicationRule `xml:"Rule"`
}

// ReplicationRule is a single replication rule within a ReplicationConfiguration.
type ReplicationRule struct {
	Destination ReplicationDestination `xml:"Destination"`
	ID          string                 `xml:"ID,omitempty"`
	Prefix      string                 `xml:"Prefix,omitempty"`
	Status      string                 `xml:"Status"`
}

// ReplicationDestination specifies the destination bucket for replication.
type ReplicationDestination struct {
	Bucket       string `xml:"Bucket"`
	StorageClass string `xml:"StorageClass,omitempty"`
}
