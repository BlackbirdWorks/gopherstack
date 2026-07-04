package rekognition

import (
	"context"
	"time"
)

// StorageBackend is the interface for Rekognition storage operations.
type StorageBackend interface {
	CreateCollection(collectionID string, tags map[string]string) (*Collection, error)
	DeleteCollection(collectionID string) error
	DescribeCollection(collectionID string) (*Collection, error)
	ListCollections(maxResults int32, nextToken string) ([]*Collection, string, error)

	IndexFaces(collectionID, externalImageID string) ([]*Face, error)
	DeleteFaces(collectionID string, faceIDs []string) ([]string, error)
	ListFaces(collectionID string, maxResults int32, nextToken string) ([]*Face, string, error)
	SearchFaces(collectionID, faceID string, maxFaces int32) ([]*FaceMatch, error)
	SearchFacesByImage(collectionID string, maxFaces int32, imageKey string) ([]*FaceMatch, error)

	CreateStreamProcessor(name, roleARN string, tags map[string]string) (*StreamProcessor, error)
	DeleteStreamProcessor(name string) error
	DescribeStreamProcessor(name string) (*StreamProcessor, error)
	ListStreamProcessors(maxResults int32, nextToken string) ([]*StreamProcessor, string, error)
	StartStreamProcessor(name string) error
	StopStreamProcessor(name string) error
	UpdateStreamProcessor(name string) error

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Projects and Project Versions
	CreateProject(name string) (*Project, error)
	DeleteProject(projectARN string) error
	DescribeProjects(projectARNs []string, maxResults int32, nextToken string) ([]*Project, string, error)
	CreateProjectVersion(projectARN, versionName string) (*ProjectVersion, error)
	DeleteProjectVersion(projectVersionARN string) error
	DescribeProjectVersions(projectARN string, versionNames []string, maxResults int32, nextToken string) (
		[]*ProjectVersion, string, error)
	CopyProjectVersion(sourceProjectVersionARN, destinationProjectARN, versionName string) (*ProjectVersion, error)
	StartProjectVersion(projectVersionARN string, minInferenceUnits int32) error
	StopProjectVersion(projectVersionARN string) error
	ListProjectPolicies(projectARN string, maxResults int32, nextToken string) ([]*ProjectPolicy, string, error)
	PutProjectPolicy(projectARN, policyName, policyDocument, policyRevisionID string) (string, error)
	DeleteProjectPolicy(projectARN, policyName, policyRevisionID string) error

	// Datasets
	CreateDataset(projectARN, datasetType string) (*Dataset, error)
	DeleteDataset(datasetARN string) error
	DescribeDataset(datasetARN string) (*Dataset, error)
	ListDatasetEntries(datasetARN string, maxResults int32, nextToken string) ([]string, string, error)
	ListDatasetLabels(datasetARN string, maxResults int32, nextToken string) ([]*DatasetLabel, string, error)
	UpdateDatasetEntries(datasetARN string, changes []byte) error
	DistributeDatasetEntries(datasets []DatasetDistribution) error

	// Users
	CreateUser(collectionID, userID string) error
	DeleteUser(collectionID, userID string) error
	ListUsers(collectionID string, maxResults int32, nextToken string) ([]*User, string, error)
	AssociateFaces(
		collectionID, userID string,
		faceIDs []string,
	) ([]*AssociatedFace, []*UnsuccessfulFaceAssociation, error)
	DisassociateFaces(
		collectionID, userID string,
		faceIDs []string,
	) ([]*DisassociatedFace, []*UnsuccessfulFaceDisassociation, error)
	SearchUsers(collectionID, userID string, maxUsers int32) ([]*UserMatch, error)
	SearchUsersByImage(collectionID string, maxUsers int32, imageKey string) ([]*UserMatch, error)

	// Face Liveness
	CreateFaceLivenessSession() (string, error)
	GetFaceLivenessSessionResults(sessionID string) (*LivenessSessionResult, error)

	// Async video jobs
	StartAsyncJob(jobType, collectionID string) (string, error)
	GetAsyncJob(jobID string) (*AsyncJob, error)
	StartMediaAnalysisJob(jobName string) (string, error)
	GetMediaAnalysisJob(jobID string) (*MediaAnalysisJob, error)
	ListMediaAnalysisJobs(maxResults int32, nextToken string) ([]*MediaAnalysisJob, string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Collection represents an Amazon Rekognition face collection.
// CreationTimestamp is first so its non-pointer prefix reduces GC pointer bytes.
type Collection struct {
	CreationTimestamp time.Time
	Tags              map[string]string
	CollectionID      string
	CollectionARN     string
	FaceModelVersion  string
}

// Face represents an indexed face.
type Face struct {
	FaceID          string
	ImageID         string
	ExternalImageID string
	CollectionID    string
	Confidence      float64
}

// FaceMatch represents a face match result.
type FaceMatch struct {
	Face       *Face
	Similarity float64
}

// StreamProcessor represents a Rekognition stream processor.
// CreationTimestamp is first so its non-pointer prefix reduces GC pointer bytes.
type StreamProcessor struct {
	CreationTimestamp  time.Time
	Tags               map[string]string
	Name               string
	StreamProcessorARN string
	RoleARN            string
	Status             string
}

// Project represents a Rekognition Custom Labels project.
type Project struct {
	CreationTimestamp time.Time
	ProjectARN        string
	Status            string
}

// ProjectVersion represents a model version within a project.
type ProjectVersion struct {
	CreationTimestamp time.Time
	ProjectVersionARN string
	ProjectARN        string
	VersionName       string
	Status            string
	StatusMessage     string
	MinInferenceUnits int32
}

// ProjectPolicy represents a project policy.
type ProjectPolicy struct {
	CreationTimestamp    time.Time
	LastUpdatedTimestamp time.Time
	ProjectARN           string
	PolicyName           string
	PolicyRevisionID     string
	PolicyDocument       string
}

// Dataset represents a Rekognition Custom Labels dataset.
type Dataset struct {
	CreationTimestamp    time.Time
	LastUpdatedTimestamp time.Time
	DatasetARN           string
	ProjectARN           string
	DatasetType          string
	Status               string
	StatusMessage        string
}

// DatasetLabel represents a label entry in a dataset.
type DatasetLabel struct {
	LabelName  string
	EntryCount int64
}

// DatasetDistribution is a dataset reference for DistributeDatasetEntries.
type DatasetDistribution struct {
	DatasetARN string
}

// User represents a Rekognition user in a collection.
type User struct {
	UserID     string
	UserStatus string
}

// AssociatedFace is a face successfully associated to a user.
type AssociatedFace struct {
	FaceID string
}

// UnsuccessfulFaceAssociation represents a face that couldn't be associated.
type UnsuccessfulFaceAssociation struct {
	FaceID  string
	Reasons []string
}

// DisassociatedFace is a face successfully disassociated from a user.
type DisassociatedFace struct {
	FaceID string
}

// UnsuccessfulFaceDisassociation represents a face that couldn't be disassociated.
type UnsuccessfulFaceDisassociation struct {
	FaceID  string
	Reasons []string
}

// UserMatch represents a user match result.
type UserMatch struct {
	User       *User
	Similarity float64
}

// LivenessSessionResult holds the result of a face liveness session.
type LivenessSessionResult struct {
	SessionID  string
	Status     string
	Confidence float32
}

// AsyncJob represents a Rekognition async video analysis job.
type AsyncJob struct {
	JobID     string
	JobStatus string
	NextToken string
}

// MediaAnalysisJob represents a Rekognition media analysis job.
type MediaAnalysisJob struct {
	CreationTimestamp time.Time
	JobID             string
	JobName           string
	Status            string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
