package rekognition

import "time"

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
	SearchFacesByImage(collectionID string, maxFaces int32) ([]*FaceMatch, error)

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

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
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
	Similarity float64
	Face       *Face
}

// StreamProcessor represents a Rekognition stream processor.
// CreationTimestamp is first so its non-pointer prefix reduces GC pointer bytes.
type StreamProcessor struct {
	CreationTimestamp time.Time
	Tags              map[string]string
	Name              string
	StreamProcessorARN string
	RoleARN           string
	Status            string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
