package rekognition

import (
	"maps"
	"time"
)

// storedCollection holds a face collection with all fields.
// CreationTimestamp is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type storedCollection struct {
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	Tags              map[string]string `json:"tags"`
	CollectionID      string            `json:"collectionId"`
	CollectionARN     string            `json:"collectionArn"`
	FaceModelVersion  string            `json:"faceModelVersion"`
}

func (c *storedCollection) toCollection() *Collection {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	return &Collection{
		CollectionID:      c.CollectionID,
		CollectionARN:     c.CollectionARN,
		FaceModelVersion:  c.FaceModelVersion,
		CreationTimestamp: c.CreationTimestamp,
		Tags:              tags,
	}
}

// storedFace holds an indexed face.
type storedFace struct {
	FaceID          string  `json:"faceId"`
	ImageID         string  `json:"imageId"`
	ExternalImageID string  `json:"externalImageId"`
	CollectionID    string  `json:"collectionId"`
	Confidence      float64 `json:"confidence"`
}

func (f *storedFace) toFace() *Face {
	return &Face{
		FaceID:          f.FaceID,
		ImageID:         f.ImageID,
		ExternalImageID: f.ExternalImageID,
		CollectionID:    f.CollectionID,
		Confidence:      f.Confidence,
	}
}

// storedStreamProcessor holds a stream processor with all fields.
// CreationTimestamp is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type storedStreamProcessor struct {
	CreationTimestamp  time.Time         `json:"creationTimestamp"`
	Tags               map[string]string `json:"tags"`
	Name               string            `json:"name"`
	StreamProcessorARN string            `json:"streamProcessorArn"`
	RoleARN            string            `json:"roleArn"`
	Status             string            `json:"status"`
}

func (p *storedStreamProcessor) toStreamProcessor() *StreamProcessor {
	tags := make(map[string]string, len(p.Tags))
	maps.Copy(tags, p.Tags)

	return &StreamProcessor{
		Name:               p.Name,
		StreamProcessorARN: p.StreamProcessorARN,
		RoleARN:            p.RoleARN,
		Status:             p.Status,
		CreationTimestamp:  p.CreationTimestamp,
		Tags:               tags,
	}
}

// storedProject holds a Rekognition Custom Labels project.
type storedProject struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	ProjectARN        string    `json:"projectArn"`
	Status            string    `json:"status"`
}

func (p *storedProject) toProject() *Project {
	return &Project{
		CreationTimestamp: p.CreationTimestamp,
		ProjectARN:        p.ProjectARN,
		Status:            p.Status,
	}
}

// storedProjectVersion holds a model version within a project.
type storedProjectVersion struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	ProjectVersionARN string    `json:"projectVersionArn"`
	ProjectARN        string    `json:"projectArn"`
	VersionName       string    `json:"versionName"`
	Status            string    `json:"status"`
	StatusMessage     string    `json:"statusMessage"`
	MinInferenceUnits int32     `json:"minInferenceUnits"`
}

func (v *storedProjectVersion) toProjectVersion() *ProjectVersion {
	return &ProjectVersion{
		CreationTimestamp: v.CreationTimestamp,
		ProjectVersionARN: v.ProjectVersionARN,
		ProjectARN:        v.ProjectARN,
		VersionName:       v.VersionName,
		Status:            v.Status,
		StatusMessage:     v.StatusMessage,
		MinInferenceUnits: v.MinInferenceUnits,
	}
}

// storedProjectPolicy holds a project policy.
type storedProjectPolicy struct {
	CreationTimestamp    time.Time `json:"creationTimestamp"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	ProjectARN           string    `json:"projectArn"`
	PolicyName           string    `json:"policyName"`
	PolicyRevisionID     string    `json:"policyRevisionId"`
	PolicyDocument       string    `json:"policyDocument"`
}

func (p *storedProjectPolicy) toProjectPolicy() *ProjectPolicy {
	return &ProjectPolicy{
		CreationTimestamp:    p.CreationTimestamp,
		LastUpdatedTimestamp: p.LastUpdatedTimestamp,
		ProjectARN:           p.ProjectARN,
		PolicyName:           p.PolicyName,
		PolicyRevisionID:     p.PolicyRevisionID,
		PolicyDocument:       p.PolicyDocument,
	}
}

// storedDataset holds a Rekognition Custom Labels dataset.
type storedDataset struct {
	CreationTimestamp    time.Time `json:"creationTimestamp"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	DatasetARN           string    `json:"datasetArn"`
	ProjectARN           string    `json:"projectArn"`
	DatasetType          string    `json:"datasetType"`
	Status               string    `json:"status"`
	StatusMessage        string    `json:"statusMessage"`
}

func (d *storedDataset) toDataset() *Dataset {
	return &Dataset{
		CreationTimestamp:    d.CreationTimestamp,
		LastUpdatedTimestamp: d.LastUpdatedTimestamp,
		DatasetARN:           d.DatasetARN,
		ProjectARN:           d.ProjectARN,
		DatasetType:          d.DatasetType,
		Status:               d.Status,
		StatusMessage:        d.StatusMessage,
	}
}

// storedUser holds a Rekognition user in a collection. CollectionID is
// additive (Phase 3.3): the nested map[string]map[string]*storedUser this
// table replaced implied CollectionID only via its outer key, so the field
// is added here to give the flattened table a composite key. storedUser is
// never marshaled to an AWS-facing response directly (see toUser), so no
// json:"-" hiding is needed -- unlike a live *T type that IS part of the AWS
// wire response shape.
type storedUser struct {
	CollectionID string   `json:"collectionId"`
	UserID       string   `json:"userId"`
	UserStatus   string   `json:"userStatus"`
	FaceIDs      []string `json:"faceIds"`
}

func (u *storedUser) toUser() *User {
	return &User{
		UserID:     u.UserID,
		UserStatus: u.UserStatus,
	}
}

// storedLivenessSession holds a face liveness session.
type storedLivenessSession struct {
	SessionID  string  `json:"sessionId"`
	Status     string  `json:"status"`
	Confidence float32 `json:"confidence"`
}

// storedAsyncJob holds an async video analysis job.
type storedAsyncJob struct {
	JobID        string `json:"jobId"`
	JobType      string `json:"jobType"`
	CollectionID string `json:"collectionId"`
	JobStatus    string `json:"jobStatus"`
	PollCount    int    `json:"pollCount"`
}

// storedMediaAnalysisJob holds a media analysis job.
type storedMediaAnalysisJob struct {
	CreationTimestamp time.Time `json:"creationTimestamp"`
	JobID             string    `json:"jobId"`
	JobName           string    `json:"jobName"`
	Status            string    `json:"status"`
}

func (j *storedMediaAnalysisJob) toMediaAnalysisJob() *MediaAnalysisJob {
	return &MediaAnalysisJob{
		CreationTimestamp: j.CreationTimestamp,
		JobID:             j.JobID,
		JobName:           j.JobName,
		Status:            j.Status,
	}
}
