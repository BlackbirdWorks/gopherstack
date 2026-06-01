package rekognition

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errValidation        = "ValidationException"

	faceModelVersion = "7.0"
	processorRunning = "RUNNING"
	processorStopped = "STOPPED"

	maxCollectionsPerPage      = 4096
	maxFacesPerPage            = 4096
	maxStreamProcessorsPerPage = 1000
)

var (
	// ErrCollectionNotFound is returned when a collection does not exist.
	ErrCollectionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCollectionAlreadyExists is returned when a collection already exists.
	ErrCollectionAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrStreamProcessorNotFound is returned when a stream processor does not exist.
	ErrStreamProcessorNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrStreamProcessorAlreadyExists is returned when a stream processor already exists.
	ErrStreamProcessorAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrFaceNotFound is returned when a face does not exist in a collection.
	ErrFaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrUnknownOperation is returned when the requested operation is not implemented.
	ErrUnknownOperation = errors.New("unknown operation")
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

// snapshot holds serializable backend state.
type snapshot struct {
	Tags             map[string]map[string]string `json:"tags"`
	AccountID        string                       `json:"accountId"`
	Region           string                       `json:"region"`
	Collections      []*storedCollection          `json:"collections"`
	Faces            []*storedFace                `json:"faces"`
	StreamProcessors []*storedStreamProcessor     `json:"streamProcessors"`
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	collections      map[string]*storedCollection
	faces            map[string][]*storedFace // collectionID -> faces
	streamProcessors map[string]*storedStreamProcessor
	tags             map[string]map[string]string // resourceARN -> tags
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new in-memory backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("rekognition"),
		accountID:        accountID,
		region:           region,
		collections:      make(map[string]*storedCollection),
		faces:            make(map[string][]*storedFace),
		streamProcessors: make(map[string]*storedStreamProcessor),
		tags:             make(map[string]map[string]string),
	}
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) collectionARN(collectionID string) string {
	return fmt.Sprintf("arn:aws:rekognition:%s:%s:collection/%s", b.region, b.accountID, collectionID)
}

func (b *InMemoryBackend) streamProcessorARN(name string) string {
	return fmt.Sprintf("arn:aws:rekognition:%s:%s:streamprocessor/%s", b.region, b.accountID, name)
}

// CreateCollection creates a new face collection.
func (b *InMemoryBackend) CreateCollection(collectionID string, tags map[string]string) (*Collection, error) {
	b.mu.Lock("CreateCollection")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; exists {
		return nil, ErrCollectionAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	c := &storedCollection{
		CollectionID:      collectionID,
		CollectionARN:     b.collectionARN(collectionID),
		FaceModelVersion:  faceModelVersion,
		CreationTimestamp: time.Now(),
		Tags:              tagsCopy,
	}
	b.collections[collectionID] = c

	if len(tagsCopy) > 0 {
		b.tags[c.CollectionARN] = tagsCopy
	}

	return c.toCollection(), nil
}

// DeleteCollection deletes a face collection.
func (b *InMemoryBackend) DeleteCollection(collectionID string) error {
	b.mu.Lock("DeleteCollection")
	defer b.mu.Unlock()

	c, exists := b.collections[collectionID]
	if !exists {
		return ErrCollectionNotFound
	}

	delete(b.collections, collectionID)
	delete(b.faces, collectionID)
	delete(b.tags, c.CollectionARN)

	return nil
}

// DescribeCollection returns details about a collection.
func (b *InMemoryBackend) DescribeCollection(collectionID string) (*Collection, error) {
	b.mu.RLock("DescribeCollection")
	defer b.mu.RUnlock()

	c, exists := b.collections[collectionID]
	if !exists {
		return nil, ErrCollectionNotFound
	}

	result := c.toCollection()
	result.Tags = b.tags[c.CollectionARN]

	return result, nil
}

// ListCollections returns a paginated list of collections.
func (b *InMemoryBackend) ListCollections(maxResults int32, nextToken string) ([]*Collection, string, error) {
	b.mu.RLock("ListCollections")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.collections))
	for id := range b.collections {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i
				break
			}
		}
	}

	limit := int32(maxCollectionsPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := start + int(limit)
	if end > len(ids) {
		end = len(ids)
	}

	result := make([]*Collection, 0, end-start)
	for _, id := range ids[start:end] {
		result = append(result, b.collections[id].toCollection())
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// IndexFaces indexes faces into a collection (simulated — no real image processing).
func (b *InMemoryBackend) IndexFaces(collectionID, externalImageID string) ([]*Face, error) {
	b.mu.Lock("IndexFaces")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	face := &storedFace{
		FaceID:          uuid.NewString(),
		ImageID:         uuid.NewString(),
		ExternalImageID: externalImageID,
		CollectionID:    collectionID,
		Confidence:      99.9,
	}
	b.faces[collectionID] = append(b.faces[collectionID], face)

	return []*Face{face.toFace()}, nil
}

// DeleteFaces removes faces from a collection.
func (b *InMemoryBackend) DeleteFaces(collectionID string, faceIDs []string) ([]string, error) {
	b.mu.Lock("DeleteFaces")
	defer b.mu.Unlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	toDelete := make(map[string]bool, len(faceIDs))
	for _, id := range faceIDs {
		toDelete[id] = true
	}

	var deleted []string
	var remaining []*storedFace

	for _, f := range b.faces[collectionID] {
		if toDelete[f.FaceID] {
			deleted = append(deleted, f.FaceID)
		} else {
			remaining = append(remaining, f)
		}
	}

	b.faces[collectionID] = remaining

	return deleted, nil
}

// ListFaces returns a paginated list of faces in a collection.
func (b *InMemoryBackend) ListFaces(collectionID string, maxResults int32, nextToken string) ([]*Face, string, error) {
	b.mu.RLock("ListFaces")
	defer b.mu.RUnlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, "", ErrCollectionNotFound
	}

	faces := b.faces[collectionID]

	start := 0
	if nextToken != "" {
		for i, f := range faces {
			if f.FaceID == nextToken {
				start = i
				break
			}
		}
	}

	limit := int32(maxFacesPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := start + int(limit)
	if end > len(faces) {
		end = len(faces)
	}

	result := make([]*Face, 0, end-start)
	for _, f := range faces[start:end] {
		result = append(result, f.toFace())
	}

	var outToken string
	if end < len(faces) {
		outToken = faces[end].FaceID
	}

	return result, outToken, nil
}

// SearchFaces searches for faces that match a given face ID.
func (b *InMemoryBackend) SearchFaces(collectionID, faceID string, maxFaces int32) ([]*FaceMatch, error) {
	b.mu.RLock("SearchFaces")
	defer b.mu.RUnlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	var found bool

	for _, f := range b.faces[collectionID] {
		if f.FaceID == faceID {
			found = true
			break
		}
	}

	if !found {
		return nil, ErrFaceNotFound
	}

	limit := int(maxFaces)
	if limit <= 0 {
		limit = 5
	}

	var matches []*FaceMatch

	for _, f := range b.faces[collectionID] {
		if f.FaceID == faceID {
			continue
		}

		matches = append(matches, &FaceMatch{
			Similarity: 90.0,
			Face:       f.toFace(),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// SearchFacesByImage searches for faces matching an image (simulated).
func (b *InMemoryBackend) SearchFacesByImage(collectionID string, maxFaces int32) ([]*FaceMatch, error) {
	b.mu.RLock("SearchFacesByImage")
	defer b.mu.RUnlock()

	if _, exists := b.collections[collectionID]; !exists {
		return nil, ErrCollectionNotFound
	}

	limit := int(maxFaces)
	if limit <= 0 {
		limit = 5
	}

	var matches []*FaceMatch

	for _, f := range b.faces[collectionID] {
		matches = append(matches, &FaceMatch{
			Similarity: 90.0,
			Face:       f.toFace(),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// CreateStreamProcessor creates a new stream processor.
func (b *InMemoryBackend) CreateStreamProcessor(
	name, roleARN string,
	tags map[string]string,
) (*StreamProcessor, error) {
	b.mu.Lock("CreateStreamProcessor")
	defer b.mu.Unlock()

	if _, exists := b.streamProcessors[name]; exists {
		return nil, ErrStreamProcessorAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	arn := b.streamProcessorARN(name)
	p := &storedStreamProcessor{
		Name:               name,
		StreamProcessorARN: arn,
		RoleARN:            roleARN,
		Status:             processorStopped,
		CreationTimestamp:  time.Now(),
		Tags:               tagsCopy,
	}
	b.streamProcessors[name] = p

	if len(tagsCopy) > 0 {
		b.tags[arn] = tagsCopy
	}

	return p.toStreamProcessor(), nil
}

// DeleteStreamProcessor deletes a stream processor.
func (b *InMemoryBackend) DeleteStreamProcessor(name string) error {
	b.mu.Lock("DeleteStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors[name]
	if !exists {
		return ErrStreamProcessorNotFound
	}

	delete(b.streamProcessors, name)
	delete(b.tags, p.StreamProcessorARN)

	return nil
}

// DescribeStreamProcessor returns details about a stream processor.
func (b *InMemoryBackend) DescribeStreamProcessor(name string) (*StreamProcessor, error) {
	b.mu.RLock("DescribeStreamProcessor")
	defer b.mu.RUnlock()

	p, exists := b.streamProcessors[name]
	if !exists {
		return nil, ErrStreamProcessorNotFound
	}

	return p.toStreamProcessor(), nil
}

// ListStreamProcessors returns a paginated list of stream processors.
func (b *InMemoryBackend) ListStreamProcessors(maxResults int32, nextToken string) ([]*StreamProcessor, string, error) {
	b.mu.RLock("ListStreamProcessors")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streamProcessors))
	for name := range b.streamProcessors {
		names = append(names, name)
	}

	sort.Strings(names)

	start := 0
	if nextToken != "" {
		for i, name := range names {
			if name == nextToken {
				start = i
				break
			}
		}
	}

	limit := int32(maxStreamProcessorsPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := start + int(limit)
	if end > len(names) {
		end = len(names)
	}

	result := make([]*StreamProcessor, 0, end-start)
	for _, name := range names[start:end] {
		result = append(result, b.streamProcessors[name].toStreamProcessor())
	}

	var outToken string
	if end < len(names) {
		outToken = names[end]
	}

	return result, outToken, nil
}

// StartStreamProcessor starts a stream processor.
func (b *InMemoryBackend) StartStreamProcessor(name string) error {
	b.mu.Lock("StartStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors[name]
	if !exists {
		return ErrStreamProcessorNotFound
	}

	p.Status = processorRunning

	return nil
}

// StopStreamProcessor stops a stream processor.
func (b *InMemoryBackend) StopStreamProcessor(name string) error {
	b.mu.Lock("StopStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors[name]
	if !exists {
		return ErrStreamProcessorNotFound
	}

	p.Status = processorStopped

	return nil
}

// UpdateStreamProcessor updates a stream processor (no-op for metadata).
func (b *InMemoryBackend) UpdateStreamProcessor(name string) error {
	b.mu.Lock("UpdateStreamProcessor")
	defer b.mu.Unlock()

	if _, exists := b.streamProcessors[name]; !exists {
		return ErrStreamProcessorNotFound
	}

	return nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrCollectionNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrCollectionNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, ErrCollectionNotFound
	}

	tags := make(map[string]string)
	maps.Copy(tags, b.tags[resourceARN])

	return tags, nil
}

func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	for _, c := range b.collections {
		if c.CollectionARN == resourceARN {
			return true
		}
	}

	for _, p := range b.streamProcessors {
		if p.StreamProcessorARN == resourceARN {
			return true
		}
	}

	return false
}

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.collections = make(map[string]*storedCollection)
	b.faces = make(map[string][]*storedFace)
	b.streamProcessors = make(map[string]*storedStreamProcessor)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	colls := make([]*storedCollection, 0, len(b.collections))
	for _, c := range b.collections {
		colls = append(colls, c)
	}

	var faces []*storedFace
	for _, ff := range b.faces {
		faces = append(faces, ff...)
	}

	procs := make([]*storedStreamProcessor, 0, len(b.streamProcessors))
	for _, p := range b.streamProcessors {
		procs = append(procs, p)
	}

	tagsCopy := make(map[string]map[string]string, len(b.tags))
	for arn, t := range b.tags {
		tc := make(map[string]string, len(t))
		maps.Copy(tc, t)
		tagsCopy[arn] = tc
	}

	data, _ := json.Marshal(&snapshot{
		Collections:      colls,
		Faces:            faces,
		StreamProcessors: procs,
		Tags:             tagsCopy,
		AccountID:        b.accountID,
		Region:           b.region,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.accountID = s.AccountID
	b.region = s.Region
	b.collections = make(map[string]*storedCollection, len(s.Collections))

	for _, c := range s.Collections {
		b.collections[c.CollectionID] = c
	}

	b.faces = make(map[string][]*storedFace)

	for _, f := range s.Faces {
		b.faces[f.CollectionID] = append(b.faces[f.CollectionID], f)
	}

	b.streamProcessors = make(map[string]*storedStreamProcessor, len(s.StreamProcessors))

	for _, p := range s.StreamProcessors {
		b.streamProcessors[p.Name] = p
	}

	b.tags = s.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}
