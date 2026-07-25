package pinpoint

import (
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddSegmentInternal seeds a segment directly without going through the HTTP layer.
func (b *InMemoryBackend) AddSegmentInternal(s *Segment) {
	b.mu.Lock("AddSegmentInternal")
	defer b.mu.Unlock()

	b.segments.Put(s)
	b.arnIndex[s.ARN] = s
}

// CreateSegment creates a new Pinpoint segment for an application.
func (b *InMemoryBackend) CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error) {
	b.mu.Lock("CreateSegment")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	segmentARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/segments/%s", appID, id))

	now2 := nowRFC3339()
	segType := segmentTypeDimensional

	if len(req.ImportDefinition) > 0 {
		segType = segmentTypeImport
	}

	s := &Segment{
		ApplicationID:    appID,
		ARN:              segmentARN,
		ID:               id,
		Name:             req.Name,
		SegmentType:      segType,
		Tags:             nonNilTagsCopy(req.Tags),
		Dimensions:       cloneAnyMap(req.Dimensions),
		SegmentGroups:    cloneAnyMap(req.SegmentGroups),
		ImportDefinition: cloneAnyMap(req.ImportDefinition),
		CreationDate:     now2,
		LastModifiedDate: now2,
	}

	s.Version = 1
	b.segments.Put(s)
	b.arnIndex[segmentARN] = s

	// Track segment version history.
	versionKey := appID + "/" + id
	b.segmentVersions[versionKey] = []*Segment{cloneSegment(s)}

	return cloneSegment(s), nil
}

func cloneSegment(s *Segment) *Segment {
	cp := *s
	cp.Tags = nonNilTagsCopy(s.Tags)
	cp.Dimensions = cloneAnyMap(s.Dimensions)
	cp.SegmentGroups = cloneAnyMap(s.SegmentGroups)
	cp.ImportDefinition = cloneAnyMap(s.ImportDefinition)

	return &cp
}

// GetSegment retrieves a Pinpoint segment by appID and segmentID.
func (b *InMemoryBackend) GetSegment(appID, segmentID string) (*Segment, error) {
	b.mu.RLock("GetSegment")
	defer b.mu.RUnlock()

	s, ok := b.segments.Get(segmentID)
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	return cloneSegment(s), nil
}

// GetSegments returns all segments for an application.
func (b *InMemoryBackend) GetSegments(appID string) ([]*Segment, error) {
	b.mu.RLock("GetSegments")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	var segments []*Segment

	for _, s := range b.segments.All() {
		if s.ApplicationID == appID {
			segments = append(segments, cloneSegment(s))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Name < segments[j].Name
	})

	return segments, nil
}

// UpdateSegment updates an existing Pinpoint segment.
func (b *InMemoryBackend) UpdateSegment(
	appID, segmentID string,
	req updateSegmentRequest,
) (*Segment, error) {
	b.mu.Lock("UpdateSegment")
	defer b.mu.Unlock()

	s, ok := b.segments.Get(segmentID)
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	if req.Name != "" {
		s.Name = req.Name
	}

	if len(req.Dimensions) > 0 {
		s.Dimensions = cloneAnyMap(req.Dimensions)
	}

	if len(req.SegmentGroups) > 0 {
		s.SegmentGroups = cloneAnyMap(req.SegmentGroups)
	}

	if len(req.ImportDefinition) > 0 {
		s.ImportDefinition = cloneAnyMap(req.ImportDefinition)
		s.SegmentType = segmentTypeImport
	}

	s.LastModifiedDate = nowRFC3339()
	s.Version++

	// Track segment version history.
	versionKey := appID + "/" + segmentID
	b.segmentVersions[versionKey] = append(b.segmentVersions[versionKey], cloneSegment(s))

	return cloneSegment(s), nil
}

// DeleteSegment deletes a Pinpoint segment.
func (b *InMemoryBackend) DeleteSegment(appID, segmentID string) (*Segment, error) {
	b.mu.Lock("DeleteSegment")
	defer b.mu.Unlock()

	s, ok := b.segments.Get(segmentID)
	if !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	b.segments.Delete(segmentID)
	delete(b.arnIndex, s.ARN)
	delete(b.segmentVersions, appID+"/"+segmentID)

	return cloneSegment(s), nil
}

// GetSegmentVersion returns a specific segment version.
func (b *InMemoryBackend) GetSegmentVersion(
	appID, segmentID string,
	version int,
) (*Segment, error) {
	b.mu.RLock("GetSegmentVersion")
	defer b.mu.RUnlock()

	versionKey := appID + "/" + segmentID
	versions := b.segmentVersions[versionKey]

	for _, v := range versions {
		if v.Version == version {
			return cloneSegment(v), nil
		}
	}

	// AWS's GetSegmentVersion resource docs list 404 NotFoundException as the
	// documented response when "the specified resource was not found" -- a
	// requested version number that isn't in this segment's history is
	// exactly that case, so it must 404 rather than silently substitute the
	// current segment (which would return a Version the caller didn't ask
	// for under the version they did ask for). Mirrors GetCampaignVersion.
	return nil, ErrAppNotFound
}

// GetSegmentVersions returns all stored versions of a segment.
func (b *InMemoryBackend) GetSegmentVersions(appID, segmentID string) ([]*Segment, error) {
	b.mu.RLock("GetSegmentVersions")
	defer b.mu.RUnlock()

	if s, ok := b.segments.Get(segmentID); !ok || s.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	versionKey := appID + "/" + segmentID
	versions := b.segmentVersions[versionKey]

	result := make([]*Segment, len(versions))
	for i, v := range versions {
		result[i] = cloneSegment(v)
	}

	return result, nil
}
