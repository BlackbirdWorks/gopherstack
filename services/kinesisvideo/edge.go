package kinesisvideo

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	syncStatusSyncing = "SYNCING"
	syncStatusInSync  = "IN_SYNC"

	// edgeSyncDelay is how long a pushed edge config stays SYNCING before the
	// emulator reports IN_SYNC, standing in for the edge agent's acknowledgement.
	edgeSyncDelay = 2 * time.Second

	minEdgeScheduleSeconds = 60
	maxEdgeScheduleSeconds = 86400
)

func validEdgeConfig(cfg *EdgeConfig) error {
	if cfg.HubDeviceARN == "" || cfg.Recorder == nil || cfg.Recorder.MediaSource == nil {
		return ErrValidation
	}

	switch cfg.Recorder.MediaSource.MediaURIType {
	case "RTSP_URI", "FILE_URI":
	default:
		return ErrValidation
	}

	for _, sc := range []*EdgeSchedule{cfg.Recorder.Schedule, uploaderSchedule(cfg.Uploader)} {
		if sc != nil &&
			(sc.DurationInSeconds < minEdgeScheduleSeconds || sc.DurationInSeconds > maxEdgeScheduleSeconds) {
			return ErrValidation
		}
	}

	if d := cfg.Deletion; d != nil && d.LocalSize != nil {
		switch d.LocalSize.StrategyOnFullSize {
		case "", "DELETE_OLDEST_MEDIA", "DENY_NEW_MEDIA":
		default:
			return ErrValidation
		}
	}

	return nil
}

func uploaderSchedule(u *EdgeUploader) *EdgeSchedule {
	if u == nil {
		return nil
	}

	return u.Schedule
}

// edgeViewLocked returns a copy of the stream's edge state with the effective
// sync status. Callers must hold b.mu.
func edgeViewLocked(s *Stream, now time.Time) *EdgeState {
	v := s.Edge.clone()
	v.StreamARN = s.ARN
	v.StreamName = s.Name

	if v.SyncStatus == syncStatusSyncing && now.Sub(v.LastUpdatedTime) >= edgeSyncDelay {
		v.SyncStatus = syncStatusInSync
	}

	return v
}

// StartEdgeConfigurationUpdate stores or replaces a stream's edge agent configuration.
func (b *InMemoryBackend) StartEdgeConfigurationUpdate(name, streamARN string, cfg EdgeConfig) (*EdgeState, error) {
	b.mu.Lock("StartEdgeConfigurationUpdate")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	if err = validEdgeConfig(&cfg); err != nil {
		return nil, err
	}

	if s.DataRetentionInHours == 0 {
		return nil, ErrNoDataRetention
	}

	now := time.Now().UTC()
	created := now

	if s.Edge != nil {
		created = s.Edge.CreationTime
	}

	s.Edge = &EdgeState{
		CreationTime:    created,
		LastUpdatedTime: now,
		SyncStatus:      syncStatusSyncing,
		Config:          cfg.clone(),
	}

	v := s.Edge.clone()
	v.StreamARN = s.ARN
	v.StreamName = s.Name

	return v, nil
}

// DescribeEdgeConfiguration returns a stream's edge agent configuration.
func (b *InMemoryBackend) DescribeEdgeConfiguration(name, streamARN string) (*EdgeState, error) {
	b.mu.RLock("DescribeEdgeConfiguration")
	defer b.mu.RUnlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	if s.Edge == nil {
		return nil, ErrEdgeConfigNotFound
	}

	return edgeViewLocked(s, time.Now().UTC()), nil
}

// DeleteEdgeConfiguration removes a stream's edge agent configuration.
func (b *InMemoryBackend) DeleteEdgeConfiguration(name, streamARN string) error {
	b.mu.Lock("DeleteEdgeConfiguration")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if s.Edge == nil {
		return ErrEdgeConfigNotFound
	}

	s.Edge = nil

	return nil
}

// ListEdgeAgentConfigurations lists the edge configurations of streams bound to hubDeviceARN.
func (b *InMemoryBackend) ListEdgeAgentConfigurations(
	hubDeviceARN, nextToken string,
	maxResults int,
) ([]*EdgeState, string, error) {
	if hubDeviceARN == "" {
		return nil, "", ErrValidation
	}

	b.mu.RLock("ListEdgeAgentConfigurations")
	defer b.mu.RUnlock()

	now := time.Now().UTC()
	all := b.streams.All()
	matched := make([]*EdgeState, 0, len(all))

	for _, s := range all {
		if s.Edge != nil && s.Edge.Config.HubDeviceARN == hubDeviceARN {
			matched = append(matched, edgeViewLocked(s, now))
		}
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].StreamName < matched[j].StreamName })

	p := page.New(matched, nextToken, maxResults, defaultListEdgeLimit)

	return p.Data, p.Next, nil
}
