package redshift

import (
	"fmt"
	"strconv"
)

// ---------------------------------------------------------------------------
// Serverless Tracks
//
// GetTrack/ListTracks read a static, built-in catalog (defaultServerlessTracks)
// rather than any store.Table -- there is no CreateTrack/DeleteTrack operation
// anywhere in the real API (confirmed by enumerating every operation name in
// the pinned service-2.json), matching the read-only-catalog treatment
// classic Redshift's own DescribeClusterTracks already established
// (handler_cluster_info.go).
// ---------------------------------------------------------------------------

func defaultServerlessTracks() []*ServerlessTrack {
	return []*ServerlessTrack{
		{TrackName: slTrackCurrent, WorkgroupVersion: modelVersion10},
		{TrackName: slTrackTrailing, WorkgroupVersion: modelVersion10},
	}
}

// GetServerlessTrack returns a single named maintenance track.
func (b *InMemoryBackend) GetServerlessTrack(trackName string) (*ServerlessTrack, error) {
	b.mu.RLock("GetServerlessTrack")
	defer b.mu.RUnlock()

	for _, t := range defaultServerlessTracks() {
		if t.TrackName == trackName {
			cp := *t

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: track %q not found", ErrServerlessTrackNotFound, trackName)
}

// ListServerlessTracks returns the fixed set of tracks, paginated the same
// way every other serverless List op is.
func (b *InMemoryBackend) ListServerlessTracks(maxResults int, nextToken string) ([]*ServerlessTrack, string) {
	b.mu.RLock("ListServerlessTracks")
	defer b.mu.RUnlock()

	list := defaultServerlessTracks()

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*ServerlessTrack{}, ""
	}

	end := startIdx + maxResults

	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	out := make([]*ServerlessTrack, 0, end-startIdx)
	for _, t := range list[startIdx:end] {
		cp := *t
		out = append(out, &cp)
	}

	return out, outToken
}
