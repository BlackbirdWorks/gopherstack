package ssm

import (
	"context"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) sessionsStore(region string) *store.Table[Session] {
	return getOrCreateTable(b, b.sessions, "sessions", region, sessionKeyFn)
}

// DescribeSessions returns sessions from the in-memory store.
func (b *InMemoryBackend) DescribeSessions(
	ctx context.Context,
	input *DescribeSessionsInput,
) (*DescribeSessionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeSessions")
	defer b.mu.RUnlock()

	sessions := b.sessionsStore(region)
	list := make([]Session, 0, sessions.Len())
	for _, s := range sessions.All() {
		if input.State == "" || s.Status == input.State {
			list = append(list, *s)
		}
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].SessionID < list[k].SessionID
	})

	return &DescribeSessionsOutputFull{Sessions: list}, nil
}

// GetConnectionStatus returns the connection status of a target session.
func (b *InMemoryBackend) GetConnectionStatus(
	ctx context.Context,
	input *GetConnectionStatusInput,
) (*GetConnectionStatusOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetConnectionStatus")
	defer b.mu.RUnlock()

	target := input.Target
	status := "notConnected"

	for _, s := range b.sessionsStore(region).All() {
		if s.Target == target && s.Status == sessionStatusConnected {
			status = connectionStatusConnected

			break
		}
	}

	return &GetConnectionStatusOutputFull{Target: target, Status: status}, nil
}

// GetAccessToken returns a mock access token for an active session.
func (b *InMemoryBackend) GetAccessToken(
	_ context.Context,
	input *GetAccessTokenInput,
) (*GetAccessTokenOutputFull, error) {
	b.mu.RLock("GetAccessToken")
	defer b.mu.RUnlock()

	_ = input

	return &GetAccessTokenOutputFull{
		TokenValue: "gph-mock-access-token-" + uuid.NewString(),
	}, nil
}

// ResumeSession resumes a disconnected session.
func (b *InMemoryBackend) ResumeSession(
	ctx context.Context,
	input *ResumeSessionInput,
) (*ResumeSessionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("ResumeSession")
	defer b.mu.Unlock()

	sessions := b.sessionsStore(region)
	sessPtr, exists := sessions.Get(input.SessionID)
	if !exists {
		return &ResumeSessionOutputFull{SessionID: input.SessionID, StreamURL: ""}, nil
	}

	sess := *sessPtr
	sess.Status = sessionStatusConnected
	sessions.Put(&sess)

	return &ResumeSessionOutputFull{
		SessionID:  sess.SessionID,
		StreamURL:  "wss://gopherstack.mock/" + sess.SessionID,
		TokenValue: "gph-resume-token-" + uuid.NewString(),
	}, nil
}

// StartAccessRequest creates an access request record.
func (b *InMemoryBackend) StartAccessRequest(
	_ context.Context,
	input *StartAccessRequestInput,
) (*StartAccessRequestOutputFull, error) {
	b.mu.Lock("StartAccessRequest")
	defer b.mu.Unlock()

	_ = input

	return &StartAccessRequestOutputFull{
		AccessRequestID: "ar-" + uuid.NewString(),
	}, nil
}

// StartSession creates a new SSM Session Manager session.
func (b *InMemoryBackend) StartSession(
	ctx context.Context,
	input *StartSessionInput,
) (*StartSessionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	sessionID := sessionIDPrefix + uuid.NewString()

	sess := Session{
		SessionID:               sessionID,
		Target:                  input.Target,
		Status:                  sessionStatusConnected,
		StartDate:               UnixTimeFloat(timeNow()),
		StreamURL:               "wss://gopherstack-ssm-session/" + sessionID,
		TokenValue:              uuid.NewString(),
		DocumentName:            input.DocumentName,
		Reason:                  input.Reason,
		CloudWatchOutputEnabled: input.CloudWatchOutputEnabled,
		CloudWatchLogGroupName:  input.CloudWatchLogGroupName,
		Parameters:              input.Parameters,
	}

	if input.OutputS3BucketName != "" {
		sess.OutputURL = &SessionOutputS3{
			S3BucketName: input.OutputS3BucketName,
			S3KeyPrefix:  input.OutputS3KeyPrefix,
		}
	}

	b.sessionsStore(region).Put(&sess)

	return &StartSessionOutput{
		SessionID:  sessionID,
		StreamURL:  sess.StreamURL,
		TokenValue: sess.TokenValue,
	}, nil
}

// TerminateSession terminates an active SSM session.
func (b *InMemoryBackend) TerminateSession(
	ctx context.Context,
	input *TerminateSessionInput,
) (*TerminateSessionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	sessions := b.sessionsStore(region)
	sessPtr, exists := sessions.Get(input.SessionID)
	if !exists {
		return &TerminateSessionOutput{SessionID: input.SessionID}, nil
	}

	sess := *sessPtr
	sess.Status = sessionStatusTerminated
	sess.EndDate = UnixTimeFloat(timeNow())
	sessions.Put(&sess)

	// Bound retained terminated (history) sessions so the store cannot grow
	// without limit under repeated Start/Terminate cycles.
	b.evictExcessTerminatedSessionsLocked(region)

	return &TerminateSessionOutput{SessionID: input.SessionID}, nil
}

// evictExcessTerminatedSessionsLocked removes the oldest terminated sessions
// once their count in the region exceeds maxTerminatedSessionsPerRegion.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) evictExcessTerminatedSessionsLocked(region string) {
	sessions := b.sessionsStore(region)

	terminated := make([]Session, 0, sessions.Len())
	for _, s := range sessions.All() {
		if s.Status == sessionStatusTerminated {
			terminated = append(terminated, *s)
		}
	}

	if len(terminated) <= maxTerminatedSessionsPerRegion {
		return
	}

	// Oldest first by EndDate (tie-broken by SessionID for determinism).
	slices.SortFunc(terminated, func(a, c Session) int {
		if a.EndDate != c.EndDate {
			if a.EndDate < c.EndDate {
				return -1
			}

			return 1
		}

		return strings.Compare(a.SessionID, c.SessionID)
	})

	for _, s := range terminated[:len(terminated)-maxTerminatedSessionsPerRegion] {
		sessions.Delete(s.SessionID)
	}
}
