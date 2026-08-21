package ssm

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	connectionStatusNotConnected = "notconnected"
	accessTypeStandard           = "Standard"
	accessRequestIDPrefix        = "ar-"
	accessRequestStatusApproved  = "Approved"
	// jitCredentialTTL is the lifetime of mock temporary credentials minted
	// by GetAccessToken for an approved just-in-time access request.
	jitCredentialTTL = 15 * time.Minute
)

func (b *InMemoryBackend) sessionsStore(region string) *store.Table[Session] {
	return getOrCreateTable(b, b.sessions, "sessions", region, sessionKeyFn)
}

func (b *InMemoryBackend) accessRequestsStore(region string) *store.Table[AccessRequest] {
	return getOrCreateTable(b, b.accessRequests, "accessRequests", region, accessRequestKeyFn)
}

// sessionStateMatchesFilter reports whether a session's real Status
// (Connected/Connecting/Disconnected/Terminated/Terminating/Failed) belongs
// to the coarse State bucket ("Active"/"History") DescribeSessions filters
// on. Per aws-sdk-go-v2/service/ssm@v1.73.4's types.SessionState enum, State
// only ever takes those two values — it is NOT the same enum as
// types.SessionStatus, so a naive `session.Status == input.State` comparison
// (the previous implementation) could never match a real client's request.
func sessionStateMatchesFilter(status, state string) bool {
	if state == "" {
		return true
	}

	switch state {
	case "Active":
		return status == sessionStatusConnected || status == "Connecting"
	case "History":
		return status == sessionStatusTerminated || status == "Terminating" ||
			status == "Disconnected" || status == "Failed"
	default:
		return false
	}
}

// sessionMatchesFilter evaluates one SessionFilter (Key/Value) against a
// session. Key semantics per types.SessionFilterKey: Target/Owner/SessionId/
// AccessType are exact-match; Status is an exact-match against the real
// SessionStatus (unlike the coarse State field); InvokedAfter/InvokedBefore
// compare against StartDate as a Unix-seconds string.
func sessionMatchesFilter(s *Session, f SessionFilter) bool {
	switch f.Key {
	case "Target":
		return s.Target == f.Value
	case "Owner":
		return s.Owner == f.Value
	case "SessionId":
		return s.SessionID == f.Value
	case "AccessType":
		return s.AccessType == f.Value
	case "Status":
		return s.Status == f.Value
	case "InvokedAfter":
		return sessionTimestampCompare(s.StartDate, f.Value) >= 0
	case "InvokedBefore":
		return sessionTimestampCompare(s.StartDate, f.Value) <= 0
	default:
		return true
	}
}

// sessionTimestampCompare compares a session's Unix-seconds StartDate
// against an ISO-8601 filter value, returning -1/0/1. An unparsable filter
// value never excludes a session (fails open), matching this package's
// existing convention for tolerant filter parsing (see parameters.go).
func sessionTimestampCompare(startDate float64, iso8601 string) int {
	t, err := time.Parse(time.RFC3339, iso8601)
	if err != nil {
		return 0
	}

	want := float64(t.Unix())
	switch {
	case startDate < want:
		return -1
	case startDate > want:
		return 1
	default:
		return 0
	}
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
	list := make([]SessionOutput, 0, sessions.Len())

	for _, s := range sessions.All() {
		if !sessionStateMatchesFilter(s.Status, input.State) {
			continue
		}

		matched := true

		for _, f := range input.Filters {
			if !sessionMatchesFilter(s, f) {
				matched = false

				break
			}
		}

		if matched {
			list = append(list, s.toOutput())
		}
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].SessionID < list[k].SessionID
	})

	maxResults := int(input.MaxResults)
	if maxResults <= 0 {
		maxResults = defaultDescribeMaxResults
	}

	start := min(parseNextToken(input.NextToken), len(list))
	end := min(start+maxResults, len(list))

	out := &DescribeSessionsOutputFull{Sessions: list[start:end]}
	if end < len(list) {
		out.NextToken = strconv.Itoa(end)
	}

	return out, nil
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
	status := connectionStatusNotConnected

	for _, s := range b.sessionsStore(region).All() {
		if s.Target == target && s.Status == sessionStatusConnected {
			status = connectionStatusConnected

			break
		}
	}

	return &GetConnectionStatusOutputFull{Target: target, Status: status}, nil
}

// GetAccessToken exchanges an approved just-in-time access request for
// temporary security credentials.
func (b *InMemoryBackend) GetAccessToken(
	ctx context.Context,
	input *GetAccessTokenInput,
) (*GetAccessTokenOutputFull, error) {
	if input.AccessRequestID == "" {
		return nil, ErrValidationException
	}

	region := getRegion(ctx)
	b.mu.RLock("GetAccessToken")
	defer b.mu.RUnlock()

	req, exists := b.accessRequestsStore(region).Get(input.AccessRequestID)
	if !exists {
		return nil, ErrAccessRequestNotFound
	}

	out := &GetAccessTokenOutputFull{AccessRequestStatus: req.Status}
	if req.Status == accessRequestStatusApproved {
		out.Credentials = mockJITCredentials()
	}

	return out, nil
}

// mockJITCredentials fabricates a temporary-credential set for an approved
// just-in-time access request. No real STS token is minted (gopherstack has
// no cross-service call from ssm into sts for this), matching the emulator's
// existing convention of self-contained mock tokens (see StartSession's
// TokenValue / GetAccessToken's old TokenValue field).
func mockJITCredentials() *Credentials {
	return &Credentials{
		AccessKeyID:     "ASIA" + strings.ToUpper(uuid.NewString()[:16]),
		SecretAccessKey: uuid.NewString() + uuid.NewString(),
		SessionToken:    "gph-jit-token-" + uuid.NewString(),
		ExpirationTime:  UnixTimeFloat(timeNow().Add(jitCredentialTTL)),
	}
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

// StartAccessRequest creates a just-in-time node access request. gopherstack
// has no approver workflow to model, so every request is auto-approved
// immediately (Status="Approved") rather than left "Pending" forever, which
// would make GetAccessToken permanently unusable against it.
func (b *InMemoryBackend) StartAccessRequest(
	ctx context.Context,
	input *StartAccessRequestInput,
) (*StartAccessRequestOutputFull, error) {
	if input.Reason == "" || len(input.Targets) == 0 {
		return nil, ErrValidationException
	}

	region := getRegion(ctx)
	b.mu.Lock("StartAccessRequest")
	defer b.mu.Unlock()

	id := accessRequestIDPrefix + uuid.NewString()
	req := AccessRequest{
		AccessRequestID: id,
		Reason:          input.Reason,
		Targets:         input.Targets,
		Status:          accessRequestStatusApproved,
		CreatedAt:       UnixTimeFloat(timeNow()),
	}

	b.accessRequestsStore(region).Put(&req)

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}

		miscTags := b.miscResourceTagsStore(region)
		if miscTags[id] == nil {
			miscTags[id] = make(map[string]string)
		}

		for _, t := range input.Tags {
			miscTags[id][t.Key] = t.Value
		}
	}

	return &StartAccessRequestOutputFull{AccessRequestID: id}, nil
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
		SessionID:    sessionID,
		Target:       input.Target,
		Status:       sessionStatusConnected,
		StartDate:    UnixTimeFloat(timeNow()),
		StreamURL:    "wss://gopherstack-ssm-session/" + sessionID,
		TokenValue:   uuid.NewString(),
		DocumentName: input.DocumentName,
		Reason:       input.Reason,
		Parameters:   input.Parameters,
		AccessType:   accessTypeStandard,
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
