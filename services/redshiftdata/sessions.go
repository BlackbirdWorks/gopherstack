package redshiftdata

import (
	"context"
	"fmt"
	"sort"
	"time"
)

const (
	// statusSessionAvailable is the AVAILABLE status for a Redshift Data API session
	// (open and ready to run a SQL statement).
	statusSessionAvailable = "AVAILABLE"
	// statusSessionBusy is never reported: statements complete synchronously.
	statusSessionBusy = "BUSY"
	// statusSessionClosed is reported once a session's TTL has passed.
	statusSessionClosed = "CLOSED"

	// defaultListSessionsResults is the default page size for ListSessions when
	// MaxResults is unset. The installed SDK's ListSessionsInput.MaxResults doc
	// comment (go doc, aws-sdk-go-v2/service/redshiftdata) states no explicit
	// numeric bound the way ListStatementsInput's does, so no upper-bound
	// ValidationException is enforced here -- only a default page size, matching
	// this package's other List* conventions.
	defaultListSessionsResults = 100

	maxSessionLifetime = 24 * time.Hour
)

// ValidateListSessionsRequest enforces the mutual-exclusivity constraints documented
// on ListSessionsInput (aws-sdk-go-v2/service/redshiftdata's api_op_ListSessions.go):
// SessionId can't be combined with Status/ClusterIdentifier/WorkgroupName/Database, and
// ClusterIdentifier/WorkgroupName can't both be set.
func ValidateListSessionsRequest(sessionID, status, clusterIdentifier, workgroupName, database string) error {
	if sessionID != "" && (status != "" || clusterIdentifier != "" || workgroupName != "" || database != "") {
		return fmt.Errorf(
			"%w: SessionId can't be specified together with Status, ClusterIdentifier, WorkgroupName, or Database",
			ErrValidation,
		)
	}

	if clusterIdentifier != "" && workgroupName != "" {
		return fmt.Errorf(
			"%w: specify either ClusterIdentifier or WorkgroupName, not both",
			ErrValidation,
		)
	}

	switch status {
	case "", statusSessionAvailable, statusSessionBusy, statusSessionClosed:
		return nil
	default:
		return fmt.Errorf(
			"%w: Status %q is invalid; valid values are AVAILABLE, BUSY, CLOSED",
			ErrValidation, status,
		)
	}
}

// ListSessions returns a page of sessions derived from stored statements that share a
// non-empty SessionID (see groupSessions), sorted newest-first by CreatedAt to match
// ListStatements' ordering convention.
func (b *InMemoryBackend) ListSessions(
	ctx context.Context,
	filter ListSessionsFilter,
) ([]*SessionData, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	store := b.storeForRead(region)
	if store == nil {
		return nil, "", nil
	}

	caller := callerFromContext(ctx)
	visible := make(map[string]*Statement, len(store.statements))

	for id, stmt := range store.statements {
		if caller.sees(stmt.Owner, stmt.OwnerSession, filter.RoleLevel) {
			visible[id] = stmt
		}
	}

	sessions := groupSessions(visible, time.Now())

	result := make([]*SessionData, 0, len(sessions))

	for _, sess := range sessions {
		if sessionMatchesFilter(sess, filter) {
			result = append(result, sess)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].SessionID > result[j].SessionID
		}

		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	start, err := sessionPageStart(result, filter.NextToken)
	if err != nil {
		return nil, "", err
	}

	result = result[start:]

	limit := filter.MaxResults
	if limit <= 0 {
		limit = defaultListSessionsResults
	}

	if len(result) <= limit {
		return result, "", nil
	}

	return result[:limit], result[limit].SessionID, nil
}

// groupSessions derives one SessionData per distinct non-empty Statement.SessionID,
// taking the connection target from the most recently updated statement, CreatedAt
// as the earliest statement CreatedAt and UpdatedAt as the latest. A session's TTL is
// the last statement's UpdatedAt plus its SessionKeepAliveSeconds, capped at 24h after
// creation (api_op_ExecuteStatement.go: "After 24 hours, the session is forced
// closed"); it is CLOSED once now reaches the TTL. Caller must hold b.mu.
func groupSessions(statements map[string]*Statement, now time.Time) []*SessionData {
	grouped := make(map[string]*SessionData)

	for _, stmt := range statements {
		if stmt.SessionID == "" {
			continue
		}

		sess, ok := grouped[stmt.SessionID]
		if !ok {
			sess = &SessionData{SessionID: stmt.SessionID, CreatedAt: stmt.CreatedAt}
			grouped[stmt.SessionID] = sess
		}

		if stmt.CreatedAt.Before(sess.CreatedAt) {
			sess.CreatedAt = stmt.CreatedAt
		}

		if stmt.SessionKeepAliveSeconds > sess.AliveSeconds {
			sess.AliveSeconds = stmt.SessionKeepAliveSeconds
		}

		if stmt.UpdatedAt.After(sess.UpdatedAt) {
			sess.UpdatedAt = stmt.UpdatedAt
			sess.ClusterIdentifier = stmt.ClusterIdentifier
			sess.WorkgroupName = stmt.WorkgroupName
			sess.Database = stmt.Database
			sess.DBUser = stmt.DBUser
		}
	}

	result := make([]*SessionData, 0, len(grouped))

	for _, sess := range grouped {
		sess.TTL = sess.CreatedAt.Add(maxSessionLifetime)
		if sess.AliveSeconds > 0 {
			sess.TTL = minTime(sess.TTL, sess.UpdatedAt.Add(time.Duration(sess.AliveSeconds)*time.Second))
		}

		sess.Status = statusSessionAvailable
		if !now.Before(sess.TTL) {
			sess.Status = statusSessionClosed
		}

		result = append(result, sess)
	}

	return result
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}

	return a
}

// rejectClosedSession returns ErrValidation when sessionID names a session whose TTL
// has passed. Caller must hold b.mu.
func rejectClosedSession(statements map[string]*Statement, sessionID string, now time.Time) error {
	if sessionID == "" {
		return nil
	}

	for _, sess := range groupSessions(statements, now) {
		if sess.SessionID == sessionID && sess.Status == statusSessionClosed {
			return fmt.Errorf("%w: session %s is closed", ErrValidation, sessionID)
		}
	}

	return nil
}

// SetStatementSessionKeepAlive records the SessionKeepAliveSeconds a request supplied
// on the statement it created, so its session can expire.
func (b *InMemoryBackend) SetStatementSessionKeepAlive(ctx context.Context, id string, seconds int32) {
	if seconds <= 0 {
		return
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SetStatementSessionKeepAlive")
	defer b.mu.Unlock()

	if stmt, ok := b.storeFor(region).statements[id]; ok {
		stmt.SessionKeepAliveSeconds = seconds
	}
}

// sessionMatchesFilter reports whether sess satisfies every set field of filter.
// When SessionID is set, no other filter field may be set too (enforced by
// ValidateListSessionsRequest before this is ever called), so only an exact SessionID
// match is checked in that case.
func sessionMatchesFilter(sess *SessionData, filter ListSessionsFilter) bool {
	if filter.SessionID != "" {
		return sess.SessionID == filter.SessionID
	}

	if filter.ClusterIdentifier != "" && sess.ClusterIdentifier != filter.ClusterIdentifier {
		return false
	}

	if filter.WorkgroupName != "" && sess.WorkgroupName != filter.WorkgroupName {
		return false
	}

	if filter.Database != "" && sess.Database != filter.Database {
		return false
	}

	return matchesSessionStatus(sess.Status, filter.Status)
}

// matchesSessionStatus implements ListSessionsInput.Status filtering: "If no status is
// specified, sessions with a status of AVAILABLE or BUSY are returned" (excludes CLOSED
// by default).
func matchesSessionStatus(actual, requested string) bool {
	if requested == "" {
		return actual == statusSessionAvailable || actual == statusSessionBusy
	}

	return actual == requested
}

func sessionPageStart(sessions []*SessionData, nextToken string) (int, error) {
	if nextToken == "" {
		return 0, nil
	}

	for i, sess := range sessions {
		if sess.SessionID == nextToken {
			return i, nil
		}
	}

	return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
}
