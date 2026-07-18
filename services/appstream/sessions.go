package appstream

import (
	"fmt"
	"time"
)

const (
	sessionStateActive = "ACTIVE"
	sessionConnected   = "CONNECTED"
)

type storedSession struct {
	StartTime          time.Time `json:"startTime"`
	ID                 string    `json:"id"`
	FleetName          string    `json:"fleetName"`
	StackName          string    `json:"stackName"`
	UserID             string    `json:"userId"`
	State              string    `json:"state"`
	ConnectionState    string    `json:"connectionState"`
	AuthenticationType string    `json:"authenticationType"`
}

func (s *storedSession) toSession() *Session {
	return &Session{
		StartTime:          s.StartTime,
		ID:                 s.ID,
		FleetName:          s.FleetName,
		StackName:          s.StackName,
		UserID:             s.UserID,
		State:              s.State,
		ConnectionState:    s.ConnectionState,
		AuthenticationType: s.AuthenticationType,
	}
}

func (b *InMemoryBackend) nextSessionID() string {
	b.sessionSeq++

	return fmt.Sprintf("session-%010d", b.sessionSeq)
}

// DescribeSessions returns sessions filtered by stack, fleet, and/or user.
func (b *InMemoryBackend) DescribeSessions(stackName, fleetName, userID string) ([]*Session, error) {
	b.mu.RLock("DescribeSessions")
	defer b.mu.RUnlock()

	var result []*Session

	for _, s := range b.sessions.All() {
		if stackName != "" && s.StackName != stackName {
			continue
		}

		if fleetName != "" && s.FleetName != fleetName {
			continue
		}

		if userID != "" && s.UserID != userID {
			continue
		}

		result = append(result, s.toSession())
	}

	return result, nil
}

// DrainSessionInstance removes a session.
func (b *InMemoryBackend) DrainSessionInstance(sessionID string) error {
	b.mu.Lock("DrainSessionInstance")
	defer b.mu.Unlock()

	if !b.sessions.Has(sessionID) {
		return ErrNotFound
	}

	b.sessions.Delete(sessionID)

	return nil
}

// ExpireSession marks a session as expired (and removes it).
func (b *InMemoryBackend) ExpireSession(sessionID string) error {
	b.mu.Lock("ExpireSession")
	defer b.mu.Unlock()

	if !b.sessions.Has(sessionID) {
		return ErrNotFound
	}

	b.sessions.Delete(sessionID)

	return nil
}

// CreateStreamingURL creates a session and returns a streaming URL.
func (b *InMemoryBackend) CreateStreamingURL(stackName, fleetName, userID string) (string, error) {
	b.mu.Lock("CreateStreamingURL")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackName) {
		return "", ErrNotFound
	}

	if !b.fleets.Has(fleetName) {
		return "", ErrNotFound
	}

	sessionID := b.nextSessionID()
	s := &storedSession{
		StartTime:          time.Now().UTC(),
		ID:                 sessionID,
		FleetName:          fleetName,
		StackName:          stackName,
		UserID:             userID,
		State:              sessionStateActive,
		ConnectionState:    sessionConnected,
		AuthenticationType: "API",
	}
	b.sessions.Put(s)

	url := fmt.Sprintf(
		"https://appstream2.%s.aws.amazon.com/authenticate?param=%s", b.region, sessionID,
	)

	return url, nil
}
