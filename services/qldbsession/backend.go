package qldbsession

import (
	"encoding/base64"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrSessionNotFound is returned when a session token does not correspond to an active session.
	ErrSessionNotFound = awserr.New("InvalidSessionException", awserr.ErrNotFound)
	// ErrTransactionNotFound is returned when a transaction ID does not exist in the session.
	ErrTransactionNotFound = awserr.New("InvalidSessionException", awserr.ErrNotFound)
	// ErrNoActiveTransaction is returned when an operation requires a transaction but none is active.
	ErrNoActiveTransaction = awserr.New("InvalidSessionException", awserr.ErrNotFound)
)

// Session represents an active QLDB session.
type Session struct {
	CreatedAt      time.Time
	TransactionIDs map[string]bool
	Token          string
	LedgerName     string
}

// InMemoryBackend stores QLDB sessions in memory.
type InMemoryBackend struct {
	sessions  map[string]*Session
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory QLDB Session backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		sessions:  make(map[string]*Session),
		mu:        lockmetrics.New("qldbsession"),
		accountID: accountID,
		region:    region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// StartSession creates a new session for the given ledger and returns its token.
func (b *InMemoryBackend) StartSession(ledgerName string) (*Session, error) {
	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	token := newSessionToken()
	sess := &Session{
		Token:          token,
		LedgerName:     ledgerName,
		CreatedAt:      time.Now(),
		TransactionIDs: make(map[string]bool),
	}
	b.sessions[token] = sess

	return cloneSession(sess), nil
}

// GetSession returns the session for a given token.
func (b *InMemoryBackend) GetSession(token string) (*Session, error) {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	sess, ok := b.sessions[token]
	if !ok {
		return nil, fmt.Errorf("%w: session token not found", ErrSessionNotFound)
	}

	return cloneSession(sess), nil
}

// ListSessions returns all active sessions.
func (b *InMemoryBackend) ListSessions() []*Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	list := make([]*Session, 0, len(b.sessions))
	for _, s := range b.sessions {
		list = append(list, cloneSession(s))
	}

	return list
}

// StartTransaction creates a new transaction ID for the given session token.
func (b *InMemoryBackend) StartTransaction(token string) (string, error) {
	b.mu.Lock("StartTransaction")
	defer b.mu.Unlock()

	sess, ok := b.sessions[token]
	if !ok {
		return "", fmt.Errorf("%w: session token not found", ErrSessionNotFound)
	}

	txID := newTransactionID()
	sess.TransactionIDs[txID] = true

	return txID, nil
}

// CommitTransaction commits the named transaction, removing it from the session.
func (b *InMemoryBackend) CommitTransaction(token, txID string, _ []byte) error {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	sess, ok := b.sessions[token]
	if !ok {
		return fmt.Errorf("%w: session token not found", ErrSessionNotFound)
	}

	if !sess.TransactionIDs[txID] {
		return fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, txID)
	}

	delete(sess.TransactionIDs, txID)

	return nil
}

// AbortTransaction aborts the named transaction, removing it from the session.
func (b *InMemoryBackend) AbortTransaction(token, txID string) error {
	b.mu.Lock("AbortTransaction")
	defer b.mu.Unlock()

	sess, ok := b.sessions[token]
	if !ok {
		return fmt.Errorf("%w: session token not found", ErrSessionNotFound)
	}

	if !sess.TransactionIDs[txID] {
		return fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, txID)
	}

	delete(sess.TransactionIDs, txID)

	return nil
}

// EndSession removes the session with the given token.
func (b *InMemoryBackend) EndSession(token string) error {
	b.mu.Lock("EndSession")
	defer b.mu.Unlock()

	if _, ok := b.sessions[token]; !ok {
		return fmt.Errorf("%w: session token not found", ErrSessionNotFound)
	}

	delete(b.sessions, token)

	return nil
}

// newSessionToken creates a unique, URL-safe session token.
func newSessionToken() string {
	raw := uuid.New()

	return base64.URLEncoding.EncodeToString(raw[:])
}

// newTransactionID creates a unique transaction ID.
func newTransactionID() string {
	raw := uuid.New()

	return fmt.Sprintf("%X", raw[:])
}

// cloneSession returns a shallow copy of the session with a new TransactionIDs map.
func cloneSession(s *Session) *Session {
	cp := *s
	cp.TransactionIDs = make(map[string]bool, len(s.TransactionIDs))

	maps.Copy(cp.TransactionIDs, s.TransactionIDs)

	return &cp
}
