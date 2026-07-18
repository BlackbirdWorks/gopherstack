package rekognition

import (
	"github.com/google/uuid"
)

// =============================================================================
// Face Liveness
// =============================================================================

// CreateFaceLivenessSession creates a new face liveness session.
func (b *InMemoryBackend) CreateFaceLivenessSession() (string, error) {
	b.mu.Lock("CreateFaceLivenessSession")
	defer b.mu.Unlock()

	sessionID := uuid.NewString()

	// Derive confidence from session ID hash: range 75.0-99.9
	var h uint32
	for _, c := range sessionID {
		h = h*31 + uint32(c) //nolint:mnd,gosec // hash multiplier; G115 safe: unicode codepoints are non-negative
	}

	confidence := float32(75.0) + float32(h%250)/10.0 //nolint:mnd // confidence range

	b.livenessSessions.Put(&storedLivenessSession{
		SessionID:  sessionID,
		Status:     jobStatusSucceeded,
		Confidence: confidence,
	})

	return sessionID, nil
}

// GetFaceLivenessSessionResults returns the result of a liveness session.
func (b *InMemoryBackend) GetFaceLivenessSessionResults(sessionID string) (*LivenessSessionResult, error) {
	b.mu.RLock("GetFaceLivenessSessionResults")
	defer b.mu.RUnlock()

	session, exists := b.livenessSessions.Get(sessionID)
	if !exists {
		return nil, ErrLivenessSessionNotFound
	}

	return &LivenessSessionResult{
		SessionID:  session.SessionID,
		Status:     session.Status,
		Confidence: session.Confidence,
	}, nil
}
