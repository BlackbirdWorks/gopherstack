package cognitoidp

import "time"

// UserPoolCount returns the number of user pools in the backend. For testing only.
func (b *InMemoryBackend) UserPoolCount() int {
	b.mu.RLock("UserPoolCount")
	defer b.mu.RUnlock()

	return b.pools.Len()
}

// UserCount returns the total number of users across all pools. For testing only.
func (b *InMemoryBackend) UserCount() int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return b.users.Len()
}

// ClientCount returns the number of user pool clients. For testing only.
func (b *InMemoryBackend) ClientCount() int {
	b.mu.RLock("ClientCount")
	defer b.mu.RUnlock()

	return b.clients.Len()
}

// RefreshTokenCount returns the number of refresh tokens in the backend. For testing only.
func (b *InMemoryBackend) RefreshTokenCount() int {
	b.mu.RLock("RefreshTokenCount")
	defer b.mu.RUnlock()

	return len(b.refreshTokens)
}

// SetRefreshTokenExpiry sets expiry for a refresh token. For testing only.
func (b *InMemoryBackend) SetRefreshTokenExpiry(token string, expiresAt time.Time) bool {
	b.mu.Lock("SetRefreshTokenExpiry")
	defer b.mu.Unlock()

	entry, ok := b.refreshTokens[token]
	if !ok {
		return false
	}

	entry.ExpiresAt = expiresAt

	return true
}

// ExpireMFASessionForTest forces a session's ExpiresAt to a past time. For testing only.
func (b *InMemoryBackend) ExpireMFASessionForTest(session string) {
	b.mu.Lock("ExpireMFASessionForTest")
	defer b.mu.Unlock()

	if entry, ok := b.mfaSessions[session]; ok {
		entry.ExpiresAt = time.Now().Add(-time.Hour)
	}
}

// ClearConfirmCodeForTest clears a user's stored confirmation code. For testing only.
func (b *InMemoryBackend) ClearConfirmCodeForTest(poolID, username string) {
	b.mu.Lock("ClearConfirmCodeForTest")
	defer b.mu.Unlock()

	if u, ok := b.users.Get(userKey(poolID, username)); ok {
		u.ConfirmCode = ""
		u.ConfirmCodeExpiresAt = time.Time{}
	}
}

// ExpireConfirmCodeForTest sets a user's confirmation code expiry to the past. For testing only.
func (b *InMemoryBackend) ExpireConfirmCodeForTest(poolID, username string) {
	b.mu.Lock("ExpireConfirmCodeForTest")
	defer b.mu.Unlock()

	if u, ok := b.users.Get(userKey(poolID, username)); ok {
		u.ConfirmCodeExpiresAt = time.Now().Add(-time.Hour)
	}
}

// UserPoolID returns the pool ID for a client. For testing only.
func (b *InMemoryBackend) UserPoolID(clientID string) string {
	b.mu.RLock("UserPoolID")
	defer b.mu.RUnlock()

	if c, ok := b.clients.Get(clientID); ok {
		return c.UserPoolID
	}

	return ""
}

// GetMFASessionCodeForTest returns the one-time SMS_MFA/EMAIL_OTP code generated for a
// pending MFA session (SOFTWARE_TOKEN_MFA sessions have no stored code — it's verified
// cryptographically against the user's TOTP secret instead). For testing only.
func (b *InMemoryBackend) GetMFASessionCodeForTest(session string) string {
	b.mu.RLock("GetMFASessionCodeForTest")
	defer b.mu.RUnlock()

	if entry, ok := b.mfaSessions[session]; ok {
		return entry.Code
	}

	return ""
}

// GetAttrVerificationCodeForTest returns the pending verification code for a user attribute. For testing only.
func (b *InMemoryBackend) GetAttrVerificationCodeForTest(poolID, username, attrName string) string {
	b.mu.RLock("GetAttrVerificationCodeForTest")
	defer b.mu.RUnlock()

	key := poolID + ":" + username + ":" + attrName
	if entry, ok := b.attrVerificationCodes[key]; ok {
		return entry.Code
	}

	return ""
}
