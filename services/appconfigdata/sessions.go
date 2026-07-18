package appconfigdata

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// StartSession creates a new retrieval session and returns the initial token.
// pollIntervalInSeconds must be 0 (use default) or between minPollIntervalSeconds and
// maxPollIntervalSeconds (inclusive). Returns ErrNoActiveDeployment when no configuration
// has been published for the profile.
func (b *InMemoryBackend) StartSession(
	app, env, profile string,
	pollIntervalInSeconds int,
) (string, error) {
	if pollIntervalInSeconds != 0 &&
		(pollIntervalInSeconds < minPollIntervalSeconds || pollIntervalInSeconds > maxPollIntervalSeconds) {
		return "", ErrInvalidPollInterval
	}

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	// Require an active deployment — no configuration published yet means 404 on AWS.
	key := profileKey(app, env, profile)
	if !b.profiles.Has(key) {
		return "", ErrNoActiveDeployment
	}

	familyID, err := generateFamilyID()
	if err != nil {
		return "", fmt.Errorf("generating family ID: %w", err)
	}

	token, err := b.generateToken(familyID)
	if err != nil {
		return "", fmt.Errorf("generating session token: %w", err)
	}

	now := time.Now().UTC()
	b.sessions.Put(&Session{
		Token:                          token,
		TokenFamilyID:                  familyID,
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		CreatedAt:                      now,
		LastAccessedAt:                 now,
		ExpiresAt:                      now.Add(sessionAbsoluteMaxTTL),
		PollIntervalInSeconds:          pollIntervalInSeconds,
	})

	telemetry.RecordWorkerItems("appconfigdata", "Sessions", 1)

	return token, nil
}
