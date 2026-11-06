package connect

import (
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelSession           = "Session"
	entityCustomerProfile  = "CUSTOMER_PROFILE"
	minSessionMinutes      = 1440
	maxInactivityMinutes   = 20160
	maxSessionMinutes      = 43200
	defaultSessionMinutes  = 400 * 24 * 60
	maxAuthSecurityProfile = 10
	authCodeBytes          = 32
)

type authSessionRec struct {
	CreatedAt          awstime.EpochTime `json:"createdAt"`
	ExpiresAt          awstime.EpochTime `json:"expiresAt"`
	InstanceID         string            `json:"instanceId"`
	SessionID          string            `json:"sessionId"`
	EntityType         string            `json:"entityType"`
	EntityID           string            `json:"entityId,omitempty"`
	DomainName         string            `json:"domainName,omitempty"`
	SecurityProfileIDs []string          `json:"securityProfileIds,omitempty"`
	InactivityMinutes  int32             `json:"inactivityMinutes"`
}

// AuthScope bounds what a session created from an auth code can reach.
type AuthScope struct {
	EntityType         string   `json:"EntityType"`
	EntityID           string   `json:"EntityId,omitempty"`
	DomainName         string   `json:"DomainName,omitempty"`
	SecurityProfileIDs []string `json:"SecurityProfileIds,omitempty"`
}

// CreateAuthCodeInput is the CreateAuthCode request.
type CreateAuthCodeInput struct {
	Scope                            *AuthScope
	MaxSessionDurationMinutes        *int32
	SessionInactivityDurationMinutes int32
	instPath
}

type authCodeOut struct {
	AuthCode   string `json:"AuthCode"`
	SessionID  string `json:"SessionId"`
	EntityType string `json:"EntityType"`
	EntityID   string `json:"EntityId,omitempty"`
}

type sessionRef struct {
	SessionID string `json:"-" path:"SessionId"`
	instPath
}

func checkAuthDurations(inactivity int32, maxSession *int32) error {
	if inactivity < minSessionMinutes || inactivity > maxInactivityMinutes {
		return invalidParamErr("SessionInactivityDurationMinutes must be %d-%d", minSessionMinutes, maxInactivityMinutes)
	}

	if maxSession == nil {
		return nil
	}

	if *maxSession < minSessionMinutes || *maxSession > maxSessionMinutes {
		return invalidParamErr("MaxSessionDurationMinutes must be %d-%d", minSessionMinutes, maxSessionMinutes)
	}

	if inactivity > *maxSession {
		return invalidParamErr("SessionInactivityDurationMinutes cannot exceed MaxSessionDurationMinutes")
	}

	return nil
}

// CreateAuthCode issues a one-time authorization code and records the session it opens.
func (b *Backend) CreateAuthCode(in *CreateAuthCodeInput) (*authCodeOut, error) {
	sc := in.Scope
	if sc == nil {
		return nil, invalidParamErr("Scope is required")
	}

	if err := firstError(
		oneOf("Scope.EntityType", sc.EntityType, entityCustomerProfile),
		checkAuthDurations(in.SessionInactivityDurationMinutes, in.MaxSessionDurationMinutes),
	); err != nil {
		return nil, err
	}

	if len(sc.SecurityProfileIDs) > maxAuthSecurityProfile {
		return nil, invalidParamErr("Scope.SecurityProfileIds can hold at most %d items", maxAuthSecurityProfile)
	}

	code := make([]byte, authCodeBytes)
	if _, err := rand.Read(code); err != nil {
		return nil, apiErr(500, "InternalServiceException", "an authorization code could not be generated")
	}

	return writeInstance(b, "CreateAuthCode", in.InstanceID, func(inst string) (*authCodeOut, error) {
		for _, id := range sc.SecurityProfileIDs {
			if !b.securityProfiles.has(inst, id) {
				return nil, notFoundErr("SecurityProfile", id)
			}
		}

		minutes := int64(defaultSessionMinutes)
		if in.MaxSessionDurationMinutes != nil {
			minutes = int64(*in.MaxSessionDurationMinutes)
		}

		now := b.now()
		rec := &authSessionRec{
			InstanceID: inst, SessionID: newID(), EntityType: sc.EntityType, EntityID: sc.EntityID, DomainName: sc.DomainName,
			SecurityProfileIDs: append([]string(nil), sc.SecurityProfileIDs...), InactivityMinutes: in.SessionInactivityDurationMinutes,
			CreatedAt: awstime.NewEpochTime(now), ExpiresAt: awstime.NewEpochTime(now.Add(minutesDuration(minutes))),
		}
		b.authSessions.put(rec)

		return &authCodeOut{
			AuthCode: base64.RawURLEncoding.EncodeToString(code), SessionID: rec.SessionID,
			EntityType: sc.EntityType, EntityID: sc.EntityID,
		}, nil
	})
}

// DeleteSession ends a session opened by an auth code.
func (b *Backend) DeleteSession(in *sessionRef) (*empty, error) {
	return writeInstance(b, "DeleteSession", in.InstanceID, func(inst string) (*empty, error) {
		return nil, b.authSessions.del(inst, in.SessionID)
	})
}

func minutesDuration(m int64) time.Duration { return time.Duration(m) * time.Minute }
