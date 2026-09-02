package emr

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- StartSession ---

// startSessionInput mirrors StartSessionInput. ClientRequestToken is
// accepted but not used for idempotency -- no op in this backend
// deduplicates by client token, matching the rest of this package (e.g.
// RunJobFlow accepts no such field at all).
type startSessionInput struct {
	MonitoringConfiguration     *SessionMonitoringConfiguration `json:"MonitoringConfiguration,omitempty"`
	ClusterID                   string                          `json:"ClusterId"`
	ClientRequestToken          string                          `json:"ClientRequestToken,omitempty"`
	Name                        string                          `json:"Name,omitempty"`
	ExecutionRoleArn            string                          `json:"ExecutionRoleArn,omitempty"`
	EngineConfigurations        []Configuration                 `json:"EngineConfigurations,omitempty"`
	Tags                        []Tag                           `json:"Tags,omitempty"`
	SessionIdleTimeoutInMinutes int64                           `json:"SessionIdleTimeoutInMinutes,omitempty"`
}

// startSessionOutput mirrors StartSessionOutput.
type startSessionOutput struct {
	ID        string `json:"Id"`
	AccountID string `json:"AccountId,omitempty"`
	ARN       string `json:"Arn,omitempty"`
	ClusterID string `json:"ClusterId,omitempty"`
	State     string `json:"State,omitempty"`
}

func (h *Handler) handleStartSession(ctx context.Context, in *startSessionInput) (*startSessionOutput, error) {
	session, err := h.Backend.StartSession(ctx, StartSessionParams{
		ClusterID:                   in.ClusterID,
		Name:                        in.Name,
		ExecutionRoleArn:            in.ExecutionRoleArn,
		EngineConfigurations:        in.EngineConfigurations,
		MonitoringConfiguration:     in.MonitoringConfiguration,
		SessionIdleTimeoutInMinutes: in.SessionIdleTimeoutInMinutes,
		Tags:                        in.Tags,
	})
	if err != nil {
		return nil, err
	}

	return &startSessionOutput{
		ID:        session.ID,
		AccountID: session.AccountID,
		ARN:       session.ARN,
		ClusterID: session.ClusterID,
		State:     session.State,
	}, nil
}

// --- GetSession ---

type getSessionInput struct {
	ClusterID string `json:"ClusterId"`
	SessionID string `json:"SessionId"`
}

type getSessionOutput struct {
	Session *Session `json:"Session"`
}

func (h *Handler) handleGetSession(ctx context.Context, in *getSessionInput) (*getSessionOutput, error) {
	session, err := h.Backend.GetSession(ctx, in.ClusterID, in.SessionID)
	if err != nil {
		return nil, err
	}

	return &getSessionOutput{Session: session}, nil
}

// --- ListSessions ---

// listSessionsInput mirrors ListSessionsInput.
type listSessionsInput struct {
	ClusterID     string   `json:"ClusterId"`
	NextToken     string   `json:"NextToken,omitempty"`
	SessionStates []string `json:"SessionStates,omitempty"`
	MaxResults    int32    `json:"MaxResults,omitempty"`
}

type listSessionsOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Sessions  []Session `json:"Sessions"`
}

func (h *Handler) handleListSessions(ctx context.Context, in *listSessionsInput) (*listSessionsOutput, error) {
	sessions, next, err := h.Backend.ListSessions(ctx, in.ClusterID, in.SessionStates, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	if sessions == nil {
		sessions = []Session{}
	}

	return &listSessionsOutput{Sessions: sessions, NextToken: next}, nil
}

// --- TerminateSession ---

type terminateSessionInput struct {
	ClusterID string `json:"ClusterId"`
	SessionID string `json:"SessionId"`
}

// terminateSessionOutput mirrors TerminateSessionOutput.
type terminateSessionOutput struct {
	ClusterID string `json:"ClusterId"`
	SessionID string `json:"SessionId"`
	State     string `json:"State"`
}

func (h *Handler) handleTerminateSession(
	ctx context.Context,
	in *terminateSessionInput,
) (*terminateSessionOutput, error) {
	session, err := h.Backend.TerminateSession(ctx, in.ClusterID, in.SessionID)
	if err != nil {
		return nil, err
	}

	return &terminateSessionOutput{
		ClusterID: session.ClusterID,
		SessionID: session.ID,
		State:     session.State,
	}, nil
}

// --- GetSessionEndpoint ---

type getSessionEndpointInput struct {
	ClusterID string `json:"ClusterId"`
	SessionID string `json:"SessionId"`
}

// getSessionEndpointOutput mirrors GetSessionEndpointOutput. Credentials
// mirrors getClusterSessionCredentialsOutput's Credentials shape
// (handler_persistent_app_ui.go) -- both wrap the same real
// types.Credentials union (its only member is UsernamePassword).
type getSessionEndpointOutput struct {
	Credentials             map[string]any `json:"Credentials,omitempty"`
	Endpoint                string         `json:"Endpoint"`
	AuthToken               string         `json:"AuthToken,omitempty"`
	AuthTokenExpirationTime float64        `json:"AuthTokenExpirationTime,omitempty"`
}

func (h *Handler) handleGetSessionEndpoint(
	ctx context.Context,
	in *getSessionEndpointInput,
) (*getSessionEndpointOutput, error) {
	result, err := h.Backend.GetSessionEndpoint(ctx, in.ClusterID, in.SessionID)
	if err != nil {
		return nil, err
	}

	return &getSessionEndpointOutput{
		Endpoint:                result.Endpoint,
		AuthToken:               result.AuthToken,
		AuthTokenExpirationTime: awstime.Epoch(result.Expiry),
		Credentials:             result.Credentials,
	}, nil
}
