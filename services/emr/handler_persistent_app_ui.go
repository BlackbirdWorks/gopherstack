package emr

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreatePersistentAppUI ---

type createPersistentAppUIInput struct {
	TargetResourceArn string `json:"TargetResourceArn"`
}

type createPersistentAppUIOutput struct {
	PersistentAppUIID         string `json:"PersistentAppUIId"`
	RuntimeRoleEnabledCluster bool   `json:"RuntimeRoleEnabledCluster"`
}

func (h *Handler) handleCreatePersistentAppUI(
	ctx context.Context,
	in *createPersistentAppUIInput,
) (*createPersistentAppUIOutput, error) {
	ui, err := h.Backend.CreatePersistentAppUI(ctx, in.TargetResourceArn)
	if err != nil {
		return nil, err
	}

	return &createPersistentAppUIOutput{
		PersistentAppUIID:         ui.ID,
		RuntimeRoleEnabledCluster: ui.RuntimeRoleEnabledCluster,
	}, nil
}

// --- DescribePersistentAppUI ---

type describePersistentAppUIInput struct {
	PersistentAppUIId string `json:"PersistentAppUIId"`
}

type describePersistentAppUIOutput struct {
	PersistentAppUI *PersistentAppUI `json:"PersistentAppUI"`
}

func (h *Handler) handleDescribePersistentAppUI(
	ctx context.Context,
	in *describePersistentAppUIInput,
) (*describePersistentAppUIOutput, error) {
	ui, err := h.Backend.DescribePersistentAppUI(ctx, in.PersistentAppUIId)
	if err != nil {
		return nil, err
	}

	return &describePersistentAppUIOutput{PersistentAppUI: ui}, nil
}

// --- GetOnClusterAppUIPresignedURL ---

type getOnClusterAppUIPresignedURLInput struct {
	ClusterID string `json:"ClusterId"`
}

// getOnClusterAppUIPresignedURLOutput mirrors GetOnClusterAppUIPresignedURLOutput.
// The real wire field is "PresignedURL" -- this backend previously sent
// "URL", a field name the real GetOnClusterAppUIPresignedURLOutput type does
// not define, so a real client's output.PresignedURL would always
// deserialize as nil (unknown JSON fields are silently ignored). Also
// carries PresignedURLReady, which real clients poll on; gopherstack
// provisions synchronously, so it is always true.
type getOnClusterAppUIPresignedURLOutput struct {
	PresignedURL      string `json:"PresignedURL,omitempty"`
	PresignedURLReady bool   `json:"PresignedURLReady"`
}

func (h *Handler) handleGetOnClusterAppUIPresignedURL(
	ctx context.Context,
	in *getOnClusterAppUIPresignedURLInput,
) (*getOnClusterAppUIPresignedURLOutput, error) {
	region := getRegion(ctx, h.Backend.region)
	url, err := h.Backend.GetOnClusterPresignedURL(ctx, in.ClusterID, region)
	if err != nil {
		return nil, err
	}

	return &getOnClusterAppUIPresignedURLOutput{PresignedURL: url, PresignedURLReady: true}, nil
}

// --- GetPersistentAppUIPresignedURL ---

type getPersistentAppUIPresignedURLInput struct {
	PersistentAppUIId string `json:"PersistentAppUIId"`
}

type getPersistentAppUIPresignedURLOutput struct {
	PresignedURL      string `json:"PresignedURL,omitempty"`
	PresignedURLReady bool   `json:"PresignedURLReady"`
}

func (h *Handler) handleGetPersistentAppUIPresignedURL(
	ctx context.Context,
	in *getPersistentAppUIPresignedURLInput,
) (*getPersistentAppUIPresignedURLOutput, error) {
	url := h.Backend.GetPresignedURL(in.PersistentAppUIId, getRegion(ctx, h.Backend.region))

	return &getPersistentAppUIPresignedURLOutput{PresignedURL: url, PresignedURLReady: true}, nil
}

// --- GetClusterSessionCredentials ---

type getClusterSessionCredentialsInput struct {
	ClusterID        string `json:"ClusterId"`
	ExecutionRoleArn string `json:"ExecutionRoleArn"`
}

// getClusterSessionCredentialsOutput mirrors GetClusterSessionCredentialsOutput.
// ExpiresAt is epoch seconds (float64); see blockPublicAccessConfigurationMetadata for why.
type getClusterSessionCredentialsOutput struct {
	Credentials map[string]any `json:"Credentials"`
	ExpiresAt   float64        `json:"ExpiresAt"`
}

func (h *Handler) handleGetClusterSessionCredentials(
	ctx context.Context,
	in *getClusterSessionCredentialsInput,
) (*getClusterSessionCredentialsOutput, error) {
	creds, expiry, err := h.Backend.GetClusterSessionCredentials(ctx, in.ClusterID, in.ExecutionRoleArn)
	if err != nil {
		return nil, err
	}

	return &getClusterSessionCredentialsOutput{
		Credentials: creds,
		ExpiresAt:   awstime.Epoch(expiry),
	}, nil
}
