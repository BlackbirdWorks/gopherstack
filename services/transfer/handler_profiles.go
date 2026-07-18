package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type createProfileInput struct {
	ProfileType string              `json:"ProfileType"`
	As2ID       string              `json:"As2Id"`
	Tags        []map[string]string `json:"Tags"`
}

type createProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleCreateProfile(
	_ context.Context,
	in *createProfileInput,
) (*createProfileOutput, error) {
	if in.ProfileType == "" {
		return nil, fmt.Errorf("%w: ProfileType is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	p, err := h.Backend.CreateProfile(in.ProfileType, in.As2ID, tags)
	if err != nil {
		return nil, err
	}

	return &createProfileOutput{ProfileID: p.ProfileID}, nil
}

// profileARN builds the ARN for a Transfer AS2 profile.
func profileARN(accountID, region, profileID string) string {
	return arn.Build("transfer", region, accountID, "profile/"+profileID)
}

type deleteProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleDeleteProfile(
	_ context.Context,
	in *deleteProfileInput,
) (*struct{}, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProfile(in.ProfileID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

type describeProfileOutput struct {
	Profile map[string]any `json:"Profile"`
}

func (h *Handler) handleDescribeProfile(
	_ context.Context,
	in *describeProfileInput,
) (*describeProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProfile(in.ProfileID)
	if err != nil {
		return nil, err
	}

	profileMap := map[string]any{
		"ProfileId":      p.ProfileID,
		"ProfileType":    p.ProfileType,
		"As2Id":          p.As2ID,
		keyArn:           profileARN(p.AccountID, p.Region, p.ProfileID),
		keyTags:          tagsToList(p.Tags),
		"CertificateIds": p.CertificateIDs,
	}

	if profileMap["CertificateIds"] == nil {
		profileMap["CertificateIds"] = []string{}
	}

	return &describeProfileOutput{Profile: profileMap}, nil
}

type listProfilesInput struct {
	NextToken   string `json:"NextToken"`
	ProfileType string `json:"ProfileType"`
	MaxResults  int    `json:"MaxResults"`
}

type listProfilesOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Profiles  []map[string]any `json:"Profiles"`
}

func (h *Handler) handleListProfiles(
	_ context.Context,
	in *listProfilesInput,
) (*listProfilesOutput, error) {
	items := h.Backend.ListProfiles()

	if in.ProfileType != "" {
		filtered := items[:0]
		for _, p := range items {
			if p.ProfileType == in.ProfileType {
				filtered = append(filtered, p)
			}
		}
		items = filtered
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, p := range page {
		out[i] = map[string]any{
			"ProfileId":   p.ProfileID,
			"ProfileType": p.ProfileType,
			"As2Id":       p.As2ID,
			keyArn:        profileARN(p.AccountID, p.Region, p.ProfileID),
		}
	}

	return &listProfilesOutput{Profiles: out, NextToken: next}, nil
}

type updateProfileInput struct {
	ProfileID      string   `json:"ProfileId"`
	As2ID          string   `json:"As2Id"`
	CertificateIDs []string `json:"CertificateIds,omitempty"`
}

type updateProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleUpdateProfile(
	_ context.Context,
	in *updateProfileInput,
) (*updateProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProfileFull(&UpdateProfileInput{
		ProfileID:         in.ProfileID,
		As2ID:             in.As2ID,
		CertificateIDs:    in.CertificateIDs,
		SetCertificateIDs: in.CertificateIDs != nil,
	})
	if err != nil {
		return nil, err
	}

	return &updateProfileOutput{ProfileID: p.ProfileID}, nil
}
