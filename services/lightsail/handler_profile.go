package lightsail

import "context"

type partnerInfoWire struct {
	EnrolledAt *float64 `json:"enrolledAt"`
	Status     string   `json:"status"`
	TierName   string   `json:"tierName,omitempty"`
}

type getProfileResponse struct {
	Partner     *partnerInfoWire `json:"partner,omitempty"`
	ProfileType string           `json:"profileType"`
}

func (h *Handler) handleGetProfile(_ context.Context, _ []byte) ([]byte, error) {
	profileType, partner := h.Backend.GetProfile()
	resp := getProfileResponse{ProfileType: profileType}

	if partner != nil {
		resp.Partner = &partnerInfoWire{
			EnrolledAt: epochPtr(partner.EnrolledAt),
			Status:     partner.Status,
			TierName:   partner.TierName,
		}
	}

	return marshalResponse(resp)
}
