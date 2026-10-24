package lightsail

import "time"

const (
	profileTypeLightsailor      = "Lightsailor"
	profileTypeLightsailPartner = "LightsailPartner"
)

// PartnerProfile is the Lightsail partner-program enrollment of the account.
type PartnerProfile struct {
	EnrolledAt time.Time `json:"enrolledAt"`
	TierName   string    `json:"tierName,omitempty"`
	Status     string    `json:"status"`
}

// SetPartnerProfile enrolls the account in the partner program; no Lightsail
// API does this, so it is the seeding hook for GetProfile.
func (b *InMemoryBackend) SetPartnerProfile(tierName, status string, enrolledAt time.Time) {
	b.mu.Lock("SetPartnerProfile")
	defer b.mu.Unlock()

	b.partner = &PartnerProfile{TierName: tierName, Status: status, EnrolledAt: enrolledAt.UTC()}
}

// GetProfile returns the profile type and, for enrolled accounts, a copy of the partner details.
func (b *InMemoryBackend) GetProfile() (string, *PartnerProfile) {
	b.mu.RLock("GetProfile")
	defer b.mu.RUnlock()

	if b.partner == nil {
		return profileTypeLightsailor, nil
	}

	cp := *b.partner

	return profileTypeLightsailPartner, &cp
}
