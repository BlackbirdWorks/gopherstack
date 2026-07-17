package shield

import (
	"encoding/json"
	"fmt"
)

// emergencyContactItem represents a single emergency contact in the API request/response.
type emergencyContactItem struct {
	EmailAddress string `json:"EmailAddress"`
	PhoneNumber  string `json:"PhoneNumber,omitempty"`
	ContactNotes string `json:"ContactNotes,omitempty"`
}

// associateProactiveEngagementRequest is the request body for AssociateProactiveEngagementDetails.
type associateProactiveEngagementRequest struct {
	EmergencyContactList []emergencyContactItem `json:"EmergencyContactList"`
}

func (h *Handler) handleAssociateProactiveEngagementDetails(body []byte) error {
	var req associateProactiveEngagementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.EmergencyContactList) == 0 {
		return fmt.Errorf(
			"%w: EmergencyContactList must have at least one entry",
			errInvalidRequest,
		)
	}

	contacts := make([]EmergencyContact, 0, len(req.EmergencyContactList))

	for _, c := range req.EmergencyContactList {
		if c.EmailAddress == "" {
			return fmt.Errorf(
				"%w: EmailAddress is required in each emergency contact",
				errInvalidRequest,
			)
		}

		contacts = append(contacts, EmergencyContact(c))
	}

	return h.Backend.AssociateProactiveEngagementDetails(contacts)
}

// updateEmergencyContactSettingsRequest is the request body for UpdateEmergencyContactSettings.
type updateEmergencyContactSettingsRequest struct {
	EmergencyContactList []emergencyContactItem `json:"EmergencyContactList"`
}

func (h *Handler) handleUpdateEmergencyContactSettings(body []byte) error {
	var req updateEmergencyContactSettingsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	contacts := make([]EmergencyContact, 0, len(req.EmergencyContactList))

	for _, c := range req.EmergencyContactList {
		contacts = append(contacts, EmergencyContact(c))
	}

	return h.Backend.UpdateEmergencyContactSettings(contacts)
}

func (h *Handler) handleDescribeEmergencyContactSettings() ([]byte, error) {
	contacts := h.Backend.DescribeEmergencyContactSettings()
	items := make([]emergencyContactItem, 0, len(contacts))

	for _, c := range contacts {
		items = append(items, emergencyContactItem(c))
	}

	return json.Marshal(map[string]any{
		"EmergencyContactList": items,
	})
}
