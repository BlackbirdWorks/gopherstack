package shield

import "fmt"

// AssociateProactiveEngagementDetails stores emergency contact details for proactive engagement.
// Sets status to PENDING when transitioning from DISABLED or empty.
func (b *InMemoryBackend) AssociateProactiveEngagementDetails(contacts []EmergencyContact) error {
	b.mu.Lock("AssociateProactiveEngagementDetails")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	if b.proactiveEngagementStatus == "" || b.proactiveEngagementStatus == ProactiveEngagementDisabled {
		b.proactiveEngagementStatus = ProactiveEngagementPending
	}

	return nil
}

const maxEmergencyContacts = 10

// UpdateEmergencyContactSettings replaces the emergency contact list.
// Enforces: max 10 contacts, non-empty EmailAddress required.
func (b *InMemoryBackend) UpdateEmergencyContactSettings(contacts []EmergencyContact) error {
	if len(contacts) > maxEmergencyContacts {
		return fmt.Errorf(
			"%w: EmergencyContactList cannot exceed %d contacts",
			ErrValidation,
			maxEmergencyContacts,
		)
	}

	for _, c := range contacts {
		if c.EmailAddress == "" {
			return fmt.Errorf("%w: EmailAddress is required for each emergency contact", ErrValidation)
		}
	}

	b.mu.Lock("UpdateEmergencyContactSettings")
	defer b.mu.Unlock()

	b.emergencyContacts = append([]EmergencyContact(nil), contacts...)

	return nil
}

// DescribeEmergencyContactSettings returns the current emergency contacts.
func (b *InMemoryBackend) DescribeEmergencyContactSettings() []EmergencyContact {
	b.mu.RLock("DescribeEmergencyContactSettings")
	defer b.mu.RUnlock()

	return append([]EmergencyContact(nil), b.emergencyContacts...)
}
