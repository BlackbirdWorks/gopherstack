package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestAudit_Gap16_UpdateEmergencyContactExceeds10ContactsRejected verifies cap.
func TestInMemoryBackend_UpdateEmergencyContactExceeds10ContactsRejected(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	contacts := make([]shield.EmergencyContact, 11)

	for i := range contacts {
		contacts[i] = shield.EmergencyContact{EmailAddress: "user@example.com"}
	}

	err := b.UpdateEmergencyContactSettings(contacts)
	require.Error(t, err, "More than 10 contacts should be rejected")
}

// TestAudit_Gap16_UpdateEmergencyContactMissingEmailRejected verifies email required.
func TestInMemoryBackend_UpdateEmergencyContactMissingEmailRejected(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{PhoneNumber: "+1-555-0100"},
	})
	require.Error(t, err, "Contact without EmailAddress should be rejected")
}

// TestAudit_Gap16_UpdateEmergencyContactTenContactsAccepted verifies exactly 10 allowed.
func TestInMemoryBackend_UpdateEmergencyContactTenContactsAccepted(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	contacts := make([]shield.EmergencyContact, 10)

	for i := range contacts {
		contacts[i] = shield.EmergencyContact{EmailAddress: "user@example.com"}
	}

	require.NoError(t, b.UpdateEmergencyContactSettings(contacts))
}

// --- Gap 17: DisassociateDRTRole is idempotent ---

// TestRefinement1_UpdateEmergencyContactSettings tests updating emergency contacts.
func TestInMemoryBackend_UpdateEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	contacts := []shield.EmergencyContact{
		{EmailAddress: "sec@example.com", PhoneNumber: "+15551234567"},
	}

	require.NoError(t, b.UpdateEmergencyContactSettings(contacts))
	assert.Equal(t, 1, shield.EmergencyContactCount(b))
}

// TestRefinement1_DescribeEmergencyContactSettings tests retrieval of contacts.
func TestInMemoryBackend_DescribeEmergencyContactSettings(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")

	// Initially empty.
	contacts := b.DescribeEmergencyContactSettings()
	assert.Empty(t, contacts)

	// After update.
	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	contacts = b.DescribeEmergencyContactSettings()
	require.Len(t, contacts, 1)
	assert.Equal(t, "sec@example.com", contacts[0].EmailAddress)
}

// TestRefinement1_EmergencyContactCount tests EmergencyContactCount export.
func TestInMemoryBackend_EmergencyContactCount(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, shield.EmergencyContactCount(b))

	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "a@a.com"},
		{EmailAddress: "b@b.com"},
	}))
	assert.Equal(t, 2, shield.EmergencyContactCount(b))
}
