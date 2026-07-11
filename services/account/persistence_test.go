package account_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to its fresh-construction defaults
// and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.PutAlternateContact(&account.AlternateContact{
		AlternateContactType: account.ContactTypeBilling,
		EmailAddress:         "billing@example.com",
		Name:                 "Bill Ing",
		PhoneNumber:          "555-0100",
		Title:                "Manager",
	}))

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, err = b.GetAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches accountSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied. This also covers the
// pre-Phase-3.3 on-disk shape (Account had no persistence at all before this
// conversion, so there is no legacy shape to actually collide with -- any
// input lacking "version" hits this same path).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.PutAlternateContact(&account.AlternateContact{
		AlternateContactType: account.ContactTypeBilling,
		EmailAddress:         "billing@example.com",
		Name:                 "Bill Ing",
		PhoneNumber:          "555-0100",
		Title:                "Manager",
	}))

	err := b.Restore(t.Context(), []byte(`{"alternateContacts":{}}`))
	require.NoError(t, err)

	_, err = b.GetAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

// seedState is every resource created by
// TestInMemoryBackend_SnapshotRestore_FullState, kept together so the
// post-restore assertions can refer back to the original values.
type seedState struct {
	billing      *account.AlternateContact
	contactInfo  *account.ContactInformation
	pendingEmail string
	pendingOTP   string
}

// seedFullState populates the alternateContacts store.Table, the raw
// contactInfo pointer, the raw regions slice (via EnableRegion/DisableRegion
// mutation), accountName, a pending primary-email update, and CloseAccount,
// so TestInMemoryBackend_SnapshotRestore_FullState can exercise a Snapshot ->
// Restore round trip across every stateful field the Phase 3.3 conversion
// touched.
func seedFullState(t *testing.T, b *account.InMemoryBackend) seedState {
	t.Helper()

	billing := &account.AlternateContact{
		AlternateContactType: account.ContactTypeBilling,
		EmailAddress:         "billing@example.com",
		Name:                 "Bill Ing",
		PhoneNumber:          "555-0100",
		Title:                "Manager",
	}
	require.NoError(t, b.PutAlternateContact(billing))

	security := &account.AlternateContact{
		AlternateContactType: account.ContactTypeSecurity,
		EmailAddress:         "security@example.com",
		Name:                 "Sec Urity",
		PhoneNumber:          "555-0200",
		Title:                "CISO",
	}
	require.NoError(t, b.PutAlternateContact(security))

	contactInfo := &account.ContactInformation{
		AddressLine1: "123 Main St",
		City:         "Seattle",
		CountryCode:  "US",
		FullName:     "Jane Doe",
		PhoneNumber:  "555-0300",
		PostalCode:   "98101",
	}
	require.NoError(t, b.PutContactInformation(contactInfo))

	// Mutate the raw regions slice away from its post-construction defaults:
	// disable an opt-in region that starts ENABLED.
	require.NoError(t, b.DisableRegion("ap-northeast-1"))

	require.NoError(t, b.PutAccountName("My Company"))

	otp, err := b.StartPrimaryEmailUpdate("new-primary@example.com")
	require.NoError(t, err)

	require.NoError(t, b.CloseAccount())

	return seedState{
		billing:      billing,
		contactInfo:  contactInfo,
		pendingEmail: "new-primary@example.com",
		pendingOTP:   otp,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot ->
// Restore round trip across every stateful field on InMemoryBackend: the
// alternateContacts store.Table, the raw contactInfo pointer, the raw
// regions slice, and the accountName/primaryEmail/pendingEmail/pendingOTP/
// closed scalars.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := account.NewInMemoryBackend("111122223333", "us-west-2")
	seed := seedFullState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := account.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertAlternateContactsRestored(t, fresh, seed)
	assertContactInformationRestored(t, fresh, seed)
	assertRegionsRestored(t, fresh)
	assertScalarsRestored(t, fresh, seed)
}

// assertAlternateContactsRestored checks the store.Table-backed
// alternateContacts collection: the seeded BILLING contact round-trips, and
// the never-set OPERATIONS contact still reports not-found.
func assertAlternateContactsRestored(t *testing.T, fresh *account.InMemoryBackend, seed seedState) {
	t.Helper()

	gotBilling, err := fresh.GetAlternateContact(account.ContactTypeBilling)
	require.NoError(t, err)
	assert.Equal(t, seed.billing.EmailAddress, gotBilling.EmailAddress)
	assert.Equal(t, seed.billing.Name, gotBilling.Name)

	gotSecurity, err := fresh.GetAlternateContact(account.ContactTypeSecurity)
	require.NoError(t, err)
	assert.Equal(t, "security@example.com", gotSecurity.EmailAddress)

	_, err = fresh.GetAlternateContact(account.ContactTypeOperations)
	require.Error(t, err)

	// A Put/Delete cycle post-restore still works, proving the table (not
	// just its snapshot) was rebuilt correctly.
	require.NoError(t, fresh.DeleteAlternateContact(account.ContactTypeBilling))
	_, err = fresh.GetAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

// assertContactInformationRestored checks the raw contactInfo pointer.
func assertContactInformationRestored(t *testing.T, fresh *account.InMemoryBackend, seed seedState) {
	t.Helper()

	gotInfo, err := fresh.GetContactInformation()
	require.NoError(t, err)
	assert.Equal(t, seed.contactInfo.FullName, gotInfo.FullName)
	assert.Equal(t, seed.contactInfo.AddressLine1, gotInfo.AddressLine1)
}

// assertRegionsRestored checks the raw (non-store.Table) regions slice: the
// disabled opt-in region stays disabled, and an untouched default-enabled
// region is unaffected.
func assertRegionsRestored(t *testing.T, fresh *account.InMemoryBackend) {
	t.Helper()

	status, err := fresh.GetRegionOptStatus("ap-northeast-1")
	require.NoError(t, err)
	assert.Equal(t, account.RegionOptStatusDisabled, status)

	status, err = fresh.GetRegionOptStatus("us-east-1")
	require.NoError(t, err)
	assert.Equal(t, account.RegionOptStatusEnabledDefault, status)
}

// assertScalarsRestored checks accountName (indirectly via DescribeAccount),
// the pending primary-email update (accepted post-restore to prove
// pendingEmail/pendingOTP round-tripped), and accountID/region.
func assertScalarsRestored(t *testing.T, fresh *account.InMemoryBackend, seed seedState) {
	t.Helper()

	details, err := fresh.DescribeAccount()
	require.NoError(t, err)
	assert.Equal(t, "My Company", details.Name)
	assert.Equal(t, "111122223333", details.ID)

	require.NoError(t, fresh.AcceptPrimaryEmailUpdate(seed.pendingOTP, seed.pendingEmail))
	assert.Equal(t, seed.pendingEmail, fresh.GetPrimaryEmail())
}

// TestInMemoryBackend_SnapshotRestore_ClosedFlag checks the raw closed
// scalar. CloseAccount has no other externally observable effect on this
// backend (a preexisting quirk out of scope for this conversion), so the
// only way to observe it round-tripping is through the JSON snapshot itself.
func TestInMemoryBackend_SnapshotRestore_ClosedFlag(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CloseAccount())

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	var decoded struct {
		Closed bool `json:"closed"`
	}
	require.NoError(t, json.Unmarshal(snap, &decoded))
	assert.True(t, decoded.Closed)
}

// TestHandler_SnapshotRestore verifies Handler.Snapshot/Restore delegate to
// the backend -- the dead-wiring fix this conversion adds, since neither
// Handler nor InMemoryBackend implemented persistence.Persistable before.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.PutAlternateContact(&account.AlternateContact{
		AlternateContactType: account.ContactTypeOperations,
		EmailAddress:         "ops@example.com",
		Name:                 "Ops Erator",
		PhoneNumber:          "555-0400",
		Title:                "Ops Lead",
	}))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	freshHandler := account.NewHandler(account.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, freshHandler.Restore(t.Context(), snap))

	gotOps, err := freshHandler.Backend.GetAlternateContact(account.ContactTypeOperations)
	require.NoError(t, err)
	assert.Equal(t, "ops@example.com", gotOps.EmailAddress)
}
