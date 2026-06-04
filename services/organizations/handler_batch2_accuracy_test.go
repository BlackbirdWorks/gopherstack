package organizations_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// ---------------------------------------------------------------------------
// DeletePolicy: PolicyInUseException when attached to targets
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_DeletePolicy_Attached_Rejected verifies that deleting a policy
// that is still attached to a target returns PolicyInUseException (HTTP 400).
func TestBatch2Accuracy_DeletePolicy_Attached_Rejected(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	p, err := b.CreatePolicy("deny-all", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)

	err = b.AttachPolicy(p.PolicySummary.ID, rootID)
	require.NoError(t, err)

	err = b.DeletePolicy(p.PolicySummary.ID)
	require.Error(t, err, "DeletePolicy must fail while policy is attached")
}

// TestBatch2Accuracy_DeletePolicy_Attached_ViaHandler verifies HTTP 400 + PolicyInUseException.
func TestBatch2Accuracy_DeletePolicy_Attached_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	roots := doRequest(t, h, "ListRoots", nil)
	var rootsResp map[string]any
	require.NoError(t, json.Unmarshal(roots.Body.Bytes(), &rootsResp))
	rootID := rootsResp["Roots"].([]any)[0].(map[string]any)["Id"].(string)

	doRequest(t, h, "EnablePolicyType", map[string]any{"RootId": rootID, "PolicyType": "SERVICE_CONTROL_POLICY"})

	cp := doRequest(t, h, "CreatePolicy", map[string]any{
		"Name":       "deny-all",
		"Content":    `{"Version":"2012-10-17"}`,
		"Type":       "SERVICE_CONTROL_POLICY",
		"Description": "",
	})
	require.Equal(t, http.StatusOK, cp.Code)

	var cpResp map[string]any
	require.NoError(t, json.Unmarshal(cp.Body.Bytes(), &cpResp))
	policyID := cpResp["Policy"].(map[string]any)["PolicySummary"].(map[string]any)["Id"].(string)

	doRequest(t, h, "AttachPolicy", map[string]any{"PolicyId": policyID, "TargetId": rootID})

	rec := doRequest(t, h, "DeletePolicy", map[string]any{"PolicyId": policyID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "PolicyInUseException", errResp["__type"])
}

// TestBatch2Accuracy_DeletePolicy_AfterDetach_OK verifies that detaching then
// deleting succeeds.
func TestBatch2Accuracy_DeletePolicy_AfterDetach_OK(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	p, err := b.CreatePolicy("deny-all", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)

	require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID))
	require.NoError(t, b.DetachPolicy(p.PolicySummary.ID, rootID))
	require.NoError(t, b.DeletePolicy(p.PolicySummary.ID))
}

// TestBatch2Accuracy_DeletePolicy_NotAttached_OK verifies that deleting an
// unattached policy (never attached) succeeds.
func TestBatch2Accuracy_DeletePolicy_NotAttached_OK(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	p, err := b.CreatePolicy("no-attach", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeletePolicy(p.PolicySummary.ID))
}

// ---------------------------------------------------------------------------
// DeleteOrganization: OrganizationNotEmptyException when member accounts exist
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_DeleteOrganization_WithMembers_Rejected verifies that deleting
// an organization that still has member accounts returns OrganizationNotEmptyException.
func TestBatch2Accuracy_DeleteOrganization_WithMembers_Rejected(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	_, err := b.CreateAccount("member", "member@example.com", "", "", nil)
	require.NoError(t, err)

	err = b.DeleteOrganization()
	require.Error(t, err, "DeleteOrganization must fail while member accounts exist")
}

// TestBatch2Accuracy_DeleteOrganization_WithMembers_ViaHandler verifies HTTP 400 +
// OrganizationNotEmptyException.
func TestBatch2Accuracy_DeleteOrganization_WithMembers_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	ca := doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "member",
		"Email":       "member@example.com",
	})
	require.Equal(t, http.StatusOK, ca.Code)

	rec := doRequest(t, h, "DeleteOrganization", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "OrganizationNotEmptyException", errResp["__type"])
}

// TestBatch2Accuracy_DeleteOrganization_OnlyMaster_OK verifies that deleting an
// org with only the management account succeeds.
func TestBatch2Accuracy_DeleteOrganization_OnlyMaster_OK(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	require.NoError(t, b.DeleteOrganization())
}

// TestBatch2Accuracy_DeleteOrganization_AfterRemovingMembers_OK verifies that after
// removing all member accounts, DeleteOrganization succeeds.
func TestBatch2Accuracy_DeleteOrganization_AfterRemovingMembers_OK(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	status, err := b.CreateAccount("member", "member2@example.com", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.RemoveAccountFromOrganization(status.AccountID))
	require.NoError(t, b.DeleteOrganization())
}

// ---------------------------------------------------------------------------
// DisablePolicyType: ConstraintViolationException when policies of that type are attached
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_DisablePolicyType_WithAttached_Rejected verifies that disabling
// a policy type while policies of that type are attached returns ConstraintViolationException.
func TestBatch2Accuracy_DisablePolicyType_WithAttached_Rejected(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	p, err := b.CreatePolicy("scp", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)

	require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID))

	_, err = b.DisablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.Error(t, err, "DisablePolicyType must fail while policies of that type are attached")
}

// TestBatch2Accuracy_DisablePolicyType_WithAttached_ViaHandler verifies HTTP 400 +
// ConstraintViolationException when policies are still attached.
func TestBatch2Accuracy_DisablePolicyType_WithAttached_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	roots := doRequest(t, h, "ListRoots", nil)
	var rootsResp map[string]any
	require.NoError(t, json.Unmarshal(roots.Body.Bytes(), &rootsResp))
	rootID := rootsResp["Roots"].([]any)[0].(map[string]any)["Id"].(string)

	doRequest(t, h, "EnablePolicyType", map[string]any{"RootId": rootID, "PolicyType": "TAG_POLICY"})

	cp := doRequest(t, h, "CreatePolicy", map[string]any{
		"Name":        "tag-p",
		"Content":     `{"tags":{}}`,
		"Type":        "TAG_POLICY",
		"Description": "",
	})
	require.Equal(t, http.StatusOK, cp.Code)

	var cpResp map[string]any
	require.NoError(t, json.Unmarshal(cp.Body.Bytes(), &cpResp))
	policyID := cpResp["Policy"].(map[string]any)["PolicySummary"].(map[string]any)["Id"].(string)

	doRequest(t, h, "AttachPolicy", map[string]any{"PolicyId": policyID, "TargetId": rootID})

	rec := doRequest(t, h, "DisablePolicyType", map[string]any{"RootId": rootID, "PolicyType": "TAG_POLICY"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ConstraintViolationException", errResp["__type"])
}

// TestBatch2Accuracy_DisablePolicyType_AfterDetach_OK verifies that detaching all policies
// of a type before disabling it succeeds.
func TestBatch2Accuracy_DisablePolicyType_AfterDetach_OK(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	p, err := b.CreatePolicy("scp", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)

	require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID))
	require.NoError(t, b.DetachPolicy(p.PolicySummary.ID, rootID))

	_, err = b.DisablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// InviteAccountToOrganization: DuplicateHandshakeException for duplicate open invites
// ---------------------------------------------------------------------------

// TestBatch2Accuracy_InviteAccount_DuplicateOpen_Rejected verifies that inviting an
// account that already has an open invitation returns DuplicateHandshakeException.
func TestBatch2Accuracy_InviteAccount_DuplicateOpen_Rejected(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	target := organizations.HandshakeParty{ID: "111111111111", Type: "ACCOUNT"}

	_, err := b.InviteAccountToOrganization(target, "")
	require.NoError(t, err)

	_, err = b.InviteAccountToOrganization(target, "second attempt")
	require.Error(t, err, "duplicate open invite must be rejected")
}

// TestBatch2Accuracy_InviteAccount_DuplicateOpen_ViaHandler verifies HTTP 400 +
// DuplicateHandshakeException for a duplicate open invitation.
func TestBatch2Accuracy_InviteAccount_DuplicateOpen_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	target := map[string]any{"Id": "222222222222", "Type": "ACCOUNT"}

	r1 := doRequest(t, h, "InviteAccountToOrganization", map[string]any{"Target": target})
	require.Equal(t, http.StatusOK, r1.Code)

	r2 := doRequest(t, h, "InviteAccountToOrganization", map[string]any{"Target": target})
	assert.Equal(t, http.StatusBadRequest, r2.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(r2.Body.Bytes(), &errResp))
	assert.Equal(t, "DuplicateHandshakeException", errResp["__type"])
}

// TestBatch2Accuracy_InviteAccount_AfterCancel_OK verifies that after canceling the
// first invitation, a new invitation to the same account succeeds.
func TestBatch2Accuracy_InviteAccount_AfterCancel_OK(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	target := organizations.HandshakeParty{ID: "333333333333", Type: "ACCOUNT"}

	h1, err := b.InviteAccountToOrganization(target, "")
	require.NoError(t, err)

	_, err = b.CancelHandshake(h1.ID)
	require.NoError(t, err)

	_, err = b.InviteAccountToOrganization(target, "re-invite after cancel")
	require.NoError(t, err, "invite after cancellation must succeed")
}

// TestBatch2Accuracy_InviteAccount_DifferentTargets_OK verifies that multiple open
// invitations to different accounts are permitted.
func TestBatch2Accuracy_InviteAccount_DifferentTargets_OK(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	_, err := b.InviteAccountToOrganization(organizations.HandshakeParty{ID: "444444444444", Type: "ACCOUNT"}, "")
	require.NoError(t, err)

	_, err = b.InviteAccountToOrganization(organizations.HandshakeParty{ID: "555555555555", Type: "ACCOUNT"}, "")
	require.NoError(t, err, "inviting different accounts simultaneously must succeed")
}
