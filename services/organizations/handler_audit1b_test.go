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
// Account lifecycle
// ---------------------------------------------------------------------------

// TestAudit1b_CreateAccount_WithTags verifies tags are stored on created accounts.
func TestAudit1b_CreateAccount_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []organizations.Tag
		wantTags int
	}{
		{name: "no_tags", tags: nil, wantTags: 0},
		{name: "single_tag", tags: []organizations.Tag{{Key: "env", Value: "prod"}}, wantTags: 1},
		{
			name: "multi_tags",
			tags: []organizations.Tag{
				{Key: "env", Value: "prod"},
				{Key: "owner", Value: "team-a"},
				{Key: "cost-center", Value: "cc-123"},
			},
			wantTags: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("tagged-account", "tagged@example.com", tt.tags)
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(status.AccountID)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestAudit1b_CreateAccount_GovCloudIDFormat verifies GovCloud account ID format.
func TestAudit1b_CreateAccount_GovCloudIDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountName string
		email       string
	}{
		{
			name:        "govcloud_id_twelve_digits",
			accountName: "gov-account",
			email:       "gov@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateGovCloudAccount(tt.accountName, tt.email, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, status.GovCloudAccountID)
			assert.Len(t, status.GovCloudAccountID, 12, "GovCloud ID must be 12 digits")
			// GovCloud IDs are offset by 1_000_000_000 from the commercial ID counter.
			// Both commercial and GovCloud IDs are returned in the same status.
			assert.NotEqual(t, status.AccountID, status.GovCloudAccountID,
				"GovCloud ID must differ from commercial ID")
		})
	}
}

// TestAudit1b_InviteHandshake_StateMachine tests the full INVITE handshake state machine
// including accept (adds account), decline (no account), cancel.
func TestAudit1b_InviteHandshake_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		targetID         string
		action           string // accept, decline, cancel
		wantFinalState   string
		wantAccountInOrg bool
	}{
		{
			name:             "accept_adds_account",
			targetID:         "444444444444",
			action:           "accept",
			wantFinalState:   "ACCEPTED",
			wantAccountInOrg: true,
		},
		{
			name:             "decline_no_account",
			targetID:         "444444444445",
			action:           "decline",
			wantFinalState:   "DECLINED",
			wantAccountInOrg: false,
		},
		{
			name:             "cancel_no_account",
			targetID:         "444444444446",
			action:           "cancel",
			wantFinalState:   "CANCELED",
			wantAccountInOrg: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			hs, err := b.InviteAccountToOrganization(
				organizations.HandshakeParty{ID: tt.targetID, Type: "ACCOUNT"},
				"test invite notes",
			)
			require.NoError(t, err)
			assert.Equal(t, "OPEN", hs.State)
			assert.Equal(t, "INVITE", hs.Action)
			assert.NotEmpty(t, hs.ID)
			assert.NotEmpty(t, hs.ARN)

			var result *organizations.Handshake
			var transErr error

			switch tt.action {
			case "accept":
				result, transErr = b.AcceptHandshake(hs.ID)
			case "decline":
				result, transErr = b.DeclineHandshake(hs.ID)
			case "cancel":
				result, transErr = b.CancelHandshake(hs.ID)
			}

			require.NoError(t, transErr)
			assert.Equal(t, tt.wantFinalState, result.State)

			_, descErr := b.DescribeAccount(tt.targetID)
			if tt.wantAccountInOrg {
				require.NoError(t, descErr, "accepted invite should add account")
			} else {
				require.Error(t, descErr, "declined/canceled invite should not add account")
			}
		})
	}
}

// TestAudit1b_InviteHandshake_Resources tests handshake resources include
// the target account, organization ID, and master email.
func TestAudit1b_InviteHandshake_Resources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		targetID        string
		notes           string
		wantNotesInRes  bool
		wantResMinCount int
	}{
		{
			name:            "without_notes",
			targetID:        "333333333333",
			notes:           "",
			wantNotesInRes:  false,
			wantResMinCount: 3,
		},
		{
			name:            "with_notes",
			targetID:        "333333333334",
			notes:           "please join our org",
			wantNotesInRes:  true,
			wantResMinCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			hs, err := b.InviteAccountToOrganization(
				organizations.HandshakeParty{ID: tt.targetID, Type: "ACCOUNT"},
				tt.notes,
			)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(hs.Resources), tt.wantResMinCount)

			hasNotes := false
			hasAccount := false
			hasOrg := false

			for _, r := range hs.Resources {
				switch r.Type {
				case "NOTES":
					hasNotes = true
					assert.Equal(t, tt.notes, r.Value)
				case "ACCOUNT":
					hasAccount = true
					assert.Equal(t, tt.targetID, r.Value)
				case "ORGANIZATION":
					hasOrg = true
				}
			}

			assert.True(t, hasAccount, "ACCOUNT resource must be present")
			assert.True(t, hasOrg, "ORGANIZATION resource must be present")
			assert.Equal(t, tt.wantNotesInRes, hasNotes)
		})
	}
}

// TestAudit1b_InviteHandshake_DuplicateAcceptFails tests that a non-OPEN handshake
// cannot be re-acted upon.
func TestAudit1b_InviteHandshake_DuplicateAcceptFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		firstOp    string
		secondOp   string
	}{
		{name: "accept_then_accept", firstOp: "accept", secondOp: "accept"},
		{name: "accept_then_cancel", firstOp: "accept", secondOp: "cancel"},
		{name: "accept_then_decline", firstOp: "accept", secondOp: "decline"},
		{name: "cancel_then_accept", firstOp: "cancel", secondOp: "accept"},
		{name: "decline_then_cancel", firstOp: "decline", secondOp: "cancel"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			hs, err := b.InviteAccountToOrganization(
				organizations.HandshakeParty{ID: "777777777777", Type: "ACCOUNT"},
				"",
			)
			require.NoError(t, err)

			applyOp := func(op, hsID string) error {
				switch op {
				case "accept":
					_, e := b.AcceptHandshake(hsID)
					return e
				case "decline":
					_, e := b.DeclineHandshake(hsID)
					return e
				case "cancel":
					_, e := b.CancelHandshake(hsID)
					return e
				}
				return nil
			}

			require.NoError(t, applyOp(tt.firstOp, hs.ID))
			require.Error(t, applyOp(tt.secondOp, hs.ID),
				"second operation on non-OPEN handshake must fail")
		})
	}
}

// TestAudit1b_MoveAccount_Scenarios tests moving accounts between OUs and roots.
func TestAudit1b_MoveAccount_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		moveTo      string // "ou", "root", "nested_ou"
		wantErr     bool
		wrongSource bool
	}{
		{name: "root_to_ou", moveTo: "ou"},
		{name: "ou_to_root", moveTo: "root"},
		{name: "root_to_nested_ou", moveTo: "nested_ou"},
		{name: "wrong_source_id_fails", moveTo: "ou", wrongSource: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			ouTop, err := b.CreateOrganizationalUnit(rootID, "top-ou", nil)
			require.NoError(t, err)

			ouNested, err := b.CreateOrganizationalUnit(ouTop.ID, "nested-ou", nil)
			require.NoError(t, err)

			status, err := b.CreateAccount("move-me", "move@example.com", nil)
			require.NoError(t, err)
			accountID := status.AccountID

			var destParentID string

			switch tt.moveTo {
			case "ou":
				destParentID = ouTop.ID
			case "root":
				// First move to OU then back to root.
				require.NoError(t, b.MoveAccount(accountID, rootID, ouTop.ID))
				destParentID = rootID
				rootID = ouTop.ID // source is now ouTop
			case "nested_ou":
				destParentID = ouNested.ID
			}

			sourceID := rootID
			if tt.wrongSource {
				sourceID = "r-wrong"
			}

			err = b.MoveAccount(accountID, sourceID, destParentID)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			parents, err := b.ListParents(accountID)
			require.NoError(t, err)
			require.Len(t, parents, 1)
			assert.Equal(t, destParentID, parents[0].ID)
		})
	}
}

// TestAudit1b_CloseAccount_Scenarios verifies close account behavior.
func TestAudit1b_CloseAccount_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		isMgmt    bool
		notFound  bool
		wantErr   bool
		wantSusp  bool
	}{
		{name: "closes_member_account", wantSusp: true},
		{name: "management_account_fails", isMgmt: true, wantErr: true},
		{name: "not_found_fails", notFound: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			org, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			var targetID string
			if tt.isMgmt {
				targetID = org.MasterAccountID
			} else if tt.notFound {
				targetID = "000000000000"
			} else {
				status, err := b.CreateAccount("close-me", "close@example.com", nil)
				require.NoError(t, err)
				targetID = status.AccountID
			}

			err = b.CloseAccount(targetID)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.wantSusp {
				acct, err := b.DescribeAccount(targetID)
				require.NoError(t, err)
				assert.Equal(t, "SUSPENDED", acct.Status)
			}
		})
	}
}

// TestAudit1b_RemoveAccount_CleansDelegatedAdmins verifies delegated admin
// records are removed when an account leaves.
func TestAudit1b_RemoveAccount_CleansDelegatedAdmins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services []string
	}{
		{name: "single_service", services: []string{"ssm.amazonaws.com"}},
		{
			name: "multi_service",
			services: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
				"guardduty.amazonaws.com",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("delegate", "delegate@example.com", nil)
			require.NoError(t, err)
			accountID := status.AccountID

			for _, svc := range tt.services {
				require.NoError(t, b.RegisterDelegatedAdministrator(accountID, svc))
			}

			require.NoError(t, b.RemoveAccountFromOrganization(accountID))

			for _, svc := range tt.services {
				admins, err := b.ListDelegatedAdministrators(svc)
				require.NoError(t, err)
				for _, a := range admins {
					assert.NotEqual(t, accountID, a.AccountID,
						"removed account must not appear as delegated admin for %s", svc)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// OrganizationalUnit hierarchy
// ---------------------------------------------------------------------------

// TestAudit1b_OU_Hierarchy tests deep OU nesting and ListOrganizationalUnitsForParent.
func TestAudit1b_OU_Hierarchy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		depth      int
		wantAtRoot int
	}{
		{name: "single_level", depth: 1, wantAtRoot: 3},
		{name: "two_levels", depth: 2, wantAtRoot: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Create OUs at root level.
			for i := range tt.wantAtRoot {
				_, err := b.CreateOrganizationalUnit(rootID, "root-ou-"+string(rune('a'+i)), nil)
				require.NoError(t, err)
			}

			rootOUs, err := b.ListOrganizationalUnitsForParent(rootID)
			require.NoError(t, err)
			assert.Len(t, rootOUs, tt.wantAtRoot)

			if tt.depth > 1 {
				// Create a child OU under the first root OU.
				_, err = b.CreateOrganizationalUnit(rootOUs[0].ID, "child-ou", nil)
				require.NoError(t, err)

				childOUs, err := b.ListOrganizationalUnitsForParent(rootOUs[0].ID)
				require.NoError(t, err)
				assert.Len(t, childOUs, 1)
			}
		})
	}
}

// TestAudit1b_OU_DeleteConstraints tests OU deletion is blocked with contents.
func TestAudit1b_OU_DeleteConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    string // "with_account", "with_child_ou", "empty"
		wantErr bool
	}{
		{name: "delete_empty_ou", kind: "empty"},
		{name: "reject_ou_with_account", kind: "with_account", wantErr: true},
		{name: "reject_ou_with_child_ou", kind: "with_child_ou", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			ou, err := b.CreateOrganizationalUnit(rootID, "target-ou", nil)
			require.NoError(t, err)

			switch tt.kind {
			case "with_account":
				status, err := b.CreateAccount("child-account", "child@example.com", nil)
				require.NoError(t, err)
				require.NoError(t, b.MoveAccount(status.AccountID, rootID, ou.ID))
			case "with_child_ou":
				_, err := b.CreateOrganizationalUnit(ou.ID, "child-ou", nil)
				require.NoError(t, err)
			}

			err = b.DeleteOrganizationalUnit(ou.ID)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			_, err = b.DescribeOrganizationalUnit(ou.ID)
			require.Error(t, err)
		})
	}
}

// TestAudit1b_OU_ListChildren tests ListChildren for both child types.
func TestAudit1b_OU_ListChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		childType  string
		wantCount  int
		addAccount bool
		addOUs     int
	}{
		{
			name:       "accounts_under_root",
			childType:  "ACCOUNT",
			addAccount: true,
			wantCount:  2, // management + created
		},
		{
			name:      "ous_under_root",
			childType: "ORGANIZATIONAL_UNIT",
			addOUs:    3,
			wantCount: 3,
		},
		{
			name:      "empty_parent_no_children",
			childType: "ACCOUNT",
			wantCount: 1, // just management account
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			if tt.addAccount {
				_, err := b.CreateAccount("child", "child@example.com", nil)
				require.NoError(t, err)
			}

			for i := range tt.addOUs {
				_, err := b.CreateOrganizationalUnit(rootID, "ou-"+string(rune('a'+i)), nil)
				require.NoError(t, err)
			}

			children, err := b.ListChildren(rootID, tt.childType)
			require.NoError(t, err)
			assert.Len(t, children, tt.wantCount)

			for _, c := range children {
				assert.Equal(t, tt.childType, c.Type)
				assert.NotEmpty(t, c.ID)
			}
		})
	}
}

// TestAudit1b_OU_ListParents tests ListParents for both accounts and OUs.
func TestAudit1b_OU_ListParents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		childKind  string // "account_in_root", "account_in_ou", "ou_in_root", "ou_in_ou"
		wantType   string
	}{
		{name: "account_in_root", childKind: "account_in_root", wantType: "ROOT"},
		{name: "account_in_ou", childKind: "account_in_ou", wantType: "ORGANIZATIONAL_UNIT"},
		{name: "ou_in_root", childKind: "ou_in_root", wantType: "ROOT"},
		{name: "ou_in_ou", childKind: "ou_in_ou", wantType: "ORGANIZATIONAL_UNIT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			ou, err := b.CreateOrganizationalUnit(rootID, "parent-ou", nil)
			require.NoError(t, err)

			var childID string

			switch tt.childKind {
			case "account_in_root":
				s, err := b.CreateAccount("a", "a@example.com", nil)
				require.NoError(t, err)
				childID = s.AccountID
			case "account_in_ou":
				s, err := b.CreateAccount("a", "a@example.com", nil)
				require.NoError(t, err)
				require.NoError(t, b.MoveAccount(s.AccountID, rootID, ou.ID))
				childID = s.AccountID
			case "ou_in_root":
				child, err := b.CreateOrganizationalUnit(rootID, "child-ou", nil)
				require.NoError(t, err)
				childID = child.ID
			case "ou_in_ou":
				child, err := b.CreateOrganizationalUnit(ou.ID, "nested-ou", nil)
				require.NoError(t, err)
				childID = child.ID
			}

			parents, err := b.ListParents(childID)
			require.NoError(t, err)
			require.Len(t, parents, 1)
			assert.Equal(t, tt.wantType, parents[0].Type)
		})
	}
}

// ---------------------------------------------------------------------------
// Policy attachment targets
// ---------------------------------------------------------------------------

// TestAudit1b_AttachPolicy_Targets tests attaching policies to root, OU, and account.
func TestAudit1b_AttachPolicy_Targets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetKind string // "root", "ou", "account"
		wantInList bool
	}{
		{name: "attach_to_root", targetKind: "root", wantInList: true},
		{name: "attach_to_ou", targetKind: "ou", wantInList: true},
		{name: "attach_to_account", targetKind: "account", wantInList: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			p, err := b.CreatePolicy("test-scp", "", `{"Version":"2012-10-17"}`,
				"SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			var targetID string

			switch tt.targetKind {
			case "root":
				targetID = rootID
			case "ou":
				ou, err := b.CreateOrganizationalUnit(rootID, "policy-target-ou", nil)
				require.NoError(t, err)
				targetID = ou.ID
			case "account":
				s, err := b.CreateAccount("policy-target", "pt@example.com", nil)
				require.NoError(t, err)
				targetID = s.AccountID
			}

			require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, targetID))

			// ListPoliciesForTarget.
			policies, err := b.ListPoliciesForTarget(targetID, "SERVICE_CONTROL_POLICY")
			require.NoError(t, err)
			found := false
			for _, lp := range policies {
				if lp.PolicySummary.ID == p.PolicySummary.ID {
					found = true
				}
			}
			assert.Equal(t, tt.wantInList, found)

			// ListTargetsForPolicy.
			targets, err := b.ListTargetsForPolicy(p.PolicySummary.ID)
			require.NoError(t, err)
			assert.Len(t, targets, 1)
			assert.Equal(t, targetID, targets[0].TargetID)
		})
	}
}

// TestAudit1b_AttachPolicy_Limits tests the 5-policy-per-target-per-type limit.
func TestAudit1b_AttachPolicy_Limits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		attachCount  int
		wantLastErr  bool
	}{
		{name: "exactly_at_limit", attachCount: 5, wantLastErr: false},
		{name: "exceeds_limit", attachCount: 6, wantLastErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			var lastErr error

			for i := range tt.attachCount {
				p, err := b.CreatePolicy(
					"scp-"+string(rune('a'+i)),
					"",
					`{"Version":"2012-10-17"}`,
					"SERVICE_CONTROL_POLICY",
					nil,
				)
				require.NoError(t, err)
				lastErr = b.AttachPolicy(p.PolicySummary.ID, rootID)
				if lastErr != nil {
					break
				}
			}

			if tt.wantLastErr {
				require.Error(t, lastErr)
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

// TestAudit1b_AttachPolicy_CrossTypes tests that the per-type limit is per-type
// (attaching 5 SCPs and 5 TAGs to the same target should succeed).
func TestAudit1b_AttachPolicy_CrossTypes(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	for _, pt := range []string{"SERVICE_CONTROL_POLICY", "TAG_POLICY"} {
		for i := range 5 {
			p, err := b.CreatePolicy(
				pt+"-"+string(rune('a'+i)),
				"",
				`{"Version":"2012-10-17"}`,
				pt,
				nil,
			)
			require.NoError(t, err)
			require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID),
				"attaching policy %d of type %s should succeed", i+1, pt)
		}
	}
}

// TestAudit1b_DetachPolicy_Scenarios tests detach lifecycle.
func TestAudit1b_DetachPolicy_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		attachFirst bool
		wantErr     bool
	}{
		{name: "detach_attached", attachFirst: true},
		{name: "detach_not_attached_fails", attachFirst: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			p, err := b.CreatePolicy("p", "", `{}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			if tt.attachFirst {
				require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID))
			}

			err = b.DetachPolicy(p.PolicySummary.ID, rootID)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// After detach, ListTargetsForPolicy should be empty.
			targets, err := b.ListTargetsForPolicy(p.PolicySummary.ID)
			require.NoError(t, err)
			assert.Empty(t, targets)
		})
	}
}

// TestAudit1b_ListPoliciesForTarget_Filter tests filtering by policy type.
func TestAudit1b_ListPoliciesForTarget_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     string
		wantCount  int
	}{
		{name: "filter_scp", filter: "SERVICE_CONTROL_POLICY", wantCount: 2},
		{name: "filter_tag", filter: "TAG_POLICY", wantCount: 1},
		{name: "filter_backup", filter: "BACKUP_POLICY", wantCount: 0},
		{name: "no_filter", filter: "", wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			attach := func(name, pt string) {
				p, err := b.CreatePolicy(name, "", `{}`, pt, nil)
				require.NoError(t, err)
				require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, rootID))
			}

			attach("scp1", "SERVICE_CONTROL_POLICY")
			attach("scp2", "SERVICE_CONTROL_POLICY")
			attach("tag1", "TAG_POLICY")

			policies, err := b.ListPoliciesForTarget(rootID, tt.filter)
			require.NoError(t, err)
			assert.Len(t, policies, tt.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// EnableAWSServiceAccess
// ---------------------------------------------------------------------------

// TestAudit1b_ServiceAccess_MultiService tests enabling multiple service principals.
func TestAudit1b_ServiceAccess_MultiService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services []string
		disable  string
		wantLeft int
	}{
		{
			name:     "enable_one",
			services: []string{"cloudtrail.amazonaws.com"},
			wantLeft: 1,
		},
		{
			name:     "enable_three",
			services: []string{"cloudtrail.amazonaws.com", "ssm.amazonaws.com", "guardduty.amazonaws.com"},
			wantLeft: 3,
		},
		{
			name:     "enable_three_disable_one",
			services: []string{"cloudtrail.amazonaws.com", "ssm.amazonaws.com", "guardduty.amazonaws.com"},
			disable:  "ssm.amazonaws.com",
			wantLeft: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			for _, svc := range tt.services {
				require.NoError(t, b.EnableAWSServiceAccess(svc))
			}

			if tt.disable != "" {
				require.NoError(t, b.DisableAWSServiceAccess(tt.disable))
			}

			enabled, err := b.ListAWSServiceAccessForOrganization()
			require.NoError(t, err)
			assert.Len(t, enabled, tt.wantLeft)

			if tt.disable != "" {
				for _, e := range enabled {
					assert.NotEqual(t, tt.disable, e.ServicePrincipal)
				}
			}
		})
	}
}

// TestAudit1b_ServiceAccess_Idempotent tests enabling same service twice is idempotent.
func TestAudit1b_ServiceAccess_Idempotent(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	svc := "cloudtrail.amazonaws.com"
	require.NoError(t, b.EnableAWSServiceAccess(svc))
	require.NoError(t, b.EnableAWSServiceAccess(svc)) // second enable overwrites timestamp

	enabled, err := b.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)
	assert.Len(t, enabled, 1, "enabling same service twice should result in one entry")
}

// TestAudit1b_ServiceAccess_SortedOutput tests that ListAWSServiceAccessForOrganization
// returns entries sorted by ServicePrincipal.
func TestAudit1b_ServiceAccess_SortedOutput(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	services := []string{
		"ssm.amazonaws.com",
		"cloudtrail.amazonaws.com",
		"guardduty.amazonaws.com",
		"access-analyzer.amazonaws.com",
	}

	for _, svc := range services {
		require.NoError(t, b.EnableAWSServiceAccess(svc))
	}

	enabled, err := b.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)
	require.Len(t, enabled, len(services))

	for i := 1; i < len(enabled); i++ {
		assert.LessOrEqual(t, enabled[i-1].ServicePrincipal, enabled[i].ServicePrincipal,
			"output must be sorted by ServicePrincipal")
	}
}

// TestAudit1b_ServiceAccess_Handler tests the HTTP handler for service access operations.
func TestAudit1b_ServiceAccess_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
		wantInList bool
	}{
		{
			name:       "enable_service",
			op:         "EnableAWSServiceAccess",
			body:       map[string]any{"ServicePrincipal": "cloudtrail.amazonaws.com"},
			wantStatus: http.StatusOK,
			wantInList: true,
		},
		{
			name:       "disable_service",
			op:         "DisableAWSServiceAccess",
			body:       map[string]any{"ServicePrincipal": "cloudtrail.amazonaws.com"},
			wantStatus: http.StatusOK,
			wantInList: false,
		},
	}

	h, _ := newHandlerWithOrg(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}

	listRec := doRequest(t, h, "ListAWSServiceAccessForOrganization", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&resp))

	principals := resp["EnabledServicePrincipals"].([]any)
	assert.Empty(t, principals, "disabled service should not appear")
}

// ---------------------------------------------------------------------------
// DelegatedAdministrator
// ---------------------------------------------------------------------------

// TestAudit1b_DelegatedAdmin_MultiService tests registering one account across multiple services.
func TestAudit1b_DelegatedAdmin_MultiService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services []string
		filter   string
		wantLen  int
	}{
		{
			name:     "single_service",
			services: []string{"ssm.amazonaws.com"},
			filter:   "ssm.amazonaws.com",
			wantLen:  1,
		},
		{
			name: "multi_service_no_filter",
			services: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
				"guardduty.amazonaws.com",
			},
			filter:  "",
			wantLen: 3,
		},
		{
			name: "multi_service_with_filter",
			services: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
			},
			filter:  "ssm.amazonaws.com",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("admin-account", "admin@example.com", nil)
			require.NoError(t, err)

			for _, svc := range tt.services {
				require.NoError(t, b.RegisterDelegatedAdministrator(status.AccountID, svc))
			}

			admins, err := b.ListDelegatedAdministrators(tt.filter)
			require.NoError(t, err)
			assert.Len(t, admins, tt.wantLen)
		})
	}
}

// TestAudit1b_DelegatedAdmin_ListServices tests ListDelegatedServicesForAccount.
func TestAudit1b_DelegatedAdmin_ListServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		registerSvcs  []string
		wantSvcCount  int
		wantSorted    bool
	}{
		{
			name:         "no_delegations",
			registerSvcs: nil,
			wantSvcCount: 0,
		},
		{
			name:         "one_delegation",
			registerSvcs: []string{"ssm.amazonaws.com"},
			wantSvcCount: 1,
		},
		{
			name: "multiple_delegations_sorted",
			registerSvcs: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
				"guardduty.amazonaws.com",
			},
			wantSvcCount: 3,
			wantSorted:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("svc-account", "svc@example.com", nil)
			require.NoError(t, err)

			for _, svc := range tt.registerSvcs {
				require.NoError(t, b.RegisterDelegatedAdministrator(status.AccountID, svc))
			}

			svcs, err := b.ListDelegatedServicesForAccount(status.AccountID)
			require.NoError(t, err)
			assert.Len(t, svcs, tt.wantSvcCount)

			if tt.wantSorted && len(svcs) > 1 {
				for i := 1; i < len(svcs); i++ {
					assert.LessOrEqual(t, svcs[i-1].ServicePrincipal, svcs[i].ServicePrincipal,
						"services must be sorted")
				}
			}
		})
	}
}

// TestAudit1b_DelegatedAdmin_ErrorCases tests error conditions.
func TestAudit1b_DelegatedAdmin_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        string // "register", "deregister"
		accountID string
		service   string
		setup     func(b *organizations.InMemoryBackend, accountID string)
		wantErr   bool
	}{
		{
			name:      "register_unknown_account",
			op:        "register",
			accountID: "999999999999",
			service:   "ssm.amazonaws.com",
			wantErr:   true,
		},
		{
			name:    "register_duplicate_fails",
			op:      "register",
			service: "ssm.amazonaws.com",
			setup: func(b *organizations.InMemoryBackend, accountID string) {
				require.NoError(t, b.RegisterDelegatedAdministrator(accountID, "ssm.amazonaws.com"))
			},
			wantErr: true,
		},
		{
			name:    "deregister_not_registered_fails",
			op:      "deregister",
			service: "ssm.amazonaws.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			accountID := tt.accountID
			if accountID == "" {
				status, err := b.CreateAccount("test-acct", "test@example.com", nil)
				require.NoError(t, err)
				accountID = status.AccountID
			}

			if tt.setup != nil {
				tt.setup(b, accountID)
			}

			var err error
			switch tt.op {
			case "register":
				err = b.RegisterDelegatedAdministrator(accountID, tt.service)
			case "deregister":
				err = b.DeregisterDelegatedAdministrator(accountID, tt.service)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// TestAudit1b_ResourcePolicy_Lifecycle tests the full resource policy lifecycle.
func TestAudit1b_ResourcePolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		replace string
		wantID  bool
		wantARN bool
	}{
		{
			name:    "put_describes_deletes",
			content: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`,
			wantID:  true,
			wantARN: true,
		},
		{
			name:    "put_replace_describes",
			content: `{"Version":"2012-10-17","Statement":[]}`,
			replace: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny"}]}`,
			wantID:  true,
			wantARN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			rp, err := b.PutResourcePolicy(tt.content)
			require.NoError(t, err)
			assert.Equal(t, tt.content, rp.Content)

			if tt.wantID {
				assert.NotEmpty(t, rp.ID)
			}

			if tt.wantARN {
				assert.NotEmpty(t, rp.ARN)
			}

			if tt.replace != "" {
				rp2, err := b.PutResourcePolicy(tt.replace)
				require.NoError(t, err)
				assert.Equal(t, tt.replace, rp2.Content)
				assert.Equal(t, rp.ID, rp2.ID, "ID should be stable after replacement")
			}

			described, err := b.DescribeResourcePolicy()
			require.NoError(t, err)

			expectedContent := tt.content
			if tt.replace != "" {
				expectedContent = tt.replace
			}

			assert.Equal(t, expectedContent, described.Content)

			require.NoError(t, b.DeleteResourcePolicy())

			_, err = b.DescribeResourcePolicy()
			require.Error(t, err, "describe after delete must fail")
		})
	}
}

// TestAudit1b_ResourcePolicy_ErrorCases tests error conditions.
func TestAudit1b_ResourcePolicy_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string // "describe", "delete", "put"
		hasRP   bool
		wantErr bool
	}{
		{name: "describe_missing", op: "describe", hasRP: false, wantErr: true},
		{name: "delete_missing", op: "delete", hasRP: false, wantErr: true},
		{name: "describe_present", op: "describe", hasRP: true},
		{name: "delete_present", op: "delete", hasRP: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.hasRP {
				_, err := b.PutResourcePolicy(`{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			}

			var err error

			switch tt.op {
			case "describe":
				_, err = b.DescribeResourcePolicy()
			case "delete":
				err = b.DeleteResourcePolicy()
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAudit1b_ResourcePolicy_Handler tests the HTTP handler for resource policy.
func TestAudit1b_ResourcePolicy_Handler(t *testing.T) {
	t.Parallel()

	const content = `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
		needRP     bool
	}{
		{
			name:       "put_resource_policy",
			op:         "PutResourcePolicy",
			body:       map[string]any{"Content": content},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe_resource_policy",
			op:         "DescribeResourcePolicy",
			body:       nil,
			wantStatus: http.StatusOK,
			needRP:     true,
		},
		{
			name:       "delete_resource_policy",
			op:         "DeleteResourcePolicy",
			body:       nil,
			wantStatus: http.StatusOK,
			needRP:     true,
		},
		{
			name:       "put_empty_content_fails",
			op:         "PutResourcePolicy",
			body:       map[string]any{"Content": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_missing_fails",
			op:         "DescribeResourcePolicy",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			if tt.needRP {
				rec := doRequest(t, h, "PutResourcePolicy", map[string]any{"Content": content})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Handler – additional operation error paths
// ---------------------------------------------------------------------------

// TestAudit1b_Handler_PolicyErrors tests policy handler error paths.
func TestAudit1b_Handler_PolicyErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "create_policy_missing_name",
			op:         "CreatePolicy",
			body:       map[string]any{"Type": "SERVICE_CONTROL_POLICY", "Content": "{}"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_policy_not_found",
			op:         "DescribePolicy",
			body:       map[string]any{"PolicyId": "p-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_policy_not_found",
			op:         "DeletePolicy",
			body:       map[string]any{"PolicyId": "p-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "attach_policy_not_found",
			op:         "AttachPolicy",
			body:       map[string]any{"PolicyId": "p-notexist", "TargetId": "r-rootid"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "enable_policy_type_invalid_root",
			op:         "EnablePolicyType",
			body:       map[string]any{"RootId": "r-notexist", "PolicyType": "SERVICE_CONTROL_POLICY"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit1b_Handler_OUErrors tests OU handler error paths.
func TestAudit1b_Handler_OUErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "create_ou_missing_name",
			op:         "CreateOrganizationalUnit",
			body:       map[string]any{"ParentId": "r-root"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_ou_not_found",
			op:         "DescribeOrganizationalUnit",
			body:       map[string]any{"OrganizationalUnitId": "ou-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_ou_not_found",
			op:         "DeleteOrganizationalUnit",
			body:       map[string]any{"OrganizationalUnitId": "ou-notexist"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit1b_Handler_AccountErrors tests account handler error paths.
func TestAudit1b_Handler_AccountErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "describe_account_not_found",
			op:         "DescribeAccount",
			body:       map[string]any{"AccountId": "999999999999"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "move_account_not_found",
			op:         "MoveAccount",
			body:       map[string]any{"AccountId": "999999999999", "SourceParentId": "r-root", "DestinationParentId": "r-root"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_create_status_not_found",
			op:         "DescribeCreateAccountStatus",
			body:       map[string]any{"CreateAccountRequestId": "car-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "remove_account_not_found",
			op:         "RemoveAccountFromOrganization",
			body:       map[string]any{"AccountId": "999999999999"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit1b_Handler_HandshakeErrors tests handshake handler error paths.
func TestAudit1b_Handler_HandshakeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "accept_not_found",
			op:         "AcceptHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cancel_not_found",
			op:         "CancelHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "decline_not_found",
			op:         "DeclineHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_not_found",
			op:         "DescribeHandshake",
			body:       map[string]any{"HandshakeId": "h-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invite_missing_target",
			op:         "InviteAccountToOrganization",
			body:       map[string]any{"Target": map[string]any{"Id": "", "Type": "ACCOUNT"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit1b_ListCreateAccountStatus_Filter tests filtering by state.
func TestAudit1b_ListCreateAccountStatus_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		states    []string
		wantCount int
	}{
		{name: "no_filter_all", states: nil, wantCount: 3},
		{name: "filter_succeeded", states: []string{"SUCCEEDED"}, wantCount: 3},
		{name: "filter_in_progress", states: []string{"IN_PROGRESS"}, wantCount: 0},
		{name: "filter_failed", states: []string{"FAILED"}, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			for i := range 3 {
				_, err := b.CreateAccount(
					"acct-"+string(rune('a'+i)),
					"acct"+string(rune('a'+i))+"@example.com",
					nil,
				)
				require.NoError(t, err)
			}

			statuses, err := b.ListCreateAccountStatus(tt.states)
			require.NoError(t, err)
			assert.Len(t, statuses, tt.wantCount)
		})
	}
}

// TestAudit1b_TagOperations_MultiResource tests tagging multiple resource types.
func TestAudit1b_TagOperations_MultiResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceKind string // "account", "ou", "policy"
	}{
		{name: "tag_account", resourceKind: "account"},
		{name: "tag_ou", resourceKind: "ou"},
		{name: "tag_policy", resourceKind: "policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			var resourceID string

			switch tt.resourceKind {
			case "account":
				s, err := b.CreateAccount("tagged", "tagged@example.com", nil)
				require.NoError(t, err)
				resourceID = s.AccountID
			case "ou":
				ou, err := b.CreateOrganizationalUnit(rootID, "tagged-ou", nil)
				require.NoError(t, err)
				resourceID = ou.ID
			case "policy":
				p, err := b.CreatePolicy("tagged-p", "", `{}`, "SERVICE_CONTROL_POLICY", nil)
				require.NoError(t, err)
				resourceID = p.PolicySummary.ID
			}

			tags := []organizations.Tag{
				{Key: "team", Value: "platform"},
				{Key: "env", Value: "staging"},
			}

			require.NoError(t, b.TagResource(resourceID, tags))

			listed, err := b.ListTagsForResource(resourceID)
			require.NoError(t, err)
			assert.Len(t, listed, 2)

			require.NoError(t, b.UntagResource(resourceID, []string{"team"}))

			listed, err = b.ListTagsForResource(resourceID)
			require.NoError(t, err)
			assert.Len(t, listed, 1)
			assert.Equal(t, "env", listed[0].Key)
		})
	}
}
