package securityhub_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestInMemoryBackend_SnapshotRestore_FullState is a regression guard for the
// Phase 3.3 pkgs/store conversion: it populates one instance of every
// resource collection on InMemoryBackend -- every store.Table-backed field
// (see store_setup.go) plus every field deliberately left as a raw map
// (tags, findings, controlParams, productSubscriptions, orgAdminAccounts)
// and every scalar/sequence field -- snapshots, restores into a fresh
// backend, and verifies each collection survived the round trip. A field
// left out of Snapshot/Restore after the conversion would show up here as a
// count mismatch or a lookup failure.
// Exhaustive round-trip coverage over every resource collection makes this
// function long; that is inherent to what it verifies, not a sign it should
// be split.
//
//nolint:maintidx // exhaustive round-trip coverage
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	// Hub + standards subscriptions (also exercises controlOverrides and the
	// flattened controlAssocOverrides composite-key table below).
	require.NoError(t, b.EnableHub(false, map[string]string{"env": "test"}))

	subs, failures := b.BatchEnableStandards([]map[string]any{
		{"StandardsArn": "arn:aws:securityhub:us-east-1::standards/pci-dss/v/3.2.1"},
	})
	require.Empty(t, failures)
	require.Len(t, subs, 1)
	subArn := subs[0].StandardsSubscriptionArn

	// Findings (raw map[string]map[string]any).
	_, _, failedFindings := b.ImportFindings([]map[string]any{
		securityhub.ValidFinding(map[string]any{
			"Id":         "finding-1",
			"ProductArn": "arn:aws:securityhub:::product/x/y",
		}),
	})
	require.Empty(t, failedFindings)

	// Insights.
	insightArn, err := b.CreateInsight("my-insight", "ResourceType", map[string]any{"k": "v"})
	require.NoError(t, err)

	// Action targets.
	actionTargetArn, err := b.CreateActionTarget("my-action", "desc", "custom-action-1")
	require.NoError(t, err)

	// controlOverrides: override one of the default controls for subArn.
	controlArn := subArn + "/control/1"
	require.NoError(t, b.UpdateStandardsControl(controlArn, "DISABLED", "not applicable"))

	// controlAssocOverrides (flattened nested map -> composite-key table).
	_, err = b.BatchUpdateStandardsControlAssociations([]map[string]any{
		{
			"SecurityControlId": "CloudTrail.1",
			"StandardsArn":      subs[0].StandardsArn,
			"AssociationStatus": "DISABLED",
			"UpdatedReason":     "test override",
		},
	})
	require.NoError(t, err)

	// productSubscriptions (raw map[string]string).
	productSubArn, err := b.EnableImportFindingsForProduct("arn:aws:securityhub:us-east-1::product/aws/guardduty")
	require.NoError(t, err)

	// controlParams (raw map[string]map[string]any).
	require.NoError(t, b.UpdateSecurityControl("S3.1", map[string]any{"threshold": float64(5)}, ""))

	// Automation rules.
	autoRuleArn, _ := b.CreateAutomationRule(map[string]any{
		"RuleName":   "my-rule",
		"RuleStatus": "ENABLED",
		"RuleOrder":  float64(1),
		"Criteria":   map[string]any{"k": "v"},
	})

	// Tags (raw map[string]map[string]string).
	require.NoError(t, b.TagResource(actionTargetArn, map[string]string{"owner": "team-x"}))

	// Members / invitations / admin / org config / org admin accounts.
	created, memberFailures := b.CreateMembers([]map[string]any{
		{"AccountId": "111111111111", "Email": "member@example.com"},
	})
	require.Empty(t, memberFailures)
	require.Len(t, created, 1)
	b.InviteMembers([]string{"111111111111"})

	require.NoError(t, b.AcceptAdministratorInvitation("222222222222", "invite-abc"))
	require.NoError(t, b.UpdateOrganizationConfiguration(true, "DEFAULT", "CENTRAL"))
	require.NoError(t, b.EnableOrganizationAdminAccount("333333333333"))

	// Finding aggregator.
	agg, err := b.CreateFindingAggregator("ALL_REGIONS", []string{"us-east-1", "us-west-2"})
	require.NoError(t, err)

	// Configuration policy + association.
	policy, err := b.CreateConfigurationPolicy("my-policy", "desc", map[string]any{"k": "v"}, nil)
	require.NoError(t, err)
	assoc, err := b.StartConfigurationPolicyAssociation(policy.Id, "444444444444", "ACCOUNT")
	require.NoError(t, err)

	// V2 resources.
	require.NoError(t, b.EnableSecurityHubV2(nil))

	aggV2, err := b.CreateAggregatorV2("ALL_REGIONS", []string{"us-east-1"})
	require.NoError(t, err)

	autoRuleV2, err := b.CreateAutomationRuleV2(
		"my-rule-v2", "ENABLED", "desc", map[string]any{"k": "v"}, nil, 1, false, nil,
	)
	require.NoError(t, err)

	connV2, err := b.CreateConnectorV2("my-connector", "desc", map[string]any{"k": "v"}, nil)
	require.NoError(t, err)

	ticketV2, err := b.CreateTicketV2(map[string]any{"k": "v"}, nil)
	require.NoError(t, err)

	recPolicy, err := b.GenerateRecommendedPolicyV2("metadata-uid-1")
	require.NoError(t, err)

	// --- Snapshot and restore into a fresh backend. ---

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Hub + tags.
	assert.True(t, securityhub.IsHubEnabled(b2))
	hub, err := b2.DescribeHub()
	require.NoError(t, err)
	assert.NotEmpty(t, hub.HubArn)

	tagsOut, err := b2.ListTagsForResource(actionTargetArn)
	require.NoError(t, err)
	assert.Equal(t, "team-x", tagsOut["owner"])

	// Findings.
	assert.Equal(t, 1, securityhub.FindingCount(b2))
	foundFindings, _ := b2.GetFindings(map[string]any{"Id": []any{map[string]any{"Value": "finding-1"}}}, nil, "", 10)
	require.Len(t, foundFindings, 1)

	// Insights.
	assert.Equal(t, 1, securityhub.InsightCount(b2))
	insights, _, err := b2.GetInsights([]string{insightArn}, "", 10)
	require.NoError(t, err)
	require.Len(t, insights, 1)
	assert.Equal(t, "my-insight", insights[0].Name)

	// Action targets.
	assert.Equal(t, 1, securityhub.ActionTargetCount(b2))
	ats, _ := b2.DescribeActionTargets([]string{actionTargetArn}, "", 10)
	require.Len(t, ats, 1)
	assert.Equal(t, "my-action", ats[0].Name)

	// Standards subscriptions.
	assert.Equal(t, 1, securityhub.StandardsSubscriptionCount(b2))
	enabledStandards, _ := b2.GetEnabledStandards([]string{subArn}, "", 10)
	require.Len(t, enabledStandards, 1)

	// controlOverrides applied on the restored backend.
	controls, _ := b2.DescribeStandardsControls(subArn, "", 10)
	var foundOverride bool

	for _, c := range controls {
		if c.StandardsControlArn == controlArn {
			assert.Equal(t, "DISABLED", c.ControlStatus)
			foundOverride = true
		}
	}

	assert.True(t, foundOverride, "control override must survive snapshot/restore")

	// controlAssocOverrides (flattened composite-key table).
	assocResults, unprocessedAssocs := b2.BatchGetStandardsControlAssociations([]map[string]any{
		{"SecurityControlId": "CloudTrail.1", "StandardsArn": subs[0].StandardsArn},
	})
	assert.Empty(t, unprocessedAssocs)
	require.Len(t, assocResults, 1)
	assert.Equal(t, "DISABLED", assocResults[0].AssociationStatus)

	// productSubscriptions (raw map).
	enabledProducts, _ := b2.ListEnabledProductsForImport("", 10)
	assert.Contains(t, enabledProducts, productSubArn)

	// controlParams (raw map) -- verified via BatchGetSecurityControls.
	secControls, secUnprocessed := b2.BatchGetSecurityControls([]string{"S3.1"})
	assert.Empty(t, secUnprocessed)
	require.Len(t, secControls, 1)
	assert.InEpsilon(t, float64(5), secControls[0].Parameters["threshold"], 0.0001)

	// Automation rules.
	autoRules, autoUnprocessed := b2.BatchGetAutomationRules([]string{autoRuleArn})
	assert.Empty(t, autoUnprocessed)
	require.Len(t, autoRules, 1)
	assert.Equal(t, "my-rule", autoRules[0].RuleName)

	// Members / invitations / admin / org config / org admin accounts.
	members, memberUnprocessed := b2.GetMembers([]string{"111111111111"})
	assert.Empty(t, memberUnprocessed)
	require.Len(t, members, 1)
	assert.Equal(t, "Invited", members[0].MemberStatus)

	invitations, _ := b2.ListInvitations("", 10)
	require.Len(t, invitations, 1)

	adminAccount, err := b2.GetAdministratorAccount()
	require.NoError(t, err)
	require.NotNil(t, adminAccount)
	assert.Equal(t, "222222222222", adminAccount.AccountId)

	orgConfig := b2.DescribeOrganizationConfiguration()
	assert.True(t, orgConfig.AutoEnable)

	orgAdmins, _ := b2.ListOrganizationAdminAccounts("", 10)
	require.Len(t, orgAdmins, 1)
	assert.Equal(t, "333333333333", orgAdmins[0].AccountId)

	// Finding aggregator.
	aggList, _ := b2.ListFindingAggregators("", 10)
	assert.Len(t, aggList, 1)
	gotAgg, err := b2.GetFindingAggregator(agg.FindingAggregatorArn)
	require.NoError(t, err)
	assert.Equal(t, "ALL_REGIONS", gotAgg.RegionLinkingMode)

	// Configuration policy + association.
	gotPolicy, err := b2.GetConfigurationPolicy(policy.Id)
	require.NoError(t, err)
	assert.Equal(t, "my-policy", gotPolicy.Name)

	gotAssoc, err := b2.GetConfigurationPolicyAssociation(assoc.TargetId, "ACCOUNT")
	require.NoError(t, err)
	assert.Equal(t, policy.Id, gotAssoc.ConfigurationPolicyId)

	// V2 resources.
	hubV2, err := b2.DescribeSecurityHubV2()
	require.NoError(t, err)
	assert.NotEmpty(t, hubV2.HubV2Arn)

	gotAggV2, err := b2.GetAggregatorV2(aggV2.AggregatorV2Arn)
	require.NoError(t, err)
	assert.Equal(t, "ALL_REGIONS", gotAggV2.RegionLinkingMode)

	gotRuleV2, err := b2.GetAutomationRuleV2(autoRuleV2.Identifier)
	require.NoError(t, err)
	assert.Equal(t, "my-rule-v2", gotRuleV2.RuleName)

	gotConnV2, err := b2.GetConnectorV2(connV2.ConnectorId)
	require.NoError(t, err)
	assert.Equal(t, "my-connector", gotConnV2.Name)

	assert.NotEmpty(t, ticketV2.TicketConfigurationArn)

	gotRecPolicy, err := b2.GetRecommendedPolicyV2(recPolicy.MetadataUid)
	require.NoError(t, err)
	assert.NotEmpty(t, gotRecPolicy.Policy)
}

// TestInMemoryBackend_RestoreDiscardsIncompatibleSnapshotVersion is a
// regression guard for the version guard introduced by the Phase 3.3
// pkgs/store conversion: a snapshot tagged with an unrecognised (or absent,
// i.e. pre-conversion) version must never be partially decoded as the
// current shape. It must be discarded cleanly -- Restore returns nil and the
// backend starts empty -- rather than surfaced as an error or silently
// misinterpreted.
func TestInMemoryBackend_RestoreDiscardsIncompatibleSnapshotVersion(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.EnableHub(false, nil))

	// Well-formed JSON, but tagged with a version this build does not
	// recognise (also covers the pre-Phase-3.3 shape, which decodes with
	// version == 0 and mismatches all the same).
	incompatibleSnapshot := []byte(`{"version":999,"hubEnabled":true,"tables":{}}`)

	err := b.Restore(t.Context(), incompatibleSnapshot)
	require.NoError(t, err,
		"an incompatible snapshot version must be discarded cleanly, not surfaced as a restore error")

	assert.False(t, securityhub.IsHubEnabled(b), "backend must start empty after discarding an incompatible snapshot")
	assert.Equal(t, 0, securityhub.ActionTargetCount(b))
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a
// service.Registerable (the *Handler returned by Provider.Init) in the
// persistence.Manager only if that Handler itself satisfies
// Snapshot(ctx)/Restore(ctx, []byte); InMemoryBackend implementing the same
// two methods (verified above by every TestInMemoryBackend_* case) is not
// enough on its own, since Handler.Backend is the StorageBackend interface
// and does not promote them. Mirrors services/appsync and services/athena's
// Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(b)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	require.NoError(t, b.EnableHub(false, nil))

	actionTargetArn, err := b.CreateActionTarget("my-action", "desc", "custom-action-1")
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := securityhub.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.True(t, securityhub.IsHubEnabled(restoredBackend))
	assert.Equal(t, 1, securityhub.ActionTargetCount(restoredBackend))

	ats, _ := restoredBackend.DescribeActionTargets([]string{actionTargetArn}, "", 10)
	require.Len(t, ats, 1)
	assert.Equal(t, "my-action", ats[0].Name)
}
