package guardduty_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state (every store.Table --
// both the "clean" ones on the registry and the "dirty" ones reset
// individually -- plus the raw tags map) and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	d, err := b.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFilter(d.DetectorID, "seed-filter", "", "NOOP", 1, nil, nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListDetectors())

	_, err = b.GetDetector(d.DetectorID)
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches guarddutySnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (one JSON field per resource map, no
// "version"/"tables" keys at all).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	d, err := b.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	oldShape := `{"detectors":{"` + d.DetectorID + `":{"detectorId":"` + d.DetectorID + `"}},"memberSeq":0}`
	err = b.Restore(ctx, []byte(oldShape))
	require.NoError(t, err)

	assert.Empty(t, b.ListDetectors())
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched:
//
//   - "clean" tables registered directly on the registry: detectors,
//     findings, members, invitations, orgAdminAccounts, malwareScans,
//     malwareProtectionPlans.
//   - "dirty" detector-composite-keyed tables carrying a hidden DetectorID
//     through a detectorDTO: filters, ipSets, threatIntelSets,
//     publishingDestinations, threatEntitySets, trustedEntitySets.
//   - "dirty" identity-less tables carrying a hidden detectorID through a
//     byDetectorDTO: orgConfigs, adminAccounts, malwareScanSettings.
//   - the raw tags map and memberSeq counter left unconverted.
//
// It also exercises the byDetector secondary indexes (via the List*
// operations), confirming they are correctly rebuilt by Restore rather than
// left stale.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := guardduty.NewInMemoryBackend("222233334444", "us-west-2")
	ctx := t.Context()

	d, err := original.CreateDetector(true, "SIX_HOURS", map[string]string{"env": "test"}, nil)
	require.NoError(t, err)
	detectorID := d.DetectorID

	filter, err := original.CreateFilter(
		detectorID, "filter-1", "a filter", "ARCHIVE", 1,
		map[string]any{"criterion": map[string]any{}}, map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	require.NoError(t, original.CreateSampleFindings(detectorID, []string{"Sample:Type"}))
	findingIDs, _, err := original.ListFindings(detectorID, guardduty.FindingsQuery{})
	require.NoError(t, err)
	require.Len(t, findingIDs, 1)

	ipSet, err := original.CreateIPSet(detectorID, "ipset-1", "TXT", "s3://bucket/key", true, nil, "")
	require.NoError(t, err)

	tiSet, err := original.CreateThreatIntelSet(detectorID, "ti-1", "TXT", "s3://bucket/ti", true, nil, "")
	require.NoError(t, err)

	teSet, err := original.CreateThreatEntitySet(detectorID, "te-1", "TXT", "s3://bucket/te", true, nil, "999988887777")
	require.NoError(t, err)

	trSet, err := original.CreateTrustedEntitySet(detectorID, "tr-1", "TXT", "s3://bucket/tr", true, nil, "")
	require.NoError(t, err)

	require.NoError(t, original.UpdateDetector(detectorID, nil, "", []guardduty.DetectorFeature{
		{Name: "AI_ANALYST", Status: "ENABLED"},
	}))
	inv, err := original.CreateInvestigation(detectorID, "Investigate finding in account 222233334444")
	require.NoError(t, err)

	created, unprocessed := original.CreateMembers(detectorID, []map[string]any{
		{"accountId": "555566667777", "email": "member@example.com"},
	})
	require.Empty(t, unprocessed)
	require.Len(t, created, 1)

	unprocessed, err = original.InviteMembers(detectorID, []string{"555566667777"})
	require.NoError(t, err)
	require.Empty(t, unprocessed)
	require.Equal(t, 1, original.GetInvitationsCount())

	require.NoError(t, original.EnableOrganizationAdminAccount("888899990000"))

	require.NoError(t, original.UpdateOrganizationConfiguration(detectorID, true, "", []guardduty.OrgFeature{
		{Name: "S3_DATA_EVENTS", AutoEnable: "NEW"},
	}))

	require.NoError(t, original.AcceptAdministratorInvitation(detectorID, "111100002222", "invite-abc"))

	dest, err := original.CreatePublishingDestination(detectorID, "S3", guardduty.DestinationProperties{
		DestinationArn: "arn:aws:s3:::bucket",
	}, nil)
	require.NoError(t, err)

	scanID, err := original.StartMalwareScan("arn:aws:ec2:us-west-2:222233334444:instance/i-1")
	require.NoError(t, err)

	require.NoError(t, original.UpdateMalwareScanSettings(detectorID, &guardduty.MalwareScanSettings{
		EbsSnapshotPreservation: "RETENTION_WITH_FINDING",
		ScanResourceCriteria:    map[string]any{},
	}))

	plan, err := original.CreateMalwareProtectionPlan(
		"arn:aws:iam::222233334444:role/scan-role", map[string]any{}, map[string]any{},
		map[string]string{"team": "sec"},
	)
	require.NoError(t, err)

	require.NoError(t, original.TagResource(dest.DestinationID, map[string]string{"extra": "tag"}))

	data := original.Snapshot(ctx)
	require.NotNil(t, data)

	restored := guardduty.NewInMemoryBackend("000000000000", "")
	require.NoError(t, restored.Restore(ctx, data))

	// account/region carried through the snapshot.
	assert.Equal(t, "222233334444", restored.AccountID())
	assert.Equal(t, "us-west-2", restored.Region())

	// detectors ("clean").
	assert.Equal(t, []string{detectorID}, restored.ListDetectors())
	gotDetector, err := restored.GetDetector(detectorID)
	require.NoError(t, err)
	assert.Equal(t, d.Tags, gotDetector.Tags)

	// filters ("dirty", detector-composite).
	gotFilterNames, err := restored.ListFilters(detectorID)
	require.NoError(t, err)
	assert.Equal(t, []string{filter.Name}, gotFilterNames)
	gotFilter, err := restored.GetFilter(detectorID, filter.Name)
	require.NoError(t, err)
	assert.Equal(t, filter.Description, gotFilter.Description)

	// findings ("clean", detector-composite).
	gotFindingIDs, _, err := restored.ListFindings(detectorID, guardduty.FindingsQuery{})
	require.NoError(t, err)
	assert.Equal(t, findingIDs, gotFindingIDs)

	// ipSets ("dirty", detector-composite).
	gotIPSetIDs, err := restored.ListIPSets(detectorID)
	require.NoError(t, err)
	assert.Equal(t, []string{ipSet.IPSetID}, gotIPSetIDs)

	// threatIntelSets ("dirty", detector-composite).
	gotTISetIDs, err := restored.ListThreatIntelSets(detectorID)
	require.NoError(t, err)
	assert.Equal(t, []string{tiSet.ThreatIntelSetID}, gotTISetIDs)

	// threatEntitySets / trustedEntitySets ("dirty", detector-composite).
	gotTESetIDs, err := restored.ListThreatEntitySets(detectorID)
	require.NoError(t, err)
	assert.Equal(t, []string{teSet.ThreatEntitySetID}, gotTESetIDs)

	gotTESet, err := restored.GetThreatEntitySet(detectorID, teSet.ThreatEntitySetID)
	require.NoError(t, err)
	assert.Equal(t, "999988887777", gotTESet.ExpectedBucketOwner)

	gotTRSetIDs, err := restored.ListTrustedEntitySets(detectorID)
	require.NoError(t, err)
	assert.Equal(t, []string{trSet.TrustedEntitySetID}, gotTRSetIDs)

	// investigations ("dirty", detector-composite).
	gotInv, err := restored.GetInvestigation(detectorID, inv.InvestigationID)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", gotInv.Status)
	assert.Equal(t, inv.TriggerPrompt, gotInv.TriggerPrompt)

	gotInvList, _, err := restored.ListInvestigations(detectorID, guardduty.InvestigationsQuery{})
	require.NoError(t, err)
	require.Len(t, gotInvList, 1)
	assert.Equal(t, inv.InvestigationID, gotInvList[0].InvestigationID)

	// members ("clean", detector-composite).
	members, err := restored.ListMembers(detectorID, false)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, "555566667777", members[0].AccountID)
	assert.Equal(t, "Invited", members[0].RelationshipStatus)

	// invitations ("clean", flat) + memberSeq (raw scalar carried through --
	// InviteMembers increments memberSeq once per account, so a second
	// invite call after restore should mint invite index 2, not repeat 1).
	assert.Equal(t, 1, restored.GetInvitationsCount())
	restored.InviteMembers(detectorID, []string{"555566667777"})
	assert.Equal(t, 2, restored.GetInvitationsCount())

	// orgAdminAccounts ("clean", flat).
	orgAdmins := restored.ListOrganizationAdminAccounts()
	require.Len(t, orgAdmins, 1)
	assert.Equal(t, "888899990000", orgAdmins[0].AdminAccountID)

	// orgConfigs ("dirty", identity-less).
	orgCfg, err := restored.DescribeOrganizationConfiguration(detectorID)
	require.NoError(t, err)
	assert.True(t, orgCfg.AutoEnable)
	require.Len(t, orgCfg.Features, 1)
	assert.Equal(t, "S3_DATA_EVENTS", orgCfg.Features[0].Name)

	// adminAccounts ("dirty", identity-less).
	admin, err := restored.GetAdministratorAccount(detectorID)
	require.NoError(t, err)
	assert.Equal(t, "111100002222", admin.AccountID)
	assert.Equal(t, "invite-abc", admin.InvitationID)

	// publishingDestinations ("dirty", detector-composite).
	destinations, err := restored.ListPublishingDestinations(detectorID)
	require.NoError(t, err)
	require.Len(t, destinations, 1)
	assert.Equal(t, dest.DestinationID, destinations[0].DestinationID)
	assert.Equal(t, "arn:aws:s3:::bucket", destinations[0].DestinationProperties.DestinationArn)

	// malwareScans ("clean", flat).
	gotScan, err := restored.GetMalwareScan(scanID)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", gotScan.ScanStatus)
	assert.Equal(t, "arn:aws:ec2:us-west-2:222233334444:instance/i-1", gotScan.ResourceArn)
	assert.Equal(t, "EC2_INSTANCE", gotScan.ResourceType)
	assert.Equal(t, detectorID, gotScan.DetectorID, "StartMalwareScan resolves the account's own detector")

	// malwareScanSettings ("dirty", identity-less).
	settings, err := restored.GetMalwareScanSettings(detectorID)
	require.NoError(t, err)
	assert.Equal(t, "RETENTION_WITH_FINDING", settings.EbsSnapshotPreservation)

	// malwareProtectionPlans ("clean", flat).
	gotPlan, err := restored.GetMalwareProtectionPlan(plan.MalwareProtectionPlanID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::222233334444:role/scan-role", gotPlan.Role)

	// tags (raw, unconverted map).
	gotTags, err := restored.ListTagsForResource(dest.DestinationID)
	require.NoError(t, err)
	assert.Equal(t, "tag", gotTags["extra"])

	// DeleteDetector cascade-deletes every detector-nested table (including
	// the byDetector indexes rebuilt by Restore); if any index were left
	// stale, this would leave orphaned entries findable via a fresh
	// List/Get after the cascade.
	require.NoError(t, restored.DeleteDetector(detectorID))
	_, err = restored.GetFilter(detectorID, filter.Name)
	require.Error(t, err)
	_, err = restored.GetInvestigation(detectorID, inv.InvestigationID)
	require.Error(t, err, "investigations must be cascade-deleted with their detector, restored index included")
}
