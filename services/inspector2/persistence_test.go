package inspector2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// persistenceFixtureIDs carries the generated identifiers a caller of
// newPersistenceTestBackend needs to look up state whose ID isn't otherwise
// derivable from a stable, human-chosen name (codeSecurityScans and
// sbomExports are both keyed by a random UUID minted at creation time).
type persistenceFixtureIDs struct {
	codeSecurityScanID string
	sbomExportReportID string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// every raw map/struct left un-converted (tags, enabledTypes,
// codeSecurityScans, config, ec2DeepConfig, orgEc2Config, orgConfig), so a
// Snapshot from it exercises the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) (*inspector2.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := inspector2.NewInMemoryBackend("111111111111", "us-east-1")

	// filters table + tags raw map (CreateFilter tags the filter's own ARN).
	f, err := b.CreateFilter("filter1", "SUPPRESS", "descr", "reason",
		map[string]any{"severity": []any{map[string]any{"comparison": "EQUALS", "value": "HIGH"}}},
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	// findings table.
	_, err = b.SeedFinding(inspector2.Finding{
		FindingArn: "arn:aws:inspector2:us-east-1:111111111111:finding/f1",
		Severity:   inspector2.FindingSeverity{Label: "HIGH"},
	})
	require.NoError(t, err)

	// enabledTypes raw map.
	require.NoError(t, b.Enable([]string{"EC2"}))

	// config raw struct.
	require.NoError(t, b.UpdateConfiguration("EC2_HYBRID", "DAYS_30"))

	// members table.
	require.NoError(t, b.AssociateMember("222222222222"))

	// delegatedAdmins table.
	require.NoError(t, b.EnableDelegatedAdminAccount("333333333333"))

	// orgConfig raw struct.
	require.NoError(t, b.UpdateOrganizationConfiguration(inspector2.OrgConfiguration{AutoEnable: true}))

	// ec2DeepConfig raw struct.
	require.NoError(t, b.UpdateEc2DeepInspectionConfiguration([]string{"/opt/pkg"}))

	// orgEc2Config raw struct.
	require.NoError(t, b.UpdateOrgEc2DeepInspectionConfiguration([]string{"/opt/org-pkg"}))

	// memberEc2Status table.
	b.BatchUpdateMemberEc2DeepInspectionStatus([]*inspector2.MemberEc2DeepInspectionStatus{
		{AccountID: "222222222222", PackagePaths: []string{"/opt/member-pkg"}},
	})

	// encryptionKeys table (composite ResourceType/ScanType key).
	require.NoError(t, b.UpdateEncryptionKey("key-1", "ECR", "PACKAGE_VULNERABILITY"))

	// cisScanConfigs table + cisScans table/index (CreateCisScanConfiguration
	// materializes a completed scan for the config).
	cisCfg, err := b.CreateCisScanConfiguration("cis1", nil, map[string]any{
		"accountIds": []any{"111111111111"},
	}, nil)
	require.NoError(t, err)

	// cisSessions table.
	_, err = b.StartCisSession("job-1", "token-1")
	require.NoError(t, err)

	// codeSecurityIntegrations table.
	integ, err := b.CreateCodeSecurityIntegration("integ1", "GITHUB", nil, nil)
	require.NoError(t, err)

	// codeSecurityScanConfigs table.
	scanCfg, err := b.CreateCodeSecurityScanConfiguration("scancfg1", nil, nil, nil)
	require.NoError(t, err)

	// scanConfigAssociations table + byConfig index.
	_, err = b.BatchAssociateCodeSecurityScanConfiguration(scanCfg.Arn, []string{"repo1"})
	require.NoError(t, err)

	// codeSecurityScans raw map.
	scanResult, err := b.StartCodeSecurityScan("repo1")
	require.NoError(t, err)

	scanID, ok := scanResult["scanId"].(string)
	require.True(t, ok)

	// findingsReports table.
	_, err = b.CreateFindingsReport(map[string]any{"s3Destination": map[string]any{"bucketName": "b1"}})
	require.NoError(t, err)

	// sbomExports table.
	export, err := b.CreateSbomExport(map[string]any{"s3Destination": map[string]any{"bucketName": "b1"}})
	require.NoError(t, err)

	require.NotEmpty(t, f.Arn)
	require.NotEmpty(t, cisCfg.Arn)
	require.NotEmpty(t, integ.IntegrationArn)
	require.NotEmpty(t, scanCfg.Arn)

	return b, persistenceFixtureIDs{
		codeSecurityScanID: scanID,
		sbomExportReportID: export.ReportID,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (filters, findings, codeSecurityIntegrations, cisScanConfigs, cisScans,
// sbomExports, findingsReports, memberEc2Status, delegatedAdmins,
// codeSecurityScanConfigs, encryptionKeys, members, cisSessions,
// scanConfigAssociations), every secondary store.Index derived from them, and
// every raw map/struct left un-converted (tags, enabledTypes,
// codeSecurityScans, config, ec2DeepConfig, orgEc2Config, orgConfig) through
// Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := inspector2.NewInMemoryBackend("111111111111", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// filters table.
	filters, err := fresh.ListFilters(nil, "")
	require.NoError(t, err)
	require.Len(t, filters, 1)
	assert.Equal(t, "filter1", filters[0].Name)

	// tags raw map (TagResource/ListTagsForResource resolve against the
	// restored filter ARN).
	tags, err := fresh.ListTagsForResource(filters[0].Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	// findings table.
	findings, _, err := fresh.ListFindings(0, "", nil)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	assert.Equal(t, "HIGH", findings[0].Severity.Label)

	// enabledTypes raw map.
	assert.True(t, fresh.IsEnabled())
	status := fresh.GetStatus()
	assert.Equal(t, "ENABLED", status.Ec2Status)

	// config raw struct.
	cfg := fresh.GetConfiguration()
	assert.Equal(t, "EC2_HYBRID", cfg.Ec2ScanMode)
	assert.Equal(t, "DAYS_30", cfg.EcrRescanDuration)

	// members table.
	member, err := fresh.GetMember("222222222222")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", member.RelationshipStatus)

	// delegatedAdmins table.
	admin, err := fresh.GetDelegatedAdminAccount()
	require.NoError(t, err)
	assert.Equal(t, "333333333333", admin.AccountID)

	// orgConfig raw struct.
	assert.True(t, fresh.DescribeOrganizationConfiguration().AutoEnable)

	// ec2DeepConfig raw struct.
	ec2Cfg := fresh.GetEc2DeepInspectionConfiguration()
	assert.Equal(t, []string{"/opt/pkg"}, ec2Cfg.PackagePaths)
	assert.Equal(t, "ENABLED", ec2Cfg.Status)

	// memberEc2Status table.
	memberStatuses := fresh.BatchGetMemberEc2DeepInspectionStatus([]string{"222222222222"})
	require.Len(t, memberStatuses, 1)
	assert.Equal(t, []string{"/opt/member-pkg"}, memberStatuses[0].PackagePaths)

	// encryptionKeys table.
	encKey, err := fresh.GetEncryptionKey("ECR", "PACKAGE_VULNERABILITY")
	require.NoError(t, err)
	assert.Equal(t, "key-1", encKey.KmsKeyID)

	// cisScanConfigs table + cisScans table/index.
	cisCfgs, err := fresh.ListCisScanConfigurations()
	require.NoError(t, err)
	require.Len(t, cisCfgs, 1)

	cisScans, err := fresh.ListCisScans()
	require.NoError(t, err)
	require.Len(t, cisScans, 1)
	assert.Equal(t, cisCfgs[0].Arn, cisScans[0]["scanConfigurationArn"])

	// cisSessions table.
	require.Error(t, func() error {
		_, startErr := fresh.StartCisSession("", "")

		return startErr
	}())

	// codeSecurityIntegrations table.
	integs, err := fresh.ListCodeSecurityIntegrations()
	require.NoError(t, err)
	require.Len(t, integs, 1)
	assert.Equal(t, "integ1", integs[0].Name)

	// codeSecurityScanConfigs table.
	scanCfgs, err := fresh.ListCodeSecurityScanConfigurations()
	require.NoError(t, err)
	require.Len(t, scanCfgs, 1)
	assert.Equal(t, "scancfg1", scanCfgs[0].Name)

	// scanConfigAssociations table + byConfig index.
	assocs, err := fresh.ListCodeSecurityScanConfigurationAssociations(scanCfgs[0].Arn)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
	assert.Equal(t, "repo1", assocs[0].Resource)

	// codeSecurityScans raw map.
	scan, err := fresh.GetCodeSecurityScan(ids.codeSecurityScanID)
	require.NoError(t, err)
	assert.Equal(t, "repo1", scan["resourceId"])

	// findingsReports table.
	report, err := fresh.GetFindingsReportStatus("")
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", report.Status)

	// sbomExports table.
	export, err := fresh.GetSbomExport(ids.sbomExportReportID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", export.Status)

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "111111111111", fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, _ := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, inspector2.FilterCount(b))
	assert.Equal(t, 0, inspector2.FindingCount(b))

	filters, err := b.ListFilters(nil, "")
	require.NoError(t, err)
	assert.Empty(t, filters)

	assert.False(t, b.IsEnabled())

	_, err = b.GetMember("222222222222")
	require.ErrorIs(t, err, inspector2.ErrMemberNotFound)

	_, err = b.GetDelegatedAdminAccount()
	require.ErrorIs(t, err, inspector2.ErrDelegatedAdminNotFound)

	assert.False(t, b.DescribeOrganizationConfiguration().AutoEnable)

	ec2Cfg := b.GetEc2DeepInspectionConfiguration()
	assert.Empty(t, ec2Cfg.PackagePaths)
	assert.Equal(t, "DISABLED", ec2Cfg.Status)

	cisCfgs, err := b.ListCisScanConfigurations()
	require.NoError(t, err)
	assert.Empty(t, cisCfgs)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("111111111111", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on -- without it inspector2 was never registered as a persistable
// service despite the backend implementing Snapshot/Restore).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	b, _ := newPersistenceTestBackend(t)
	h := inspector2.NewHandler(b)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := inspector2.NewHandler(inspector2.NewInMemoryBackend("111111111111", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	filters, err := h2.Backend.ListFilters(nil, "")
	require.NoError(t, err)
	require.Len(t, filters, 1)
	assert.Equal(t, "filter1", filters[0].Name)
}
