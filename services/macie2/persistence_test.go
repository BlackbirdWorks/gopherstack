package macie2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which never delegated through Handler.Snapshot/Restore at all and
// so decodes with Version == 0) is discarded cleanly rather than partially
// decoded: the backend resets to empty state and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	_, err := b.CreateCustomDataIdentifier("cdi1", "", "foo", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, macie2.CustomDataIDCount(b))
}

// TestInMemoryBackend_RestoreV1SensitivityScoreOverrideDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding ResourceProfile's override
// flag under the pre-1217df451 key "sensitivityScoreOverride" must be
// discarded cleanly now that macie2SnapshotVersion is 2, rather than
// silently decoding it as false under the new key
// "sensitivityScoreOverridden".
func TestInMemoryBackend_RestoreV1SensitivityScoreOverrideDiscarded(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("111111111111", "us-east-1")

	const arn = "arn:aws:s3:::old-bucket"

	v1Snapshot := `{
		"version": 1,
		"accountID": "111111111111",
		"region": "us-east-1",
		"tables": {
			"resourceProfiles": [{
				"resourceArn": "` + arn + `",
				"sensitivityScore": 50,
				"sensitivityScoreOverride": true
			}]
		}
	}`

	require.NoError(t, b.Restore(t.Context(), []byte(v1Snapshot)),
		"a v1 snapshot must be discarded via the version guard, not partially decoded")

	got, err := b.GetResourceProfile(arn)
	require.NoError(t, err, "GetResourceProfile synthesizes a default rather than erroring")
	assert.False(t, got.SensitivityScoreOverridden,
		"incompatible-version snapshot must reset to empty, "+
			"not silently decode the override as false under the wrong key")
	assert.Zero(t, got.SensitivityScore)
}

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend1 := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	h := macie2.NewHandler(backend1)
	_, err := backend1.CreateCustomDataIdentifier("cdi1", "", "foo", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	h2 := macie2.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, macie2.CustomDataIDCount(backend2))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: all 13 store.Table-backed maps (allowLists,
// customDataIDs, findingsFilters, findings, s3Buckets, classificationJobs,
// members, invitations, orgAdminAccounts, autoDiscoveryAccounts, classScopes,
// resourceProfiles, sensitivityTemplates), plus the two plain maps left
// unconverted (tags, resourceDetections) and every single-pointer field
// (session, administrator, orgConfig, autoDiscoveryConfig, classExportConfig,
// findingsPubConfig, revealConfig).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	ids := seedFullState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := macie2.NewInMemoryBackend("111111111111", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertRestoredState(t, fresh, ids)
}

type restoredIDs struct {
	allowListID  string
	allowListArn string
	cdiID        string
	ffID         string
	findingID    string
	jobID        string
	scopeID      string
	templateID   string
}

// seedFullState populates original with one entry in every resource family
// the Phase 3.3 pkgs/store conversion touched -- all 13 store.Table-backed
// maps, the two plain maps left unconverted (tags, resourceDetections is
// exercised only as an empty round trip since no exported API populates it),
// and every single-pointer config field -- and returns the identifiers the
// caller needs to look each of them back up post-restore.
func seedFullState(t *testing.T, original *macie2.InMemoryBackend) restoredIDs {
	t.Helper()

	require.NoError(t, original.EnableMacie("", "SIX_HOURS", "ENABLED"))

	allowList, err := original.CreateAllowList("allow1", "desc", macie2.AllowListCriteria{
		Regex: new("test-regex"),
	}, map[string]string{"env": "test"})
	require.NoError(t, err)

	require.NoError(t, original.TagResource(allowList.Arn, map[string]string{"k": "v"}))

	cdiID, err := original.CreateCustomDataIdentifier(
		"cdi1", "desc", "foo", []string{"ignore"}, []string{"keyword"}, nil, nil, nil,
	)
	require.NoError(t, err)

	ff, err := original.CreateFindingsFilter("ff1", "desc", "ARCHIVE", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, original.CreateSampleFindings([]string{"SensitiveData:S3Object/Personal"}))
	findingIDs, _, err := original.ListFindings(nil, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, findingIDs, 1)

	original.AddS3Bucket(macie2.S3BucketMetadata{
		AccountID:  "111111111111",
		BucketArn:  "arn:aws:s3:::bucket1",
		BucketName: "bucket1",
		Region:     "us-east-1",
	})

	jobID, _, err := original.CreateClassificationJob(
		"job1", "desc", "ONE_TIME", "token1", nil, nil, nil, nil, nil, "", nil, 100, true,
	)
	require.NoError(t, err)

	require.NoError(t, original.CreateMember("222222222222", "member@example.com", nil))

	_, err = original.CreateInvitations([]string{"333333333333"}, "", false)
	require.NoError(t, err)

	require.NoError(t, original.AcceptInvitation("444444444444", "invitation1"))
	require.NoError(t, original.EnableOrganizationAdminAccount("555555555555"))
	require.NoError(t, original.UpdateOrganizationConfiguration(true))
	require.NoError(t, original.UpdateAutomatedDiscoveryConfiguration("ALL", "ENABLED"))

	require.NoError(t, original.BatchUpdateAutomatedDiscoveryAccounts([]macie2.AutoDiscoveryAccountUpdate{
		{AccountID: "666666666666", Status: "ENABLED"},
	}))

	require.NoError(t, original.PutClassificationExportConfiguration(&macie2.ClassificationExportConfig{
		S3Destination: &macie2.ClassificationExportS3Dest{BucketName: "export-bucket"},
	}))

	scopes, err := original.ListClassificationScopes()
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	scopeID := scopes[0].ID
	require.NoError(t, original.UpdateClassificationScope(scopeID, &macie2.ClassificationScopeS3Update{
		Excludes: &macie2.ClassificationScopeS3ExclusionUpdate{
			BucketNames: []string{"bucket1"},
			Operation:   "ADD",
		},
	}))

	require.NoError(t, original.PutFindingsPublicationConfiguration(&macie2.FindingsPublicationConfig{
		SecurityHubConfiguration: &macie2.SecurityHubConfig{PublishClassificationFindings: true},
	}))

	require.NoError(t, original.UpdateResourceProfile("arn:aws:s3:::bucket1", 42))
	require.NoError(t, original.UpdateRevealConfiguration("kms-key-1", "ENABLED"))

	templates, err := original.ListSensitivityInspectionTemplates()
	require.NoError(t, err)
	require.Len(t, templates, 1)
	templateID := templates[0].ID
	desc := "desc"
	require.NoError(t, original.UpdateSensitivityInspectionTemplate(
		templateID, "tmpl1", &desc, map[string]any{"a": 1}, nil,
	))

	return restoredIDs{
		allowListID:  allowList.ID,
		allowListArn: allowList.Arn,
		cdiID:        cdiID,
		ffID:         ff.ID,
		findingID:    findingIDs[0],
		jobID:        jobID,
		scopeID:      scopeID,
		templateID:   templateID,
	}
}

func assertRestoredState(t *testing.T, fresh *macie2.InMemoryBackend, ids restoredIDs) {
	t.Helper()

	sess := fresh.GetSession()
	require.NotNil(t, sess)
	assert.Equal(t, "SIX_HOURS", sess.FindingPublishingFrequency)

	al, err := fresh.GetAllowList(ids.allowListID)
	require.NoError(t, err)
	assert.Equal(t, "test", al.Tags["env"])

	tags, err := fresh.ListTagsForResource(ids.allowListArn)
	require.NoError(t, err)
	assert.Equal(t, "v", tags["k"])

	cdi, err := fresh.GetCustomDataIdentifier(ids.cdiID)
	require.NoError(t, err)
	assert.Equal(t, "foo", cdi.Regex)

	ffDetail, err := fresh.GetFindingsFilter(ids.ffID)
	require.NoError(t, err)
	assert.Equal(t, "ARCHIVE", ffDetail.Action)

	findings, err := fresh.GetFindings([]string{ids.findingID})
	require.NoError(t, err)
	require.Len(t, findings, 1)

	buckets, _, err := fresh.DescribeBuckets(nil, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	job, err := fresh.DescribeClassificationJob(ids.jobID)
	require.NoError(t, err)
	assert.Equal(t, "job1", job.Name)

	member, err := fresh.GetMember("222222222222")
	require.NoError(t, err)
	assert.Equal(t, "member@example.com", member.Email)

	invitations, err := fresh.ListInvitations()
	require.NoError(t, err)
	require.Len(t, invitations, 1)
	assert.Equal(t, "333333333333", invitations[0].AccountID)

	admin, err := fresh.GetAdministratorAccount()
	require.NoError(t, err)
	require.NotNil(t, admin)
	assert.Equal(t, "444444444444", admin.AccountID)

	orgAdmins, err := fresh.ListOrganizationAdminAccounts()
	require.NoError(t, err)
	require.Len(t, orgAdmins, 1)

	orgConfig, err := fresh.DescribeOrganizationConfiguration()
	require.NoError(t, err)
	assert.True(t, orgConfig.AutoEnable)

	adConfig, err := fresh.GetAutomatedDiscoveryConfiguration()
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", adConfig.Status)

	adAccounts, err := fresh.ListAutomatedDiscoveryAccounts()
	require.NoError(t, err)
	require.Len(t, adAccounts, 1)

	exportCfg, err := fresh.GetClassificationExportConfiguration()
	require.NoError(t, err)
	require.NotNil(t, exportCfg.S3Destination)
	assert.Equal(t, "export-bucket", exportCfg.S3Destination.BucketName)

	scope, err := fresh.GetClassificationScope(ids.scopeID)
	require.NoError(t, err)
	assert.NotNil(t, scope.S3.Excludes)

	pubCfg, err := fresh.GetFindingsPublicationConfiguration()
	require.NoError(t, err)
	require.NotNil(t, pubCfg.SecurityHubConfiguration)
	assert.True(t, pubCfg.SecurityHubConfiguration.PublishClassificationFindings)

	profile, err := fresh.GetResourceProfile("arn:aws:s3:::bucket1")
	require.NoError(t, err)
	assert.Equal(t, int32(42), profile.SensitivityScore)

	reveal, err := fresh.GetRevealConfiguration()
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", reveal.Status)

	tmpl, err := fresh.GetSensitivityInspectionTemplate(ids.templateID)
	require.NoError(t, err)
	assert.Equal(t, "tmpl1", tmpl.Name)
}
