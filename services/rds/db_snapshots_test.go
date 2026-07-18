package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCopyTagsToSnapshotPersisted verifies CopyTagsToSnapshot is stored.
func TestCopyTagsToSnapshotPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"copytags-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"CopyTagsToSnapshot":   {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				CopyTagsToSnapshot bool `xml:"CopyTagsToSnapshot"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.CopyTagsToSnapshot)
}

// TestCopyDBSnapshotWithKmsKeyId verifies KmsKeyID and SourceRegion are persisted on copy.
func TestCopyDBSnapshotWithKmsKeyId(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("snap-src-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyID:         "arn:aws:kms:us-east-1:123:key/original",
	})
	require.NoError(t, err)

	snap, err := b.CreateDBSnapshot("src-snap", "snap-src-inst")
	require.NoError(t, err)
	require.Equal(t, "src-snap", snap.DBSnapshotIdentifier)

	// Copy with a new KMS key.
	copied, err := b.CopyDBSnapshot("src-snap", "dst-snap", rds.CopyDBSnapshotOptions{
		KmsKeyID:     "arn:aws:kms:us-east-1:123:key/new",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new", copied.KmsKeyID)
	assert.Equal(t, "us-west-2", copied.SourceRegion)
}

// TestSnapshotKmsKeyIdViaHandler verifies KmsKeyID is returned in snapshot response.
func TestSnapshotKmsKeyIdViaHandler(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("kms-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyID:         "arn:aws:kms:us-east-1:123:key/orig",
	})
	require.NoError(t, err)

	_, err = b.CreateDBSnapshot("kms-snap", "kms-inst")
	require.NoError(t, err)

	copied, err := b.CopyDBSnapshot("kms-snap", "kms-snap-copy", rds.CopyDBSnapshotOptions{
		KmsKeyID:     "arn:aws:kms:us-east-1:123:key/new-key",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new-key", copied.KmsKeyID)
	assert.Equal(t, "us-west-2", copied.SourceRegion)
}

// TestCopyDBSnapshotViaHandler verifies KmsKeyID in CopyDBSnapshot response.
func TestCopyDBSnapshotViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create instance + snapshot.
	mustCreateAccuracyRDSInstance(t, h, "cpy-snap-inst")
	snapRec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"src-snap-2"},
		"DBInstanceIdentifier": {"cpy-snap-inst"},
	})
	require.Equal(t, http.StatusOK, snapRec.Code)

	// Copy with KmsKeyId.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CopyDBSnapshot"},
		"Version":                    {"2014-10-31"},
		"SourceDBSnapshotIdentifier": {"src-snap-2"},
		"TargetDBSnapshotIdentifier": {"dst-snap-2"},
		"KmsKeyId":                   {"arn:aws:kms:us-east-1:123:key/copy-key"},
		"SourceRegion":               {"us-west-2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SourceRegion string `xml:"SourceRegion"`
			} `xml:"DBSnapshot"`
		} `xml:"CopyDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/copy-key", resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "us-west-2", resp.Result.DBSnapshot.SourceRegion)
}

// TestCreateDBSnapshotCopiesKmsKeyId verifies KmsKeyID is copied from encrypted instances.
func TestCreateDBSnapshotCopiesKmsKeyId(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"enc-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"StorageEncrypted":     {"true"},
		"KmsKeyId":             {"arn:aws:kms:us-east-1:123:key/mykey"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"enc-snap"},
		"DBInstanceIdentifier": {"enc-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SnapshotType string `xml:"SnapshotType"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/mykey", resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "manual", resp.Result.DBSnapshot.SnapshotType)
}

// TestCreateDBSnapshotNoKmsKeyIdForUnencrypted verifies KmsKeyID is not copied for unencrypted instances.
func TestCreateDBSnapshotNoKmsKeyIdForUnencrypted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "unenc-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"unenc-snap"},
		"DBInstanceIdentifier": {"unenc-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SnapshotType string `xml:"SnapshotType"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "manual", resp.Result.DBSnapshot.SnapshotType)
}

// Test_DeleteDBInstance_FinalSnapshotContract verifies AWS's
// SkipFinalSnapshot/FinalDBSnapshotIdentifier/DeleteAutomatedBackups contract
// for DeleteDBInstance: a final snapshot must be requested unless explicitly
// skipped, the two snapshot parameters are mutually exclusive, and a real
// manual snapshot is created and persisted before the instance is removed.
func Test_DeleteDBInstance_FinalSnapshotContract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		finalSnapshotID   string
		wantErrContains   string
		skipFinalSnapshot bool
	}{
		{
			name:            "missing both SkipFinalSnapshot and FinalDBSnapshotIdentifier is rejected",
			wantErrContains: "FinalDBSnapshotIdentifier",
		},
		{
			name:              "SkipFinalSnapshot with FinalDBSnapshotIdentifier is rejected",
			skipFinalSnapshot: true,
			finalSnapshotID:   "final-snap",
			wantErrContains:   "FinalDBSnapshotIdentifier",
		},
		{
			name:              "SkipFinalSnapshot alone deletes without a snapshot",
			skipFinalSnapshot: true,
		},
		{
			name:            "FinalDBSnapshotIdentifier alone takes a final snapshot",
			finalSnapshotID: "final-snap",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDBInstance(
				"del-inst", "postgres", "db.t3.micro", "", "admin", "",
				20, rds.DBInstanceOptions{},
			)
			require.NoError(t, err)

			_, err = b.DeleteDBInstanceWithOptions("del-inst", tt.skipFinalSnapshot, tt.finalSnapshotID, true)

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.Contains(t, err.Error(), "InvalidParameterCombination")
				// The instance must NOT have been deleted on a rejected request.
				_, describeErr := b.DescribeDBInstances("del-inst")
				assert.NoError(t, describeErr, "instance should still exist after a rejected delete")

				return
			}

			require.NoError(t, err)

			// Instance is gone either way.
			_, describeErr := b.DescribeDBInstances("del-inst")
			require.Error(t, describeErr, "instance should be removed after delete")

			snaps, _ := b.DescribeDBSnapshots("", "del-inst")
			if tt.finalSnapshotID == "" {
				assert.Empty(t, snaps, "no final snapshot should be created when SkipFinalSnapshot is set")

				return
			}

			require.Len(t, snaps, 1, "exactly one final snapshot should be created")
			snap := snaps[0]
			assert.Equal(t, tt.finalSnapshotID, snap.DBSnapshotIdentifier)
			assert.Equal(t, "del-inst", snap.DBInstanceIdentifier)
			assert.Equal(t, "manual", snap.SnapshotType)
			assert.Equal(t, "postgres", snap.Engine)
		})
	}
}

func TestDescribeDBSnapshots_FilterByInstance_ReturnsMatching(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-a")
	mustCreateAccOps2Instance(t, h, "inst-b")

	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-a1"}, "DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-a2"}, "DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-b1"}, "DBInstanceIdentifier": {"inst-b"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeDBSnapshotsResponse"`
		Result  struct {
			Snapshots []struct {
				DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
				DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
			} `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	ids := make([]string, 0, len(resp.Result.Snapshots))
	for _, s := range resp.Result.Snapshots {
		assert.Equal(t, "inst-a", s.DBInstanceIdentifier)
		ids = append(ids, s.DBSnapshotIdentifier)
	}
	assert.ElementsMatch(t, []string{"snap-a1", "snap-a2"}, ids)
}

func TestDescribeDBSnapshots_FilterByInstance_NoMatch_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-x")
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-x"}, "DBInstanceIdentifier": {"inst-x"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"no-such-instance"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct {
				DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
			} `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.Snapshots)
}

func TestDescribeDBSnapshots_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-p")
	mustCreateAccOps2Instance(t, h, "inst-q")

	for _, pair := range [][2]string{
		{"snap-p1", "inst-p"}, {"snap-p2", "inst-p"}, {"snap-q1", "inst-q"},
	} {
		doAccOps2(t, h, url.Values{
			"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
			"DBSnapshotIdentifier": {pair[0]}, "DBInstanceIdentifier": {pair[1]},
		}.Encode())
	}

	rec := doAccOps2(t, h, "Action=DescribeDBSnapshots&Version=2014-10-31")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct {
				DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
			} `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Snapshots, 3)
}

func TestDescribeDBSnapshots_SpecificID_StillWorks(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-s")
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-s"}, "DBInstanceIdentifier": {"inst-s"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-s"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct {
				DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
			} `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Snapshots, 1)
	assert.Equal(t, "snap-s", resp.Result.Snapshots[0].DBSnapshotIdentifier)
}

// TestSnapshotCount verifies the SnapshotCount export helper.
func TestSnapshotCount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rds.SnapshotCount(b))

	_, err := b.CreateDBInstance("i1", "mysql", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBSnapshot("snap1", "i1")
	require.NoError(t, err)

	assert.Equal(t, 1, rds.SnapshotCount(b))
}

// TestRDSBackend_CopyDBSnapshot tests snapshot copy operations.
func TestRDSBackend_CopyDBSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		setup    func(b *rds.InMemoryBackend)
		name     string
		sourceID string
		targetID string
		wantErr  bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"db",
					"postgres",
					"",
					"",
					"",
					"",
					20,
					rds.DBInstanceOptions{StorageEncrypted: true},
				)
				_, _ = b.CreateDBSnapshot("snap-src", "db")
			},
			sourceID: "snap-src",
			targetID: "snap-dst",
		},
		{
			name:     "source_not_found",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "no-snap",
			targetID: "snap-dst",
			wantErr:  true,
			errIs:    rds.ErrSnapshotNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
				_, _ = b.CreateDBSnapshot("snap-a", "db")
				_, _ = b.CreateDBSnapshot("snap-b", "db")
			},
			sourceID: "snap-a",
			targetID: "snap-b",
			wantErr:  true,
			errIs:    rds.ErrSnapshotAlreadyExists,
		},
		{
			name:     "empty_source",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "",
			targetID: "snap-dst",
			wantErr:  true,
			errIs:    rds.ErrInvalidParameter,
		},
		{
			name:     "empty_target",
			setup:    func(_ *rds.InMemoryBackend) {},
			sourceID: "snap-src",
			targetID: "",
			wantErr:  true,
			errIs:    rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			snap, err := b.CopyDBSnapshot(tt.sourceID, tt.targetID, rds.CopyDBSnapshotOptions{})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.targetID, snap.DBSnapshotIdentifier)
			assert.Equal(t, "available", snap.Status)
		})
	}
}

// TestRDSBackend_RestoreDBInstanceFromDBSnapshot tests instance restore from snapshot.
func TestRDSBackend_RestoreDBInstanceFromDBSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs      error
		setup      func(b *rds.InMemoryBackend)
		name       string
		instanceID string
		snapshotID string
		opts       rds.DBInstanceOptions
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance(
					"src-db",
					"postgres",
					"db.t3.micro",
					"mydb",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{StorageEncrypted: true},
				)
				_, _ = b.CreateDBSnapshot("snap", "src-db")
			},
			instanceID: "restored-db",
			snapshotID: "snap",
			opts:       rds.DBInstanceOptions{},
		},
		{
			name:       "snapshot_not_found",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "restored-db",
			snapshotID: "no-snap",
			wantErr:    true,
			errIs:      rds.ErrSnapshotNotFound,
		},
		{
			name: "instance_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("existing-db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
				_, _ = b.CreateDBSnapshot("snap", "existing-db")
			},
			instanceID: "existing-db",
			snapshotID: "snap",
			wantErr:    true,
			errIs:      rds.ErrInstanceAlreadyExists,
		},
		{
			name:       "empty_instance_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "",
			snapshotID: "snap",
			wantErr:    true,
			errIs:      rds.ErrInvalidParameter,
		},
		{
			name:       "empty_snapshot_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "restored-db",
			snapshotID: "",
			wantErr:    true,
			errIs:      rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			inst, err := b.RestoreDBInstanceFromDBSnapshot(tt.instanceID, tt.snapshotID, tt.opts)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, inst.DBInstanceIdentifier)
			assert.Equal(t, "available", inst.DBInstanceStatus)
		})
	}
}

func TestDescribeDBSnapshotAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		snapshotID string
		wantErr    bool
	}{
		{name: "success returns empty attrs", snapshotID: "snap-1"},
		{name: "not found", snapshotID: "missing", wantErr: true, wantErrIs: rds.ErrSnapshotNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.DescribeDBSnapshotAttributes(tt.snapshotID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.snapshotID, got.DBSnapshotIdentifier)
		})
	}
}

func TestModifyDBSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		snapshotID      string
		optionGroupName string
		engineVersion   string
		wantErr         bool
	}{
		{
			name:            "success update option group",
			snapshotID:      "snap-1",
			optionGroupName: "my-og",
			engineVersion:   "8.0.28",
		},
		{
			name:       "not found",
			snapshotID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrSnapshotNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBSnapshot(tt.snapshotID, tt.optionGroupName, tt.engineVersion)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.snapshotID, got.DBSnapshotIdentifier)
			if tt.optionGroupName != "" {
				assert.Equal(t, tt.optionGroupName, got.OptionGroupName)
			}
			if tt.engineVersion != "" {
				assert.Equal(t, tt.engineVersion, got.EngineVersion)
			}
		})
	}
}

func TestModifyDBSnapshotAttribute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs      error
		name           string
		snapshotID     string
		attributeName  string
		valuesToAdd    []string
		valuesToRemove []string
		wantCount      int
		wantErr        bool
	}{
		{
			name:          "add values",
			snapshotID:    "snap-1",
			attributeName: "restore",
			valuesToAdd:   []string{"123456789012", "all"},
			wantCount:     2,
		},
		{
			name:           "remove values",
			snapshotID:     "snap-1",
			attributeName:  "restore",
			valuesToAdd:    []string{"123456789012"},
			valuesToRemove: []string{"123456789012"},
			wantCount:      0,
		},
		{
			name:          "not found",
			snapshotID:    "missing",
			attributeName: "restore",
			wantErr:       true,
			wantErrIs:     rds.ErrSnapshotNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBSnapshotAttribute(tt.snapshotID, tt.attributeName, tt.valuesToAdd, tt.valuesToRemove)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			require.Len(t, got.DBSnapshotAttributes, 1)
			assert.Equal(t, tt.attributeName, got.DBSnapshotAttributes[0].AttributeName)
			assert.Len(t, got.DBSnapshotAttributes[0].AttributeValues, tt.wantCount)
		})
	}
}

// TestSnapshotCreateTime verifies SnapshotCreateTime and PercentProgress on CreateDBSnapshot.
func TestSnapshotCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-snap", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	before := time.Now().UTC()
	snap, err := b.CreateDBSnapshot("snap-1", "db-snap")
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, snap.SnapshotCreateTime.IsZero(), "SnapshotCreateTime should be set")
	assert.False(t, snap.SnapshotCreateTime.Before(before))
	assert.False(t, snap.SnapshotCreateTime.After(after))
	assert.Equal(t, 100, snap.PercentProgress, "completed snapshot should have 100% progress")
}

// TestDescribeDBSnapshotsSorted verifies deterministic sort order.
func TestDescribeDBSnapshotsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-sort", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	for _, id := range []string{"snap-z", "snap-a", "snap-m"} {
		_, snapErr := b.CreateDBSnapshot(id, "db-sort")
		require.NoError(t, snapErr)
	}
	got, err := b.DescribeDBSnapshots("", "")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "snap-a", got[0].DBSnapshotIdentifier)
	assert.Equal(t, "snap-m", got[1].DBSnapshotIdentifier)
	assert.Equal(t, "snap-z", got[2].DBSnapshotIdentifier)
}

// TestCopyDBSnapshotSetsTimestamp verifies CopyDBSnapshot sets SnapshotCreateTime and PercentProgress.
func TestCopyDBSnapshotSetsTimestamp(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-copy", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBSnapshot("snap-src", "db-copy")
	require.NoError(t, err)
	before := time.Now().UTC()
	copied, err := b.CopyDBSnapshot("snap-src", "snap-copy", rds.CopyDBSnapshotOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, copied.SnapshotCreateTime.IsZero())
	assert.False(t, copied.SnapshotCreateTime.Before(before))
	assert.False(t, copied.SnapshotCreateTime.After(after))
	assert.Equal(t, 100, copied.PercentProgress)
}
