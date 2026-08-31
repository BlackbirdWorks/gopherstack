package dynamodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCollectBackupSummaries_TimeRangeBoundsInclusivity drives
// collectBackupSummaries directly (whitebox: package dynamodb, not
// dynamodb_test) against a backup whose CreationDateTime is constructed with
// zero sub-second fraction, so an exact-boundary comparison is meaningful.
//
// api_op_ListBackups.go doc comments: TimeRangeLowerBound is inclusive
// (only backups created after or at that time are listed) and
// TimeRangeUpperBound is exclusive (only backups created strictly before it).
func TestCollectBackupSummaries_TimeRangeBoundsInclusivity(t *testing.T) {
	t.Parallel()

	db := NewInMemoryDB()
	region := db.defaultRegion

	boundarySec := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	created := time.Unix(boundarySec, 0).UTC()

	backup := Backup{
		BackupArn:        "arn:aws:dynamodb:" + region + ":123456789012:table/T/backup/0000000000000-abc",
		BackupName:       "b1",
		BackupStatus:     "AVAILABLE",
		BackupType:       "USER",
		TableName:        "T",
		CreationDateTime: created,
	}
	db.backups.Put(&backup)

	boundary := float64(boundarySec)

	t.Run("lower_bound_inclusive", func(t *testing.T) {
		t.Parallel()

		summaries := collectBackupSummariesRLocked(db, region, "", "", &boundary, nil)
		assert.Len(t, summaries, 1,
			"TimeRangeLowerBound is documented inclusive: a backup created exactly "+
				"at the bound must be included, not excluded")
	})

	t.Run("upper_bound_exclusive", func(t *testing.T) {
		t.Parallel()

		summaries := collectBackupSummariesRLocked(db, region, "", "", nil, &boundary)
		assert.Empty(t, summaries,
			"TimeRangeUpperBound is documented exclusive: a backup created exactly "+
				"at the bound must be excluded")
	})

	t.Run("lower_bound_excludes_earlier", func(t *testing.T) {
		t.Parallel()

		afterBoundary := boundary + 1
		summaries := collectBackupSummariesRLocked(db, region, "", "", &afterBoundary, nil)
		assert.Empty(t, summaries, "a backup created before the (inclusive) lower bound must be excluded")
	})
}
