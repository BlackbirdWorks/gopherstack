package dlm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	westRegion  = "us-west-2"
	sharedAcct  = "111122223333"
	hour        = time.Hour
	retainFive  = 5
	archiveTier = "archive"
)

func rulesSchedule(mod func(*dlmtypes.Schedule)) dlmtypes.Schedule {
	s := intervalSchedule(&dlmtypes.RetainRule{Count: aws.Int32(retainFive)})
	if mod != nil {
		mod(&s)
	}

	return s
}

func (e *execEnv) runAt(t *testing.T, created time.Time, offsets ...time.Duration) {
	t.Helper()

	for _, d := range offsets {
		e.dlmBk.RunSchedules(t.Context(), created.Add(d))
	}
}

func (e *execEnv) fastRestoreEnabled(t *testing.T) map[string]bool {
	t.Helper()

	out, err := e.ec2Client.DescribeFastSnapshotRestores(t.Context(), &ec2sdk.DescribeFastSnapshotRestoresInput{})
	require.NoError(t, err)

	got := map[string]bool{}

	for _, f := range out.FastSnapshotRestores {
		if f.State == ec2types.FastSnapshotRestoreStateCodeEnabled {
			got[aws.ToString(f.SnapshotId)+"@"+aws.ToString(f.AvailabilityZone)] = true
		}
	}

	return got
}

func TestPolicyExecution_FastRestoreRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rule    *dlmtypes.FastRestoreRule
		wantNew int
		wantOld int
	}{
		{
			name:    "count",
			rule:    &dlmtypes.FastRestoreRule{AvailabilityZones: []string{"us-east-1a"}, Count: aws.Int32(1)},
			wantNew: 1,
		},
		{
			name: "interval",
			rule: &dlmtypes.FastRestoreRule{
				AvailabilityZones: []string{"us-east-1a", "us-east-1b"}, Interval: aws.Int32(1),
				IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			wantNew: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes"})
			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
				volumePolicyDetails(rulesSchedule(func(s *dlmtypes.Schedule) { s.FastRestoreRule = tt.rule })))

			env.runAt(t, created, time.Second)

			first := aws.ToString(env.managedSnapshots(t, policyID)[0].SnapshotId)
			assert.Len(t, env.fastRestoreEnabled(t), len(tt.rule.AvailabilityZones), "enabled on creation")

			env.runAt(t, created, 13*hour, 25*hour)

			for key := range env.fastRestoreEnabled(t) {
				assert.NotContains(t, key, first, "the oldest snapshot lost fast restore")
			}

			assert.Len(t, env.managedSnapshots(t, policyID), 3)
			assert.NotEmpty(t, env.fastRestoreEnabled(t))
		})
	}
}

func TestPolicyExecution_ShareRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		unshare      *int32
		wantAfterDay bool
	}{
		{name: "unshare_after_a_day", unshare: aws.Int32(1)},
		{name: "shared_until_deleted", wantAfterDay: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes"})
			rule := dlmtypes.ShareRule{TargetAccounts: []string{sharedAcct}}

			if tt.unshare != nil {
				rule.UnshareInterval = tt.unshare
				rule.UnshareIntervalUnit = dlmtypes.RetentionIntervalUnitValuesDays
			}

			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
				volumePolicyDetails(rulesSchedule(func(s *dlmtypes.Schedule) { s.ShareRules = []dlmtypes.ShareRule{rule} })))

			var snap string

			perms := func() []string {
				out, err := env.ec2Client.DescribeSnapshotAttribute(t.Context(), &ec2sdk.DescribeSnapshotAttributeInput{
					SnapshotId: aws.String(snap), Attribute: ec2types.SnapshotAttributeNameCreateVolumePermission,
				})
				require.NoError(t, err)

				var ids []string
				for _, p := range out.CreateVolumePermissions {
					ids = append(ids, aws.ToString(p.UserId))
				}

				return ids
			}

			env.runAt(t, created, time.Second)
			snap = aws.ToString(env.managedSnapshots(t, policyID)[0].SnapshotId)
			assert.Equal(t, []string{sharedAcct}, perms())

			env.runAt(t, created, 25*hour)

			if tt.wantAfterDay {
				assert.Equal(t, []string{sharedAcct}, perms())
			} else {
				assert.Empty(t, perms())
			}
		})
	}
}

func TestPolicyExecution_CrossRegionCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceRetain *dlmtypes.RetainRule
		copyTags     bool
		wantCopies   int
		wantSources  int
		runs         []time.Duration
	}{
		{
			name: "copy_expires_after_its_own_retention", copyTags: true, wantCopies: 1, wantSources: 3,
			sourceRetain: &dlmtypes.RetainRule{Count: aws.Int32(retainFive)},
			runs:         []time.Duration{time.Second, 25 * hour, 50 * hour},
		},
		{
			name: "copies_outlive_source", wantCopies: 3, wantSources: 1,
			sourceRetain: &dlmtypes.RetainRule{Count: aws.Int32(1)},
			runs:         []time.Duration{time.Second, 13 * hour, 25 * hour},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes", "app": "web"})

			retain := &dlmtypes.CrossRegionCopyRetainRule{
				Interval: aws.Int32(1), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			}
			if tt.name == "copies_outlive_source" {
				retain.Interval = aws.Int32(30)
			}

			rule := dlmtypes.CrossRegionCopyRule{
				TargetRegion: aws.String(westRegion), Encrypted: aws.Bool(true),
				CopyTags: aws.Bool(tt.copyTags), RetainRule: retain,
			}
			sched := intervalSchedule(tt.sourceRetain)
			sched.CrossRegionCopyRules = []dlmtypes.CrossRegionCopyRule{rule}

			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled, volumePolicyDetails(sched))
			env.runAt(t, created, tt.runs...)

			west := env.ec2H.BackendFor(westRegion)
			copies, err := west.DescribeSnapshots(nil)
			require.NoError(t, err)
			require.Len(t, copies, tt.wantCopies)

			for _, c := range copies {
				assert.True(t, c.Encrypted)

				tags := west.TagsForResource(c.SnapshotID)
				assert.Equal(t, policyID, tags["aws:dlm:lifecycle-policy-id"])
				assert.NotContains(t, tags, "instance", "variable tags are not copied")

				if tt.copyTags {
					assert.Equal(t, "dlm", tags["owner"])
				} else {
					assert.NotContains(t, tags, "owner")
				}
			}

			assert.Len(t, env.managedSnapshots(t, policyID), tt.wantSources)
		})
	}
}

func TestPolicyExecution_ArchiveRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		retain       *dlmtypes.RetainRule
		tier         *dlmtypes.RetentionArchiveTier
		runs         []time.Duration
		wantArchived int
		wantStandard int
	}{
		{
			name:   "count",
			retain: &dlmtypes.RetainRule{Count: aws.Int32(1)},
			tier:   &dlmtypes.RetentionArchiveTier{Count: aws.Int32(1)},
			runs:   []time.Duration{time.Second, 13 * hour, 25 * hour}, wantArchived: 1, wantStandard: 1,
		},
		{
			name:   "archive_immediately",
			retain: &dlmtypes.RetainRule{Count: aws.Int32(0)},
			tier:   &dlmtypes.RetentionArchiveTier{Count: aws.Int32(3)},
			runs:   []time.Duration{time.Second, 13 * hour}, wantArchived: 2,
		},
		{
			name: "age",
			retain: &dlmtypes.RetainRule{
				Interval: aws.Int32(1), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			tier: &dlmtypes.RetentionArchiveTier{
				Interval: aws.Int32(90), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			runs: []time.Duration{time.Second, 25 * hour}, wantArchived: 1, wantStandard: 1,
		},
		{
			name: "age_archive_expires",
			retain: &dlmtypes.RetainRule{
				Interval: aws.Int32(1), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			tier: &dlmtypes.RetentionArchiveTier{
				Interval: aws.Int32(90), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			runs: []time.Duration{time.Second, 25 * hour, 25*hour + 91*24*hour}, wantArchived: 1, wantStandard: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes"})

			sched := intervalSchedule(tt.retain)
			sched.ArchiveRule = &dlmtypes.ArchiveRule{
				RetainRule: &dlmtypes.ArchiveRetainRule{RetentionArchiveTier: tt.tier},
			}

			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled, volumePolicyDetails(sched))
			env.runAt(t, created, tt.runs...)

			tiers, err := env.ec2Client.DescribeSnapshotTierStatus(t.Context(), &ec2sdk.DescribeSnapshotTierStatusInput{})
			require.NoError(t, err)

			archived := map[string]bool{}

			for _, ts := range tiers.SnapshotTierStatuses {
				if ts.StorageTier == ec2types.StorageTierArchive {
					archived[aws.ToString(ts.SnapshotId)] = true
				}
			}

			snaps := env.managedSnapshots(t, policyID)
			standard := 0

			for _, s := range snaps {
				if archived[aws.ToString(s.SnapshotId)] {
					continue
				}

				standard++
			}

			assert.Len(t, archived, tt.wantArchived)
			assert.Equal(t, tt.wantStandard, standard)
			assert.Len(t, snaps, tt.wantArchived+tt.wantStandard, "deleted snapshots are gone")
		})
	}
}

func TestPolicyExecution_DeprecateRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		rule           *dlmtypes.DeprecateRule
		runs           []time.Duration
		wantDeprecated int
		wantImages     int
	}{
		{
			name: "interval",
			rule: &dlmtypes.DeprecateRule{
				Interval: aws.Int32(2), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			runs: []time.Duration{time.Second}, wantDeprecated: 1, wantImages: 1,
		},
		{
			name: "count", rule: &dlmtypes.DeprecateRule{Count: aws.Int32(1)},
			runs: []time.Duration{time.Second, 13 * hour, 25 * hour}, wantDeprecated: 2, wantImages: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)

			insts, err := env.ec2.RunInstances("ami-fake", "t3.micro", "", 1)
			require.NoError(t, err)
			require.NoError(t, env.ec2.CreateTags([]string{insts[0].ID}, map[string]string{"backup": "yes"}))

			sched := rulesSchedule(func(s *dlmtypes.Schedule) { s.DeprecateRule = tt.rule })
			details := volumePolicyDetails(sched)
			details.PolicyType = dlmtypes.PolicyTypeValuesImageManagement
			details.ResourceTypes = []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesInstance}

			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled, details)
			env.runAt(t, created, tt.runs...)

			imgs, err := env.ec2Client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				IncludeDeprecated: aws.Bool(true),
				Filters: []ec2types.Filter{
					{Name: aws.String("tag:aws:dlm:lifecycle-policy-id"), Values: []string{policyID}},
				},
			})
			require.NoError(t, err)
			require.Len(t, imgs.Images, tt.wantImages)

			deprecated := 0

			for _, img := range imgs.Images {
				if aws.ToString(img.DeprecationTime) != "" {
					deprecated++
				}
			}

			assert.Equal(t, tt.wantDeprecated, deprecated)
		})
	}
}
