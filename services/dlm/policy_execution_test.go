package dlm_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dlm"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const execRole = "arn:aws:iam::000000000000:role/AWSDataLifecycleManagerRole"

type execEnv struct {
	dlmClient *dlmsdk.Client
	ec2Client *ec2sdk.Client
	ec2       *ec2backend.InMemoryBackend
	dlmBk     *dlm.InMemoryBackend
}

func newExecEnv(t *testing.T) *execEnv {
	t.Helper()

	ec2Bk := ec2backend.NewInMemoryBackend("000000000000", "us-east-1")
	ec2H := ec2backend.NewHandler(ec2Bk)
	dlmBk := dlm.NewInMemoryBackend("000000000000", "us-east-1")
	dlmH := dlm.NewHandler(dlmBk)
	dlmH.SetCompute(dlm.NewEC2Compute(ec2H))

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(ec2H))
	require.NoError(t, registry.Register(dlmH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &execEnv{
		dlmClient: dlmsdk.NewFromConfig(cfg, func(o *dlmsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		ec2Client: ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		ec2:       ec2Bk,
		dlmBk:     dlmBk,
	}
}

func (e *execEnv) volume(t *testing.T, tags map[string]string) string {
	t.Helper()

	v, err := e.ec2.CreateVolume("us-east-1a", "gp3", 8, "")
	require.NoError(t, err)
	require.NoError(t, e.ec2.CreateTags([]string{v.ID}, tags))

	return v.ID
}

func (e *execEnv) policy(
	t *testing.T, state dlmtypes.SettablePolicyStateValues, details *dlmtypes.PolicyDetails,
) (string, time.Time) {
	t.Helper()

	out, err := e.dlmClient.CreateLifecyclePolicy(t.Context(), &dlmsdk.CreateLifecyclePolicyInput{
		Description: aws.String("exec"), ExecutionRoleArn: aws.String(execRole),
		State: state, PolicyDetails: details,
	})
	require.NoError(t, err)

	got, err := e.dlmClient.GetLifecyclePolicy(t.Context(), &dlmsdk.GetLifecyclePolicyInput{PolicyId: out.PolicyId})
	require.NoError(t, err)

	return aws.ToString(out.PolicyId), aws.ToTime(got.Policy.DateCreated)
}

func (e *execEnv) managedSnapshots(t *testing.T, policyID string) []ec2types.Snapshot {
	t.Helper()

	out, err := e.ec2Client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
		Filters: []ec2types.Filter{{Name: aws.String("tag:aws:dlm:lifecycle-policy-id"), Values: []string{policyID}}},
	})
	require.NoError(t, err)

	return out.Snapshots
}

func volumePolicyDetails(sched dlmtypes.Schedule) *dlmtypes.PolicyDetails {
	return &dlmtypes.PolicyDetails{
		PolicyType:    dlmtypes.PolicyTypeValuesEbsSnapshotManagement,
		ResourceTypes: []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesVolume},
		TargetTags:    []dlmtypes.Tag{{Key: aws.String("backup"), Value: aws.String("yes")}},
		Schedules:     []dlmtypes.Schedule{sched},
	}
}

func intervalSchedule(retain *dlmtypes.RetainRule) dlmtypes.Schedule {
	return dlmtypes.Schedule{
		Name: aws.String("daily"),
		CreateRule: &dlmtypes.CreateRule{
			Interval: aws.Int32(12), IntervalUnit: dlmtypes.IntervalUnitValuesHours,
		},
		RetainRule: retain,
		CopyTags:   aws.Bool(true),
		TagsToAdd:  []dlmtypes.Tag{{Key: aws.String("owner"), Value: aws.String("dlm")}},
		VariableTags: []dlmtypes.Tag{
			{Key: aws.String("instance"), Value: aws.String("$(instance-id)")},
		},
	}
}

func TestPolicyExecution_CreatesTaggedSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      dlmtypes.SettablePolicyStateValues
		volTags    map[string]string
		wantSnaps  int
		wantTagged bool
	}{
		{
			name: "enabled_matching", state: dlmtypes.SettablePolicyStateValuesEnabled,
			volTags: map[string]string{"backup": "yes", "app": "web"}, wantSnaps: 1, wantTagged: true,
		},
		{
			name: "disabled", state: dlmtypes.SettablePolicyStateValuesDisabled,
			volTags: map[string]string{"backup": "yes"}, wantSnaps: 0,
		},
		{
			name: "tag_mismatch", state: dlmtypes.SettablePolicyStateValuesEnabled,
			volTags: map[string]string{"backup": "no"}, wantSnaps: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			volID := env.volume(t, tt.volTags)
			policyID, created := env.policy(t, tt.state,
				volumePolicyDetails(intervalSchedule(&dlmtypes.RetainRule{Count: aws.Int32(3)})))

			env.dlmBk.RunSchedules(t.Context(), created.Add(time.Second))

			snaps := env.managedSnapshots(t, policyID)
			require.Len(t, snaps, tt.wantSnaps)

			if !tt.wantTagged {
				return
			}

			assert.Equal(t, volID, aws.ToString(snaps[0].VolumeId))

			tags := map[string]string{}
			for _, tg := range snaps[0].Tags {
				tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}

			assert.Equal(t, "dlm", tags["owner"])
			assert.Equal(t, "web", tags["app"])
			assert.Equal(t, "true", tags["dlm:managed"])
			assert.Equal(t, "daily", tags["aws:dlm:lifecycle-schedule-name"])
			assert.NotEmpty(t, tags["aws:dlm:timestamp"])

			env.dlmBk.RunSchedules(t.Context(), created.Add(time.Hour))
			assert.Len(t, env.managedSnapshots(t, policyID), 1, "not due again before the interval elapses")
		})
	}
}

func TestPolicyExecution_Retention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		retain   *dlmtypes.RetainRule
		runAt    []time.Duration
		wantKept int
	}{
		{
			name: "count", retain: &dlmtypes.RetainRule{Count: aws.Int32(2)},
			runAt: []time.Duration{time.Second, 13 * time.Hour, 25 * time.Hour, 37 * time.Hour}, wantKept: 2,
		},
		{
			name: "interval",
			retain: &dlmtypes.RetainRule{
				Interval: aws.Int32(1), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
			},
			runAt: []time.Duration{time.Second, 13 * time.Hour, 25 * time.Hour, 37 * time.Hour}, wantKept: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes"})
			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
				volumePolicyDetails(intervalSchedule(tt.retain)))

			var first string

			for i, d := range tt.runAt {
				env.dlmBk.RunSchedules(t.Context(), created.Add(d))

				if i == 0 {
					first = aws.ToString(env.managedSnapshots(t, policyID)[0].SnapshotId)
				}
			}

			assert.Len(t, env.managedSnapshots(t, policyID), tt.wantKept)

			_, err := env.ec2Client.DescribeSnapshots(t.Context(),
				&ec2sdk.DescribeSnapshotsInput{SnapshotIds: []string{first}})
			require.Error(t, err, "oldest snapshot expired")
		})
	}
}

func TestPolicyExecution_DisabledPolicyDoesNotExpire(t *testing.T) {
	t.Parallel()

	env := newExecEnv(t)
	env.volume(t, map[string]string{"backup": "yes"})
	policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
		volumePolicyDetails(intervalSchedule(&dlmtypes.RetainRule{Count: aws.Int32(1)})))

	env.dlmBk.RunSchedules(t.Context(), created.Add(time.Second))
	require.Len(t, env.managedSnapshots(t, policyID), 1)

	_, err := env.dlmClient.UpdateLifecyclePolicy(t.Context(), &dlmsdk.UpdateLifecyclePolicyInput{
		PolicyId: aws.String(policyID), State: dlmtypes.SettablePolicyStateValuesDisabled,
	})
	require.NoError(t, err)

	env.dlmBk.RunSchedules(t.Context(), created.Add(13*time.Hour))
	env.dlmBk.RunSchedules(t.Context(), created.Add(26*time.Hour))
	assert.Len(t, env.managedSnapshots(t, policyID), 1)
}

func TestPolicyExecution_CronSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cron     string
		want     int
		advanceM int
	}{
		{name: "every_minute", cron: "cron(* * * * ? *)", advanceM: 3, want: 1},
		{name: "other_year", cron: "cron(* * * * ? 2099)", advanceM: 3, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			env.volume(t, map[string]string{"backup": "yes"})

			sched := dlmtypes.Schedule{
				Name:       aws.String("cron"),
				CreateRule: &dlmtypes.CreateRule{CronExpression: aws.String(tt.cron)},
				RetainRule: &dlmtypes.RetainRule{Count: aws.Int32(5)},
			}
			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled, volumePolicyDetails(sched))

			env.dlmBk.RunSchedules(t.Context(), created.Add(time.Duration(tt.advanceM)*time.Minute))
			assert.Len(t, env.managedSnapshots(t, policyID), tt.want)
		})
	}
}

func TestPolicyExecution_InstanceTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType dlmtypes.PolicyTypeValues
		wantSnaps  int
		wantImages int
	}{
		{name: "snapshots_all_volumes", policyType: dlmtypes.PolicyTypeValuesEbsSnapshotManagement, wantSnaps: 2},
		{name: "ami", policyType: dlmtypes.PolicyTypeValuesImageManagement, wantImages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)

			insts, err := env.ec2.RunInstances("ami-fake", "t3.micro", "", 1)
			require.NoError(t, err)
			require.NoError(t, env.ec2.CreateTags([]string{insts[0].ID}, map[string]string{"backup": "yes"}))

			for _, dev := range []string{"/dev/sdf", "/dev/sdg"} {
				v := env.volume(t, nil)
				_, err = env.ec2.AttachVolume(v, insts[0].ID, dev)
				require.NoError(t, err)
			}

			sched := intervalSchedule(&dlmtypes.RetainRule{Count: aws.Int32(1)})
			details := volumePolicyDetails(sched)
			details.PolicyType = tt.policyType
			details.ResourceTypes = []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesInstance}

			policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled, details)
			env.dlmBk.RunSchedules(t.Context(), created.Add(time.Second))

			assert.Len(t, env.managedSnapshots(t, policyID), tt.wantSnaps)

			imgs, err := env.ec2Client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				Filters: []ec2types.Filter{{Name: aws.String("tag:aws:dlm:lifecycle-policy-id"), Values: []string{policyID}}},
			})
			require.NoError(t, err)
			assert.Len(t, imgs.Images, tt.wantImages)

			env.dlmBk.RunSchedules(t.Context(), created.Add(13*time.Hour))

			imgs, err = env.ec2Client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				Filters: []ec2types.Filter{{Name: aws.String("tag:aws:dlm:lifecycle-policy-id"), Values: []string{policyID}}},
			})
			require.NoError(t, err)
			assert.Len(t, imgs.Images, tt.wantImages, "count retention keeps one set")
		})
	}
}

func TestPolicyExecution_PersistsManagedResources(t *testing.T) {
	t.Parallel()

	env := newExecEnv(t)
	env.volume(t, map[string]string{"backup": "yes"})
	policyID, created := env.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
		volumePolicyDetails(intervalSchedule(&dlmtypes.RetainRule{Count: aws.Int32(1)})))

	env.dlmBk.RunSchedules(t.Context(), created.Add(time.Second))
	require.Len(t, env.managedSnapshots(t, policyID), 1)

	snap := env.dlmBk.Snapshot(context.Background())
	require.NotEmpty(t, snap)
	env.dlmBk.Reset()
	require.NoError(t, env.dlmBk.Restore(t.Context(), snap))

	env.dlmBk.RunSchedules(t.Context(), created.Add(13*time.Hour))
	assert.Len(t, env.managedSnapshots(t, policyID), 1, "restored records let the new run expire the old snapshot")
}
