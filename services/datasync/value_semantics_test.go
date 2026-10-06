package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func newValueSemanticsClient(t *testing.T) *datasyncsdk.Client {
	t.Helper()

	return newTestDataSyncClient(t, datasync.NewHandler(datasync.NewInMemoryBackend("123456789012", "us-east-1")))
}

func TestTask_OptionsDefaultsAndPartialUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		create     *types.Options
		update     *types.Options
		name       string
		mode       types.TaskMode
		wantVerify types.VerifyMode
		wantMtime  types.Mtime
	}{
		{name: "basic omitted", mode: types.TaskModeBasic, wantVerify: types.VerifyModePointInTimeConsistent},
		{name: "enhanced omitted", mode: types.TaskModeEnhanced, wantVerify: types.VerifyModeOnlyFilesTransferred},
		{
			name: "partial update keeps verify", mode: types.TaskModeBasic,
			create:     &types.Options{VerifyMode: types.VerifyModeNone},
			update:     &types.Options{Mtime: types.MtimeNone},
			wantVerify: types.VerifyModeNone, wantMtime: types.MtimeNone,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newValueSemanticsClient(t)
			src, err := c.CreateLocationObjectStorage(t.Context(), &datasyncsdk.CreateLocationObjectStorageInput{
				ServerHostname: aws.String("s.example.com"), BucketName: aws.String("b"),
			})
			require.NoError(t, err)

			task, err := c.CreateTask(t.Context(), &datasyncsdk.CreateTaskInput{
				SourceLocationArn:      src.LocationArn,
				DestinationLocationArn: src.LocationArn,
				TaskMode:               tc.mode,
				Options:                tc.create,
				CloudWatchLogGroupArn:  aws.String("arn:aws:logs:us-east-1:123456789012:log-group:/aws/datasync:*"),
			})
			require.NoError(t, err)

			_, err = c.UpdateTask(t.Context(), &datasyncsdk.UpdateTaskInput{TaskArn: task.TaskArn, Options: tc.update})
			require.NoError(t, err)

			got, err := c.DescribeTask(t.Context(), &datasyncsdk.DescribeTaskInput{TaskArn: task.TaskArn})
			require.NoError(t, err)
			require.NotNil(t, got.Options)
			assert.Equal(t, tc.wantVerify, got.Options.VerifyMode)
			assert.Equal(t, types.TransferModeChanged, got.Options.TransferMode)
			assert.Equal(t, types.TaskQueueingEnabled, got.Options.TaskQueueing)
			assert.Equal(t, types.OverwriteModeAlways, got.Options.OverwriteMode)
			assert.NotEmpty(t, aws.ToString(got.CloudWatchLogGroupArn))

			if tc.wantMtime != "" {
				assert.Equal(t, tc.wantMtime, got.Options.Mtime)
			}
		})
	}
}

func TestAgent_UpdateWithoutNameKeepsName(t *testing.T) {
	t.Parallel()

	c := newValueSemanticsClient(t)
	a, err := c.CreateAgent(t.Context(), &datasyncsdk.CreateAgentInput{
		ActivationKey: aws.String("AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"), AgentName: aws.String("keep"),
	})
	require.NoError(t, err)

	_, err = c.UpdateAgent(t.Context(), &datasyncsdk.UpdateAgentInput{AgentArn: a.AgentArn})
	require.NoError(t, err)

	got, err := c.DescribeAgent(t.Context(), &datasyncsdk.DescribeAgentInput{AgentArn: a.AgentArn})
	require.NoError(t, err)
	assert.Equal(t, "keep", aws.ToString(got.Name))
}

func TestNfsLocation_MountOptionsDefaultAutomatic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		opts *types.NfsMountOptions
		name string
		want types.NfsVersion
	}{
		{name: "omitted", want: types.NfsVersionAutomatic},
		{name: "empty block", opts: &types.NfsMountOptions{}, want: types.NfsVersionAutomatic},
		{name: "explicit", opts: &types.NfsMountOptions{Version: types.NfsVersionNfs3}, want: types.NfsVersionNfs3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newValueSemanticsClient(t)
			a, err := c.CreateAgent(t.Context(), &datasyncsdk.CreateAgentInput{
				ActivationKey: aws.String("AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"),
			})
			require.NoError(t, err)

			loc, err := c.CreateLocationNfs(t.Context(), &datasyncsdk.CreateLocationNfsInput{
				ServerHostname: aws.String("nfs.example.com"), Subdirectory: aws.String("/x"),
				OnPremConfig: &types.OnPremConfig{AgentArns: []string{aws.ToString(a.AgentArn)}},
				MountOptions: tc.opts,
			})
			require.NoError(t, err)

			got, err := c.DescribeLocationNfs(
				t.Context(),
				&datasyncsdk.DescribeLocationNfsInput{LocationArn: loc.LocationArn},
			)
			require.NoError(t, err)
			require.NotNil(t, got.MountOptions)
			assert.Equal(t, tc.want, got.MountOptions.Version)
		})
	}
}
