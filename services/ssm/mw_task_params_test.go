package ssm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func registerParamTask(t *testing.T, client *ssmsdk.Client) (*string, *string) {
	t.Helper()

	win, err := client.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
		Name: aws.String("params-mw"), Schedule: aws.String("rate(7 days)"), Duration: aws.Int32(2), Cutoff: 1,
	})
	require.NoError(t, err)

	task, err := client.RegisterTaskWithMaintenanceWindow(t.Context(), &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: win.WindowId,
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		Targets:  []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-abc"}}},
		LoggingInfo: &ssmtypes.LoggingInfo{
			S3BucketName: aws.String("logs"), S3Region: aws.String("us-east-1"), S3KeyPrefix: aws.String("p/"),
		},
		TaskParameters: map[string]ssmtypes.MaintenanceWindowTaskParameterValueExpression{
			"commands": {Values: []string{"echo hi"}},
		},
		TaskInvocationParameters: &ssmtypes.MaintenanceWindowTaskInvocationParameters{
			RunCommand: &ssmtypes.MaintenanceWindowRunCommandParameters{
				Comment:        aws.String("c1"),
				TimeoutSeconds: aws.Int32(90),
				Parameters:     map[string][]string{"commands": {"echo hi"}},
				NotificationConfig: &ssmtypes.NotificationConfig{
					NotificationArn:    aws.String("arn:aws:sns:us-east-1:000000000000:t"),
					NotificationEvents: []ssmtypes.NotificationEvent{ssmtypes.NotificationEventSuccess},
					NotificationType:   ssmtypes.NotificationTypeCommand,
				},
			},
		},
	})
	require.NoError(t, err)

	return win.WindowId, task.WindowTaskId
}

func TestMaintenanceWindowTask_InvocationParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, got *ssmsdk.GetMaintenanceWindowTaskOutput)
		update func(win, task *string) *ssmsdk.UpdateMaintenanceWindowTaskInput
		name   string
	}{
		{
			name:   "register round trips",
			update: nil,
			check: func(t *testing.T, got *ssmsdk.GetMaintenanceWindowTaskOutput) {
				t.Helper()
				assert.Equal(t, "logs", aws.ToString(got.LoggingInfo.S3BucketName))
				assert.Equal(t, []string{"echo hi"}, got.TaskParameters["commands"].Values)

				rc := got.TaskInvocationParameters.RunCommand
				require.NotNil(t, rc)
				assert.Equal(t, "c1", aws.ToString(rc.Comment))
				assert.Equal(t, int32(90), aws.ToInt32(rc.TimeoutSeconds))
				assert.Equal(t, ssmtypes.NotificationTypeCommand, rc.NotificationConfig.NotificationType)
				assert.Equal(t, []string{"echo hi"}, rc.Parameters["commands"])
			},
		},
		{
			name: "merge update replaces only supplied",
			update: func(win, task *string) *ssmsdk.UpdateMaintenanceWindowTaskInput {
				return &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId:     win,
					WindowTaskId: task,
					LoggingInfo: &ssmtypes.LoggingInfo{
						S3BucketName: aws.String("other"),
						S3Region:     aws.String("eu-west-1"),
					},
				}
			},
			check: func(t *testing.T, got *ssmsdk.GetMaintenanceWindowTaskOutput) {
				t.Helper()
				assert.Equal(t, "other", aws.ToString(got.LoggingInfo.S3BucketName))
				assert.Empty(t, aws.ToString(got.LoggingInfo.S3KeyPrefix))
				assert.Equal(t, []string{"echo hi"}, got.TaskParameters["commands"].Values)
				require.NotNil(t, got.TaskInvocationParameters)
			},
		},
		{
			name: "replace update nulls omitted",
			update: func(win, task *string) *ssmsdk.UpdateMaintenanceWindowTaskInput {
				return &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId:     win,
					WindowTaskId: task,
					Replace:      aws.Bool(true),
					TaskArn:      aws.String("AWS-RunShellScript"),
					TaskInvocationParameters: &ssmtypes.MaintenanceWindowTaskInvocationParameters{
						StepFunctions: &ssmtypes.MaintenanceWindowStepFunctionsParameters{Name: aws.String("sf")},
					},
				}
			},
			check: func(t *testing.T, got *ssmsdk.GetMaintenanceWindowTaskOutput) {
				t.Helper()
				assert.Nil(t, got.LoggingInfo)
				assert.Empty(t, got.TaskParameters)
				require.NotNil(t, got.TaskInvocationParameters.StepFunctions)
				assert.Nil(t, got.TaskInvocationParameters.RunCommand)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			win, task := registerParamTask(t, client)

			if tt.update != nil {
				out, err := client.UpdateMaintenanceWindowTask(t.Context(), tt.update(win, task))
				require.NoError(t, err)
				require.NotNil(t, out)
			}

			got, err := client.GetMaintenanceWindowTask(t.Context(), &ssmsdk.GetMaintenanceWindowTaskInput{
				WindowId: win, WindowTaskId: task,
			})
			require.NoError(t, err)
			tt.check(t, got)

			listed, err := client.DescribeMaintenanceWindowTasks(
				t.Context(), &ssmsdk.DescribeMaintenanceWindowTasksInput{WindowId: win},
			)
			require.NoError(t, err)
			require.Len(t, listed.Tasks, 1)
			assert.Equal(t, got.LoggingInfo, listed.Tasks[0].LoggingInfo)
		})
	}
}

func TestListAssociations_NarrowSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		absent   []string
		present  []string
		duration int32
	}{
		{
			name:     "omits describe-only members",
			duration: 4,
			absent:   []string{"MaxConcurrency", "ComplianceSeverity", "SyncCompliance", "LastUpdateAssociationDate"},
			present:  []string{"AssociationId", "Duration", "Overview"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := ssm.NewHandler(ssm.NewInMemoryBackend())
			client := newTestSSMClient(t, handler)

			_, err := client.CreateAssociation(t.Context(), &ssmsdk.CreateAssociationInput{
				Name:               aws.String("AWS-RunShellScript"),
				InstanceId:         aws.String("i-narrow"),
				Duration:           aws.Int32(tt.duration),
				MaxConcurrency:     aws.String("2"),
				ComplianceSeverity: ssmtypes.AssociationComplianceSeverityHigh,
				SyncCompliance:     ssmtypes.AssociationSyncComplianceManual,
			})
			require.NoError(t, err)

			out, err := client.ListAssociations(t.Context(), &ssmsdk.ListAssociationsInput{})
			require.NoError(t, err)
			require.Len(t, out.Associations, 1)
			assert.Equal(t, tt.duration, aws.ToInt32(out.Associations[0].Duration))

			body := doRequest(t, handler, "ListAssociations", `{}`).Body.String()
			for _, k := range tt.absent {
				assert.NotContains(t, body, `"`+k+`"`)
			}

			for _, k := range tt.present {
				assert.Contains(t, body, `"`+k+`"`)
			}
		})
	}
}

func TestResourceDataSync_OrganizationsMembersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *ssmsdk.CreateResourceDataSyncInput
		check func(t *testing.T, item ssmtypes.ResourceDataSyncItem)
		name  string
	}{
		{
			name: "destination data sharing",
			input: &ssmsdk.CreateResourceDataSyncInput{
				SyncName: aws.String("dest"),
				S3Destination: &ssmtypes.ResourceDataSyncS3Destination{
					BucketName: aws.String("b"), Region: aws.String("us-east-1"),
					SyncFormat: ssmtypes.ResourceDataSyncS3FormatJsonSerde,
					DestinationDataSharing: &ssmtypes.ResourceDataSyncDestinationDataSharing{
						DestinationDataSharingType: aws.String("Organization"),
					},
				},
			},
			check: func(t *testing.T, item ssmtypes.ResourceDataSyncItem) {
				t.Helper()
				require.NotNil(t, item.S3Destination.DestinationDataSharing)
				sharing := item.S3Destination.DestinationDataSharing
				assert.Equal(t, "Organization", aws.ToString(sharing.DestinationDataSharingType))
			},
		},
		{
			name: "organizations source",
			input: &ssmsdk.CreateResourceDataSyncInput{
				SyncName: aws.String("org"),
				SyncType: aws.String("SyncFromSource"),
				SyncSource: &ssmtypes.ResourceDataSyncSource{
					SourceType:    aws.String("AwsOrganizations"),
					SourceRegions: []string{"us-east-1"},
					AwsOrganizationsSource: &ssmtypes.ResourceDataSyncAwsOrganizationsSource{
						OrganizationSourceType: aws.String("OrganizationalUnits"),
						OrganizationalUnits: []ssmtypes.ResourceDataSyncOrganizationalUnit{
							{OrganizationalUnitId: aws.String("ou-1234-abcdefgh")},
						},
					},
				},
			},
			check: func(t *testing.T, item ssmtypes.ResourceDataSyncItem) {
				t.Helper()
				org := item.SyncSource.AwsOrganizationsSource
				require.NotNil(t, org)
				assert.Equal(t, "OrganizationalUnits", aws.ToString(org.OrganizationSourceType))
				require.Len(t, org.OrganizationalUnits, 1)
				assert.Equal(t, "ou-1234-abcdefgh", aws.ToString(org.OrganizationalUnits[0].OrganizationalUnitId))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateResourceDataSync(t.Context(), tt.input)
			require.NoError(t, err)

			out, err := client.ListResourceDataSync(t.Context(), &ssmsdk.ListResourceDataSyncInput{})
			require.NoError(t, err)
			require.Len(t, out.ResourceDataSyncItems, 1)
			tt.check(t, out.ResourceDataSyncItems[0])
		})
	}
}

func TestListCommands_ExecutionStageFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stage string
		want  []string
	}{
		{name: "executing", stage: "Executing", want: []string{"running"}},
		{name: "complete", stage: "Complete", want: []string{"cancelled"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend().WithCommandExecDelay(time.Hour)))
			ids := map[string]string{}

			for _, label := range []string{"running", "cancelled"} {
				out, err := client.SendCommand(t.Context(), &ssmsdk.SendCommandInput{
					DocumentName: aws.String("AWS-RunShellScript"),
					InstanceIds:  []string{"i-stage"},
					Comment:      aws.String(label),
				})
				require.NoError(t, err)

				ids[label] = aws.ToString(out.Command.CommandId)
			}

			_, err := client.CancelCommand(
				t.Context(), &ssmsdk.CancelCommandInput{CommandId: aws.String(ids["cancelled"])},
			)
			require.NoError(t, err)

			out, err := client.ListCommands(t.Context(), &ssmsdk.ListCommandsInput{
				Filters: []ssmtypes.CommandFilter{
					{Key: ssmtypes.CommandFilterKeyExecutionStage, Value: aws.String(tt.stage)},
				},
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Commands))
			for _, c := range out.Commands {
				got = append(got, aws.ToString(c.Comment))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
