package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func newDroppedMembersClient(t *testing.T) *inspector2sdk.Client {
	t.Helper()

	return newRoundTripClient(t, inspector2.NewHandler(inspector2.NewInMemoryBackend("123456789012", "us-east-1")))
}

func TestUpdateFilter_SDKRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newName string
		wantErr bool
	}{
		{name: "rename", newName: "renamed"},
		{name: "taken_name", newName: "other", wantErr: true},
		{name: "invalid_name", newName: "bad name!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()

			created, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
				Name: aws.String("orig"), Action: types.FilterActionNone, FilterCriteria: &types.FilterCriteria{},
			})
			require.NoError(t, err)

			_, err = client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
				Name: aws.String("other"), Action: types.FilterActionNone, FilterCriteria: &types.FilterCriteria{},
			})
			require.NoError(t, err)

			_, err = client.UpdateFilter(ctx, &inspector2sdk.UpdateFilterInput{
				FilterArn: created.Arn, Name: aws.String(tt.newName),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, err := client.ListFilters(
				ctx,
				&inspector2sdk.ListFiltersInput{Arns: []string{aws.ToString(created.Arn)}},
			)
			require.NoError(t, err)
			require.Len(t, list.Filters, 1)
			assert.Equal(t, tt.newName, aws.ToString(list.Filters[0].Name))
		})
	}
}

func TestCisScanConfiguration_SDKSecurityLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		create types.CisSecurityLevel
		update types.CisSecurityLevel
		want   types.CisSecurityLevel
	}{
		{name: "create_level2", create: types.CisSecurityLevelLevel2, want: types.CisSecurityLevelLevel2},
		{
			name:   "update_to_level2",
			create: types.CisSecurityLevelLevel1,
			update: types.CisSecurityLevelLevel2,
			want:   types.CisSecurityLevelLevel2,
		},
		{name: "update_omitted_keeps", create: types.CisSecurityLevelLevel2, want: types.CisSecurityLevelLevel2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()

			created, err := client.CreateCisScanConfiguration(ctx, &inspector2sdk.CreateCisScanConfigurationInput{
				ScanName:      aws.String("nightly"),
				SecurityLevel: tt.create,
				Schedule: &types.ScheduleMemberDaily{Value: types.DailySchedule{
					StartTime: &types.Time{TimeOfDay: aws.String("02:00"), Timezone: aws.String("UTC")},
				}},
				Targets: &types.CreateCisTargets{
					AccountIds:         []string{"123456789012"},
					TargetResourceTags: map[string][]string{},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateCisScanConfiguration(ctx, &inspector2sdk.UpdateCisScanConfigurationInput{
				ScanConfigurationArn: created.ScanConfigurationArn,
				SecurityLevel:        tt.update,
			})
			require.NoError(t, err)

			list, err := client.ListCisScanConfigurations(ctx, &inspector2sdk.ListCisScanConfigurationsInput{})
			require.NoError(t, err)
			require.Len(t, list.ScanConfigurations, 1)
			assert.Equal(t, tt.want, list.ScanConfigurations[0].SecurityLevel)
		})
	}
}

func TestUpdateEc2DeepInspection_SDKActivate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		activate  *bool
		wantState types.Ec2DeepInspectionStatus
	}{
		{name: "activate_true", activate: aws.Bool(true), wantState: types.Ec2DeepInspectionStatusActivated},
		{name: "activate_false", activate: aws.Bool(false), wantState: types.Ec2DeepInspectionStatusDeactivated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()

			_, err := client.UpdateEc2DeepInspectionConfiguration(
				ctx,
				&inspector2sdk.UpdateEc2DeepInspectionConfigurationInput{
					PackagePaths: []string{"/opt/a"},
				},
			)
			require.NoError(t, err)

			out, err := client.UpdateEc2DeepInspectionConfiguration(
				ctx,
				&inspector2sdk.UpdateEc2DeepInspectionConfigurationInput{
					ActivateDeepInspection: tt.activate,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, out.Status)
			assert.Equal(t, []string{"/opt/a"}, out.PackagePaths)
		})
	}
}

func TestCodeSecurityScan_SDKResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		getProject string
		wantErr    bool
	}{
		{name: "matching_project", getProject: "proj-1"},
		{name: "other_project", getProject: "proj-2", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()

			started, err := client.StartCodeSecurityScan(ctx, &inspector2sdk.StartCodeSecurityScanInput{
				Resource: &types.CodeSecurityResourceMemberProjectId{Value: "proj-1"},
			})
			require.NoError(t, err)

			got, err := client.GetCodeSecurityScan(ctx, &inspector2sdk.GetCodeSecurityScanInput{
				Resource: &types.CodeSecurityResourceMemberProjectId{Value: tt.getProject},
				ScanId:   started.ScanId,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			res, ok := got.Resource.(*types.CodeSecurityResourceMemberProjectId)
			require.True(t, ok)
			assert.Equal(t, "proj-1", res.Value)
			assert.Equal(t, "123456789012", aws.ToString(got.AccountId))
			assert.NotNil(t, got.CreatedAt)
			assert.NotNil(t, got.UpdatedAt)
		})
	}
}
