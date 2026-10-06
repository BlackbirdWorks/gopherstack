package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func detectorFeatureStatuses(t *testing.T, c *guarddutysdk.Client, detectorID string) map[string]types.FeatureStatus {
	t.Helper()

	got, err := c.GetDetector(t.Context(), &guarddutysdk.GetDetectorInput{DetectorId: aws.String(detectorID)})
	require.NoError(t, err)

	out := map[string]types.FeatureStatus{}
	for _, f := range got.Features {
		out[string(f.Name)] = f.Status
	}

	return out
}

func TestDetectorConfig_DataSourcesAndPartialFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create *guarddutysdk.CreateDetectorInput
		update *guarddutysdk.UpdateDetectorInput
		want   map[string]types.FeatureStatus
		name   string
	}{
		{
			name: "create_data_sources_map_to_features",
			create: &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
				DataSources: &types.DataSourceConfigurations{ //nolint:staticcheck // deprecated member under test
					S3Logs: &types.S3LogsConfiguration{Enable: aws.Bool(true)},
					Kubernetes: &types.KubernetesConfiguration{
						AuditLogs: &types.KubernetesAuditLogsConfiguration{Enable: aws.Bool(false)},
					},
				},
			},
			want: map[string]types.FeatureStatus{
				"S3_DATA_EVENTS": types.FeatureStatusEnabled, "EKS_AUDIT_LOGS": types.FeatureStatusDisabled,
			},
		},
		{
			name: "explicit_feature_wins_over_data_source",
			create: &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
				DataSources: &types.DataSourceConfigurations{ //nolint:staticcheck // deprecated member under test
					S3Logs: &types.S3LogsConfiguration{Enable: aws.Bool(true)},
				},
				Features: []types.DetectorFeatureConfiguration{
					{Name: types.DetectorFeatureS3DataEvents, Status: types.FeatureStatusDisabled},
				},
			},
			want: map[string]types.FeatureStatus{"S3_DATA_EVENTS": types.FeatureStatusDisabled},
		},
		{
			name: "update_changes_only_named_features",
			create: &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
				Features: []types.DetectorFeatureConfiguration{
					{Name: types.DetectorFeatureS3DataEvents, Status: types.FeatureStatusEnabled},
					{Name: types.DetectorFeatureRdsLoginEvents, Status: types.FeatureStatusEnabled},
				},
			},
			update: &guarddutysdk.UpdateDetectorInput{
				Features: []types.DetectorFeatureConfiguration{
					{Name: types.DetectorFeatureRdsLoginEvents, Status: types.FeatureStatusDisabled},
				},
			},
			want: map[string]types.FeatureStatus{
				"S3_DATA_EVENTS": types.FeatureStatusEnabled, "RDS_LOGIN_EVENTS": types.FeatureStatusDisabled,
			},
		},
		{
			name: "update_data_sources_map_to_features",
			create: &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
				Features: []types.DetectorFeatureConfiguration{
					{Name: types.DetectorFeatureRdsLoginEvents, Status: types.FeatureStatusEnabled},
				},
			},
			update: &guarddutysdk.UpdateDetectorInput{
				DataSources: &types.DataSourceConfigurations{ //nolint:staticcheck // deprecated member under test
					MalwareProtection: &types.MalwareProtectionConfiguration{
						ScanEc2InstanceWithFindings: &types.ScanEc2InstanceWithFindings{EbsVolumes: aws.Bool(true)},
					},
				},
			},
			want: map[string]types.FeatureStatus{
				"RDS_LOGIN_EVENTS": types.FeatureStatusEnabled, "EBS_MALWARE_PROTECTION": types.FeatureStatusEnabled,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			created, err := client.CreateDetector(t.Context(), tt.create)
			require.NoError(t, err)

			detectorID := aws.ToString(created.DetectorId)

			if tt.update != nil {
				tt.update.DetectorId = aws.String(detectorID)
				_, err = client.UpdateDetector(t.Context(), tt.update)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, detectorFeatureStatuses(t, client, detectorID))
		})
	}
}

func TestUpdateMemberDetectors_Features(t *testing.T) {
	t.Parallel()

	const member = "444455556666"

	feature := func(name types.OrgFeature, status types.FeatureStatus) types.MemberFeaturesConfiguration {
		return types.MemberFeaturesConfiguration{Name: name, Status: status}
	}

	tests := []struct {
		want    map[string]types.FeatureStatus
		name    string
		updates [][]types.MemberFeaturesConfiguration
		wantErr bool
	}{
		{name: "none"},
		{
			name: "applied",
			updates: [][]types.MemberFeaturesConfiguration{
				{feature(types.OrgFeatureS3DataEvents, types.FeatureStatusEnabled)},
			},
			want: map[string]types.FeatureStatus{"S3_DATA_EVENTS": types.FeatureStatusEnabled},
		},
		{
			name: "later_update_merges",
			updates: [][]types.MemberFeaturesConfiguration{
				{feature(types.OrgFeatureS3DataEvents, types.FeatureStatusEnabled)},
				{
					feature(types.OrgFeatureS3DataEvents, types.FeatureStatusDisabled),
					feature(types.OrgFeatureEksAuditLogs, types.FeatureStatusEnabled),
				},
			},
			want: map[string]types.FeatureStatus{
				"S3_DATA_EVENTS": types.FeatureStatusDisabled, "EKS_AUDIT_LOGS": types.FeatureStatusEnabled,
			},
		},
		{
			name: "invalid_name",
			updates: [][]types.MemberFeaturesConfiguration{
				{feature("NOT_A_FEATURE", types.FeatureStatusEnabled)},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			_, err := client.CreateMembers(t.Context(), &guarddutysdk.CreateMembersInput{
				DetectorId: aws.String(detectorID),
				AccountDetails: []types.AccountDetail{
					{AccountId: aws.String(member), Email: aws.String("m@example.com")},
				},
			})
			require.NoError(t, err)

			for _, features := range tt.updates {
				_, err = client.UpdateMemberDetectors(t.Context(), &guarddutysdk.UpdateMemberDetectorsInput{
					DetectorId: aws.String(detectorID), AccountIds: []string{member}, Features: features,
				})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			got, err := client.GetMemberDetectors(t.Context(), &guarddutysdk.GetMemberDetectorsInput{
				DetectorId: aws.String(detectorID), AccountIds: []string{member},
			})
			require.NoError(t, err)
			require.Len(t, got.MemberDataSourceConfigurations, 1)

			have := map[string]types.FeatureStatus{}
			for _, f := range got.MemberDataSourceConfigurations[0].Features {
				have[string(f.Name)] = f.Status

				assert.NotNil(t, f.UpdatedAt)
			}

			assert.Len(t, have, len(tt.want))

			for k, v := range tt.want {
				assert.Equal(t, v, have[k])
			}
		})
	}
}

func TestGetFindings_SortCriteria(t *testing.T) {
	t.Parallel()

	const (
		sortScan  = "Recon:EC2/Portscan"
		sortBrute = "UnauthorizedAccess:EC2/SSHBruteForce"
	)

	tests := []struct {
		name  string
		order types.OrderBy
		want  []string
	}{
		{name: "asc", order: types.OrderByAsc, want: []string{sortScan, sortBrute}},
		{name: "desc", order: types.OrderByDesc, want: []string{sortBrute, sortScan}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			_, err := client.CreateSampleFindings(t.Context(), &guarddutysdk.CreateSampleFindingsInput{
				DetectorId:   aws.String(detectorID),
				FindingTypes: []string{sortBrute, sortScan},
			})
			require.NoError(t, err)

			listed, err := client.ListFindings(t.Context(), &guarddutysdk.ListFindingsInput{
				DetectorId: aws.String(detectorID),
			})
			require.NoError(t, err)
			require.Len(t, listed.FindingIds, 2)

			got, err := client.GetFindings(t.Context(), &guarddutysdk.GetFindingsInput{
				DetectorId: aws.String(detectorID),
				FindingIds: listed.FindingIds,
				SortCriteria: &types.SortCriteria{
					AttributeName: aws.String("type"), OrderBy: tt.order,
				},
			})
			require.NoError(t, err)

			gotTypes := make([]string, 0, len(got.Findings))
			for _, f := range got.Findings {
				gotTypes = append(gotTypes, aws.ToString(f.Type))
			}

			assert.Equal(t, tt.want, gotTypes)
		})
	}
}

func TestOrganizationConfiguration_PartialUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		first        *guarddutysdk.UpdateOrganizationConfigurationInput
		second       *guarddutysdk.UpdateOrganizationConfigurationInput
		wantFeatures map[string]types.OrgFeatureStatus
		name         string
		wantAuto     bool
	}{
		{
			name: "omitted_autoenable_is_kept",
			first: &guarddutysdk.UpdateOrganizationConfigurationInput{
				AutoEnable: aws.Bool(true), //nolint:staticcheck // deprecated member under test
			},
			second: &guarddutysdk.UpdateOrganizationConfigurationInput{
				AutoEnableOrganizationMembers: types.AutoEnableMembersAll,
			},
			wantAuto: true, wantFeatures: map[string]types.OrgFeatureStatus{},
		},
		{
			name: "features_merge_by_name",
			first: &guarddutysdk.UpdateOrganizationConfigurationInput{
				Features: []types.OrganizationFeatureConfiguration{
					{Name: types.OrgFeatureS3DataEvents, AutoEnable: types.OrgFeatureStatusAll},
					{Name: types.OrgFeatureEksAuditLogs, AutoEnable: types.OrgFeatureStatusAll},
				},
			},
			second: &guarddutysdk.UpdateOrganizationConfigurationInput{
				Features: []types.OrganizationFeatureConfiguration{
					{Name: types.OrgFeatureEksAuditLogs, AutoEnable: types.OrgFeatureStatusNone},
				},
			},
			wantFeatures: map[string]types.OrgFeatureStatus{
				"S3_DATA_EVENTS": types.OrgFeatureStatusAll, "EKS_AUDIT_LOGS": types.OrgFeatureStatusNone,
			},
		},
		{
			name: "data_sources_map_to_features",
			first: &guarddutysdk.UpdateOrganizationConfigurationInput{
				DataSources: &types.OrganizationDataSourceConfigurations{ //nolint:staticcheck // deprecated member under test
					S3Logs: &types.OrganizationS3LogsConfiguration{AutoEnable: aws.Bool(true)},
				},
			},
			second:       &guarddutysdk.UpdateOrganizationConfigurationInput{},
			wantFeatures: map[string]types.OrgFeatureStatus{"S3_DATA_EVENTS": types.OrgFeatureStatusNew},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			for _, in := range []*guarddutysdk.UpdateOrganizationConfigurationInput{tt.first, tt.second} {
				in.DetectorId = aws.String(detectorID)
				_, err := client.UpdateOrganizationConfiguration(t.Context(), in)
				require.NoError(t, err)
			}

			got, err := client.DescribeOrganizationConfiguration(
				t.Context(), &guarddutysdk.DescribeOrganizationConfigurationInput{DetectorId: aws.String(detectorID)},
			)
			require.NoError(t, err)
			//nolint:staticcheck // deprecated member under test
			assert.Equal(t, tt.wantAuto, aws.ToBool(got.AutoEnable))

			have := map[string]types.OrgFeatureStatus{}
			for _, f := range got.Features {
				have[string(f.Name)] = f.AutoEnable
			}

			assert.Equal(t, tt.wantFeatures, have)
		})
	}
}

func TestUpdateMalwareScanSettings_PartialUpdates(t *testing.T) {
	t.Parallel()

	criteria := &types.ScanResourceCriteria{Include: map[string]types.ScanCondition{
		"EC2_INSTANCE_TAG": {MapEquals: []types.ScanConditionPair{{Key: aws.String("k"), Value: aws.String("v")}}},
	}}
	retain := types.EbsSnapshotPreservationRetentionWithFinding

	tests := []struct {
		first        *guarddutysdk.UpdateMalwareScanSettingsInput
		second       *guarddutysdk.UpdateMalwareScanSettingsInput
		name         string
		wantPreserve types.EbsSnapshotPreservation
		wantInclude  int
	}{
		{
			name:         "criteria_only_keeps_preservation",
			first:        &guarddutysdk.UpdateMalwareScanSettingsInput{EbsSnapshotPreservation: retain},
			second:       &guarddutysdk.UpdateMalwareScanSettingsInput{ScanResourceCriteria: criteria},
			wantPreserve: retain,
			wantInclude:  1,
		},
		{
			name:         "preservation_only_keeps_criteria",
			first:        &guarddutysdk.UpdateMalwareScanSettingsInput{ScanResourceCriteria: criteria},
			second:       &guarddutysdk.UpdateMalwareScanSettingsInput{EbsSnapshotPreservation: retain},
			wantPreserve: retain,
			wantInclude:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			for _, in := range []*guarddutysdk.UpdateMalwareScanSettingsInput{tt.first, tt.second} {
				in.DetectorId = aws.String(detectorID)
				_, err := client.UpdateMalwareScanSettings(t.Context(), in)
				require.NoError(t, err)
			}

			got, err := client.GetMalwareScanSettings(t.Context(), &guarddutysdk.GetMalwareScanSettingsInput{
				DetectorId: aws.String(detectorID),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantPreserve, got.EbsSnapshotPreservation)
			require.NotNil(t, got.ScanResourceCriteria)
			assert.Len(t, got.ScanResourceCriteria.Include, tt.wantInclude)
		})
	}
}

func TestStartMalwareScan_ScanConfiguration(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"

	tests := []struct {
		name     string
		config   *types.StartMalwareScanConfiguration
		wantRole string
		wantBase string
		wantErr  bool
	}{
		{name: "absent"},
		{
			name: "role_and_baseline_echoed",
			config: &types.StartMalwareScanConfiguration{
				Role: aws.String("arn:aws:iam::000000000000:role/scan"),
				IncrementalScanDetails: &types.IncrementalScanDetails{
					BaselineResourceArn: aws.String("arn:aws:ec2:us-east-1:000000000000:snapshot/snap-1"),
				},
			},
			wantRole: "arn:aws:iam::000000000000:role/scan",
			wantBase: "arn:aws:ec2:us-east-1:000000000000:snapshot/snap-1",
		},
		{
			name:    "role_required",
			config:  &types.StartMalwareScanConfiguration{IncrementalScanDetails: &types.IncrementalScanDetails{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			createRealClientDetector(t, client)

			started, err := client.StartMalwareScan(t.Context(), &guarddutysdk.StartMalwareScanInput{
				ResourceArn: aws.String(arn), ScanConfiguration: tt.config,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetMalwareScan(t.Context(), &guarddutysdk.GetMalwareScanInput{ScanId: started.ScanId})
			require.NoError(t, err)

			if tt.config == nil {
				assert.Nil(t, got.ScanConfiguration)

				return
			}

			require.NotNil(t, got.ScanConfiguration)
			assert.Equal(t, tt.wantRole, aws.ToString(got.ScanConfiguration.Role))
			assert.Equal(t, tt.wantBase, aws.ToString(got.ScanConfiguration.IncrementalScanDetails.BaselineResourceArn))
		})
	}
}
