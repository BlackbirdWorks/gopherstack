package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestRealClient_DescribeDBEngineVersionsFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, vs []types.DBEngineVersion)
		name  string
		in    rdssdk.DescribeDBEngineVersionsInput
	}{
		{
			name: "deprecated_hidden_by_default",
			in:   rdssdk.DescribeDBEngineVersionsInput{Engine: aws.String("postgres")},
			check: func(t *testing.T, vs []types.DBEngineVersion) {
				t.Helper()

				for _, v := range vs {
					assert.NotEqual(t, "9.6.24", aws.ToString(v.EngineVersion))
				}
				assert.NotEmpty(t, vs)
			},
		},
		{
			name: "include_all_lists_deprecated",
			in:   rdssdk.DescribeDBEngineVersionsInput{Engine: aws.String("postgres"), IncludeAll: aws.Bool(true)},
			check: func(t *testing.T, vs []types.DBEngineVersion) {
				t.Helper()

				var found bool
				for _, v := range vs {
					if aws.ToString(v.EngineVersion) == "9.6.24" {
						found = true
						assert.Equal(t, "deprecated", aws.ToString(v.Status))
					}
				}
				assert.True(t, found)
			},
		},
		{
			name: "character_sets_listed",
			in: rdssdk.DescribeDBEngineVersionsInput{
				Engine: aws.String("oracle-ee"), ListSupportedCharacterSets: aws.Bool(true),
			},
			check: func(t *testing.T, vs []types.DBEngineVersion) {
				t.Helper()

				require.Len(t, vs, 1)
				require.NotEmpty(t, vs[0].SupportedCharacterSets)
				assert.Equal(t, "AL32UTF8", aws.ToString(vs[0].SupportedCharacterSets[0].CharacterSetName))
				assert.Empty(t, vs[0].SupportedTimezones)
			},
		},
		{
			name: "character_sets_omitted_by_default",
			in:   rdssdk.DescribeDBEngineVersionsInput{Engine: aws.String("oracle-ee")},
			check: func(t *testing.T, vs []types.DBEngineVersion) {
				t.Helper()

				require.Len(t, vs, 1)
				assert.Empty(t, vs[0].SupportedCharacterSets)
			},
		},
		{
			name: "timezones_listed",
			in: rdssdk.DescribeDBEngineVersionsInput{
				Engine: aws.String("sqlserver-se"), ListSupportedTimezones: aws.Bool(true),
			},
			check: func(t *testing.T, vs []types.DBEngineVersion) {
				t.Helper()

				require.Len(t, vs, 1)
				require.NotEmpty(t, vs[0].SupportedTimezones)
				assert.Equal(
					t,
					"UTC",
					aws.ToString(vs[0].SupportedTimezones[len(vs[0].SupportedTimezones)-1].TimezoneName),
				)
				assert.Empty(t, vs[0].SupportedCharacterSets)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)
			out, err := client.DescribeDBEngineVersions(t.Context(), &tt.in)
			require.NoError(t, err)
			tt.check(t, out.DBEngineVersions)
		})
	}
}

func TestRealClient_ModifyDBInstanceAutomationMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		mode       types.AutomationMode
		minutes    *int32
		wantCode   string
		wantMode   types.AutomationMode
		wantResume bool
	}{
		{name: "paused_with_minutes", engine: "custom-oracle-ee", mode: types.AutomationModeAllPaused,
			minutes: aws.Int32(120), wantMode: types.AutomationModeAllPaused, wantResume: true},
		{name: "paused_default_window", engine: "custom-oracle-ee", mode: types.AutomationModeAllPaused,
			wantMode: types.AutomationModeAllPaused, wantResume: true},
		{name: "minutes_too_small", engine: "custom-oracle-ee", mode: types.AutomationModeAllPaused,
			minutes: aws.Int32(30), wantCode: "InvalidParameterValue"},
		{name: "minutes_too_large", engine: "custom-oracle-ee", mode: types.AutomationModeAllPaused,
			minutes: aws.Int32(1441), wantCode: "InvalidParameterValue"},
		{name: "minutes_with_full", engine: "custom-oracle-ee", mode: types.AutomationModeFull,
			minutes: aws.Int32(60), wantCode: "InvalidParameterCombination"},
		{name: "not_custom_engine", engine: "mysql", mode: types.AutomationModeAllPaused,
			minutes: aws.Int32(60), wantCode: "InvalidParameterCombination"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			_, err := backend.CreateDBInstance("auto-i", tt.engine, "db.m5.large", "db", "admin", "", 20,
				rds.DBInstanceOptions{})
			require.NoError(t, err)

			before := time.Now()
			out, err := client.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("auto-i"), AutomationMode: tt.mode,
				ResumeFullAutomationModeMinutes: tt.minutes,
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrCode(t, err))

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, out.DBInstance.AutomationMode)
			require.NotNil(t, out.DBInstance.ResumeFullAutomationModeTime)
			minutes := int32(60)
			if tt.minutes != nil {
				minutes = *tt.minutes
			}
			assert.WithinDuration(t, before.Add(time.Duration(minutes)*time.Minute),
				*out.DBInstance.ResumeFullAutomationModeTime, time.Minute)
		})
	}
}

func TestRealClient_ModifyDBProxyTargetGroupNewName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		group    string
		wantCode string
		wantErr  bool
	}{
		{name: "no_rename", newName: ""},
		{name: "rename_default_rejected", newName: "renamed", wantErr: true, wantCode: "InvalidParameterValue"},
		{name: "invalid_identifier", newName: "bad--name", wantErr: true, wantCode: "InvalidParameterValue"},
		{name: "unknown_group", group: "gone", wantErr: true, wantCode: "DBProxyTargetGroupNotFoundFault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)
			_, err := client.CreateDBProxy(t.Context(), &rdssdk.CreateDBProxyInput{
				DBProxyName: aws.String("px"), EngineFamily: types.EngineFamilyMysql,
				RoleArn:      aws.String("arn:aws:iam::123456789012:role/px"),
				VpcSubnetIds: []string{"subnet-1"},
				Auth: []types.UserAuthConfig{{AuthScheme: types.AuthSchemeSecrets,
					SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:123456789012:secret:s")}},
			})
			require.NoError(t, err)

			group := tt.group
			if group == "" {
				group = "default"
			}

			out, err := client.ModifyDBProxyTargetGroup(t.Context(), &rdssdk.ModifyDBProxyTargetGroupInput{
				DBProxyName: aws.String("px"), TargetGroupName: aws.String(group),
				NewName: aws.String(tt.newName),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrCode(t, err))

				return
			}
			require.NoError(t, err)
			assert.Equal(t, "default", aws.ToString(out.DBProxyTargetGroup.TargetGroupName))
		})
	}
}
