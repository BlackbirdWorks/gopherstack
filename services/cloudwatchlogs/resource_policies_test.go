package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func newTestBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })

	return b
}

func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_and_describe",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`, "", nil)
				require.NoError(t, err)
				assert.Equal(t, "my-policy", p.PolicyName)
				assert.Equal(t, "ACCOUNT", p.PolicyScope)
				assert.Equal(t, "1", p.RevisionID)
				assert.NotZero(t, p.LastUpdatedTime)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies("", "")
				require.Len(t, policies, 1)
				assert.Equal(t, "my-policy", policies[0].PolicyName)
				assert.JSONEq(t, `{"Version":"2012-10-17"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "put_multiple_sorted",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("z-policy", `{}`, "", nil)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy("a-policy", `{}`, "", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies("", "")
				require.Len(t, policies, 2)
				assert.Equal(t, "a-policy", policies[0].PolicyName)
				assert.Equal(t, "z-policy", policies[1].PolicyName)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("my-policy", `{"old":"doc"}`, "", nil)
				require.NoError(t, err)
				p, err := b.PutResourcePolicy("my-policy", `{"new":"doc"}`, "", nil)
				require.NoError(t, err)
				assert.Equal(t, "2", p.RevisionID)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies("", "")
				require.Len(t, policies, 1)
				assert.JSONEq(t, `{"new":"doc"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "delete_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("del-policy", `{}`, "", nil)
				require.NoError(t, err)
				err = b.DeleteResourcePolicy("del-policy", "", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.DescribeResourcePolicies("", ""))
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteResourcePolicy("ghost", "", nil)
				require.ErrorIs(t, err, cloudwatchlogs.ErrResourcePolicyNotFound)
			},
		},
		{
			name: "empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("", `{}`, "", nil)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "resource_scoped_policy_keyed_by_arn",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				arn := "arn:aws:logs:us-east-1:000000000000:log-group:/my/group"
				p, err := b.PutResourcePolicy("route53", `{}`, arn, nil)
				require.NoError(t, err)
				assert.Equal(t, "RESOURCE", p.PolicyScope)
				assert.Equal(t, arn, p.ResourceArn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				arn := "arn:aws:logs:us-east-1:000000000000:log-group:/my/group"
				accountScoped := b.DescribeResourcePolicies("", "")
				assert.Empty(t, accountScoped)
				resourceScoped := b.DescribeResourcePolicies("", arn)
				require.Len(t, resourceScoped, 1)
				assert.Equal(t, arn, resourceScoped[0].ResourceArn)
			},
		},
		{
			name: "expected_revision_mismatch_rejected",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("rev-policy", `{}`, "", nil)
				require.NoError(t, err)
				wrong := "999"
				_, err = b.PutResourcePolicy("rev-policy", `{"v":2}`, "", &wrong)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)

				deleteWrong := "999"
				err = b.DeleteResourcePolicy("rev-policy", "", &deleteWrong)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestLookupTable_CRUD(t *testing.T) {
	t.Parallel()

	const csvBody = "id,name\n1,foo\n2,bar\n"

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "create_and_get",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				table, err := b.CreateLookupTable("my_table", csvBody, "desc", "kms-1")
				require.NoError(t, err)
				assert.Equal(t, "my_table", table.LookupTableName)
				assert.Equal(t, []string{"id", "name"}, table.TableFields)
				assert.Equal(t, int64(2), table.RecordsCount)
				assert.Equal(t, int64(len(csvBody)), table.SizeBytes)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tables, _ := b.DescribeLookupTables("", "", 100)
				require.Len(t, tables, 1)
				got, err := b.GetLookupTable(tables[0].LookupTableArn)
				require.NoError(t, err)
				assert.Equal(t, csvBody, got.TableBody)
				assert.Equal(t, "desc", got.Description)
			},
		},
		{
			name: "create_duplicate_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLookupTable("dup_table", csvBody, "", "")
				require.NoError(t, err)
				_, err = b.CreateLookupTable("dup_table", csvBody, "", "")
				require.ErrorIs(t, err, cloudwatchlogs.ErrLookupTableAlreadyExists)
			},
		},
		{
			name: "create_invalid_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLookupTable("bad name!", csvBody, "", "")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "create_empty_body_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLookupTable("empty_body", "", "", "")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "create_malformed_csv_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLookupTable("malformed", `"unterminated`, "", "")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "update_replaces_body",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				table, err := b.CreateLookupTable("upd_table", csvBody, "old", "")
				require.NoError(t, err)

				newBody := "id,name,extra\n1,foo,x\n"
				newDesc := "new"
				updated, err := b.UpdateLookupTable(table.LookupTableArn, newBody, &newDesc, nil)
				require.NoError(t, err)
				assert.Equal(t, table.LookupTableArn, updated.LookupTableArn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tables, _ := b.DescribeLookupTables("", "", 100)
				require.Len(t, tables, 1)
				assert.Equal(t, []string{"id", "name", "extra"}, tables[0].TableFields)
				assert.Equal(t, int64(1), tables[0].RecordsCount)
				assert.Equal(t, "new", tables[0].Description)
			},
		},
		{
			name: "update_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateLookupTable(
					"arn:aws:logs:us-east-1:000000000000:lookup-table:ghost", csvBody, nil, nil,
				)
				require.ErrorIs(t, err, cloudwatchlogs.ErrLookupTableNotFound)
			},
		},
		{
			name: "delete_removes",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				table, err := b.CreateLookupTable("del_table", csvBody, "", "")
				require.NoError(t, err)
				require.NoError(t, b.DeleteLookupTable(table.LookupTableArn))
				_, err = b.GetLookupTable(table.LookupTableArn)
				require.ErrorIs(t, err, cloudwatchlogs.ErrLookupTableNotFound)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tables, _ := b.DescribeLookupTables("", "", 100)
				assert.Empty(t, tables)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteLookupTable("arn:aws:logs:us-east-1:000000000000:lookup-table:ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrLookupTableNotFound)
			},
		},
		{
			name: "describe_filters_by_prefix",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLookupTable("prod_users", csvBody, "", "")
				require.NoError(t, err)
				_, err = b.CreateLookupTable("dev_users", csvBody, "", "")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tables, _ := b.DescribeLookupTables("prod_", "", 100)
				require.Len(t, tables, 1)
				assert.Equal(t, "prod_users", tables[0].LookupTableName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestSyslogConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_requires_existing_log_group",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutSyslogConfiguration(context.Background(), "/ghost/group", "vpce-1")
				require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
			},
		},
		{
			name: "put_and_list",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/my/group", "", "")
				require.NoError(t, err)
				cfg, err := b.PutSyslogConfiguration(context.Background(), "/my/group", "vpce-123")
				require.NoError(t, err)
				assert.Equal(t, "/my/group", cfg.LogGroupIdentifier)
				assert.Equal(t, "vpce-123", cfg.VpcEndpointID)
				assert.NotEmpty(t, cfg.LogGroupArn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				configs, _ := b.ListSyslogConfigurations("/my/group", "", "", 100)
				require.Len(t, configs, 1)
				assert.Equal(t, "vpce-123", configs[0].VpcEndpointID)
			},
		},
		{
			name: "put_replaces_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/replace/group", "", "")
				require.NoError(t, err)
				_, err = b.PutSyslogConfiguration(context.Background(), "/replace/group", "vpce-old")
				require.NoError(t, err)
				_, err = b.PutSyslogConfiguration(context.Background(), "/replace/group", "vpce-new")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				configs, _ := b.ListSyslogConfigurations("/replace/group", "", "", 100)
				require.Len(t, configs, 1)
				assert.Equal(t, "vpce-new", configs[0].VpcEndpointID)
			},
		},
		{
			name: "delete_removes",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/del/group", "", "")
				require.NoError(t, err)
				_, err = b.PutSyslogConfiguration(context.Background(), "/del/group", "vpce-1")
				require.NoError(t, err)
				require.NoError(t, b.DeleteSyslogConfiguration("/del/group", ""))
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				configs, _ := b.ListSyslogConfigurations("/del/group", "", "", 100)
				assert.Empty(t, configs)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteSyslogConfiguration("/ghost/group", "")
				require.ErrorIs(t, err, cloudwatchlogs.ErrSyslogConfigurationNotFound)
			},
		},
		{
			name: "delete_vpc_endpoint_mismatch_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/mismatch/group", "", "")
				require.NoError(t, err)
				_, err = b.PutSyslogConfiguration(context.Background(), "/mismatch/group", "vpce-real")
				require.NoError(t, err)
				err = b.DeleteSyslogConfiguration("/mismatch/group", "vpce-wrong")
				require.ErrorIs(t, err, cloudwatchlogs.ErrSyslogConfigurationNotFound)
			},
		},
		{
			name: "delete_log_group_cascades",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/cascade/group", "", "")
				require.NoError(t, err)
				_, err = b.PutSyslogConfiguration(context.Background(), "/cascade/group", "vpce-1")
				require.NoError(t, err)
				require.NoError(t, b.DeleteLogGroup(context.Background(), "/cascade/group"))
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				configs, _ := b.ListSyslogConfigurations("/cascade/group", "", "", 100)
				assert.Empty(t, configs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestStorageTierPolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "default_is_standard",
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p := b.GetStorageTierPolicy()
				assert.Equal(t, cloudwatchlogs.StorageTierStandard, p.StorageTier)
				assert.Zero(t, p.LastUpdatedTime)
			},
		},
		{
			name: "put_intelligent_tiering",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutStorageTierPolicy(cloudwatchlogs.StorageTierIntelligentTiering)
				require.NoError(t, err)
				assert.Equal(t, cloudwatchlogs.StorageTierIntelligentTiering, p.StorageTier)
				assert.NotZero(t, p.LastUpdatedTime)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p := b.GetStorageTierPolicy()
				assert.Equal(t, cloudwatchlogs.StorageTierIntelligentTiering, p.StorageTier)
			},
		},
		{
			name: "put_invalid_tier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutStorageTierPolicy("BOGUS_TIER")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}
