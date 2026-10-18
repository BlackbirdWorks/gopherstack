package timestreaminfluxdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBInstanceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "create reports status then endpoint", run: func(t *testing.T, f *fixture) {
			t.Helper()

			in := instanceInput("prod-db")
			in.Tags = map[string]string{"env": "prod"}

			out, err := f.client.CreateDbInstance(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, types.StatusCreating, out.Status)
			assert.Empty(t, aws.ToString(out.Endpoint))
			assert.Regexp(t, `^[0-9a-f]{10}$`, aws.ToString(out.Id))
			assert.Regexp(
				t,
				`^arn:aws:timestream-influxdb:us-east-1:\d{12}:db-instance/`+aws.ToString(out.Id)+`$`,
				aws.ToString(out.Arn),
			)
			assert.Equal(t, int32(8086), aws.ToInt32(out.Port))
			assert.Equal(t, types.DbStorageTypeInfluxIoIncludedT1, out.DbStorageType)
			assert.Equal(t, types.DeploymentTypeSingleAz, out.DeploymentType)
			assert.Equal(t, types.NetworkTypeIpv4, out.NetworkType)
			assert.Equal(t, "us-east-1a", aws.ToString(out.AvailabilityZone))

			f.settle()

			got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: out.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusAvailable, got.Status)
			assert.Contains(t, aws.ToString(got.Endpoint), aws.ToString(out.Id))
			assert.Contains(t, aws.ToString(got.Endpoint), ".timestream-influxdb.us-east-1.on.aws")
			assert.Equal(t, "Sun:02:00-Sun:06:00", aws.ToString(got.MaintenanceSchedule.PreferredMaintenanceWindow))
			assert.Equal(t, "UTC", aws.ToString(got.MaintenanceSchedule.Timezone))
			require.NotNil(t, got.NextMaintenanceTime)
			assert.Equal(t, "Sun", got.NextMaintenanceTime.Weekday().String()[:3])
			assert.True(t, got.NextMaintenanceTime.After(f.clock.now()))
			assert.Nil(t, got.DbClusterId)
		}},
		{name: "name uniqueness is case insensitive", run: func(t *testing.T, f *fixture) {
			t.Helper()

			f.instance(t, "orders")

			_, err := f.client.CreateDbInstance(t.Context(), instanceInput("ORDERS"))
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "get missing", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: aws.String("abcdefghij")})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "parameter group must exist", run: func(t *testing.T, f *fixture) {
			t.Helper()

			in := instanceInput("withgroup")
			in.DbParameterGroupIdentifier = aws.String("nonexistent")
			_, err := f.client.CreateDbInstance(t.Context(), in)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			g, err := f.client.CreateDbParameterGroup(
				t.Context(),
				&sdk.CreateDbParameterGroupInput{Name: aws.String("tuned")},
			)
			require.NoError(t, err)

			in.DbParameterGroupIdentifier = g.Id
			out, err := f.client.CreateDbInstance(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(g.Id), aws.ToString(out.DbParameterGroupIdentifier))
		}},
		{name: "subnets are checked when a resolver is wired", run: func(t *testing.T, f *fixture) {
			t.Helper()

			f.backend.SetSubnetResolver(fakeSubnets{"subnet-aaa111": "10.0.0.0/24"})

			_, err := f.client.CreateDbInstance(t.Context(), instanceInput("known"))
			require.NoError(t, err)

			in := instanceInput("unknown")
			in.VpcSubnetIds = []string{"subnet-zzz999"}
			_, err = f.client.CreateDbInstance(t.Context(), in)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "list is name ordered and paginated", run: func(t *testing.T, f *fixture) {
			t.Helper()

			for i := range 5 {
				_, err := f.client.CreateDbInstance(t.Context(), instanceInput(fmt.Sprintf("db-%d", 4-i)))
				require.NoError(t, err)
			}

			var names []string

			p := sdk.NewListDbInstancesPaginator(f.client, &sdk.ListDbInstancesInput{MaxResults: aws.Int32(2)})
			for p.HasMorePages() {
				pg, err := p.NextPage(t.Context())
				require.NoError(t, err)

				for _, it := range pg.Items {
					names = append(names, aws.ToString(it.Name))
				}
			}

			assert.Equal(t, []string{"db-0", "db-1", "db-2", "db-3", "db-4"}, names)

			_, err := f.client.ListDbInstances(t.Context(), &sdk.ListDbInstancesInput{MaxResults: aws.Int32(500)})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "update storage and type", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "growing")

			_, err := f.client.UpdateDbInstance(
				t.Context(),
				&sdk.UpdateDbInstanceInput{Identifier: inst.Id, AllocatedStorage: aws.Int32(50)},
			)
			assert.Equal(t, "ValidationException", apiCode(err))

			up, err := f.client.UpdateDbInstance(t.Context(), &sdk.UpdateDbInstanceInput{
				Identifier:       inst.Id,
				AllocatedStorage: aws.Int32(200),
				DbInstanceType:   types.DbInstanceTypeDbInfluxLarge,
				Port:             aws.Int32(9000),
			})
			require.NoError(t, err)
			assert.Equal(t, types.StatusUpdatingInstanceType, up.Status)
			assert.Equal(t, int32(200), aws.ToInt32(up.AllocatedStorage))

			_, err = f.client.UpdateDbInstance(
				t.Context(),
				&sdk.UpdateDbInstanceInput{Identifier: inst.Id, Port: aws.Int32(9001)},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			f.settle()

			got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusAvailable, got.Status)
			assert.Equal(t, types.DbInstanceTypeDbInfluxLarge, got.DbInstanceType)
			assert.Equal(t, int32(9000), aws.ToInt32(got.Port))
		}},
		{name: "deployment type update", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "ha-db")
			up, err := f.client.UpdateDbInstance(t.Context(), &sdk.UpdateDbInstanceInput{
				Identifier: inst.Id, DeploymentType: types.DeploymentTypeWithMultiazStandby,
			})
			require.NoError(t, err)
			assert.Equal(t, types.StatusUpdatingDeploymentType, up.Status)
			assert.Equal(t, "us-east-1b", aws.ToString(up.SecondaryAvailabilityZone))
		}},
		{name: "reboot", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "reboot")
			out, err := f.client.RebootDbInstance(t.Context(), &sdk.RebootDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusRebooting, out.Status)

			_, err = f.client.RebootDbInstance(t.Context(), &sdk.RebootDbInstanceInput{Identifier: inst.Id})
			assert.Equal(t, "ConflictException", apiCode(err))

			f.settle()

			got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusAvailable, got.Status)
		}},
		{name: "delete", run: func(t *testing.T, f *fixture) {
			t.Helper()

			inst := f.instance(t, "doomed")
			out, err := f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusDeleting, out.Status)

			got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
			require.NoError(t, err)
			assert.Equal(t, types.StatusDeleting, got.Status)

			f.settle()

			_, err = f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			f.instance(t, "after")
			assert.Equal(t, 1, f.backend.InstanceCount())
		}},
		{name: "delete while creating conflicts", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateDbInstance(t.Context(), instanceInput("early"))
			require.NoError(t, err)

			_, err = f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: out.Id})
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newFixture(t))
		})
	}
}

func TestDBInstanceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *sdk.CreateDbInstanceInput)
		name   string
	}{
		{name: "short name", mutate: func(in *sdk.CreateDbInstanceInput) { in.Name = aws.String("ab") }},
		{name: "name starts with digit", mutate: func(in *sdk.CreateDbInstanceInput) { in.Name = aws.String("1abc") }},
		{name: "double hyphen", mutate: func(in *sdk.CreateDbInstanceInput) { in.Name = aws.String("a--bc") }},
		{name: "short password", mutate: func(in *sdk.CreateDbInstanceInput) { in.Password = aws.String("short") }},
		{
			name:   "password symbols",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.Password = aws.String("Pass word!!") },
		},
		{
			name:   "username digit first",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.Username = aws.String("1user") },
		},
		{
			name:   "bucket underscore",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.Bucket = aws.String("_internal") },
		},
		{
			name:   "storage too small",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.AllocatedStorage = aws.Int32(10) },
		},
		{
			name:   "storage too large",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.AllocatedStorage = aws.Int32(20000) },
		},
		{name: "reserved port", mutate: func(in *sdk.CreateDbInstanceInput) { in.Port = aws.Int32(8090) }},
		{name: "low port", mutate: func(in *sdk.CreateDbInstanceInput) { in.Port = aws.Int32(80) }},
		{name: "too many subnets", mutate: func(in *sdk.CreateDbInstanceInput) {
			in.VpcSubnetIds = []string{"subnet-a1", "subnet-a2", "subnet-a3", "subnet-a4"}
		}},
		{name: "bad subnet", mutate: func(in *sdk.CreateDbInstanceInput) { in.VpcSubnetIds = []string{"net-1"} }},
		{
			name:   "bad security group",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.VpcSecurityGroupIds = []string{"nsg-1"} },
		},
		{
			name:   "bad instance type",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.DbInstanceType = "db.influx.huge" },
		},
		{
			name:   "bad deployment",
			mutate: func(in *sdk.CreateDbInstanceInput) { in.DeploymentType = "MULTI_NODE_READ_REPLICAS" },
		},
		{name: "bad log bucket", mutate: func(in *sdk.CreateDbInstanceInput) {
			in.LogDeliveryConfiguration = &types.LogDeliveryConfiguration{S3Configuration: &types.S3Configuration{
				BucketName: aws.String("X"), Enabled: aws.Bool(true),
			}}
		}},
		{name: "bad window", mutate: func(in *sdk.CreateDbInstanceInput) {
			in.MaintenanceSchedule = &types.MaintenanceSchedule{
				PreferredMaintenanceWindow: aws.String("Sunday"),
				Timezone:                   aws.String("UTC"),
			}
		}},
		{name: "bad timezone", mutate: func(in *sdk.CreateDbInstanceInput) {
			in.MaintenanceSchedule = &types.MaintenanceSchedule{
				PreferredMaintenanceWindow: aws.String("Sun:02:00-Sun:06:00"),
				Timezone:                   aws.String("Mars/Base"),
			}
		}},
		{name: "custom schedule without cron", mutate: func(in *sdk.CreateDbInstanceInput) {
			in.DbBackupConfigurations = []types.DbBackupConfiguration{
				{Enabled: aws.Bool(true), RetentionDays: aws.Int32(7), Type: types.AutomatedDbBackupTypeCustomSchedule},
			}
		}},
		{name: "bad tag", mutate: func(in *sdk.CreateDbInstanceInput) { in.Tags = map[string]string{"aws:x": "y"} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			in := instanceInput("valid-name")
			tt.mutate(in)

			_, err := f.client.CreateDbInstance(t.Context(), in)
			assert.Equal(t, "ValidationException", apiCode(err))
			assert.Equal(t, 0, f.backend.InstanceCount())
		})
	}
}

func TestDBInstanceOptionalConfiguration(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	in := instanceInput("configured")
	in.Port = aws.Int32(8087)
	in.PubliclyAccessible = aws.Bool(true)
	in.NetworkType = types.NetworkTypeDual
	in.DbStorageType = types.DbStorageTypeInfluxIoIncludedT2
	in.LogDeliveryConfiguration = &types.LogDeliveryConfiguration{S3Configuration: &types.S3Configuration{
		BucketName: aws.String("my-log-bucket"), Enabled: aws.Bool(true),
	}}
	in.MaintenanceSchedule = &types.MaintenanceSchedule{
		PreferredMaintenanceWindow: aws.String("Wed:10:00-Wed:11:00"), Timezone: aws.String("America/New_York"),
	}
	in.DbBackupConfigurations = []types.DbBackupConfiguration{
		{Enabled: aws.Bool(true), RetentionDays: aws.Int32(14), Type: types.AutomatedDbBackupTypeDaily},
		{
			Enabled:        aws.Bool(false),
			RetentionDays:  aws.Int32(30),
			Type:           types.AutomatedDbBackupTypeCustomSchedule,
			CustomSchedule: aws.String("cron(0 3 * * ? *)"),
		},
	}

	out, err := f.client.CreateDbInstance(t.Context(), in)
	require.NoError(t, err)
	f.settle()

	got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: out.Id})
	require.NoError(t, err)

	assert.Equal(t, int32(8087), aws.ToInt32(got.Port))
	assert.True(t, aws.ToBool(got.PubliclyAccessible))
	assert.Equal(t, types.NetworkTypeDual, got.NetworkType)
	assert.Equal(t, types.DbStorageTypeInfluxIoIncludedT2, got.DbStorageType)
	assert.Equal(t, "my-log-bucket", aws.ToString(got.LogDeliveryConfiguration.S3Configuration.BucketName))
	assert.Equal(t, "America/New_York", aws.ToString(got.MaintenanceSchedule.Timezone))
	require.Len(t, got.DbBackupConfigurations, 2)
	assert.NotNil(t, got.DbBackupConfigurations[0].NextAutomatedBackupTime)
	assert.Nil(t, got.DbBackupConfigurations[1].NextAutomatedBackupTime)
	assert.Equal(t, "cron(0 3 * * ? *)", aws.ToString(got.DbBackupConfigurations[1].CustomSchedule))
	assert.Equal(t, 10, got.NextMaintenanceTime.In(mustZone(t, "America/New_York")).Hour())
}

func mustZone(t *testing.T, name string) *time.Location {
	t.Helper()

	loc, err := time.LoadLocation(name)
	require.NoError(t, err)

	return loc
}
