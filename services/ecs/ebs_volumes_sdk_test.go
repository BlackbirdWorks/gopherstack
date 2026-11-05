package ecs_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

var errFakeEBS = errors.New("snapshot not found")

type fakeEBS struct {
	failFor string
	specs   []ecs.EBSVolumeSpec
	deleted []string
	mu      sync.Mutex
}

func (f *fakeEBS) CreateTaskVolume(spec ecs.EBSVolumeSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failFor != "" && spec.SnapshotID == f.failFor {
		return "", errFakeEBS
	}

	f.specs = append(f.specs, spec)

	return "vol-0fake" + string(rune('a'+len(f.specs))), nil
}

func (f *fakeEBS) DeleteTaskVolume(_, volumeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleted = append(f.deleted, volumeID)

	return nil
}

func ebsTestEnv(t *testing.T, withEBS bool) (*ecssdk.Client, *fakeEBS) {
	t.Helper()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	fake := &fakeEBS{}

	if withEBS {
		backend.SetEBSProvisioner(fake)
	}

	_, err := backend.RegisterContainerInstance("default", "i-ebs")
	require.NoError(t, err)

	client := newTestECSClient(t, ecs.NewHandler(backend))

	_, err = client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family:      aws.String("ebs"),
		NetworkMode: ecstypes.NetworkModeBridge,
		Volumes:     []ecstypes.Volume{{Name: aws.String("data"), ConfiguredAtLaunch: aws.Bool(true)}},
		ContainerDefinitions: []ecstypes.ContainerDefinition{{
			Name: aws.String("app"), Image: aws.String("busybox"), Essential: aws.Bool(true), Memory: aws.Int32(64),
			MountPoints: []ecstypes.MountPoint{{SourceVolume: aws.String("data"), ContainerPath: aws.String("/data")}},
		}},
	})
	require.NoError(t, err)

	return client, fake
}

func ebsConfig(managed *ecstypes.TaskManagedEBSVolumeConfiguration) []ecstypes.TaskVolumeConfiguration {
	return []ecstypes.TaskVolumeConfiguration{{Name: aws.String("data"), ManagedEBSVolume: managed}}
}

func TestRunTask_ManagedEBSVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		managed    *ecstypes.TaskManagedEBSVolumeConfiguration
		name       string
		wantDelete bool
	}{
		{
			name: "deleted on termination by default",
			managed: &ecstypes.TaskManagedEBSVolumeConfiguration{
				RoleArn: aws.String("arn:aws:iam::000000000000:role/infra"), SizeInGiB: aws.Int32(20),
				VolumeType: aws.String("gp3"), Iops: aws.Int32(3000), Encrypted: aws.Bool(true),
				TagSpecifications: []ecstypes.EBSTagSpecification{{
					ResourceType: ecstypes.EBSResourceTypeVolume,
					Tags:         []ecstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				}},
			},
			wantDelete: true,
		},
		{
			name: "kept when deleteOnTermination is false",
			managed: &ecstypes.TaskManagedEBSVolumeConfiguration{
				RoleArn:   aws.String("arn:aws:iam::000000000000:role/infra"),
				SizeInGiB: aws.Int32(8),
				TerminationPolicy: &ecstypes.TaskManagedEBSVolumeTerminationPolicy{
					DeleteOnTermination: aws.Bool(false),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, fake := ebsTestEnv(t, true)

			out, err := client.RunTask(t.Context(), &ecssdk.RunTaskInput{
				TaskDefinition: aws.String("ebs"), LaunchType: ecstypes.LaunchTypeEc2,
				VolumeConfigurations: ebsConfig(tt.managed),
			})
			require.NoError(t, err)

			require.Empty(t, out.Failures)

			require.Len(t, fake.specs, 1)
			assert.Equal(t, aws.ToInt32(tt.managed.SizeInGiB), int32(fake.specs[0].SizeInGiB))

			desc, err := client.DescribeTasks(t.Context(), &ecssdk.DescribeTasksInput{
				Tasks: []string{aws.ToString(out.Tasks[0].TaskArn)},
			})
			require.NoError(t, err)

			var att *ecstypes.Attachment

			for i := range desc.Tasks[0].Attachments {
				if aws.ToString(desc.Tasks[0].Attachments[i].Type) == "AmazonElasticBlockStorage" {
					att = &desc.Tasks[0].Attachments[i]
				}
			}

			require.NotNil(t, att)
			assert.Equal(t, "ATTACHED", aws.ToString(att.Status))

			details := map[string]string{}
			for _, d := range att.Details {
				details[aws.ToString(d.Name)] = aws.ToString(d.Value)
			}

			assert.Equal(t, "data", details["volumeName"])
			assert.Equal(t, "arn:aws:iam::000000000000:role/infra", details["roleArn"])
			assert.Contains(t, details["volumeId"], "vol-")

			_, err = client.StopTask(t.Context(), &ecssdk.StopTaskInput{Task: out.Tasks[0].TaskArn})
			require.NoError(t, err)

			if tt.wantDelete {
				assert.Equal(t, []string{details["volumeId"]}, fake.deleted)
			} else {
				assert.Empty(t, fake.deleted)
			}
		})
	}
}

func TestRunTask_ManagedEBSVolumeValidation(t *testing.T) {
	t.Parallel()

	role := aws.String("arn:aws:iam::000000000000:role/infra")

	tests := []struct {
		name    string
		wantErr string
		cfg     []ecstypes.TaskVolumeConfiguration
		withEBS bool
	}{
		{name: "missing configuration", withEBS: true, wantErr: "needs a volume configuration"},
		{
			name: "unknown volume", withEBS: true, wantErr: "not configuredAtLaunch",
			cfg: []ecstypes.TaskVolumeConfiguration{{
				Name:             aws.String("nope"),
				ManagedEBSVolume: &ecstypes.TaskManagedEBSVolumeConfiguration{RoleArn: role, SizeInGiB: aws.Int32(1)},
			}},
		},
		{
			name:    "role required",
			withEBS: true,
			wantErr: "roleArn is required",
			cfg: ebsConfig(
				&ecstypes.TaskManagedEBSVolumeConfiguration{RoleArn: aws.String(""), SizeInGiB: aws.Int32(1)},
			),
		},
		{
			name: "size or snapshot", withEBS: true, wantErr: "sizeInGiB or snapshotId",
			cfg: ebsConfig(&ecstypes.TaskManagedEBSVolumeConfiguration{RoleArn: role}),
		},
		{
			name: "bad volume type", withEBS: true, wantErr: "volumeType",
			cfg: ebsConfig(&ecstypes.TaskManagedEBSVolumeConfiguration{
				RoleArn: role, SizeInGiB: aws.Int32(1), VolumeType: aws.String("fast"),
			}),
		},
		{
			name: "ec2 unavailable", withEBS: false, wantErr: "EC2 service is not enabled",
			cfg: ebsConfig(&ecstypes.TaskManagedEBSVolumeConfiguration{RoleArn: role, SizeInGiB: aws.Int32(1)}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := ebsTestEnv(t, tt.withEBS)

			_, err := client.RunTask(t.Context(), &ecssdk.RunTaskInput{
				TaskDefinition: aws.String("ebs"), LaunchType: ecstypes.LaunchTypeEc2, VolumeConfigurations: tt.cfg,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidParameterException")
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestRunTask_ManagedEBSVolumeCreateFailureStopsTask(t *testing.T) {
	t.Parallel()

	client, fake := ebsTestEnv(t, true)
	fake.failFor = "snap-missing"

	out, err := client.RunTask(t.Context(), &ecssdk.RunTaskInput{
		TaskDefinition: aws.String("ebs"), LaunchType: ecstypes.LaunchTypeEc2,
		VolumeConfigurations: ebsConfig(&ecstypes.TaskManagedEBSVolumeConfiguration{
			RoleArn: aws.String("arn:aws:iam::000000000000:role/infra"), SnapshotId: aws.String("snap-missing"),
		}),
	})
	require.NoError(t, err)

	require.Empty(t, out.Failures)

	assert.Equal(t, "STOPPED", aws.ToString(out.Tasks[0].LastStatus))
	assert.Equal(t, ecstypes.TaskStopCodeTaskFailedToStart, out.Tasks[0].StopCode)
	assert.Contains(t, aws.ToString(out.Tasks[0].StoppedReason), "snapshot not found")
}

func TestCreateService_ManagedEBSVolumeConfiguration(t *testing.T) {
	t.Parallel()

	client, fake := ebsTestEnv(t, true)
	role := aws.String("arn:aws:iam::000000000000:role/infra")

	_, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
		ServiceName: aws.String("no-config"), TaskDefinition: aws.String("ebs"), LaunchType: ecstypes.LaunchTypeEc2,
	})
	require.Error(t, err, "a configuredAtLaunch volume needs a service volume configuration")

	out, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
		ServiceName: aws.String("ebs-svc"), TaskDefinition: aws.String("ebs"), LaunchType: ecstypes.LaunchTypeEc2,
		DesiredCount: aws.Int32(1),
		VolumeConfigurations: []ecstypes.ServiceVolumeConfiguration{{
			Name: aws.String("data"),
			ManagedEBSVolume: &ecstypes.ServiceManagedEBSVolumeConfiguration{
				RoleArn: role, SizeInGiB: aws.Int32(5), VolumeType: aws.String("gp3"),
			},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Service.Deployments)
	require.Len(t, out.Service.Deployments[0].VolumeConfigurations, 1)
	assert.Equal(t, "data", aws.ToString(out.Service.Deployments[0].VolumeConfigurations[0].Name))
	assert.Equal(
		t,
		int32(5),
		aws.ToInt32(out.Service.Deployments[0].VolumeConfigurations[0].ManagedEBSVolume.SizeInGiB),
	)

	_, err = client.UpdateService(t.Context(), &ecssdk.UpdateServiceInput{
		Service: aws.String("ebs-svc"),
		VolumeConfigurations: []ecstypes.ServiceVolumeConfiguration{{
			Name: aws.String("data"),
			ManagedEBSVolume: &ecstypes.ServiceManagedEBSVolumeConfiguration{
				RoleArn: role, SizeInGiB: aws.Int32(9), VolumeType: aws.String("fast"),
			},
		}},
	})
	require.Error(t, err, "invalid volumeType")

	_ = fake
}
