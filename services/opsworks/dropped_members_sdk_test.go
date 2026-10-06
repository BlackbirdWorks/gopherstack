package opsworks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const filtered = "*****FILTERED*****"

func TestSDK_AppOptionsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, app types.App)
		name  string
	}{
		{
			name: "create members round trip with masking",
			check: func(t *testing.T, app types.App) {
				t.Helper()

				assert.Equal(t, "d", aws.ToString(app.Description))
				assert.Equal(t, "short", aws.ToString(app.Shortname))
				assert.Equal(t, []string{"a.example.com"}, app.Domains)
				assert.True(t, aws.ToBool(app.EnableSsl))
				assert.Equal(t, "v", app.Attributes["k"])
				require.NotNil(t, app.SslConfiguration)
				assert.Equal(t, "cert", aws.ToString(app.SslConfiguration.Certificate))
				require.NotNil(t, app.AppSource)
				assert.Equal(t, types.SourceTypeGit, app.AppSource.Type)
				assert.Equal(t, "https://example.com/r.git", aws.ToString(app.AppSource.Url))
				assert.Equal(t, filtered, aws.ToString(app.AppSource.Password))
				assert.Equal(t, filtered, aws.ToString(app.AppSource.SshKey))
				require.Len(t, app.DataSources, 1)
				assert.Equal(t, "RdsDbInstance", aws.ToString(app.DataSources[0].Type))
				require.Len(t, app.Environment, 2)
				assert.Equal(t, "plain", aws.ToString(app.Environment[0].Value))
				assert.Equal(t, filtered, aws.ToString(app.Environment[1].Value))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			stackID := newTestStack(t, client)

			created, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
				StackId:     aws.String(stackID),
				Name:        aws.String("a"),
				Type:        types.AppTypeOther,
				Description: aws.String("d"),
				Shortname:   aws.String("short"),
				Domains:     []string{"a.example.com"},
				EnableSsl:   aws.Bool(true),
				Attributes:  map[string]string{"k": "v"},
				SslConfiguration: &types.SslConfiguration{
					Certificate: aws.String("cert"),
					PrivateKey:  aws.String("key"),
				},
				AppSource: &types.Source{
					Type: types.SourceTypeGit, Url: aws.String("https://example.com/r.git"),
					Password: aws.String("pw"), SshKey: aws.String("ssh"),
				},
				DataSources: []types.DataSource{{Type: aws.String("RdsDbInstance"), Arn: aws.String("arn:x")}},
				Environment: []types.EnvironmentVariable{
					{Key: aws.String("P"), Value: aws.String("plain")},
					{Key: aws.String("S"), Value: aws.String("secret"), Secure: aws.Bool(true)},
				},
			})
			require.NoError(t, err)

			out, err := client.DescribeApps(
				t.Context(),
				&opsworkssdk.DescribeAppsInput{AppIds: []string{aws.ToString(created.AppId)}},
			)
			require.NoError(t, err)
			require.Len(t, out.Apps, 1)
			tc.check(t, out.Apps[0])
		})
	}
}

func TestSDK_UpdateAppAppliesMembers(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := newTestStack(t, client)

	created, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
		StackId: aws.String(stackID), Name: aws.String("a"), Type: types.AppTypeOther,
		Description: aws.String("old"), Domains: []string{"keep.example.com"},
	})
	require.NoError(t, err)

	_, err = client.UpdateApp(t.Context(), &opsworkssdk.UpdateAppInput{
		AppId: created.AppId, Type: types.AppTypeNodejs, Description: aws.String("new"),
		Attributes: map[string]string{"x": "y"},
	})
	require.NoError(t, err)

	out, err := client.DescribeApps(
		t.Context(),
		&opsworkssdk.DescribeAppsInput{AppIds: []string{aws.ToString(created.AppId)}},
	)
	require.NoError(t, err)
	require.Len(t, out.Apps, 1)
	assert.Equal(t, types.AppTypeNodejs, out.Apps[0].Type)
	assert.Equal(t, "new", aws.ToString(out.Apps[0].Description))
	assert.Equal(t, []string{"keep.example.com"}, out.Apps[0].Domains)
	assert.Equal(t, map[string]string{"x": "y"}, out.Apps[0].Attributes)
}

func TestSDK_LayerSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := newTestStack(t, client)

	created, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
		StackId:                  aws.String(stackID),
		Type:                     types.LayerTypeCustom,
		Name:                     aws.String("l"),
		Shortname:                aws.String("l"),
		AutoAssignElasticIps:     aws.Bool(true),
		AutoAssignPublicIps:      aws.Bool(false),
		EnableAutoHealing:        aws.Bool(true),
		UseEbsOptimizedInstances: aws.Bool(true),
		CustomInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/p"),
		CustomJson:               aws.String(`{"a":1}`),
		CustomSecurityGroupIds:   []string{"sg-1"},
		Packages:                 []string{"git"},
		CustomRecipes:            &types.Recipes{Setup: []string{"x::y"}},
		LifecycleEventConfiguration: &types.LifecycleEventConfiguration{
			Shutdown: &types.ShutdownEventConfiguration{ExecutionTimeout: aws.Int32(60)},
		},
		CloudWatchLogsConfiguration: &types.CloudWatchLogsConfiguration{Enabled: aws.Bool(true)},
		VolumeConfigurations: []types.VolumeConfiguration{
			{MountPoint: aws.String("/data"), NumberOfDisks: aws.Int32(1), Size: aws.Int32(10)},
		},
		Attributes: map[string]string{"MysqlRootPassword": "secret", "Other": "v"},
	})
	require.NoError(t, err)

	describe := func() types.Layer {
		out, descErr := client.DescribeLayers(t.Context(), &opsworkssdk.DescribeLayersInput{
			LayerIds: []string{aws.ToString(created.LayerId)},
		})
		require.NoError(t, descErr)
		require.Len(t, out.Layers, 1)

		return out.Layers[0]
	}

	l := describe()
	assert.True(t, aws.ToBool(l.AutoAssignElasticIps))
	assert.False(t, aws.ToBool(l.AutoAssignPublicIps))
	assert.NotNil(t, l.AutoAssignPublicIps)
	assert.True(t, aws.ToBool(l.EnableAutoHealing))
	assert.True(t, aws.ToBool(l.UseEbsOptimizedInstances))
	assert.Equal(t, `{"a":1}`, aws.ToString(l.CustomJson))
	assert.Equal(t, []string{"sg-1"}, l.CustomSecurityGroupIds)
	assert.Equal(t, []string{"git"}, l.Packages)
	require.NotNil(t, l.CustomRecipes)
	assert.Equal(t, []string{"x::y"}, l.CustomRecipes.Setup)
	require.NotNil(t, l.LifecycleEventConfiguration)
	assert.EqualValues(t, 60, aws.ToInt32(l.LifecycleEventConfiguration.Shutdown.ExecutionTimeout))
	require.NotNil(t, l.CloudWatchLogsConfiguration)
	assert.True(t, aws.ToBool(l.CloudWatchLogsConfiguration.Enabled))
	require.Len(t, l.VolumeConfigurations, 1)
	assert.Equal(t, "/data", aws.ToString(l.VolumeConfigurations[0].MountPoint))
	assert.Equal(t, filtered, l.Attributes["MysqlRootPassword"])
	assert.Equal(t, "v", l.Attributes["Other"])

	_, err = client.UpdateLayer(t.Context(), &opsworkssdk.UpdateLayerInput{
		LayerId:           created.LayerId,
		Shortname:         aws.String("renamed"),
		Packages:          []string{"git", "curl"},
		EnableAutoHealing: aws.Bool(false),
	})
	require.NoError(t, err)

	l = describe()
	assert.Equal(t, "renamed", aws.ToString(l.Shortname))
	assert.Equal(t, []string{"git", "curl"}, l.Packages)
	assert.False(t, aws.ToBool(l.EnableAutoHealing))
	assert.True(t, aws.ToBool(l.AutoAssignElasticIps), "unset members keep their value")
}

func TestSDK_InstanceHostnameAndUpdateMembers(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID, layerID := newTestStackWithLayer(t, client)

	layer2, err := client.CreateLayer(t.Context(), &opsworkssdk.CreateLayerInput{
		StackId: aws.String(stackID), Type: types.LayerTypeCustom, Name: aws.String("l2"), Shortname: aws.String("l2"),
	})
	require.NoError(t, err)

	created, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
		StackId: aws.String(stackID), LayerIds: []string{layerID},
		InstanceType: aws.String("c5.large"), Hostname: aws.String("web1"),
	})
	require.NoError(t, err)

	_, err = client.UpdateInstance(t.Context(), &opsworkssdk.UpdateInstanceInput{
		InstanceId:   created.InstanceId,
		InstanceType: aws.String("m5.large"),
		LayerIds:     []string{aws.ToString(layer2.LayerId)},
	})
	require.NoError(t, err)

	out, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
		InstanceIds: []string{aws.ToString(created.InstanceId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)
	assert.Equal(t, "web1", aws.ToString(out.Instances[0].Hostname))
	assert.Equal(t, "m5.large", aws.ToString(out.Instances[0].InstanceType))
	assert.Equal(t, []string{aws.ToString(layer2.LayerId)}, out.Instances[0].LayerIds)

	_, err = client.UpdateInstance(t.Context(), &opsworkssdk.UpdateInstanceInput{
		InstanceId: created.InstanceId, LayerIds: []string{"missing"},
	})
	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

func TestSDK_CreateDeploymentTargetsAndComment(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID, layerID := newTestStackWithLayer(t, client)

	inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
		StackId: aws.String(stackID), LayerIds: []string{layerID}, InstanceType: aws.String("c5.large"),
	})
	require.NoError(t, err)

	tests := []struct {
		want []string
		name string
		req  opsworkssdk.CreateDeploymentInput
	}{
		{
			name: "instance ids",
			req:  opsworkssdk.CreateDeploymentInput{InstanceIds: []string{aws.ToString(inst.InstanceId)}},
			want: []string{aws.ToString(inst.InstanceId)},
		},
		{
			name: "layer ids resolve to instances",
			req:  opsworkssdk.CreateDeploymentInput{LayerIds: []string{layerID}},
			want: []string{aws.ToString(inst.InstanceId)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.req
			in.StackId = aws.String(stackID)
			in.Command = &types.DeploymentCommand{Name: types.DeploymentCommandNameInstallDependencies}
			in.Comment = aws.String("note")

			dep, depErr := client.CreateDeployment(t.Context(), &in)
			require.NoError(t, depErr)

			out, depErr := client.DescribeDeployments(t.Context(), &opsworkssdk.DescribeDeploymentsInput{
				DeploymentIds: []string{aws.ToString(dep.DeploymentId)},
			})
			require.NoError(t, depErr)
			require.Len(t, out.Deployments, 1)
			assert.Equal(t, tc.want, out.Deployments[0].InstanceIds)
			assert.Equal(t, "note", aws.ToString(out.Deployments[0].Comment))
		})
	}
}

func TestSDK_InstanceExtrasRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID, layerID := newTestStackWithLayer(t, client)

	created, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
		StackId:      aws.String(stackID),
		LayerIds:     []string{layerID},
		InstanceType: aws.String("c5.large"),
		AmiId: aws.String(
			"ami-1",
		),
		AutoScalingType:    types.AutoScalingTypeLoad,
		AvailabilityZone:   aws.String("us-east-1a"),
		EbsOptimized:       aws.Bool(true),
		RootDeviceType:     types.RootDeviceTypeEbs,
		SshKeyName:         aws.String("k1"),
		VirtualizationType: aws.String("hvm"),
	})
	require.NoError(t, err)

	_, err = client.UpdateInstance(t.Context(), &opsworkssdk.UpdateInstanceInput{
		InstanceId: created.InstanceId, SshKeyName: aws.String("k2"), EbsOptimized: aws.Bool(false),
		Architecture: types.ArchitectureI386,
	})
	require.NoError(t, err)

	out, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
		InstanceIds: []string{aws.ToString(created.InstanceId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)

	i := out.Instances[0]
	assert.Equal(t, "ami-1", aws.ToString(i.AmiId))
	assert.Equal(t, types.AutoScalingTypeLoad, i.AutoScalingType)
	assert.Equal(t, "us-east-1a", aws.ToString(i.AvailabilityZone))
	assert.Equal(t, types.RootDeviceTypeEbs, i.RootDeviceType)
	assert.EqualValues(t, "hvm", i.VirtualizationType)
	assert.Equal(t, "k2", aws.ToString(i.SshKeyName))
	assert.Equal(t, types.ArchitectureI386, i.Architecture)
	require.NotNil(t, i.EbsOptimized)
	assert.False(t, *i.EbsOptimized)
}

func TestSDK_RegisterInstanceIPs(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := newTestStack(t, client)

	reg, err := client.RegisterInstance(t.Context(), &opsworkssdk.RegisterInstanceInput{
		StackId: aws.String(stackID), PrivateIp: aws.String("10.0.0.5"), PublicIp: aws.String("1.2.3.4"),
	})
	require.NoError(t, err)

	out, err := client.DescribeInstances(t.Context(), &opsworkssdk.DescribeInstancesInput{
		InstanceIds: []string{aws.ToString(reg.InstanceId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)
	assert.Equal(t, "10.0.0.5", aws.ToString(out.Instances[0].PrivateIp))
	assert.Equal(t, "1.2.3.4", aws.ToString(out.Instances[0].PublicIp))
}

func TestSDK_CloneStackAttributesAppsAndPermissions(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	src, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
		Name: aws.String("src"), Region: aws.String(rtTestRegion),
		DefaultInstanceProfileArn: aws.String(testInstanceProfileArn), ServiceRoleArn: aws.String(testServiceRoleArn),
		Attributes: map[string]string{"Color": "red"},
	})
	require.NoError(t, err)

	app, err := client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
		StackId: src.StackId, Name: aws.String("a1"), Type: types.AppTypeOther, Description: aws.String("d"),
	})
	require.NoError(t, err)

	_, err = client.CreateApp(t.Context(), &opsworkssdk.CreateAppInput{
		StackId: src.StackId, Name: aws.String("not-cloned"), Type: types.AppTypeOther,
	})
	require.NoError(t, err)

	_, err = client.SetPermission(t.Context(), &opsworkssdk.SetPermissionInput{
		StackId: src.StackId, IamUserArn: aws.String("arn:aws:iam::000000000000:user/u"), Level: aws.String("deploy"),
	})
	require.NoError(t, err)

	tests := []struct {
		wantAttrs map[string]string
		name      string
		in        opsworkssdk.CloneStackInput
		wantApps  int
		wantPerms int
	}{
		{
			name:      "defaults inherit attributes only",
			wantAttrs: map[string]string{"Color": "red"},
		},
		{
			name: "clone apps permissions and override attributes",
			in: opsworkssdk.CloneStackInput{
				CloneAppIds: []string{aws.ToString(app.AppId)}, ClonePermissions: aws.Bool(true),
				Attributes: map[string]string{"Color": "blue"},
			},
			wantApps: 1, wantPerms: 1, wantAttrs: map[string]string{"Color": "blue"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.in
			in.SourceStackId = src.StackId
			in.ServiceRoleArn = aws.String(testServiceRoleArn)

			cl, cloneErr := client.CloneStack(t.Context(), &in)
			require.NoError(t, cloneErr)

			apps, descErr := client.DescribeApps(t.Context(), &opsworkssdk.DescribeAppsInput{StackId: cl.StackId})
			require.NoError(t, descErr)
			assert.Len(t, apps.Apps, tc.wantApps)

			perms, permErr := client.DescribePermissions(
				t.Context(),
				&opsworkssdk.DescribePermissionsInput{StackId: cl.StackId},
			)
			require.NoError(t, permErr)
			assert.Len(t, perms.Permissions, tc.wantPerms)

			stacks, stErr := client.DescribeStacks(
				t.Context(),
				&opsworkssdk.DescribeStacksInput{StackIds: []string{aws.ToString(cl.StackId)}},
			)
			require.NoError(t, stErr)
			require.Len(t, stacks.Stacks, 1)
			assert.Equal(t, tc.wantAttrs, stacks.Stacks[0].Attributes)
		})
	}
}

func TestSDK_DeleteInstanceElasticIPAndVolumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		deleteIP bool
		deleteVo bool
	}{
		{name: "keep", deleteIP: false, deleteVo: false},
		{name: "delete", deleteIP: true, deleteVo: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			stackID, layerID := newTestStackWithLayer(t, client)

			inst, err := client.CreateInstance(t.Context(), &opsworkssdk.CreateInstanceInput{
				StackId: aws.String(stackID), LayerIds: []string{layerID}, InstanceType: aws.String("c5.large"),
			})
			require.NoError(t, err)

			_, err = client.RegisterElasticIp(t.Context(), &opsworkssdk.RegisterElasticIpInput{
				ElasticIp: aws.String("9.9.9.9"), StackId: aws.String(stackID),
			})
			require.NoError(t, err)

			_, err = client.AssociateElasticIp(t.Context(), &opsworkssdk.AssociateElasticIpInput{
				ElasticIp: aws.String("9.9.9.9"), InstanceId: inst.InstanceId,
			})
			require.NoError(t, err)

			vol, err := client.RegisterVolume(t.Context(), &opsworkssdk.RegisterVolumeInput{
				StackId: aws.String(stackID), Ec2VolumeId: aws.String("vol-1"),
			})
			require.NoError(t, err)

			_, err = client.AssignVolume(t.Context(), &opsworkssdk.AssignVolumeInput{
				VolumeId: vol.VolumeId, InstanceId: inst.InstanceId,
			})
			require.NoError(t, err)

			_, err = client.DeleteInstance(t.Context(), &opsworkssdk.DeleteInstanceInput{
				InstanceId:      inst.InstanceId,
				DeleteElasticIp: aws.Bool(tc.deleteIP),
				DeleteVolumes:   aws.Bool(tc.deleteVo),
			})
			require.NoError(t, err)

			ips, err := client.DescribeElasticIps(
				t.Context(),
				&opsworkssdk.DescribeElasticIpsInput{StackId: aws.String(stackID)},
			)
			require.NoError(t, err)

			vols, err := client.DescribeVolumes(
				t.Context(),
				&opsworkssdk.DescribeVolumesInput{StackId: aws.String(stackID)},
			)
			require.NoError(t, err)

			if tc.deleteIP {
				assert.Empty(t, ips.ElasticIps)
			} else {
				require.Len(t, ips.ElasticIps, 1)
				assert.Empty(t, aws.ToString(ips.ElasticIps[0].InstanceId))
			}

			if tc.deleteVo {
				assert.Empty(t, vols.Volumes)
			} else {
				require.Len(t, vols.Volumes, 1)
				assert.Empty(t, aws.ToString(vols.Volumes[0].InstanceId))
			}
		})
	}
}
