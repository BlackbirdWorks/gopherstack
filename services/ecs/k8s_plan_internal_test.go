package ecs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func planInput(td *TaskDefinition) k8sPlanInput {
	idx, err := orderContainers(td.ContainerDefinitions)
	if err != nil {
		panic(err)
	}

	order := make([]ContainerDefinition, len(idx))
	for pos, i := range idx {
		order[pos] = td.ContainerDefinitions[i]
	}

	return k8sPlanInput{
		td: td, order: order, podName: "ecs-task1",
		plain:     map[string][]string{},
		sensitive: map[string][]string{},
	}
}

func TestPlanK8sPod_Mapping(t *testing.T) {
	t.Parallel()

	td := &TaskDefinition{
		CPU: "512", Memory: "1 GB",
		Volumes: []Volume{{Name: "scratch"}, {Name: "data", DockerVolumeConfig: &DockerVolumeConfig{Scope: "task"}}},
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:                   "Web_App",
				Image:                  "nginx:1",
				Essential:              true,
				EntryPoint:             []string{"/entry"},
				Command:                []string{"serve"},
				WorkingDirectory:       "/srv",
				User:                   "1000:2000",
				ReadonlyRootFilesystem: true,
				Memory:                 512,
				MemoryReservation:      256,
				CPU:                    256,
				StopTimeout:            new(45),
				PortMappings: []PortMapping{
					{ContainerPort: 8080, HostPort: 18080},
					{ContainerPort: 53, Protocol: "udp"},
				},
				MountPoints: []MountPoint{
					{SourceVolume: "scratch", ContainerPath: "/tmp/s"},
					{SourceVolume: "data", ContainerPath: "/d", ReadOnly: true},
				},
				HealthCheck: &HealthCheck{
					Command: []string{
						"CMD-SHELL",
						"curl -f localhost",
					},
					Interval:    10,
					Timeout:     2,
					Retries:     5,
					StartPeriod: 7,
				},
				LinuxParameters: &LinuxParameters{
					Capabilities:     &KernelCapabilities{Add: []string{"NET_ADMIN"}, Drop: []string{"MKNOD"}},
					Tmpfs:            []Tmpfs{{ContainerPath: "/run", Size: 64}},
					SharedMemorySize: new(128),
				},
				DNSServers: []string{"10.0.0.2"},
				ExtraHosts: []HostEntry{{Hostname: "db", IPAddress: "10.9.9.9"}},
			},
			{Name: "side", Image: "busybox", Interactive: true, PseudoTerminal: true},
		},
	}

	in := planInput(td)
	in.plain["Web_App"] = []string{"A=1", "B=2", "A=3"}
	in.sensitive["Web_App"] = []string{"TOKEN=s3cret", "B=override"}

	plan, err := planK8sPod(in)
	require.NoError(t, err)

	require.Len(t, plan.spec.Containers, 2)
	assert.Empty(t, plan.spec.InitContainers)
	assert.Equal(t, []string{"web-app", "side"}, []string{plan.containers[0].name, plan.containers[1].name})

	web := plan.spec.Containers[0]
	assert.Equal(t, "nginx:1", web.Image)
	assert.Equal(t, []string{"/entry"}, web.Command)
	assert.Equal(t, []string{"serve"}, web.Args)
	assert.Equal(t, "/srv", web.WorkingDir)
	assert.Equal(t, int64(1000), *web.SecurityContext.RunAsUser)
	assert.Equal(t, int64(2000), *web.SecurityContext.RunAsGroup)
	assert.True(t, *web.SecurityContext.ReadOnlyRootFilesystem)
	assert.False(t, *web.SecurityContext.AllowPrivilegeEscalation)
	assert.Equal(t, []corev1.Capability{"NET_ADMIN"}, web.SecurityContext.Capabilities.Add)
	assert.Equal(t, int64(45), *plan.spec.TerminationGracePeriodSeconds)

	assert.Equal(t, []corev1.ContainerPort{
		{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}, {ContainerPort: 53, Protocol: corev1.ProtocolUDP},
	}, web.Ports, "hostPort is not mapped: reachability is by Pod IP")

	assert.Equal(t, int64(512<<20), web.Resources.Limits.Memory().Value())
	assert.Equal(t, int64(256<<20), web.Resources.Requests.Memory().Value())
	assert.Equal(t, int64(500), web.Resources.Limits.Cpu().MilliValue(), "task cpu caps the container")
	assert.Equal(t, int64(250), web.Resources.Requests.Cpu().MilliValue())

	require.NotNil(t, web.ReadinessProbe)
	assert.Equal(t, []string{"/bin/sh", "-c", "curl -f localhost"}, web.ReadinessProbe.Exec.Command)
	assert.Equal(t, int32(10), web.ReadinessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), web.ReadinessProbe.FailureThreshold)
	assert.Equal(t, int32(7), web.ReadinessProbe.InitialDelaySeconds)

	env := map[string]corev1.EnvVar{}
	for _, e := range web.Env {
		env[e.Name] = e
	}

	assert.Equal(t, "3", env["A"].Value)
	require.NotNil(t, env["B"].ValueFrom, "a later sensitive value overrides the plain one")
	assert.Equal(t, "web-app.B", env["B"].ValueFrom.SecretKeyRef.Key)
	assert.Equal(t, "ecs-task1", env["TOKEN"].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, map[string][]byte{"web-app.TOKEN": []byte("s3cret"), "web-app.B": []byte("override")}, plan.secret)

	raw, err := json.Marshal(plan.spec)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "s3cret", "secret values never appear in the Pod spec")
	assert.NotContains(t, string(raw), "override")

	mounts := map[string]corev1.VolumeMount{}
	for _, m := range web.VolumeMounts {
		mounts[m.MountPath] = m
	}

	assert.Contains(t, mounts, "/tmp/s")
	assert.True(t, mounts["/d"].ReadOnly)
	assert.Contains(t, mounts, "/run")
	assert.Contains(t, mounts, "/dev/shm")

	for _, v := range plan.spec.Volumes {
		require.NotNil(t, v.EmptyDir, v.Name)
	}

	assert.Equal(t, []string{"10.0.0.2"}, plan.spec.DNSConfig.Nameservers)
	assert.Equal(t, corev1.DNSNone, plan.spec.DNSPolicy)
	assert.Equal(t, []corev1.HostAlias{{IP: "10.9.9.9", Hostnames: []string{"db"}}}, plan.spec.HostAliases)

	side := plan.spec.Containers[1]
	assert.True(t, side.Stdin)
	assert.True(t, side.TTY)
	assert.Nil(t, side.ReadinessProbe)
}

func TestPlanK8sPod_InitContainersAndVolumesFrom(t *testing.T) {
	t.Parallel()

	td := &TaskDefinition{
		Volumes: []Volume{{Name: "shared"}},
		ContainerDefinitions: []ContainerDefinition{
			{
				Name: "app", Image: "app", Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "migrate", Condition: dependsSuccess}},
			},
			{
				Name: "migrate", Image: "mig",
				DependsOn:   []ContainerDependency{{ContainerName: "seed", Condition: dependsSuccess}},
				MountPoints: []MountPoint{{SourceVolume: "shared", ContainerPath: "/data"}},
			},
			{Name: "seed", Image: "seed", PortMappings: []PortMapping{{ContainerPort: 1}}},
			{
				Name: "reader", Image: "r", VolumesFrom: []VolumeFrom{{SourceContainer: "migrate", ReadOnly: true}},
				DependsOn: []ContainerDependency{{ContainerName: "app", Condition: dependsStart}},
			},
		},
	}

	plan, err := planK8sPod(planInput(td))
	require.NoError(t, err)

	require.Len(t, plan.spec.InitContainers, 2)
	assert.Equal(t, "seed", plan.spec.InitContainers[0].Name, "init containers run in dependency order")
	assert.Equal(t, "migrate", plan.spec.InitContainers[1].Name)
	assert.Empty(t, plan.spec.InitContainers[0].Ports, "init containers expose no ports")

	require.Len(t, plan.spec.Containers, 2)
	assert.Equal(t, "app", plan.spec.Containers[0].Name)
	assert.Equal(t, "reader", plan.spec.Containers[1].Name)

	vm := plan.spec.Containers[1].VolumeMounts
	require.Len(t, vm, 1)
	assert.Equal(t, "/data", vm[0].MountPath)
	assert.True(t, vm[0].ReadOnly)

	assert.Equal(t, []string{"app", "migrate", "seed", "reader"}, []string{
		plan.containers[0].def.Name,
		plan.containers[1].def.Name,
		plan.containers[2].def.Name,
		plan.containers[3].def.Name,
	}, "containers stay in task definition order")
	assert.True(t, plan.containers[1].init)
	assert.False(t, plan.containers[0].init)
}

func TestPlanK8sPod_Unsupported(t *testing.T) {
	t.Parallel()

	one := func(mut func(*ContainerDefinition)) *TaskDefinition {
		cd := ContainerDefinition{Name: "a", Image: "i", Essential: true}
		mut(&cd)

		return &TaskDefinition{ContainerDefinitions: []ContainerDefinition{cd}}
	}

	tests := []struct {
		td   *TaskDefinition
		name string
	}{
		{name: "host path volume", td: &TaskDefinition{
			Volumes:              []Volume{{Name: "h", Host: &HostVolumeProperties{SourcePath: "/etc"}}},
			ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
		}},
		{name: "efs volume", td: &TaskDefinition{
			Volumes: []Volume{
				{Name: "e", EFSVolumeConfiguration: &EFSVolumeConfiguration{FileSystemID: "fs-1"}},
			},
			ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
		}},
		{name: "configured at launch", td: &TaskDefinition{
			Volumes:              []Volume{{Name: "e", ConfiguredAtLaunch: true}},
			ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
		}},
		{name: "shared docker volume", td: &TaskDefinition{
			Volumes:              []Volume{{Name: "e", DockerVolumeConfig: &DockerVolumeConfig{Scope: "shared"}}},
			ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true}},
		}},
		{name: "host network", td: &TaskDefinition{
			NetworkMode: "host", ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i"}},
		}},
		{name: "pid host", td: &TaskDefinition{
			PidMode: "host", ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i"}},
		}},
		{name: "privileged", td: one(func(c *ContainerDefinition) { c.Privileged = true })},
		{name: "disable networking", td: one(func(c *ContainerDefinition) { c.DisableNetworking = true })},
		{
			name: "restart policy",
			td:   one(func(c *ContainerDefinition) { c.RestartPolicy = &ContainerRestartPolicy{Enabled: true} }),
		},
		{name: "repository credentials", td: one(func(c *ContainerDefinition) {
			c.RepositoryCredentials = &RepositoryCredentials{CredentialsParameter: "arn"}
		})},
		{name: "devices", td: one(func(c *ContainerDefinition) {
			c.LinuxParameters = &LinuxParameters{Devices: []Device{{HostPath: "/dev/fuse"}}}
		})},
		{name: "named user", td: one(func(c *ContainerDefinition) { c.User = "nginx" })},
		{name: "healthy dependency", td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
			{Name: "db", Image: "db", Essential: true},
			{
				Name:      "a",
				Image:     "i",
				Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "db", Condition: dependsHealthy}},
			},
		}}},
		{name: "complete dependency", td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
			{Name: "db", Image: "db"},
			{
				Name:      "a",
				Image:     "i",
				Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "db", Condition: dependsComplete}},
			},
		}}},
		{name: "essential init", td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
			{Name: "db", Image: "db", Essential: true},
			{
				Name:      "a",
				Image:     "i",
				Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "db", Condition: dependsSuccess}},
			},
		}}},
		{name: "init depends on app container", td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "i", Essential: true},
			{
				Name:      "init",
				Image:     "i",
				DependsOn: []ContainerDependency{{ContainerName: "app", Condition: dependsStart}},
			},
			{
				Name:      "b",
				Image:     "i",
				Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "init", Condition: dependsSuccess}},
			},
		}}},
		{name: "unknown condition", td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
			{Name: "db", Image: "db", Essential: true},
			{
				Name:      "a",
				Image:     "i",
				Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "db", Condition: "WHENEVER"}},
			},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := planK8sPod(planInput(tt.td))
			require.ErrorIs(t, err, container.ErrSpecUnsupported)
		})
	}
}

func TestPlanK8sPod_PodLevel(t *testing.T) {
	t.Parallel()

	td := &TaskDefinition{
		PidMode: "task", NetworkMode: "awsvpc",
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:           "a",
				Image:          "i",
				Essential:      true,
				SystemControls: []SystemControl{{Namespace: "net.core.somaxconn", Value: "1024"}},
				Hostname:       "Box_1",
			},
			{Name: "A", Image: "i"},
		},
	}

	plan, err := planK8sPod(planInput(td))
	require.NoError(t, err)

	assert.True(t, *plan.spec.ShareProcessNamespace)
	assert.Equal(t, "box-1", plan.spec.Hostname)
	assert.Equal(t, []corev1.Sysctl{{Name: "net.core.somaxconn", Value: "1024"}}, plan.spec.SecurityContext.Sysctls)
	assert.Equal(t, int64(30), *plan.spec.TerminationGracePeriodSeconds, "ECS default stopTimeout")
	assert.Equal(t, "a", plan.spec.Containers[0].Name)
	assert.Equal(t, "a-2", plan.spec.Containers[1].Name, "colliding sanitised names are made unique")
	assert.Nil(t, plan.secret)
}

func TestK8sSizing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantCPU int
		wantMiB int
		okCPU   bool
		okMem   bool
	}{
		{name: "units", in: "256", wantCPU: 256, okCPU: true, wantMiB: 256, okMem: true},
		{name: "vcpu and gb", in: "0.5 vCPU", wantCPU: 512, okCPU: true},
		{name: "gb", in: "2 GB", wantMiB: 2048, okMem: true},
		{name: "empty", in: ""},
		{name: "garbage", in: "lots"},
		{name: "negative", in: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cpu, okC := parseTaskCPUUnits(tt.in)
			mem, okM := parseTaskMemoryMiB(tt.in)

			assert.Equal(t, tt.okCPU, okC)
			assert.Equal(t, tt.okMem, okM)

			if tt.okCPU {
				assert.Equal(t, tt.wantCPU, cpu)
			}

			if tt.okMem {
				assert.Equal(t, tt.wantMiB, mem)
			}
		})
	}
}

func TestK8sProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hc   *HealthCheck
		name string
		want []string
	}{
		{name: "cmd", hc: &HealthCheck{Command: []string{"CMD", "ls", "/"}}, want: []string{"ls", "/"}},
		{
			name: "shell",
			hc:   &HealthCheck{Command: []string{"CMD-SHELL", "true"}},
			want: []string{"/bin/sh", "-c", "true"},
		},
		{name: "none", hc: &HealthCheck{Command: []string{"NONE"}}},
		{name: "nil"},
		{name: "empty cmd", hc: &HealthCheck{Command: []string{"CMD"}}},
		{name: "bare shell", hc: &HealthCheck{Command: []string{"CMD-SHELL"}}},
		{name: "unknown", hc: &HealthCheck{Command: []string{"EXEC", "x"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := k8sProbe(tt.hc)
			if tt.want == nil {
				assert.Nil(t, p)

				return
			}

			require.NotNil(t, p)
			assert.Equal(t, tt.want, p.Exec.Command)
			assert.Equal(t, int32(30), p.PeriodSeconds, "ECS default interval")
			assert.Equal(t, int32(5), p.TimeoutSeconds)
			assert.Equal(t, int32(3), p.FailureThreshold)
		})
	}
}

func TestK8sPodName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ecs-0123abcd", k8sPodName("0123abcd"))
	assert.Equal(t, "ecs-my-task", k8sPodName("My_Task"))
	assert.LessOrEqual(t, len(k8sPodName(string(make([]byte, 200)))), 63)
}

func TestPlanK8sPod_UndefinedVolume(t *testing.T) {
	t.Parallel()

	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "a", Image: "i", MountPoints: []MountPoint{{SourceVolume: "nope", ContainerPath: "/x"}}},
	}}

	_, err := planK8sPod(planInput(td))
	require.ErrorIs(t, err, errUndefinedVolume)
}
