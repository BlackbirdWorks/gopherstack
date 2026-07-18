package ecs

import (
	"strings"
	"testing"
)

// ---- validateFargateCPUMemory tests ----

func TestValidateFargateCPUMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cpu     string
		memory  string
		wantErr bool
	}{
		{"empty both", "", "", false},
		{"empty cpu", "", "512", false},
		{"empty memory", "256", "", false},
		{"valid 256/512", "256", "512", false},
		{"valid 256/1024", "256", "1024", false},
		{"valid 256/2048", "256", "2048", false},
		{"valid 512/1024", "512", "1024", false},
		{"valid 1024/2048", "1024", "2048", false},
		{"valid 2048/4096", "2048", "4096", false},
		{"valid 4096/8192", "4096", "8192", false},
		{"valid 8192/16384", "8192", "16384", false},
		{"valid 16384/32768", "16384", "32768", false},
		{"invalid cpu", "128", "512", true},
		{"invalid memory for cpu", "256", "8192", true},
		{"invalid memory 256/512+1", "256", "513", true},
		{"unknown cpu string", "notanumber", "512", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateFargateCPUMemory(tt.cpu, tt.memory)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"validateFargateCPUMemory(%q, %q) error = %v, wantErr %v",
					tt.cpu,
					tt.memory,
					err,
					tt.wantErr,
				)
			}
		})
	}
}

func TestRegisterTaskDefinition_RequiresCompatibilities(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "myapp",
		RequiresCompatibilities: []string{"FARGATE"},
		NetworkMode:             networkModeAwsvpc,
		CPU:                     "256",
		Memory:                  "512",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(td.RequiresCompatibilities) != 1 || td.RequiresCompatibilities[0] != "FARGATE" {
		t.Errorf("RequiresCompatibilities = %v, want [FARGATE]", td.RequiresCompatibilities)
	}
}

func TestRegisterTaskDefinition_FargateValidation_InvalidCPU(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "myapp",
		RequiresCompatibilities: []string{"FARGATE"},
		NetworkMode:             networkModeAwsvpc,
		CPU:                     "128",
		Memory:                  "512",
		ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err == nil {
		t.Fatal("expected error for invalid Fargate CPU")
	}
	if !strings.Contains(err.Error(), "invalid Fargate CPU") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRegisterTaskDefinition_FargateValidation_InvalidMemory(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "myapp",
		RequiresCompatibilities: []string{"FARGATE"},
		NetworkMode:             networkModeAwsvpc,
		CPU:                     "256",
		Memory:                  "9999",
		ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err == nil {
		t.Fatal("expected error for invalid Fargate memory")
	}
	if !strings.Contains(err.Error(), "invalid Fargate memory") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRegisterTaskDefinition_EC2_NoFargateValidation(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// EC2 does not require valid Fargate CPU/memory pairs
	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "myapp",
		RequiresCompatibilities: []string{"EC2"},
		CPU:                     "128",
		Memory:                  "256",
		ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("unexpected error for EC2 task definition: %v", err)
	}
}

func TestRegisterTaskDefinition_TaskRoleArn(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:           "myapp",
		TaskRoleArn:      "arn:aws:iam::123456789012:role/task-role",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/exec-role",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if td.TaskRoleArn != "arn:aws:iam::123456789012:role/task-role" {
		t.Errorf("TaskRoleArn = %q, want task-role ARN", td.TaskRoleArn)
	}
	if td.ExecutionRoleArn != "arn:aws:iam::123456789012:role/exec-role" {
		t.Errorf("ExecutionRoleArn = %q, want exec-role ARN", td.ExecutionRoleArn)
	}
}

func TestRegisterTaskDefinition_Volumes(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	vols := []Volume{
		{Name: "data", Host: &HostVolumeProperties{SourcePath: "/tmp/data"}},
		{Name: "efs", EFSVolumeConfiguration: &EFSVolumeConfiguration{
			FileSystemID:      "fs-12345678",
			RootDirectory:     "/data",
			TransitEncryption: "ENABLED",
		}},
	}

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:  "myapp",
		Volumes: vols,
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", MountPoints: []MountPoint{
				{SourceVolume: "data", ContainerPath: "/data"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(td.Volumes) != 2 {
		t.Fatalf("len(Volumes) = %d, want 2", len(td.Volumes))
	}
	if td.Volumes[0].Name != "data" {
		t.Errorf("Volumes[0].Name = %q, want data", td.Volumes[0].Name)
	}
	if td.Volumes[1].EFSVolumeConfiguration == nil {
		t.Fatal("EFSVolumeConfiguration is nil")
	}
}

func TestListTaskDefinitionsFiltered_Status(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Register two definitions in the same family.
	td1, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx:1"}},
	})
	if err != nil {
		t.Fatalf("register td1: %v", err)
	}

	td2, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx:2"}},
	})
	if err != nil {
		t.Fatalf("register td2: %v", err)
	}

	// Deregister td1.
	_, err = b.DeregisterTaskDefinition(td1.TaskDefinitionArn)
	if err != nil {
		t.Fatalf("deregister td1: %v", err)
	}

	// Default (ACTIVE only).
	arns, err := b.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{FamilyPrefix: "myapp"})
	if err != nil {
		t.Fatalf("ListTaskDefinitionsFiltered: %v", err)
	}
	if len(arns) != 1 || arns[0] != td2.TaskDefinitionArn {
		t.Errorf("active arns = %v, want [%s]", arns, td2.TaskDefinitionArn)
	}

	// Explicit ACTIVE.
	arns, err = b.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{
		FamilyPrefix: "myapp",
		Status:       "ACTIVE",
	})
	if err != nil {
		t.Fatalf("ListTaskDefinitionsFiltered ACTIVE: %v", err)
	}
	if len(arns) != 1 {
		t.Errorf("ACTIVE arns = %v, want 1", arns)
	}

	// INACTIVE.
	arns, err = b.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{
		FamilyPrefix: "myapp",
		Status:       "INACTIVE",
	})
	if err != nil {
		t.Fatalf("ListTaskDefinitionsFiltered INACTIVE: %v", err)
	}
	if len(arns) != 1 || arns[0] != td1.TaskDefinitionArn {
		t.Errorf("inactive arns = %v, want [%s]", arns, td1.TaskDefinitionArn)
	}
}

func TestContainerDefinition_NewFields(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	logCfg := &LogConfiguration{
		LogDriver: "awslogs",
		Options: map[string]string{
			"awslogs-group":  "/ecs/myapp",
			"awslogs-region": "us-east-1",
		},
	}

	hc := &HealthCheck{
		Command:     []string{"CMD", "curl", "-f", "http://localhost/health"},
		Interval:    30,
		Timeout:     5,
		Retries:     3,
		StartPeriod: 10,
	}

	repoCreds := &RepositoryCredentials{
		CredentialsParameter: "arn:aws:secretsmanager:us-east-1:123456789012:secret:registry-creds",
	}

	secrets := []SecretReference{
		{
			Name:      "DB_PASSWORD",
			ValueFrom: "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-pass",
		},
	}

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "myapp",
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:                  "app",
				Image:                 "myregistry.example.com/myapp:latest",
				LogConfiguration:      logCfg,
				HealthCheck:           hc,
				RepositoryCredentials: repoCreds,
				Secrets:               secrets,
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	cd := td.ContainerDefinitions[0]

	if cd.LogConfiguration == nil || cd.LogConfiguration.LogDriver != "awslogs" {
		t.Errorf("LogConfiguration = %v, want awslogs driver", cd.LogConfiguration)
	}
	if cd.HealthCheck == nil || cd.HealthCheck.Interval != 30 {
		t.Errorf("HealthCheck = %v, want interval=30", cd.HealthCheck)
	}
	if cd.RepositoryCredentials == nil || cd.RepositoryCredentials.CredentialsParameter == "" {
		t.Errorf("RepositoryCredentials = %v, want credentials", cd.RepositoryCredentials)
	}
	if len(cd.Secrets) != 1 || cd.Secrets[0].Name != "DB_PASSWORD" {
		t.Errorf("Secrets = %v, want DB_PASSWORD", cd.Secrets)
	}
}

func TestPortMapping_ExtendedFields(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	pm := PortMapping{
		ContainerPort:      8080,
		HostPort:           0,
		Protocol:           "tcp",
		AppProtocol:        "http",
		ContainerPortRange: "8080-8090",
		Name:               "web",
	}

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "myapp",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: "nginx", PortMappings: []PortMapping{pm}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	got := td.ContainerDefinitions[0].PortMappings[0]

	if got.AppProtocol != "http" {
		t.Errorf("AppProtocol = %q, want http", got.AppProtocol)
	}
	if got.ContainerPortRange != "8080-8090" {
		t.Errorf("ContainerPortRange = %q, want 8080-8090", got.ContainerPortRange)
	}
	if got.Name != "web" {
		t.Errorf("Name = %q, want web", got.Name)
	}
}

func TestMountPoints_VolumeFrom(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "myapp",
		Volumes: []Volume{
			{Name: "shared", Host: &HostVolumeProperties{SourcePath: "/tmp/shared"}},
		},
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:  "app",
				Image: "nginx",
				MountPoints: []MountPoint{
					{SourceVolume: "shared", ContainerPath: "/shared", ReadOnly: false},
				},
			},
			{
				Name:  "sidecar",
				Image: "busybox",
				VolumesFrom: []VolumeFrom{
					{SourceContainer: "app", ReadOnly: true},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	if len(td.ContainerDefinitions[0].MountPoints) != 1 {
		t.Errorf("MountPoints = %v, want 1", td.ContainerDefinitions[0].MountPoints)
	}
	if len(td.ContainerDefinitions[1].VolumesFrom) != 1 {
		t.Errorf("VolumesFrom = %v, want 1", td.ContainerDefinitions[1].VolumesFrom)
	}
	if td.ContainerDefinitions[1].VolumesFrom[0].SourceContainer != "app" {
		t.Errorf(
			"SourceContainer = %q, want app",
			td.ContainerDefinitions[1].VolumesFrom[0].SourceContainer,
		)
	}
}

func TestFirelensConfiguration(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	fls := &FirelensConfiguration{
		Type: "fluentbit",
		Options: map[string]string{
			"enable-ecs-log-metadata": "true",
		},
	}

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "myapp",
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:                  "log_router",
				Image:                 "amazon/aws-for-fluent-bit:latest",
				Essential:             true,
				FirelensConfiguration: fls,
			},
			{Name: "app", Image: "nginx", LogConfiguration: &LogConfiguration{
				LogDriver: "awsfirelens",
				Options: map[string]string{
					"Name": "firehose",
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	router := td.ContainerDefinitions[0]
	if router.FirelensConfiguration == nil || router.FirelensConfiguration.Type != "fluentbit" {
		t.Errorf("FirelensConfiguration = %v, want fluentbit", router.FirelensConfiguration)
	}
}

func TestContainerDependency(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "myapp",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "init", Image: "busybox", Essential: false},
			{
				Name:  "app",
				Image: "nginx",
				DependsOn: []ContainerDependency{
					{ContainerName: "init", Condition: "COMPLETE"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	if len(td.ContainerDefinitions[1].DependsOn) != 1 {
		t.Fatalf("DependsOn len = %d, want 1", len(td.ContainerDefinitions[1].DependsOn))
	}
	if td.ContainerDefinitions[1].DependsOn[0].Condition != "COMPLETE" {
		t.Errorf(
			"DependsOn condition = %q, want COMPLETE",
			td.ContainerDefinitions[1].DependsOn[0].Condition,
		)
	}
}
