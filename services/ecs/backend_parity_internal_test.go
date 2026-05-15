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
				t.Errorf("validateFargateCPUMemory(%q, %q) error = %v, wantErr %v", tt.cpu, tt.memory, err, tt.wantErr)
			}
		})
	}
}

// ---- validatePlatformVersion tests ----

func TestValidatePlatformVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pv      string
		wantErr bool
	}{
		{"empty", "", false},
		{"LATEST", "LATEST", false},
		{"latest lowercase", "latest", false},
		{"1.4.0", "1.4.0", false},
		{"1.3.0", "1.3.0", false},
		{"1.0.0", "1.0.0", false},
		{"unknown", "2.0.0", true},
		{"garbage", "notaversion", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validatePlatformVersion(tt.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePlatformVersion(%q) error = %v, wantErr %v", tt.pv, err, tt.wantErr)
			}
		})
	}
}

// ---- validateDeploymentController tests ----

func TestValidateDeploymentController(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dc      *DeploymentController
		name    string
		wantErr bool
	}{
		{nil, "nil", false},
		{&DeploymentController{Type: "ROLLING"}, "ROLLING", false},
		{&DeploymentController{Type: "rolling"}, "rolling lowercase", false},
		{&DeploymentController{Type: "EXTERNAL"}, "EXTERNAL", false},
		{&DeploymentController{Type: "CODE_DEPLOY"}, "CODE_DEPLOY", true},
		{&DeploymentController{Type: "code_deploy"}, "code_deploy lowercase", true},
		{&DeploymentController{Type: "UNKNOWN"}, "unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateDeploymentController(tt.dc)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDeploymentController(%v) error = %v, wantErr %v", tt.dc, err, tt.wantErr)
			}
		})
	}
}

// ---- DeploymentConfiguration.withAWSDefaults tests ----

func TestDeploymentConfigurationWithAWSDefaults(t *testing.T) {
	t.Parallel()
	t.Run("nil returns defaults", func(t *testing.T) {
		t.Parallel()
		var dc *DeploymentConfiguration
		out := dc.withAWSDefaults()
		if out == nil {
			t.Fatal("expected non-nil result")
		}
		if out.MinimumHealthyPercent == nil || *out.MinimumHealthyPercent != defaultMinimumHealthyPercent {
			t.Errorf("MinimumHealthyPercent = %v, want %d", out.MinimumHealthyPercent, defaultMinimumHealthyPercent)
		}
		if out.MaximumPercent == nil || *out.MaximumPercent != defaultMaximumPercent {
			t.Errorf("MaximumPercent = %v, want %d", out.MaximumPercent, defaultMaximumPercent)
		}
	})

	t.Run("existing values preserved", func(t *testing.T) {
		t.Parallel()
		minPct := 50
		maxPct := 150
		dc := &DeploymentConfiguration{
			MinimumHealthyPercent: &minPct,
			MaximumPercent:        &maxPct,
		}
		out := dc.withAWSDefaults()
		if *out.MinimumHealthyPercent != minPct {
			t.Errorf("MinimumHealthyPercent = %d, want 50", *out.MinimumHealthyPercent)
		}
		if *out.MaximumPercent != maxPct {
			t.Errorf("MaximumPercent = %d, want 150", *out.MaximumPercent)
		}
	})

	t.Run("partial fill", func(t *testing.T) {
		t.Parallel()
		minPct := 75
		dc := &DeploymentConfiguration{MinimumHealthyPercent: &minPct}
		out := dc.withAWSDefaults()
		if *out.MinimumHealthyPercent != minPct {
			t.Errorf("MinimumHealthyPercent = %d, want 75", *out.MinimumHealthyPercent)
		}
		if out.MaximumPercent == nil || *out.MaximumPercent != defaultMaximumPercent {
			t.Errorf("MaximumPercent = %v, want %d", out.MaximumPercent, defaultMaximumPercent)
		}
	})
}

// ---- Backend integration tests for new fields ----

func newTestBackend() *InMemoryBackend {
	return NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
}

func TestRegisterTaskDefinition_RequiresCompatibilities(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "myapp",
		RequiresCompatibilities: []string{"FARGATE"},
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

func TestCreateService_DeploymentConfigurationDefaults(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   1,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	if svc.DeploymentConfiguration == nil {
		t.Fatal("DeploymentConfiguration should not be nil")
	}
	if svc.DeploymentConfiguration.MinimumHealthyPercent == nil ||
		*svc.DeploymentConfiguration.MinimumHealthyPercent != defaultMinimumHealthyPercent {
		t.Errorf("MinimumHealthyPercent = %v, want %d",
			svc.DeploymentConfiguration.MinimumHealthyPercent, defaultMinimumHealthyPercent)
	}
	if svc.DeploymentConfiguration.MaximumPercent == nil ||
		*svc.DeploymentConfiguration.MaximumPercent != defaultMaximumPercent {
		t.Errorf("MaximumPercent = %v, want %d",
			svc.DeploymentConfiguration.MaximumPercent, defaultMaximumPercent)
	}
}

func TestCreateService_DeploymentController_Valid(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:          "my-svc",
		TaskDefinition:       "myapp",
		DesiredCount:         1,
		DeploymentController: &DeploymentController{Type: "ROLLING"},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svc.DeploymentController == nil || svc.DeploymentController.Type != "ROLLING" {
		t.Errorf("DeploymentController = %v, want ROLLING", svc.DeploymentController)
	}
}

func TestCreateService_DeploymentController_CodeDeploy_Rejected(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	_, err = b.CreateService(CreateServiceInput{
		ServiceName:          "my-svc",
		TaskDefinition:       "myapp",
		DesiredCount:         1,
		DeploymentController: &DeploymentController{Type: "CODE_DEPLOY"},
	})
	if err == nil {
		t.Fatal("expected error for CODE_DEPLOY controller")
	}
	if !strings.Contains(err.Error(), "CODE_DEPLOY") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCreateService_LoadBalancers(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	lbs := []LoadBalancer{
		{
			TargetGroupArn: "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abcd1234",
			ContainerName:  "app",
			ContainerPort:  8080,
		},
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   1,
		LoadBalancers:  lbs,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if len(svc.LoadBalancers) != 1 {
		t.Fatalf("len(LoadBalancers) = %d, want 1", len(svc.LoadBalancers))
	}
	if svc.LoadBalancers[0].ContainerPort != 8080 {
		t.Errorf("ContainerPort = %d, want 8080", svc.LoadBalancers[0].ContainerPort)
	}
}

func TestCreateService_ServiceRegistries(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	regs := []ServiceRegistry{
		{
			RegistryArn:   "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-abc123",
			ContainerName: "app",
			ContainerPort: 80,
		},
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:       "my-svc",
		TaskDefinition:    "myapp",
		DesiredCount:      1,
		ServiceRegistries: regs,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if len(svc.ServiceRegistries) != 1 {
		t.Fatalf("len(ServiceRegistries) = %d, want 1", len(svc.ServiceRegistries))
	}
}

func TestCreateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	nc := &NetworkConfiguration{
		AwsvpcConfiguration: &AwsvpcConfiguration{
			Subnets:        []string{"subnet-abc123", "subnet-def456"},
			SecurityGroups: []string{"sg-12345678"},
			AssignPublicIP: assignPublicIPEnabled,
		},
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:          "my-svc",
		TaskDefinition:       "myapp",
		DesiredCount:         1,
		NetworkConfiguration: nc,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svc.NetworkConfiguration == nil || svc.NetworkConfiguration.AwsvpcConfiguration == nil {
		t.Fatal("NetworkConfiguration not stored")
	}
	if len(svc.NetworkConfiguration.AwsvpcConfiguration.Subnets) != 2 {
		t.Errorf("Subnets = %v, want 2 subnets", svc.NetworkConfiguration.AwsvpcConfiguration.Subnets)
	}
}

func TestCreateService_PropagateTags(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   1,
		PropagateTags:  propagateTagsService,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svc.PropagateTags != propagateTagsService {
		t.Errorf("PropagateTags = %q, want SERVICE", svc.PropagateTags)
	}
}

func TestCreateService_PropagateTags_DefaultNone(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   1,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svc.PropagateTags != propagateTagsNone {
		t.Errorf("PropagateTags = %q, want NONE (default)", svc.PropagateTags)
	}
}

func TestRunTask_NetworkConfiguration(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	nc := &NetworkConfiguration{
		AwsvpcConfiguration: &AwsvpcConfiguration{
			Subnets:        []string{"subnet-abc123"},
			AssignPublicIP: assignPublicIPDisabled,
		},
	}

	tasks, err := b.RunTask(RunTaskInput{
		TaskDefinition:       "myapp",
		NetworkConfiguration: nc,
		Count:                1,
	})
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("len(tasks) = %d, want 1", len(tasks))
	}
	if tasks[0].NetworkConfiguration == nil ||
		tasks[0].NetworkConfiguration.AwsvpcConfiguration == nil {
		t.Fatal("task NetworkConfiguration not stored")
	}
	if tasks[0].NetworkConfiguration.AwsvpcConfiguration.Subnets[0] != "subnet-abc123" {
		t.Errorf("subnet = %q, want subnet-abc123",
			tasks[0].NetworkConfiguration.AwsvpcConfiguration.Subnets[0])
	}
}

func TestRunTask_PlatformVersionValidation(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	_, err = b.RunTask(RunTaskInput{
		TaskDefinition:  "myapp",
		PlatformVersion: "9.9.9",
	})
	if err == nil {
		t.Fatal("expected error for unknown platform version")
	}
	if !strings.Contains(err.Error(), "platform version") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunTask_PlatformVersionLatest(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	tasks, err := b.RunTask(RunTaskInput{
		TaskDefinition:  "myapp",
		PlatformVersion: "LATEST",
	})
	if err != nil {
		t.Fatalf("RunTask with LATEST: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("expected at least 1 task")
	}
}

func TestRunTask_PropagateTags(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	tasks, err := b.RunTask(RunTaskInput{
		TaskDefinition: "myapp",
		PropagateTags:  propagateTagsTaskDefinition,
	})
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if tasks[0].PropagateTags != propagateTagsTaskDefinition {
		t.Errorf("PropagateTags = %q, want TASK_DEFINITION", tasks[0].PropagateTags)
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

func TestDeleteCluster_CascadesServiceDeployments(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "test-cluster"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		Cluster:        "test-cluster",
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   0,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Inject a fake service deployment entry to test cascade deletion.
	b.mu.Lock("test-inject")
	b.serviceDeployments[svc.ServiceArn] = &ServiceDeployment{
		ServiceDeploymentArn: "arn:aws:ecs:us-east-1:123456789012:service-deployment/test-cluster/my-svc/abc",
	}
	b.mu.Unlock()

	_, err = b.DeleteCluster("test-cluster")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	// Service deployment should be gone.
	b.mu.RLock("test-verify")
	_, stillExists := b.serviceDeployments[svc.ServiceArn]
	b.mu.RUnlock()

	if stillExists {
		t.Error("service deployment not cascade-deleted with cluster")
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
		{Name: "DB_PASSWORD", ValueFrom: "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-pass"},
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

func TestUpdateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	_, err = b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   0,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	newNC := &NetworkConfiguration{
		AwsvpcConfiguration: &AwsvpcConfiguration{
			Subnets:        []string{"subnet-new123"},
			AssignPublicIP: assignPublicIPEnabled,
		},
	}

	updated, err := b.UpdateService(UpdateServiceInput{
		Service:              "my-svc",
		NetworkConfiguration: newNC,
	})
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if updated.NetworkConfiguration == nil ||
		len(updated.NetworkConfiguration.AwsvpcConfiguration.Subnets) == 0 ||
		updated.NetworkConfiguration.AwsvpcConfiguration.Subnets[0] != "subnet-new123" {
		t.Errorf("NetworkConfiguration not updated: %v", updated.NetworkConfiguration)
	}
}

func TestUpdateService_LoadBalancers(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	_, err = b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   0,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	updated, err := b.UpdateService(UpdateServiceInput{
		Service: "my-svc",
		LoadBalancers: []LoadBalancer{
			{TargetGroupArn: "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/new-tg/abc"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if len(updated.LoadBalancers) != 1 {
		t.Errorf("LoadBalancers = %v, want 1", updated.LoadBalancers)
	}
}

func TestDeploymentConfiguration_MinMaxPercent_Stored(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "myapp",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("register task def: %v", err)
	}

	minPct := 75
	maxPct := 150

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "my-svc",
		TaskDefinition: "myapp",
		DesiredCount:   1,
		DeploymentConfiguration: &DeploymentConfiguration{
			MinimumHealthyPercent: &minPct,
			MaximumPercent:        &maxPct,
		},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	if svc.DeploymentConfiguration.MinimumHealthyPercent == nil ||
		*svc.DeploymentConfiguration.MinimumHealthyPercent != 75 {
		t.Errorf("MinimumHealthyPercent = %v, want 75", svc.DeploymentConfiguration.MinimumHealthyPercent)
	}
	if svc.DeploymentConfiguration.MaximumPercent == nil ||
		*svc.DeploymentConfiguration.MaximumPercent != 150 {
		t.Errorf("MaximumPercent = %v, want 150", svc.DeploymentConfiguration.MaximumPercent)
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
		t.Errorf("SourceContainer = %q, want app", td.ContainerDefinitions[1].VolumesFrom[0].SourceContainer)
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
		t.Errorf("DependsOn condition = %q, want COMPLETE", td.ContainerDefinitions[1].DependsOn[0].Condition)
	}
}
