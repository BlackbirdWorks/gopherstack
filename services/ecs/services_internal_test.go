package ecs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newTestBackend builds a fresh in-memory ECS backend with a no-op task
// runner, for tests that only exercise backend state transitions and don't
// need real (or fake) Docker interaction. Shared across the internal test
// files in this package.
func newTestBackend() *InMemoryBackend {
	return NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
}

func TestServiceKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-service",
			want:  "my-service",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:service/cluster/my-service",
			want:  "my-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, serviceKey(tt.input))
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
				t.Errorf(
					"validateDeploymentController(%v) error = %v, wantErr %v",
					tt.dc,
					err,
					tt.wantErr,
				)
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
		if out.MinimumHealthyPercent == nil ||
			*out.MinimumHealthyPercent != defaultMinimumHealthyPercent {
			t.Errorf(
				"MinimumHealthyPercent = %v, want %d",
				out.MinimumHealthyPercent,
				defaultMinimumHealthyPercent,
			)
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
		t.Errorf(
			"Subnets = %v, want 2 subnets",
			svc.NetworkConfiguration.AwsvpcConfiguration.Subnets,
		)
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
			{
				TargetGroupArn: "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/new-tg/abc",
			},
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
		t.Errorf(
			"MinimumHealthyPercent = %v, want 75",
			svc.DeploymentConfiguration.MinimumHealthyPercent,
		)
	}
	if svc.DeploymentConfiguration.MaximumPercent == nil ||
		*svc.DeploymentConfiguration.MaximumPercent != 150 {
		t.Errorf("MaximumPercent = %v, want 150", svc.DeploymentConfiguration.MaximumPercent)
	}
}
