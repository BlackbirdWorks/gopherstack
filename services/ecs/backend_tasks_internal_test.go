package ecs

import (
	"strings"
	"testing"
)

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
				t.Errorf(
					"validatePlatformVersion(%q) error = %v, wantErr %v",
					tt.pv,
					err,
					tt.wantErr,
				)
			}
		})
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
