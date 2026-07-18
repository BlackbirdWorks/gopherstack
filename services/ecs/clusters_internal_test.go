package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-cluster",
			want:  "my-cluster",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			want:  "my-cluster",
		},
		{
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clusterKey(tt.input))
		})
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

	// CreateService itself records a real ServiceDeployment for the initial
	// PRIMARY deployment (see syncServiceDeploymentsLocked in
	// service_deployments.go), keyed by ServiceDeploymentArn — not by
	// ServiceArn. Confirm it exists before asserting the cascade delete.
	deploymentArns, err := b.ListServiceDeployments("test-cluster", "my-svc")
	if err != nil {
		t.Fatalf("ListServiceDeployments: %v", err)
	}

	if len(deploymentArns) != 1 {
		t.Fatalf("ListServiceDeployments before delete = %d entries, want 1", len(deploymentArns))
	}

	// Also inject a second, independently-keyed entry (as an external caller
	// or an older revision might leave behind) to prove the cascade matches on
	// the ServiceArn field rather than assuming a single well-known key.
	extraArn := "arn:aws:ecs:us-east-1:123456789012:service-deployment/test-cluster/my-svc/abc"
	b.mu.Lock("test-inject")
	b.serviceDeployments.Put(&ServiceDeployment{
		ServiceDeploymentArn: extraArn,
		ServiceArn:           svc.ServiceArn,
	})
	b.mu.Unlock()

	_, err = b.DeleteCluster("test-cluster")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	// Both the real and the injected service deployment should be gone.
	b.mu.RLock("test-verify")
	_, realStillExists := b.serviceDeployments.Get(deploymentArns[0])
	_, extraStillExists := b.serviceDeployments.Get(extraArn)
	b.mu.RUnlock()

	if realStillExists {
		t.Error("real service deployment not cascade-deleted with cluster")
	}

	if extraStillExists {
		t.Error("injected service deployment not cascade-deleted with cluster")
	}
}
