package eks //nolint:testpackage // existing issue.

import (
	"testing"
	"time"
)

// TestAsyncLifecycle_Cluster verifies CreateCluster starts in CREATING and
// transitions to ACTIVE after the async delay, observable via DescribeCluster.
func TestAsyncLifecycle_Cluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wait       bool
	}{
		{
			name:       "immediately after create is CREATING",
			wait:       false,
			wantStatus: statusCreating,
		},
		{
			name:       "after delay is ACTIVE",
			wait:       true,
			wantStatus: statusActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			defer b.Reset()

			created, err := b.CreateCluster("clstr", "", "arn:aws:iam::123456789012:role/eks", nil, nil, nil)
			if err != nil {
				t.Fatalf("CreateCluster: %v", err)
			}

			if created.Status != statusCreating {
				t.Fatalf("returned status = %q, want %q", created.Status, statusCreating)
			}

			if tt.wait {
				time.Sleep(clusterTransitionDelay + 50*time.Millisecond)
			}

			got, err := b.DescribeCluster("clstr")
			if err != nil {
				t.Fatalf("DescribeCluster: %v", err)
			}

			if got.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got.Status, tt.wantStatus)
			}
		})
	}
}

// TestAsyncLifecycle_Nodegroup verifies CreateNodegroup starts in CREATING and
// transitions to ACTIVE after the async delay, observable via DescribeNodegroup.
func TestAsyncLifecycle_Nodegroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wait       bool
	}{
		{
			name:       "immediately after create is CREATING",
			wait:       false,
			wantStatus: statusCreating,
		},
		{
			name:       "after delay is ACTIVE",
			wait:       true,
			wantStatus: statusActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			defer b.Reset()

			// Wait for the cluster to become ACTIVE so nodegroup creation is valid
			// and not confused with the cluster's own transition.
			if _, err := b.CreateCluster("clstr", "", "arn:aws:iam::123456789012:role/eks", nil, nil, nil); err != nil {
				t.Fatalf("CreateCluster: %v", err)
			}

			time.Sleep(clusterTransitionDelay + 50*time.Millisecond)

			created, err := b.CreateNodegroup(
				"clstr", "ng1", "arn:aws:iam::123456789012:role/node",
				"", "", "", "",
				[]string{"t3.medium"},
				2, 1, 3,
				NodegroupInput{},
				nil,
			)
			if err != nil {
				t.Fatalf("CreateNodegroup: %v", err)
			}

			if created.Status != statusCreating {
				t.Fatalf("returned status = %q, want %q", created.Status, statusCreating)
			}

			if tt.wait {
				time.Sleep(nodegroupTransitionDelay + 50*time.Millisecond)
			}

			got, err := b.DescribeNodegroup("clstr", "ng1")
			if err != nil {
				t.Fatalf("DescribeNodegroup: %v", err)
			}

			if got.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got.Status, tt.wantStatus)
			}
		})
	}
}
