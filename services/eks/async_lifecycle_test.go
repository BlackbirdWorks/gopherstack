package eks //nolint:testpackage // existing issue.

import (
	"testing"
	"testing/synctest"
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

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
				defer b.Reset()

				created, err := b.CreateCluster(
					"clstr",
					"",
					"arn:aws:iam::123456789012:role/eks",
					nil,
					nil,
					nil,
				)
				if err != nil {
					t.Fatalf("CreateCluster: %v", err)
				}

				if created.Status != statusCreating {
					t.Fatalf("returned status = %q, want %q", created.Status, statusCreating)
				}

				if tt.wait {
					// Strictly longer than the transition delay: two timers due
					// at the same fake instant have no guaranteed fire order.
					time.Sleep(clusterTransitionDelay + time.Millisecond)
				}

				got, err := b.DescribeCluster("clstr")
				if err != nil {
					t.Fatalf("DescribeCluster: %v", err)
				}

				if got.Status != tt.wantStatus {
					t.Fatalf("status = %q, want %q", got.Status, tt.wantStatus)
				}
			})
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

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
				defer b.Reset()

				// Wait for the cluster to become ACTIVE so nodegroup creation is
				// valid and not confused with the cluster's own transition.
				_, err := b.CreateCluster("clstr", "", "arn:aws:iam::123456789012:role/eks", nil, nil, nil)
				if err != nil {
					t.Fatalf("CreateCluster: %v", err)
				}

				time.Sleep(clusterTransitionDelay + time.Millisecond)

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
					time.Sleep(nodegroupTransitionDelay + time.Millisecond)
				}

				got, err := b.DescribeNodegroup("clstr", "ng1")
				if err != nil {
					t.Fatalf("DescribeNodegroup: %v", err)
				}

				if got.Status != tt.wantStatus {
					t.Fatalf("status = %q, want %q", got.Status, tt.wantStatus)
				}
			})
		})
	}
}

// TestUpdateClusterVersion_ReturnedUpdateIsNotLiveAliased reproduces the CI
// race in updates.go:30: UpdateClusterVersion returned the live stored *Update
// with no copy, so scheduleUpdateTransition's timer callback later mutated
// u.Status in place, racing any unsynchronized read of the field the caller
// was handed. This asserts the returned pointer is a private snapshot: it
// must stay InProgress even once the async transition has actually run.
func TestUpdateClusterVersion_ReturnedUpdateIsNotLiveAliased(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
		defer b.Reset()

		_, err := b.CreateCluster("cl", "1.31", "arn:aws:iam::123456789012:role/eks", nil, nil, nil)
		if err != nil {
			t.Fatalf("CreateCluster: %v", err)
		}

		u, err := b.UpdateClusterVersion("cl", "1.32")
		if err != nil {
			t.Fatalf("UpdateClusterVersion: %v", err)
		}

		if u.Status != statusInProgress {
			t.Fatalf("returned status = %q, want %q", u.Status, statusInProgress)
		}

		// Let scheduleUpdateTransition's timer fire. On unfixed code this
		// mutates the exact object u points to, and -race flags the
		// unsynchronized read above against that write.
		time.Sleep(updateTransitionDelay + time.Millisecond)

		if u.Status != statusInProgress {
			t.Fatalf(
				"returned *Update must be a private copy: u.Status changed to %q after the async transition ran",
				u.Status,
			)
		}

		got, err := b.DescribeUpdate("cl", u.ID)
		if err != nil {
			t.Fatalf("DescribeUpdate: %v", err)
		}

		if got.Status != statusSuccessful {
			t.Fatalf("stored update status = %q, want %q", got.Status, statusSuccessful)
		}
	})
}
