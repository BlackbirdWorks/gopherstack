package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newAuditBackend(t *testing.T) *rds.InMemoryBackend {
	t.Helper()

	return rds.NewInMemoryBackend("123456789012", "us-east-1")
}

func createAuditInstance(t *testing.T, b *rds.InMemoryBackend) *rds.DBInstance {
	t.Helper()

	inst, err := b.CreateDBInstance("mydb", "postgres", "db.t3.medium", "", "admin", "", 20, rds.DBInstanceOptions{})
	if err != nil {
		t.Fatalf("CreateDBInstance: %v", err)
	}
	rds.FlushInstanceLifecycle(b)

	return inst
}

// TestApplyImmediately_True verifies that when ApplyImmediately=true all changes are
// applied at once and no PendingModifiedValues are stored.
func TestApplyImmediately_True(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPending      func(*testing.T, *rds.PendingModifiedValues)
		wantAfterFlush   func(*testing.T, rds.DBInstance)
		name             string
		instanceClass    string
		opts             rds.DBInstanceOptions
		allocatedStorage int
	}{
		{
			name:          "class change immediate",
			instanceClass: "db.r5.large",
			opts:          rds.DBInstanceOptions{ApplyImmediately: true},
		},
		{
			name:             "storage change immediate",
			allocatedStorage: 100,
			opts:             rds.DBInstanceOptions{ApplyImmediately: true},
		},
		{
			name: "engine version change immediate",
			opts: rds.DBInstanceOptions{ApplyImmediately: true, EngineVersion: "15.3"},
		},
		{
			name: "iops change immediate",
			opts: rds.DBInstanceOptions{ApplyImmediately: true, Iops: 3000},
		},
		{
			name: "multiAZ enable immediate",
			opts: rds.DBInstanceOptions{ApplyImmediately: true, MultiAZ: true, MultiAZSet: true},
		},
		{
			name: "storage type change immediate",
			opts: rds.DBInstanceOptions{ApplyImmediately: true, StorageType: "io1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			createAuditInstance(t, b)

			modified, err := b.ModifyDBInstance("mydb", tc.instanceClass, tc.allocatedStorage, tc.opts)
			if err != nil {
				t.Fatalf("ModifyDBInstance: %v", err)
			}

			if modified.PendingModifiedValues != nil {
				t.Errorf(
					"PendingModifiedValues should be nil with ApplyImmediately=true, got %+v",
					modified.PendingModifiedValues,
				)
			}
		})
	}
}

// TestApplyImmediately_False verifies that deferrable changes are stored in
// PendingModifiedValues and applied only when the instance next becomes available.
func TestApplyImmediately_False(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPending    func(*testing.T, *rds.PendingModifiedValues)
		wantAfterFlush func(*testing.T, rds.DBInstance)
		name           string
		instanceClass  string
		opts           rds.DBInstanceOptions
		allocSt        int
	}{
		{
			name:          "class deferred",
			instanceClass: "db.r5.large",
			opts:          rds.DBInstanceOptions{ApplyImmediately: false},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil, want non-nil")
				}
				if pv.DBInstanceClass != "db.r5.large" {
					t.Errorf("pending DBInstanceClass = %q, want db.r5.large", pv.DBInstanceClass)
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if inst.DBInstanceClass != "db.r5.large" {
					t.Errorf("after flush DBInstanceClass = %q, want db.r5.large", inst.DBInstanceClass)
				}
				if inst.PendingModifiedValues != nil {
					t.Errorf("PendingModifiedValues should be nil after flush")
				}
			},
		},
		{
			name:    "storage deferred",
			allocSt: 200,
			opts:    rds.DBInstanceOptions{ApplyImmediately: false},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil")
				}
				if pv.AllocatedStorage != 200 {
					t.Errorf("pending AllocatedStorage = %d, want 200", pv.AllocatedStorage)
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if inst.AllocatedStorage != 200 {
					t.Errorf("after flush AllocatedStorage = %d, want 200", inst.AllocatedStorage)
				}
			},
		},
		{
			name: "engine version deferred",
			opts: rds.DBInstanceOptions{ApplyImmediately: false, EngineVersion: "16.0"},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil")
				}
				if pv.EngineVersion != "16.0" {
					t.Errorf("pending EngineVersion = %q, want 16.0", pv.EngineVersion)
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if inst.EngineVersion != "16.0" {
					t.Errorf("after flush EngineVersion = %q, want 16.0", inst.EngineVersion)
				}
			},
		},
		{
			name: "iops deferred",
			opts: rds.DBInstanceOptions{ApplyImmediately: false, Iops: 5000},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil")
				}
				if pv.Iops != 5000 {
					t.Errorf("pending Iops = %d, want 5000", pv.Iops)
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if inst.Iops != 5000 {
					t.Errorf("after flush Iops = %d, want 5000", inst.Iops)
				}
			},
		},
		{
			name: "multiAZ deferred",
			opts: rds.DBInstanceOptions{ApplyImmediately: false, MultiAZ: true, MultiAZSet: true},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil")
				}
				if pv.MultiAZChange == nil || !*pv.MultiAZChange {
					t.Errorf("pending MultiAZChange should be &true")
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if !inst.MultiAZ {
					t.Errorf("after flush MultiAZ = false, want true")
				}
			},
		},
		{
			name: "storage type deferred",
			opts: rds.DBInstanceOptions{ApplyImmediately: false, StorageType: "io2"},
			wantPending: func(t *testing.T, pv *rds.PendingModifiedValues) {
				t.Helper()
				if pv == nil {
					t.Fatal("PendingModifiedValues is nil")
				}
				if pv.StorageType != "io2" {
					t.Errorf("pending StorageType = %q, want io2", pv.StorageType)
				}
			},
			wantAfterFlush: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if inst.StorageType != "io2" {
					t.Errorf("after flush StorageType = %q, want io2", inst.StorageType)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			createAuditInstance(t, b)

			modified, err := b.ModifyDBInstance("mydb", tc.instanceClass, tc.allocSt, tc.opts)
			if err != nil {
				t.Fatalf("ModifyDBInstance: %v", err)
			}

			tc.wantPending(t, modified.PendingModifiedValues)

			// After flush (reconciler fires): pending applied, status available.
			rds.FlushInstanceLifecycle(b)
			instances, err := b.DescribeDBInstances("mydb")
			if err != nil {
				t.Fatalf("DescribeDBInstances: %v", err)
			}

			tc.wantAfterFlush(t, instances[0])
		})
	}
}

// TestApplyImmediately_NoPendingWhenSameValue verifies that PendingModifiedValues
// is nil when the requested values match the current instance values.
func TestApplyImmediately_NoPendingWhenSameValue(t *testing.T) {
	t.Parallel()

	b := newAuditBackend(t)
	createAuditInstance(t, b)

	// Request same class and same storage — nothing actually changes.
	modified, err := b.ModifyDBInstance("mydb", "db.t3.medium", 20, rds.DBInstanceOptions{
		ApplyImmediately: false,
	})
	if err != nil {
		t.Fatalf("ModifyDBInstance: %v", err)
	}

	if modified.PendingModifiedValues != nil {
		t.Errorf(
			"PendingModifiedValues should be nil when nothing changed, got %+v",
			modified.PendingModifiedValues,
		)
	}
}

// TestApplyImmediately_ImmediateFieldsApplyWithoutImmediately verifies that fields
// like VpcSecurityGroups and DeletionProtection apply immediately even without
// ApplyImmediately=true.
func TestApplyImmediately_ImmediateFieldsApplyWithoutImmediately(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, rds.DBInstance)
		name  string
		opts  rds.DBInstanceOptions
	}{
		{
			name: "deletion protection always immediate",
			opts: rds.DBInstanceOptions{
				ApplyImmediately:      false,
				DeletionProtection:    true,
				DeletionProtectionSet: true,
			},
			check: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if !inst.DeletionProtection {
					t.Errorf("DeletionProtection not applied immediately")
				}
			},
		},
		{
			name: "vpc security groups always immediate",
			opts: rds.DBInstanceOptions{
				ApplyImmediately:    false,
				VpcSecurityGroupIDs: []string{"sg-111"},
			},
			check: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if len(inst.VpcSecurityGroups) == 0 || inst.VpcSecurityGroups[0].VpcSecurityGroupID != "sg-111" {
					t.Errorf("VpcSecurityGroups not applied immediately: %+v", inst.VpcSecurityGroups)
				}
			},
		},
		{
			name: "cloudwatch logs always immediate",
			opts: rds.DBInstanceOptions{
				ApplyImmediately:             false,
				EnabledCloudwatchLogsExports: []string{"postgresql", "upgrade"},
			},
			check: func(t *testing.T, inst rds.DBInstance) {
				t.Helper()
				if len(inst.EnabledCloudwatchLogsExports) == 0 {
					t.Errorf("EnabledCloudwatchLogsExports not applied immediately")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			createAuditInstance(t, b)

			modified, err := b.ModifyDBInstance("mydb", "", 0, tc.opts)
			if err != nil {
				t.Fatalf("ModifyDBInstance: %v", err)
			}

			// Check applied on the returned (modifying) instance without flush.
			tc.check(t, *modified)
		})
	}
}

// TestApplyImmediately_ClearsPendingOnNextImmediate verifies that a subsequent
// ModifyDBInstance with ApplyImmediately=true clears any pending changes and
// applies the new values.
func TestApplyImmediately_ClearsPendingOnNextImmediate(t *testing.T) {
	t.Parallel()

	b := newAuditBackend(t)
	createAuditInstance(t, b)

	// First modify: deferred class change to db.r5.large.
	_, err := b.ModifyDBInstance("mydb", "db.r5.large", 0, rds.DBInstanceOptions{ApplyImmediately: false})
	if err != nil {
		t.Fatalf("ModifyDBInstance deferred: %v", err)
	}

	// Second modify: immediate class change to db.m5.xlarge — should override pending.
	modified, err := b.ModifyDBInstance("mydb", "db.m5.xlarge", 0, rds.DBInstanceOptions{ApplyImmediately: true})
	if err != nil {
		t.Fatalf("ModifyDBInstance immediate: %v", err)
	}

	if modified.PendingModifiedValues != nil {
		t.Errorf(
			"PendingModifiedValues should be nil after ApplyImmediately=true, got %+v",
			modified.PendingModifiedValues,
		)
	}

	if modified.DBInstanceClass != "db.m5.xlarge" {
		t.Errorf("DBInstanceClass = %q, want db.m5.xlarge", modified.DBInstanceClass)
	}
}
