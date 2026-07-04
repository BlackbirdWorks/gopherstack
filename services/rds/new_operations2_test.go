package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestRDSBackend_AddRoleToDBCluster tests AddRoleToDBCluster backend method.
func TestRDSBackend_AddRoleToDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *rds.InMemoryBackend)
		name      string
		clusterID string
		roleARN   string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster(
					"my-cluster",
					"aurora-postgresql",
					"admin",
					"mydb",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
			},
			clusterID: "my-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
		},
		{
			name:      "cluster_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "no-such-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
			wantErr:   true,
			wantErrIs: rds.ErrClusterNotFound,
		},
		{
			name:      "empty_cluster_id",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name: "empty_role_arn",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			clusterID: "my-cluster",
			roleARN:   "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
				_ = b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000000000000:role/MyRole")
			},
			clusterID: "my-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.AddRoleToDBCluster(tt.clusterID, tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRDSBackend_AddRoleToDBInstance tests AddRoleToDBInstance backend method.
func TestRDSBackend_AddRoleToDBInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		setup      func(b *rds.InMemoryBackend)
		name       string
		instanceID string
		roleARN    string
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			instanceID: "my-db",
			roleARN:    "arn:aws:iam::000000000000:role/MyRole",
		},
		{
			name:       "instance_not_found",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "no-such-db",
			roleARN:    "arn:aws:iam::000000000000:role/MyRole",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceNotFound,
		},
		{
			name:       "empty_instance_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "",
			roleARN:    "arn:aws:iam::000000000000:role/MyRole",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
		{
			name: "empty_role_arn",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			instanceID: "my-db",
			roleARN:    "",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.AddRoleToDBInstance(tt.instanceID, tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRDSBackend_AddSourceIdentifierToSubscription tests AddSourceIdentifierToSubscription.
func TestRDSBackend_AddSourceIdentifierToSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs        error
		setup            func(b *rds.InMemoryBackend)
		name             string
		subscriptionName string
		sourceIdentifier string
		wantSourceIDs    []string
		wantErr          bool
	}{
		{
			name:             "creates_new_subscription",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "my-sub",
			sourceIdentifier: "my-db-instance",
			wantSourceIDs:    []string{"my-db-instance"},
		},
		{
			name: "adds_to_existing",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AddSourceIdentifierToSubscription("my-sub", "first-db")
			},
			subscriptionName: "my-sub",
			sourceIdentifier: "second-db",
			wantSourceIDs:    []string{"first-db", "second-db"},
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AddSourceIdentifierToSubscription("my-sub", "my-db")
			},
			subscriptionName: "my-sub",
			sourceIdentifier: "my-db",
			wantSourceIDs:    []string{"my-db"},
		},
		{
			name:             "empty_subscription_name",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "",
			sourceIdentifier: "my-db",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
		{
			name:             "empty_source_identifier",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "my-sub",
			sourceIdentifier: "",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sub, err := b.AddSourceIdentifierToSubscription(tt.subscriptionName, tt.sourceIdentifier)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.subscriptionName, sub.SubscriptionName)
			assert.Equal(t, tt.wantSourceIDs, sub.SourceIDs)
		})
	}
}

// TestRDSBackend_ApplyPendingMaintenanceAction tests ApplyPendingMaintenanceAction.
func TestRDSBackend_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		resourceID  string
		applyAction string
		wantErr     bool
	}{
		{
			name: "success_for_instance",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			resourceID:  "my-db",
			applyAction: "system-update",
		},
		{
			name: "success_for_cluster",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			resourceID:  "my-cluster",
			applyAction: "system-update",
		},
		{
			name:        "resource_not_found",
			setup:       func(_ *rds.InMemoryBackend) {},
			resourceID:  "no-such-resource",
			applyAction: "system-update",
			wantErr:     true,
			wantErrIs:   rds.ErrInstanceNotFound,
		},
		{
			name:        "empty_resource_id",
			setup:       func(_ *rds.InMemoryBackend) {},
			resourceID:  "",
			applyAction: "system-update",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
		{
			name: "empty_apply_action",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			resourceID:  "my-db",
			applyAction: "",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			result, err := b.ApplyPendingMaintenanceAction(tt.resourceID, tt.applyAction)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, result)
		})
	}
}

// TestRDSBackend_AuthorizeDBSecurityGroupIngress tests AuthorizeDBSecurityGroupIngress.
func TestRDSBackend_AuthorizeDBSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *rds.InMemoryBackend)
		name      string
		groupName string
		cidrIP    string
		wantErrIs error
		wantCIDRs []string
		wantErr   bool
	}{
		{
			name:      "creates_new_group",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "my-sg",
			cidrIP:    "10.0.0.0/8",
			wantCIDRs: []string{"10.0.0.0/8"},
		},
		{
			name: "adds_to_existing",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
			},
			groupName: "my-sg",
			cidrIP:    "192.168.0.0/16",
			wantCIDRs: []string{"10.0.0.0/8", "192.168.0.0/16"},
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
			},
			groupName: "my-sg",
			cidrIP:    "10.0.0.0/8",
			wantCIDRs: []string{"10.0.0.0/8"},
		},
		{
			name:      "empty_group_name",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "",
			cidrIP:    "10.0.0.0/8",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_cidr",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "my-sg",
			cidrIP:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sg, err := b.AuthorizeDBSecurityGroupIngress(tt.groupName, tt.cidrIP)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupName, sg.DBSecurityGroupName)

			cidrs := make([]string, 0, len(sg.IPRanges))
			for _, r := range sg.IPRanges {
				cidrs = append(cidrs, r.CIDRIP)
			}

			assert.Equal(t, tt.wantCIDRs, cidrs)
		})
	}
}

// TestRDSBackend_BacktrackDBCluster tests BacktrackDBCluster.
func TestRDSBackend_BacktrackDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		clusterID   string
		backtrackTo string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			clusterID:   "my-cluster",
			backtrackTo: "2024-01-01T00:00:00Z",
		},
		{
			name:        "cluster_not_found",
			setup:       func(_ *rds.InMemoryBackend) {},
			clusterID:   "no-such-cluster",
			backtrackTo: "2024-01-01T00:00:00Z",
			wantErr:     true,
			wantErrIs:   rds.ErrClusterNotFound,
		},
		{
			name:        "empty_cluster_id",
			setup:       func(_ *rds.InMemoryBackend) {},
			clusterID:   "",
			backtrackTo: "2024-01-01T00:00:00Z",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			bt, err := b.BacktrackDBCluster(tt.clusterID, tt.backtrackTo)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, bt.DBClusterIdentifier)
			assert.Equal(t, "applying", bt.Status)
		})
	}
}

// TestRDSBackend_CopyDBClusterParameterGroup tests CopyDBClusterParameterGroup.
func TestRDSBackend_CopyDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		source      string
		target      string
		description string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Source group")
			},
			source:      "src-pg",
			target:      "dst-pg",
			description: "Copied group",
		},
		{
			name: "inherits_description",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Original desc")
			},
			source: "src-pg",
			target: "dst-pg",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBClusterParameterGroup("src-pg", "aurora-postgresql14", "Source")
				_, _ = b.CreateDBClusterParameterGroup("dst-pg", "aurora-postgresql14", "Dest")
			},
			source:    "src-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_target",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "src-pg",
			target:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			pg, err := b.CopyDBClusterParameterGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, pg.DBParameterGroupName)
		})
	}
}

// TestRDSBackend_CopyDBParameterGroup tests CopyDBParameterGroup.
func TestRDSBackend_CopyDBParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		source      string
		target      string
		description string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBParameterGroup("src-pg", "postgres14", "Source group")
			},
			source:      "src-pg",
			target:      "dst-pg",
			description: "Copied group",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBParameterGroup("src-pg", "postgres14", "Source")
				_, _ = b.CreateDBParameterGroup("dst-pg", "postgres14", "Dest")
			},
			source:    "src-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			pg, err := b.CopyDBParameterGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, pg.DBParameterGroupName)
		})
	}
}

// TestRDSBackend_CopyOptionGroup tests CopyOptionGroup.
func TestRDSBackend_CopyOptionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		source      string
		target      string
		description string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Source group")
			},
			source:      "src-og",
			target:      "dst-og",
			description: "Copied group",
		},
		{
			name: "inherits_description",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Original desc")
			},
			source: "src-og",
			target: "dst-og",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-og",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrOptionGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Source")
				_, _ = b.CreateOptionGroup("dst-og", "mysql", "8.0", "Dest")
			},
			source:    "src-og",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrOptionGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			og, err := b.CopyOptionGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, og.OptionGroupName)
		})
	}
}

// TestRDSBackend_CreateBlueGreenDeployment tests CreateBlueGreenDeployment.
func TestRDSBackend_CreateBlueGreenDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *rds.InMemoryBackend)
		name      string
		deplName  string
		source    string
		wantErr   bool
	}{
		{
			name:     "success",
			setup:    func(_ *rds.InMemoryBackend) {},
			deplName: "my-deployment",
			source:   "arn:aws:rds:us-east-1:000000000000:db:my-db",
		},
		{
			name: "duplicate_name",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateBlueGreenDeployment("my-deployment", "arn:aws:rds:us-east-1:000000000000:db:my-db")
			},
			deplName:  "my-deployment",
			source:    "arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantErr:   true,
			wantErrIs: rds.ErrBlueGreenDeploymentAlreadyExists,
		},
		{
			name:      "empty_name",
			setup:     func(_ *rds.InMemoryBackend) {},
			deplName:  "",
			source:    "arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			deplName:  "my-deployment",
			source:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			d, err := b.CreateBlueGreenDeployment(tt.deplName, tt.source)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.deplName, d.BlueGreenDeploymentName)
			assert.Equal(t, "available", d.Status)
			assert.Equal(t, tt.source, d.Source)
		})
	}
}

// TestRDSHandler_NewOperations2 tests the HTTP handlers for the 10 new operations.
func TestRDSHandler_NewOperations2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		setupBodies     []string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		// AddRoleToDBCluster
		{
			name: "AddRoleToDBCluster_success",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=role-cluster&Engine=aurora-postgresql",
			},
			body: "Action=AddRoleToDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=role-cluster&RoleArn=arn:aws:iam::000000000000:role/MyRole",
			wantCode:     http.StatusOK,
			wantContains: []string{"AddRoleToDBClusterResponse"},
		},
		{
			name: "AddRoleToDBCluster_cluster_not_found",
			body: "Action=AddRoleToDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=no-such-cluster&RoleArn=arn:aws:iam::000000000000:role/MyRole",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "AddRoleToDBCluster_missing_role_arn",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=role-cluster2",
			},
			body:         "Action=AddRoleToDBCluster&Version=2014-10-31&DBClusterIdentifier=role-cluster2&RoleArn=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// AddRoleToDBInstance
		{
			name: "AddRoleToDBInstance_success",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=role-inst&Engine=postgres",
			},
			body: "Action=AddRoleToDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=role-inst&RoleArn=arn:aws:iam::000000000000:role/MyRole",
			wantCode:     http.StatusOK,
			wantContains: []string{"AddRoleToDBInstanceResponse"},
		},
		{
			name: "AddRoleToDBInstance_not_found",
			body: "Action=AddRoleToDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=no-such-db&RoleArn=arn:aws:iam::000000000000:role/MyRole",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		// AddSourceIdentifierToSubscription
		{
			name: "AddSourceIdentifierToSubscription_success",
			body: "Action=AddSourceIdentifierToSubscription&Version=2014-10-31" +
				"&SubscriptionName=my-sub&SourceIdentifier=my-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"AddSourceIdentifierToSubscriptionResponse", "my-sub"},
		},
		{
			name: "AddSourceIdentifierToSubscription_empty_subscription",
			body: "Action=AddSourceIdentifierToSubscription&Version=2014-10-31" +
				"&SubscriptionName=&SourceIdentifier=my-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// ApplyPendingMaintenanceAction
		{
			name: "ApplyPendingMaintenanceAction_success_instance",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=maint-db&Engine=postgres",
			},
			body: "Action=ApplyPendingMaintenanceAction&Version=2014-10-31" +
				"&ResourceIdentifier=maint-db&ApplyAction=system-update&OptInType=immediate",
			wantCode:     http.StatusOK,
			wantContains: []string{"ApplyPendingMaintenanceActionResponse"},
		},
		{
			name: "ApplyPendingMaintenanceAction_not_found",
			body: "Action=ApplyPendingMaintenanceAction&Version=2014-10-31" +
				"&ResourceIdentifier=no-such-resource&ApplyAction=system-update&OptInType=immediate",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "ApplyPendingMaintenanceAction_empty_resource_id",
			body:         "Action=ApplyPendingMaintenanceAction&Version=2014-10-31&ApplyAction=system-update",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// AuthorizeDBSecurityGroupIngress
		{
			name: "AuthorizeDBSecurityGroupIngress_success",
			body: "Action=AuthorizeDBSecurityGroupIngress&Version=2014-10-31" +
				"&DBSecurityGroupName=my-sg&CIDRIP=10.0.0.0/8",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeDBSecurityGroupIngressResponse", "my-sg", "10.0.0.0/8"},
		},
		{
			name: "AuthorizeDBSecurityGroupIngress_empty_group",
			body: "Action=AuthorizeDBSecurityGroupIngress&Version=2014-10-31" +
				"&DBSecurityGroupName=&CIDRIP=10.0.0.0/8",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// BacktrackDBCluster
		{
			name: "BacktrackDBCluster_success",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=bt-cluster&Engine=aurora-mysql",
			},
			body: "Action=BacktrackDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=bt-cluster&BacktrackTo=2024-01-01T00%3A00%3A00Z",
			wantCode:     http.StatusOK,
			wantContains: []string{"BacktrackDBClusterResponse", "bt-cluster"},
		},
		{
			name: "BacktrackDBCluster_cluster_not_found",
			body: "Action=BacktrackDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=no-such-cluster&BacktrackTo=2024-01-01T00:00:00Z",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		// CopyDBClusterParameterGroup
		{
			name: "CopyDBClusterParameterGroup_success",
			setupBodies: []string{
				"Action=CreateDBClusterParameterGroup&Version=2014-10-31" +
					"&DBClusterParameterGroupName=src-cpg&DBParameterGroupFamily=aurora-postgresql14&Description=Source",
			},
			body: "Action=CopyDBClusterParameterGroup&Version=2014-10-31" +
				"&SourceDBClusterParameterGroupIdentifier=src-cpg" +
				"&TargetDBClusterParameterGroupIdentifier=dst-cpg" +
				"&TargetDBClusterParameterGroupDescription=Copied",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyDBClusterParameterGroupResponse", "dst-cpg"},
		},
		{
			name: "CopyDBClusterParameterGroup_source_not_found",
			body: "Action=CopyDBClusterParameterGroup&Version=2014-10-31" +
				"&SourceDBClusterParameterGroupIdentifier=no-such-cpg" +
				"&TargetDBClusterParameterGroupIdentifier=dst-cpg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBParameterGroupNotFound"},
		},
		// CopyDBParameterGroup
		{
			name: "CopyDBParameterGroup_success",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31" +
					"&DBParameterGroupName=src-pg&DBParameterGroupFamily=postgres14&Description=Source",
			},
			body: "Action=CopyDBParameterGroup&Version=2014-10-31" +
				"&SourceDBParameterGroupIdentifier=src-pg" +
				"&TargetDBParameterGroupIdentifier=dst-pg" +
				"&TargetDBParameterGroupDescription=Copied",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyDBParameterGroupResponse", "dst-pg"},
		},
		{
			name: "CopyDBParameterGroup_source_not_found",
			body: "Action=CopyDBParameterGroup&Version=2014-10-31" +
				"&SourceDBParameterGroupIdentifier=no-such-pg" +
				"&TargetDBParameterGroupIdentifier=dst-pg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBParameterGroupNotFound"},
		},
		// CopyOptionGroup
		{
			name: "CopyOptionGroup_success",
			setupBodies: []string{
				"Action=CreateOptionGroup&Version=2014-10-31" +
					"&OptionGroupName=src-og&EngineName=mysql&MajorEngineVersion=8.0&OptionGroupDescription=Source",
			},
			body: "Action=CopyOptionGroup&Version=2014-10-31" +
				"&SourceOptionGroupIdentifier=src-og" +
				"&TargetOptionGroupIdentifier=dst-og" +
				"&TargetOptionGroupDescription=Copied",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyOptionGroupResponse", "dst-og"},
		},
		{
			name: "CopyOptionGroup_source_not_found",
			body: "Action=CopyOptionGroup&Version=2014-10-31" +
				"&SourceOptionGroupIdentifier=no-such-og" +
				"&TargetOptionGroupIdentifier=dst-og",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"OptionGroupNotFound"},
		},
		// CreateBlueGreenDeployment
		{
			name: "CreateBlueGreenDeployment_success",
			body: "Action=CreateBlueGreenDeployment&Version=2014-10-31" +
				"&BlueGreenDeploymentName=my-deployment" +
				"&Source=arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateBlueGreenDeploymentResponse", "my-deployment"},
		},
		{
			name: "CreateBlueGreenDeployment_empty_name",
			body: "Action=CreateBlueGreenDeployment&Version=2014-10-31" +
				"&BlueGreenDeploymentName=" +
				"&Source=arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateBlueGreenDeployment_empty_source",
			body: "Action=CreateBlueGreenDeployment&Version=2014-10-31" +
				"&BlueGreenDeploymentName=my-deployment" +
				"&Source=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateBlueGreenDeployment_duplicate",
			setupBodies: []string{
				"Action=CreateBlueGreenDeployment&Version=2014-10-31" +
					"&BlueGreenDeploymentName=my-deployment" +
					"&Source=arn:aws:rds:us-east-1:000000000000:db:my-db",
			},
			body: "Action=CreateBlueGreenDeployment&Version=2014-10-31" +
				"&BlueGreenDeploymentName=my-deployment" +
				"&Source=arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"BlueGreenDeploymentAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()

			for _, setup := range tt.setupBodies {
				postRDSForm(t, h, setup)
			}

			rec := postRDSForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			body := rec.Body.String()
			for _, s := range tt.wantContains {
				assert.Contains(t, body, s)
			}

			for _, s := range tt.wantNotContains {
				assert.NotContains(t, body, s)
			}
		})
	}
}

// TestRDSBackend_Persistence_NewOps tests that new data is preserved through snapshot/restore cycles.
func TestRDSBackend_Persistence_NewOps(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDBCluster("pg-cluster", "aurora-postgresql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	err = b.AddRoleToDBCluster("pg-cluster", "arn:aws:iam::000000000000:role/MyRole")
	require.NoError(t, err)

	_, err = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	err = b.AddRoleToDBInstance("my-db", "arn:aws:iam::000000000000:role/InstanceRole")
	require.NoError(t, err)

	_, err = b.AddSourceIdentifierToSubscription("my-sub", "source-1")
	require.NoError(t, err)

	_, err = b.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
	require.NoError(t, err)

	_, err = b.CreateBlueGreenDeployment("my-bgd", "arn:aws:rds:us-east-1:000000000000:db:my-db")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify cluster role persisted.
	err = b2.AddRoleToDBCluster("pg-cluster", "arn:aws:iam::000000000000:role/MyRole")
	require.NoError(t, err)

	// Verify instance role persisted.
	err = b2.AddRoleToDBInstance("my-db", "arn:aws:iam::000000000000:role/InstanceRole")
	require.NoError(t, err)

	// Verify event subscription persisted.
	sub, err := b2.AddSourceIdentifierToSubscription("my-sub", "source-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"source-1"}, sub.SourceIDs)

	// Verify security group persisted.
	sg, err := b2.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
	require.NoError(t, err)
	require.Len(t, sg.IPRanges, 1)

	// Verify blue/green deployment persisted.
	_, err = b2.CreateBlueGreenDeployment("my-bgd", "arn:aws:rds:us-east-1:000000000000:db:my-db")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrBlueGreenDeploymentAlreadyExists)
}
