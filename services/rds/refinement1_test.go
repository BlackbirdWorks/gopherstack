package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newTestBackend(t *testing.T) *rds.InMemoryBackend {
	t.Helper()
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)

	return b
}

func TestCreateEventSubscription(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		subName    string
		snsARN     string
		sourceType string
		sourceIDs  []string
		wantErr    bool
	}{
		{
			name:       "success",
			subName:    "my-sub",
			snsARN:     "arn:aws:sns:us-east-1:123456789012:my-topic",
			sourceType: "db-instance",
			sourceIDs:  []string{"db-1"},
		},
		{
			name:      "empty name",
			subName:   "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			subName:   "dup-sub",
			snsARN:    "arn:aws:sns:us-east-1:123456789012:topic",
			wantErr:   true,
			wantErrIs: rds.ErrEventSubscriptionAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateEventSubscription(tt.subName, tt.snsARN, tt.sourceType, tt.sourceIDs, nil)
				require.NoError(t, err)
			}
			got, err := b.CreateEventSubscription(tt.subName, tt.snsARN, tt.sourceType, tt.sourceIDs, nil)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.subName, got.SubscriptionName)
			assert.Equal(t, tt.snsARN, got.SnsTopicArn)
			assert.True(t, got.Enabled)
			assert.Equal(t, "active", got.Status)
		})
	}
}

func TestDeleteEventSubscription(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		subName   string
		wantErr   bool
	}{
		{name: "success", subName: "my-sub"},
		{name: "not found", subName: "missing", wantErr: true, wantErrIs: rds.ErrEventSubscriptionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateEventSubscription(
					tt.subName, "arn:aws:sns:us-east-1:123456789012:topic", "", nil, nil,
				)
				require.NoError(t, err)
			}
			got, err := b.DeleteEventSubscription(tt.subName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.subName, got.SubscriptionName)
		})
	}
}

func TestDescribeEventSubscriptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		filter    string
		wantCount int
		wantErr   bool
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "named", filter: "sub-1", wantCount: 1},
		{name: "not found", filter: "missing", wantErr: true, wantErrIs: rds.ErrEventSubscriptionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			_, err := b.CreateEventSubscription("sub-1", "arn:aws:sns:us-east-1:123456789012:t", "", nil, nil)
			require.NoError(t, err)
			_, err = b.CreateEventSubscription("sub-2", "arn:aws:sns:us-east-1:123456789012:t", "", nil, nil)
			require.NoError(t, err)
			got, err := b.DescribeEventSubscriptions(tt.filter)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestModifyEventSubscription(t *testing.T) {
	t.Parallel()
	trueBool := true
	tests := []struct {
		wantErrIs   error
		enabled     *bool
		name        string
		subName     string
		newSNS      string
		newSource   string
		wantErr     bool
		wantEnabled bool
	}{
		{
			name:        "success full update",
			subName:     "my-sub",
			newSNS:      "arn:aws:sns:us-east-1:123456789012:new-topic",
			newSource:   "db-cluster",
			enabled:     &trueBool,
			wantEnabled: true,
		},
		{
			name:      "not found",
			subName:   "missing",
			wantErr:   true,
			wantErrIs: rds.ErrEventSubscriptionNotFound,
		},
		{
			name:        "partial update",
			subName:     "my-sub",
			newSNS:      "arn:aws:sns:us-east-1:123456789012:new-topic",
			wantEnabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateEventSubscription(tt.subName, "arn:aws:sns:us-east-1:123456789012:old", "", nil, nil)
				require.NoError(t, err)
			}
			got, err := b.ModifyEventSubscription(tt.subName, tt.newSNS, tt.newSource, nil, nil, tt.enabled)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			if tt.newSNS != "" {
				assert.Equal(t, tt.newSNS, got.SnsTopicArn)
			}
		})
	}
}

func TestDescribeEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		sourceID        string
		sourceType      string
		durationMinutes int
		wantMinCount    int
	}{
		{name: "all", wantMinCount: 0},
		{name: "by sourceID", sourceID: "db-1", wantMinCount: 0},
		{name: "by sourceType", sourceType: "db-instance", wantMinCount: 0},
		{name: "by duration", durationMinutes: 60, wantMinCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeEvents(tt.sourceID, tt.sourceType, tt.durationMinutes)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(got), tt.wantMinCount)
		})
	}
}

func TestDescribeEventCategories(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sourceType string
		wantLen    int
	}{
		{name: "all", sourceType: "", wantLen: 11},
		{name: "filtered", sourceType: "db-instance", wantLen: 11},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeEventCategories(tt.sourceType)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestDescribeBlueGreenDeployments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantCount int
		useID     bool
		wantErr   bool
	}{
		{name: "all", useID: false, wantCount: 2},
		{name: "by ID", useID: true, wantCount: 1},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "not found" {
				_, err := b.DescribeBlueGreenDeployments("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d1, err := b.CreateBlueGreenDeployment("bg-1", "arn:aws:rds:us-east-1:123456789012:cluster:source-1")
			require.NoError(t, err)
			_, err = b.CreateBlueGreenDeployment("bg-2", "arn:aws:rds:us-east-1:123456789012:cluster:source-2")
			require.NoError(t, err)
			if tt.useID {
				got, err2 := b.DescribeBlueGreenDeployments(d1.BlueGreenDeploymentIdentifier)
				require.NoError(t, err2)
				assert.Len(t, got, 1)

				return
			}
			got, err := b.DescribeBlueGreenDeployments("")
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDeleteBlueGreenDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.wantErr {
				_, err := b.DeleteBlueGreenDeployment("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d, err := b.CreateBlueGreenDeployment("bg-test", "arn:aws:rds:us-east-1:123456789012:cluster:source")
			require.NoError(t, err)
			got, err := b.DeleteBlueGreenDeployment(d.BlueGreenDeploymentIdentifier)
			require.NoError(t, err)
			assert.Equal(t, d.BlueGreenDeploymentIdentifier, got.BlueGreenDeploymentIdentifier)
		})
	}
}

func TestSwitchoverBlueGreenDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantErr   bool
	}{
		{name: "success sets status switchover-completed"},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.wantErr {
				_, err := b.SwitchoverBlueGreenDeployment("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d, err := b.CreateBlueGreenDeployment("bg-test", "arn:aws:rds:us-east-1:123456789012:cluster:source")
			require.NoError(t, err)
			got, err := b.SwitchoverBlueGreenDeployment(d.BlueGreenDeploymentIdentifier)
			require.NoError(t, err)
			assert.Equal(t, "switchover-completed", got.Status)
		})
	}
}

func TestDescribeDBSecurityGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		filter    string
		wantCount int
		wantErr   bool
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "by name", filter: "sg-1", wantCount: 1},
		{name: "not found", filter: "missing", wantErr: true, wantErrIs: rds.ErrDBSecurityGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			b.AddSecurityGroupInternal("sg-1", "Security group 1")
			b.AddSecurityGroupInternal("sg-2", "Security group 2")
			got, err := b.DescribeDBSecurityGroups(tt.filter)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDeleteDBSecurityGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		sgName    string
		wantErr   bool
	}{
		{name: "success", sgName: "sg-1"},
		{name: "not found", sgName: "missing", wantErr: true, wantErrIs: rds.ErrDBSecurityGroupNotFound},
		{name: "empty name", sgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				b.AddSecurityGroupInternal(tt.sgName, "desc")
			}
			err := b.DeleteDBSecurityGroup(tt.sgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRevokeDBSecurityGroupIngress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		groupName string
		cidrIP    string
		wantErr   bool
	}{
		{name: "success", groupName: "sg-1", cidrIP: "10.0.0.0/8"},
		{
			name:      "group not found",
			groupName: "missing",
			cidrIP:    "10.0.0.0/8",
			wantErr:   true,
			wantErrIs: rds.ErrDBSecurityGroupNotFound,
		},
		{
			name:      "CIDR not found",
			groupName: "sg-1",
			cidrIP:    "192.168.0.0/16",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name != "group not found" {
				b.AddSecurityGroupInternal("sg-1", "desc")
				_, err := b.AuthorizeDBSecurityGroupIngress("sg-1", "10.0.0.0/8")
				require.NoError(t, err)
			}
			got, err := b.RevokeDBSecurityGroupIngress(tt.groupName, tt.cidrIP)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, got)
		})
	}
}

func TestFailoverDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
		{
			name:      "wrong status",
			clusterID: "stopped-cluster",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidDBClusterStateFault,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			if tt.name == "wrong status" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.StopDBCluster(tt.clusterID)
				require.NoError(t, err)
			}
			got, err := b.FailoverDBCluster(tt.clusterID, "")
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
		})
	}
}

func TestRebootDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RebootDBCluster(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
		})
	}
}

func TestDeleteDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		wantErr   bool
	}{
		{name: "success", pgName: "my-cluster-pg"},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
		{name: "empty name", pgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
			}
			err := b.DeleteDBClusterParameterGroup(tt.pgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestModifyDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		params    []rds.DBParameter
		wantErr   bool
	}{
		{
			name:   "success",
			pgName: "my-cluster-pg",
			params: []rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
		},
		{
			name:      "not found",
			pgName:    "missing",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBClusterParameterGroup(tt.pgName, tt.params)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.pgName, got)
		})
	}
}

func TestDescribeDBClusterParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		wantLen   int
		wantErr   bool
	}{
		{name: "success", pgName: "my-cluster-pg", wantLen: 1},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
				_, err = b.ModifyDBClusterParameterGroup(
					tt.pgName,
					[]rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
				)
				require.NoError(t, err)
			}
			got, err := b.DescribeDBClusterParameters(tt.pgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		pgName    string
		params    []string
		resetAll  bool
		wantErr   bool
	}{
		{name: "reset all", pgName: "my-cluster-pg", resetAll: true},
		{name: "reset named", pgName: "my-cluster-pg", params: []string{"max_connections"}},
		{name: "not found", pgName: "missing", wantErr: true, wantErrIs: rds.ErrParameterGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBClusterParameterGroup(tt.pgName, "aurora-mysql8.0", "desc")
				require.NoError(t, err)
				_, err = b.ModifyDBClusterParameterGroup(
					tt.pgName,
					[]rds.DBParameter{{ParameterName: "max_connections", ParameterValue: "100"}},
				)
				require.NoError(t, err)
			}
			got, err := b.ResetDBClusterParameterGroup(tt.pgName, tt.resetAll, tt.params)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.pgName, got)
		})
	}
}

func TestModifyDBSubnetGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs   error
		name        string
		sgName      string
		description string
		subnetIDs   []string
		wantErr     bool
	}{
		{
			name:        "success updates description and subnets",
			sgName:      "my-subnet-group",
			description: "updated description",
			subnetIDs:   []string{"subnet-1111", "subnet-2222"},
		},
		{
			name:      "not found",
			sgName:    "missing",
			wantErr:   true,
			wantErrIs: rds.ErrSubnetGroupNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBSubnetGroup(tt.sgName, "original desc", "", []string{"subnet-old"})
				require.NoError(t, err)
			}
			got, err := b.ModifyDBSubnetGroup(tt.sgName, tt.description, tt.subnetIDs)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			if tt.description != "" {
				assert.Equal(t, tt.description, got.DBSubnetGroupDescription)
			}
			if len(tt.subnetIDs) > 0 {
				assert.Equal(t, tt.subnetIDs, got.SubnetIDs)
			}
		})
	}
}

func TestModifyDBClusterEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs    error
		name         string
		endpointID   string
		endpointType string
		wantErr      bool
	}{
		{
			name:         "success updates type",
			endpointID:   "my-endpoint",
			endpointType: "READER",
		},
		{
			name:       "not found",
			endpointID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrClusterEndpointNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					"my-cluster",
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBClusterEndpoint(tt.endpointID, "my-cluster", "WRITER")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBClusterEndpoint(tt.endpointID, tt.endpointType)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			if tt.endpointType != "" {
				assert.Equal(t, tt.endpointType, got.EndpointType)
			}
		})
	}
}
