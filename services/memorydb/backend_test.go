package memorydb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newTestBackend() *memorydb.InMemoryBackend {
	return memorydb.NewInMemoryBackend()
}

func TestBackend_Cluster_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		nodeType    string
		aclName     string
		wantErr     bool
	}{
		{
			name:        "create_and_describe",
			clusterName: "test-cluster",
			nodeType:    "db.r6g.large",
			aclName:     "open-access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: tt.clusterName,
				NodeType:    tt.nodeType,
				ACLName:     tt.aclName,
			}

			c, err := b.CreateCluster(testRegion, testAccountID, req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, c.Name)
			assert.NotEmpty(t, c.ARN)
			assert.Equal(t, "available", c.Status)

			clusters, err := b.DescribeClusters(tt.clusterName)
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Equal(t, tt.clusterName, clusters[0].Name)

			deleted, err := b.DeleteCluster(tt.clusterName)
			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, deleted.Name)

			_, err = b.DescribeClusters(tt.clusterName)
			require.Error(t, err)
		})
	}
}

func TestBackend_Cluster_DuplicateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "duplicate_cluster",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "dup-cluster",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}

			_, err := b.CreateCluster(testRegion, testAccountID, req)
			require.NoError(t, err)

			_, err = b.CreateCluster(testRegion, testAccountID, req)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_ACL_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
		wantErr bool
	}{
		{
			name:    "create_and_describe",
			aclName: "test-acl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateACLRequest{
				ACLName: tt.aclName,
			}

			a, err := b.CreateACL(testRegion, testAccountID, req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.aclName, a.Name)
			assert.NotEmpty(t, a.ARN)

			acls, err := b.DescribeACLs(tt.aclName)
			require.NoError(t, err)
			require.Len(t, acls, 1)

			_, err = b.DeleteACL(tt.aclName)
			require.NoError(t, err)

			_, err = b.DescribeACLs(tt.aclName)
			require.Error(t, err)
		})
	}
}

func TestBackend_SubnetGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "create_and_describe",
			sgName: "test-sg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateSubnetGroupRequest{
				SubnetGroupName: tt.sgName,
				SubnetIDs:       []string{"subnet-1", "subnet-2"},
			}

			sg, err := b.CreateSubnetGroup(testRegion, testAccountID, req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sgName, sg.Name)
			assert.NotEmpty(t, sg.ARN)

			sgs, err := b.DescribeSubnetGroups(tt.sgName)
			require.NoError(t, err)
			require.Len(t, sgs, 1)

			_, err = b.DeleteSubnetGroup(tt.sgName)
			require.NoError(t, err)
		})
	}
}

func TestBackend_User_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
		wantErr  bool
	}{
		{
			name:     "create_and_describe",
			userName: "test-user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateUserRequest{
				UserName:     tt.userName,
				AccessString: "on ~* &* +@all",
				AuthenticationMode: memorydb.ExportedAuthModeReq{
					Type:      "password",
					Passwords: []string{"mypassword"},
				},
			}

			u, err := b.CreateUser(testRegion, testAccountID, req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userName, u.Name)
			assert.NotEmpty(t, u.ARN)

			users, err := b.DescribeUsers(tt.userName)
			require.NoError(t, err)
			require.Len(t, users, 1)

			_, err = b.DeleteUser(tt.userName)
			require.NoError(t, err)
		})
	}
}

func TestBackend_ParameterGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
		family string
	}{
		{
			name:   "create_and_describe",
			pgName: "test-pg",
			family: "memorydb_redis7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateParameterGroupRequest{
				ParameterGroupName: tt.pgName,
				Family:             tt.family,
			}

			pg, err := b.CreateParameterGroup(testRegion, testAccountID, req)

			require.NoError(t, err)
			assert.Equal(t, tt.pgName, pg.Name)
			assert.Equal(t, tt.family, pg.Family)
			assert.NotEmpty(t, pg.ARN)

			pgs, err := b.DescribeParameterGroups(tt.pgName)
			require.NoError(t, err)
			require.Len(t, pgs, 1)

			_, err = b.DeleteParameterGroup(tt.pgName)
			require.NoError(t, err)
		})
	}
}

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		wantTags    map[string]string
		name        string
		clusterName string
		removedKeys []string
	}{
		{
			name:        "tag_and_untag",
			clusterName: "tagged-cluster",
			tags:        map[string]string{"Env": "test", "Team": "ops"},
			removedKeys: []string{"Team"},
			wantTags:    map[string]string{"Env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: tt.clusterName,
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}

			c, err := b.CreateCluster(testRegion, testAccountID, req)
			require.NoError(t, err)

			err = b.TagResource(c.ARN, tt.tags)
			require.NoError(t, err)

			got, err := b.ListTags(c.ARN)
			require.NoError(t, err)
			assert.Equal(t, "test", got["Env"])
			assert.Equal(t, "ops", got["Team"])

			err = b.UntagResource(c.ARN, tt.removedKeys)
			require.NoError(t, err)

			got, err = b.ListTags(c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestBackend_OpenAccessACL_Preseeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{
			name:    "open_access_exists",
			aclName: "open-access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			acls, err := b.DescribeACLs(tt.aclName)
			require.NoError(t, err)
			require.Len(t, acls, 1)
			assert.Equal(t, tt.aclName, acls[0].Name)
		})
	}
}
