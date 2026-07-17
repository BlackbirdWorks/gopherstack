package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateUser_Redis6ACL(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	u, err := b.CreateUser(context.Background(), "acl-user1", "acl-user1", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	assert.Equal(t, "acl-user1", u.UserID)
	assert.Equal(t, "on ~* +@all", u.AccessString)
	assert.Equal(t, "redis", u.Engine)
	assert.Equal(t, "active", u.Status)
	assert.Contains(t, u.ARN, "arn:aws:elasticache:")
	assert.Contains(t, u.ARN, ":user:acl-user1")
}

func TestBackend_CreateUser_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	u, err := b.CreateUser(context.Background(), "valkey-user", "valkey-user", "on ~cache:* +get", "valkey", false)
	require.NoError(t, err)
	assert.Equal(t, "valkey", u.Engine)
}

func TestBackend_CreateUser_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateUser(context.Background(), "dup-user", "dup-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b.CreateUser(context.Background(), "dup-user", "dup-user", "on ~* +@all", "redis", false)
	require.Error(t, err)
}

func TestBackend_DeleteUser_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.DeleteUser(context.Background(), "nonexistent-user")
	require.Error(t, err)
}

func TestBackend_ModifyUser_AccessString(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateUser(context.Background(), "mod-user", "mod-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	u, err := b.ModifyUser(context.Background(), "mod-user", "on ~limited:* +get", false)
	require.NoError(t, err)
	assert.Equal(t, "on ~limited:* +get", u.AccessString)
}

func TestBackend_DescribeUsers_All(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	for i := range 3 {
		_, err := b.CreateUser(context.Background(),
			"list-user-"+string(rune('a'+i)),
			"list-user-"+string(rune('a'+i)),
			"on ~* +@all",
			"redis",
			false,
		)
		require.NoError(t, err)
	}

	p, err := b.DescribeUsers(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 3)
}

func TestBackend_DescribeUsers_FilterByID(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateUser(context.Background(), "filter-user", "filter-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	_, err = b.CreateUser(context.Background(), "other-user", "other-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	p, err := b.DescribeUsers(context.Background(), "filter-user", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "filter-user", p.Data[0].UserID)
}

// ----------------------------------------
// UserGroup CRUD (issue)
// ----------------------------------------

func TestBackend_DeleteUserSafe_NotInGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateUser(context.Background(), "safe-del", "safe-del", "on ~* +@all", "redis", true)
	require.NoError(t, err)

	u, err := b.DeleteUserSafe(context.Background(), "safe-del")
	require.NoError(t, err)
	assert.Equal(t, "safe-del", u.UserID)
}

func TestBackend_DeleteUserSafe_InGroup_Fails(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateUser(context.Background(), "grp-member", "grp-member", "on ~* +@all", "redis", true)
	require.NoError(t, err)
	_, err = b.CreateUserGroup(context.Background(), "owns-member", "", "redis", []string{"grp-member"})
	require.NoError(t, err)

	_, err = b.DeleteUserSafe(context.Background(), "grp-member")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrUserNotInGroup)
}

func TestBackend_DeleteUserSafe_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.DeleteUserSafe(context.Background(), "no-such-user")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrUserNotFound)
}
