package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CacheSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	sg, err := b.CreateCacheSecurityGroup(context.Background(), "my-sg", "My security group")
	require.NoError(t, err)
	assert.Equal(t, "my-sg", sg.Name)
	assert.Contains(t, sg.ARN, "arn:aws:elasticache:")

	auth, err := b.AuthorizeCacheSecurityGroupIngress(context.Background(), "my-sg", "ec2-sg", "123456789012")
	require.NoError(t, err)
	assert.NotNil(t, auth)

	p, err := b.DescribeCacheSecurityGroups(context.Background(), "my-sg", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)

	revoked, err := b.RevokeCacheSecurityGroupIngress(context.Background(), "my-sg", "ec2-sg", "123456789012")
	require.NoError(t, err)
	assert.NotNil(t, revoked)

	err = b.DeleteCacheSecurityGroup(context.Background(), "my-sg")
	require.NoError(t, err)

	// After delete, lookup by name returns not-found error.
	_, err = b.DescribeCacheSecurityGroups(context.Background(), "my-sg", "", 0)
	require.Error(t, err)
}
