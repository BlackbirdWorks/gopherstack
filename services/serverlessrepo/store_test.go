package serverlessrepo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("app1", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
	b.Reset()
	assert.Equal(t, 0, serverlessrepo.ApplicationCount(b))
}

func TestAccountIDRegion(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("111111111111", "eu-west-1")
	assert.Equal(t, "111111111111", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestExportCounts(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("app1", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
	assert.Equal(t, 0, serverlessrepo.VersionCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.TemplateCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.ChangeSetCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.PolicyStatementCount(b, "app1"))
}
