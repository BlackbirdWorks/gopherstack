package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateLakeFormationIdentityCenterConfiguration_ReturnsARN(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	appArn, err := b.CreateLakeFormationIdentityCenterConfiguration(
		"123456789012",
		"arn:aws:sso:::instance/x",
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, appArn)
	assert.Contains(t, appArn, "123456789012")
}
