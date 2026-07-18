package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
)

func TestBackend_AssumeDecoratedRoleWithSAML_ReturnsCreds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	out := b.AssumeDecoratedRoleWithSAML("arn:principal", "arn:role", "assertion", nil)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.AccessKeyID)
	assert.NotEmpty(t, out.SecretAccessKey)
	assert.NotEmpty(t, out.SessionToken)
	assert.NotEmpty(t, out.Expiration)
}
