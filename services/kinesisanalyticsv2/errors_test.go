package kinesisanalyticsv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestErrValidation_SentinelExists(t *testing.T) {
	t.Parallel()

	assert.Error(t, kinesisanalyticsv2.ErrValidation)
}
