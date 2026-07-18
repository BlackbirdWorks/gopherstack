package elasticbeanstalk_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_CreatePlatformVersion_ARNIncludesAccountID verifies that a custom
// platform's ARN carries the caller's account ID, matching the documented
// "arn:aws:elasticbeanstalk:{region}:{account-id}:platform/{name}/{version}"
// resource-path pattern -- an empty account ID would produce a malformed
// "::platform/..." ARN that a real client constructing the ARN itself would
// never match.
func TestInMemoryBackend_CreatePlatformVersion_ARNIncludesAccountID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pv, err := b.CreatePlatformVersion(context.Background(), "my-platform", "1.0.0", nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/my-platform/1.0.0", pv.PlatformArn)
}
